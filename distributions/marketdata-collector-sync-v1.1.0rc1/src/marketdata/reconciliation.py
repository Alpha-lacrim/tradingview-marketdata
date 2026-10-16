"""Authenticated bounded financial state inspection; never grants ownership."""

import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from marketdata.candles import minute_start
from marketdata.ingest_auth import Principal
from marketdata.replication import DestinationWriter, ReplicationError
from marketdata.replication_models import fingerprint, timestamp_wire

CANDLE_FINANCIAL_FIELDS = (
    "instrument_id",
    "provider",
    "provider_symbol",
    "canonical_symbol",
    "interval",
    "bucket_start",
    "bucket_end",
    "price_basis",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source_status",
    "realtime_status",
)


def exact_decimal_text(value: Any) -> str:
    from decimal import Decimal

    number = Decimal(str(value))
    text = format(number, "f")
    return "0" if number == 0 else text.rstrip("0").rstrip(".") if "." in text else text


async def revision_summary(
    c: Any, instrument: UUID, provider: str, bucket: datetime
) -> dict[str, Any]:
    """Bounded financial audit manifest; local sequence IDs/discovery clocks differ."""
    rows = await c.fetch(
        """SELECT changed_fields,old_values,new_values FROM candle_revisions
        WHERE instrument_id=$1 AND provider=$2 AND bucket_start=$3 ORDER BY id LIMIT 129""",
        instrument,
        provider,
        bucket,
    )
    values = []
    for row in rows[:128]:
        value: dict[str, Any] = dict(changed_fields=sorted(row["changed_fields"]))
        for name in ("old_values", "new_values"):
            fields = json.loads(row[name])
            for field in ("open", "high", "low", "close", "volume"):
                if fields.get(field) is not None:
                    fields[field] = exact_decimal_text(fields[field])
            value[name] = fields
        values.append(value)
    from marketdata.replication_models import canonical_bytes

    values.sort(key=canonical_bytes)
    return dict(revision_sha256=fingerprint(values).hex(), revision_truncated=len(rows) > 128)


def candle_fingerprint(payload: dict[str, Any]) -> str:
    # Decimal scale and timezone spelling are normalized through the existing model.
    from marketdata.candles import MarketCandle

    data = MarketCandle.model_validate(payload).model_dump(mode="json")
    values = {n: data.get(n) for n in CANDLE_FINANCIAL_FIELDS}
    for name in ("open", "high", "low", "close", "volume"):
        if values[name] is not None:
            values[name] = exact_decimal_text(values[name])
    values["volume_type"] = data.get("metadata", {}).get("volume_type")
    values["volume_additive"] = data.get("metadata", {}).get("volume_additive")
    return fingerprint(values).hex()


class ReconcileKey(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instrument_id: UUID
    provider: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")
    stream: Literal["candles", "quotes"]
    source_epoch: UUID
    ownership_generation: int = Field(strict=True, gt=0, le=9223372036854775807)
    bucket_start: datetime | None = None

    @field_validator("bucket_start", mode="before")
    @classmethod
    def wire_bucket(cls, value: Any) -> Any:
        return timestamp_wire(value)

    @field_validator("bucket_start")
    @classmethod
    def minute_bucket(cls, value: datetime | None) -> datetime | None:
        if value is not None and minute_start(value) != value:
            raise ValueError("minute-aligned candle key required")
        return minute_start(value) if value is not None else None


class ReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    protocol_version: int = Field(strict=True, ge=1, le=1)
    session_id: UUID
    session_generation: int = Field(strict=True, gt=0, le=9223372036854775807)
    keys: list[ReconcileKey] = Field(min_length=1, max_length=100)


async def reconcile(
    writer: DestinationWriter, principal: Principal, request: ReconcileRequest
) -> dict[str, Any]:
    async with writer.store.connection() as c, c.transaction():
        producer = await c.fetchrow("SELECT * FROM ingest_lock_producer($1)", principal.producer_id)
        now = await c.fetchval("SELECT clock_timestamp()")
        if (
            not producer
            or not producer["enabled"]
            or producer["credential_key_id"] != principal.key_id
        ):
            raise ReplicationError("producer_not_authorized")
        if (producer["session_id"], producer["session_generation"]) != (
            request.session_id,
            request.session_generation,
        ) or producer["session_expires_at"] <= now:
            raise ReplicationError("producer_session_fenced")
        ordered = sorted(request.keys, key=lambda k: (str(k.instrument_id), k.provider, k.stream))
        for key in ordered:
            owner = await c.fetchrow(
                "SELECT * FROM ingest_lock_owner($1,$2,$3)",
                key.instrument_id,
                key.provider,
                key.stream,
            )
            if not owner or (
                owner["producer_id"],
                owner["source_epoch"],
                owner["ownership_generation"],
            ) != (principal.producer_id, key.source_epoch, key.ownership_generation):
                raise ReplicationError("source_not_authorized")
        await writer.store.lock_sources(c, [(k.instrument_id, k.provider) for k in ordered])
        results = []
        for key in request.keys:
            high = await c.fetchval(
                """SELECT coalesce(max(source_position),0)
                FROM ingest_observations WHERE producer_id=$1 AND source_epoch=$2""",
                principal.producer_id,
                key.source_epoch,
            )
            item: dict[str, Any] = {"key": key.model_dump(mode="json"), "high_water": str(high)}
            if key.stream == "quotes":
                item["quote_state"] = await writer.quote_state(c, key.instrument_id, key.provider)
            else:
                if key.bucket_start is None:
                    results.append(item)
                    continue  # Explicit stream/high-water probe without an invented candle key.
                if key.bucket_start.tzinfo is None:
                    raise ReplicationError("invalid_reconciliation")
                value = await c.fetchval(
                    """SELECT payload FROM market_candles WHERE
                    instrument_id=$1 AND provider=$2 AND interval='1m' AND price_basis='native'
                    AND bucket_start=$3""",
                    key.instrument_id,
                    key.provider,
                    key.bucket_start,
                )
                item["candle_sha256"] = candle_fingerprint(json.loads(value)) if value else None
                item["revision_count"] = await c.fetchval(
                    """SELECT count(*) FROM candle_revisions
                    WHERE instrument_id=$1 AND provider=$2 AND bucket_start=$3""",
                    key.instrument_id,
                    key.provider,
                    key.bucket_start,
                )
                item.update(
                    await revision_summary(c, key.instrument_id, key.provider, key.bucket_start)
                )
            results.append(item)
        destination = await c.fetchval("SELECT destination_id FROM ingest_destination_state")
    return dict(
        protocol_version=1,
        producer_id=str(principal.producer_id),
        destination_id=str(destination),
        server_time=now.isoformat(),
        results=results,
    )
