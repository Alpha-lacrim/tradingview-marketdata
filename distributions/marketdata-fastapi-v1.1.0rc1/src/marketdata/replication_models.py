"""Transport-neutral ingestion v1 records. No HTTP or provider connection code."""

import hashlib
import json
import re
from datetime import datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from pydantic import (
    ConfigDict,
    Field,
    ValidationInfo,
    field_serializer,
    field_validator,
    model_validator,
)

from marketdata.candles import MarketCandle, minute_start
from marketdata.domain import Model, Tick, utc
from marketdata.providers.base import ProviderHealth, SymbolHealth

Stream = Literal["candles", "quotes", "coverage", "status"]
MAX_BODY_BYTES = 2 * 1024 * 1024
MAX_POSITION = 9223372036854775807


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def fingerprint(value: Any) -> bytes:
    return hashlib.sha256(canonical_bytes(value)).digest()


def decimal_wire(value: Any) -> Any:
    if value is not None and (
        not isinstance(value, str)
        or len(value) > 128
        or not re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value)
    ):
        raise ValueError("finite plain decimal string required (maximum 128 characters)")
    return value


def timestamp_wire(value: Any) -> Any:
    if value is not None and (
        not isinstance(value, str)
        or not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})",
            value,
        )
    ):
        raise ValueError("RFC3339 timestamp with timezone and microsecond precision required")
    return value


class CandleObservation(MarketCandle):
    @field_validator("bucket_start", "bucket_end", "received_timestamp", mode="before")
    @classmethod
    def wire_times(cls, value: Any) -> Any:
        return timestamp_wire(value)

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def wire_prices(cls, value: Any) -> Any:
        return decimal_wire(value)

    @model_validator(mode="after")
    def typed_volume(self) -> Self:
        if self.volume is not None and self.metadata.get("volume_type") not in {"base", "tick"}:
            raise ValueError("typed native volume requires a verified supported volume unit")
        return self


class QuoteObservation(Tick):
    # Wire observations always supply stable identity, never the domain model's UUID default.
    tick_id: UUID
    sequence_number: int | None = Field(
        default=None, ge=-9223372036854775808, le=MAX_POSITION, strict=True
    )

    @field_validator("event_timestamp", "received_timestamp", mode="before")
    @classmethod
    def wire_times(cls, value: Any) -> Any:
        return timestamp_wire(value)

    @field_validator("event_timestamp_ns", mode="before")
    @classmethod
    def nanoseconds(cls, value: Any) -> int | None:
        if value is None:
            return None
        if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]{0,18}", value):
            raise ValueError("source nanoseconds must be a canonical decimal string")
        return int(value)

    @field_serializer("event_timestamp_ns", when_used="json")
    def serialize_nanoseconds(self, value: int | None) -> str | None:
        return str(value) if value is not None else None

    @field_validator(
        "bid", "ask", "last", "bid_size", "ask_size", "last_size", "volume", mode="before"
    )
    @classmethod
    def wire_prices(cls, value: Any) -> Any:
        return decimal_wire(value)


class Source(Model):
    instrument_id: UUID
    canonical_symbol: str
    provider: str
    provider_symbol: str


class Window(Model):
    start: datetime
    end: datetime

    @field_validator("start", "end", mode="before")
    @classmethod
    def wire_times(cls, value: Any) -> Any:
        return timestamp_wire(value)

    @field_validator("start", "end")
    @classmethod
    def times(cls, value: datetime) -> datetime:
        value = utc(value)
        if value != minute_start(value):
            raise ValueError("minute-aligned coverage timestamp required")
        return value

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.end <= self.start:
            raise ValueError("nonempty coverage window required")
        return self


class CoverageObservation(Source, Window):
    unknown: list[Window] = Field(default_factory=list, max_length=500)
    not_expected: list[Window] = Field(default_factory=list, max_length=500)
    source_missing: list[Window] = Field(default_factory=list, max_length=500)
    session_info: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def partitions(self) -> Self:
        windows = sorted(
            self.unknown + self.not_expected + self.source_missing, key=lambda w: w.start
        )
        if len(windows) > 500 or any(w.start < self.start or w.end > self.end for w in windows):
            raise ValueError("coverage partitions exceed declared window or range limit")
        if any(a.end > b.start for a, b in zip(windows, windows[1:], strict=False)):
            raise ValueError("coverage partitions overlap")
        return self


class CapturedSymbolHealth(SymbolHealth):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    @field_validator("*", mode="before")
    @classmethod
    def wire_times(cls, value: Any, info: ValidationInfo) -> Any:
        if info.field_name and (
            info.field_name.endswith("_at")
            or info.field_name
            in {
                "last_persisted_minute",
                "latest_expected_completed_bucket",
                "oldest_unresolved_expected_gap",
            }
        ):
            return timestamp_wire(value)
        return value

    @field_validator("*", mode="after")
    @classmethod
    def normalize_times(cls, value: Any) -> Any:
        return utc(value) if isinstance(value, datetime) else value

    @field_validator("*", mode="after")
    @classmethod
    def aware(cls, value: Any) -> Any:
        return utc(value) if isinstance(value, datetime) else value


class CapturedHealth(ProviderHealth):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    # An omitted default utc_now() would change the fingerprint on each retransmission.
    updated_at: datetime

    @field_validator("instrument_last_success", mode="before")
    @classmethod
    def wire_successes(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            raise ValueError("instrument success times must be an object")
        return {key: timestamp_wire(item) for key, item in value.items()}

    @field_validator("instrument_last_success")
    @classmethod
    def normalize_successes(cls, value: dict[str, datetime | None]) -> dict[str, datetime | None]:
        return {key: utc(item) if item is not None else None for key, item in value.items()}

    @field_validator("*", mode="after")
    @classmethod
    def normalize_times(cls, value: Any) -> Any:
        return utc(value) if isinstance(value, datetime) else value

    @field_validator("*", mode="before")
    @classmethod
    def wire_times(cls, value: Any, info: ValidationInfo) -> Any:
        if info.field_name and (
            info.field_name.endswith("_at") or info.field_name == "expected_closed_minute"
        ):
            return timestamp_wire(value)
        return value

    @field_validator("symbols", mode="before")
    @classmethod
    def strict_symbols(cls, value: Any) -> dict[str, CapturedSymbolHealth]:
        if not isinstance(value, dict):
            raise ValueError("source health symbols must be an object")
        return {key: CapturedSymbolHealth.model_validate(item) for key, item in value.items()}

    @field_validator("updated_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        return utc(value)


class StatusObservation(Source):
    run_id: UUID
    captured_at: datetime
    expires_at: datetime
    running: bool = Field(strict=True)
    database_healthy: bool = Field(strict=True)
    queue_depth: int = Field(ge=0, strict=True)
    health: CapturedHealth

    @field_validator("captured_at", "expires_at", mode="before")
    @classmethod
    def wire_times(cls, value: Any) -> Any:
        return timestamp_wire(value)

    @field_validator("captured_at", "expires_at")
    @classmethod
    def times(cls, value: datetime) -> datetime:
        return utc(value)

    @model_validator(mode="after")
    def expiry(self) -> Self:
        if not self.captured_at < self.expires_at <= self.captured_at + timedelta(seconds=120):
            raise ValueError("status lifetime must be greater than zero and at most 120 seconds")
        if set(self.health.symbols) != {self.canonical_symbol} or not (
            self.health.provider == self.provider
            or self.health.provider.startswith(self.provider + "_")
        ):
            raise ValueError("status must describe its authorized source only")
        symbol = self.health.symbols[self.canonical_symbol]
        if (symbol.canonical_symbol, symbol.provider_symbol) != (
            self.canonical_symbol,
            self.provider_symbol,
        ):
            raise ValueError("status symbol identity mismatch")
        return self


class Record(Model):
    observation_id: UUID
    source_position: int = Field(gt=0, le=MAX_POSITION)

    @field_validator("source_position", mode="before")
    @classmethod
    def position(cls, value: Any) -> int:
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError("source position must be a decimal string")
        if len(value) > 19 or value.startswith("0"):
            raise ValueError("canonical positive source position required")
        return int(value)

    @field_serializer("source_position")
    def serialize_position(self, value: int) -> str:
        return str(value)


class CandleRecord(Record):
    kind: Literal["candle"]
    observation: CandleObservation


class QuoteRecord(Record):
    kind: Literal["quote"]
    observation: QuoteObservation

    @model_validator(mode="after")
    def identity(self) -> Self:
        if self.observation_id != self.observation.tick_id:
            raise ValueError("quote observation_id must preserve source tick_id")
        return self


class CoverageRecord(Record):
    kind: Literal["coverage"]
    observation: CoverageObservation


class StatusRecord(Record):
    kind: Literal["status"]
    observation: StatusObservation


ObservationRecord = Annotated[
    CandleRecord | QuoteRecord | CoverageRecord | StatusRecord, Field(discriminator="kind")
]


class Batch(Model):
    protocol_version: Literal[1]
    producer_id: UUID
    session_id: UUID
    session_generation: int = Field(gt=0, le=MAX_POSITION, strict=True)
    source_epoch: UUID
    batch_id: UUID
    stream: Stream
    records: list[ObservationRecord] = Field(min_length=1, max_length=500)

    @field_validator("protocol_version", mode="before")
    @classmethod
    def strict_version(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("protocol version must be a JSON integer")
        return value

    @model_validator(mode="after")
    def coherent(self) -> Self:
        expected = {
            "candles": "candle",
            "quotes": "quote",
            "coverage": "coverage",
            "status": "status",
        }
        if any(r.kind != expected[self.stream] for r in self.records):
            raise ValueError("record kind does not match batch stream")
        if len({r.observation_id for r in self.records}) != len(self.records):
            raise ValueError("observation IDs must be unique within a batch")
        if self.stream in {"coverage", "status"} and len(self.records) > 50:
            raise ValueError("coverage/status batches are limited to 50 records")
        for record in self.records:
            metadata = getattr(record.observation, "metadata", {})
            if len(canonical_bytes(metadata)) > 16384:
                raise ValueError("observation metadata exceeds 16 KiB")
        return self


def decode_batch(payload: bytes) -> Batch:
    if len(payload) > MAX_BODY_BYTES:
        raise ValueError("batch body exceeds 2 MiB")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> Any:
        raise ValueError("nonfinite JSON number")

    value = json.loads(
        payload.decode("utf-8"), object_pairs_hook=unique, parse_constant=invalid_constant
    )
    return Batch.model_validate(value)
