"""Commit-ordered database reconciliation. No LISTEN/NOTIFY or shared collector memory."""

import base64
import json
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from marketdata.candles import MarketCandle
from marketdata.domain import Tick
from marketdata.persistence import Store


def encode_watermark(epoch: UUID, position: int) -> str:
    return base64.urlsafe_b64encode(f"1:{epoch}:{position}".encode()).decode()


def decode_watermark(value: str) -> tuple[UUID, int]:
    try:
        if len(value) > 128:
            raise ValueError
        version, epoch, position = (
            base64.b64decode(value, altchars=b"-_", validate=True).decode().split(":")
        )
        if version != "1" or not position.isascii() or not position.isdecimal():
            raise ValueError
        number = int(position)
        if number > 9223372036854775807:
            raise ValueError
        return UUID(epoch), number
    except (ValueError, UnicodeError):
        raise HTTPException(422, "invalid market-data cursor") from None


async def append_events(
    connection: Any, observations: list[tuple[str, Tick | MarketCandle]], retention: int
) -> None:
    """Caller owns the market transaction. Clock lock is held until that transaction commits.

    A sequence alone is unsafe: a later sequence can commit before an earlier transaction.
    The singleton lock serializes only the journal tail of each writer batch. Rollback undoes
    both the counter and events. API readers never acquire the write lock.
    """
    if not observations:
        return
    observations.sort(
        key=lambda item: (
            item[1].canonical_symbol,
            item[1].provider,
            item[1].bucket_start
            if isinstance(item[1], MarketCandle)
            else item[1].storage_timestamp,
        )
    )
    head = await connection.fetchval(
        "UPDATE marketdata_event_clock SET head=head+$1 WHERE singleton RETURNING head",
        len(observations),
    )
    records = []
    for offset, (kind, observation) in enumerate(observations, head - len(observations) + 1):
        # Exclude arbitrary upstream metadata from the delivery journal. Financial fields
        # remain exact decimal strings. Only the typed candle volume unit is needed.
        payload = json.loads(observation.model_dump_json(exclude={"metadata"}))
        payload["metadata"] = (
            {"volume_type": observation.metadata.get("volume_type")}
            if isinstance(observation, MarketCandle)
            else {}
        )
        records.append(
            (
                offset,
                observation.instrument_id,
                observation.provider,
                observation.canonical_symbol,
                kind,
                json.dumps(payload),
            )
        )
    await connection.copy_records_to_table(
        "marketdata_events",
        records=records,
        columns=("id", "instrument_id", "provider", "canonical_symbol", "kind", "payload"),
    )
    floor = max(0, head - retention)
    await connection.execute("DELETE FROM marketdata_events WHERE id <= $1", floor)
    await connection.execute(
        "UPDATE marketdata_event_clock SET floor=greatest(floor,$1) WHERE singleton", floor
    )


async def event_page(
    store: Store,
    after: str | None,
    limit: int,
    until: str | None = None,
) -> dict[str, Any]:
    """One MVCC view of the fence, retention floor and bounded immutable journal page."""
    async with store.connection() as c, c.transaction(isolation="repeatable_read", readonly=True):
        clock = await c.fetchrow("SELECT * FROM marketdata_event_clock WHERE singleton")
        epoch, head, floor = clock["epoch"], clock["head"], clock["floor"]
        position = head if after is None else validate_watermark(after, epoch, floor, head)
        fence = head if until is None else validate_watermark(until, epoch, floor, head)
        if fence < position:
            raise HTTPException(422, "until cursor precedes after cursor")
        records = await c.fetch(
            "SELECT e.*,i.payload AS instrument FROM marketdata_events e "
            "JOIN instruments i ON i.id=e.instrument_id "
            "WHERE e.id>$1 AND e.id<=$2 ORDER BY e.id LIMIT $3",
            position,
            fence,
            limit + 1,
        )
        rows = [dict(r) for r in records[:limit]]
        more = len(records) > limit
        progress = rows[-1]["id"] if more else fence
        return dict(
            epoch=epoch,
            rows=rows,
            cursor=encode_watermark(epoch, progress),
            head_cursor=encode_watermark(epoch, head),
            until=encode_watermark(epoch, fence),
            has_more=more,
        )


def validate_watermark(value: str, epoch: UUID, floor: int, head: int) -> int:
    supplied_epoch, position = decode_watermark(value)
    if supplied_epoch != epoch or position < floor:
        raise HTTPException(409, "resynchronization required; refresh loaded REST history")
    if position > head:
        raise HTTPException(422, "cursor is ahead of committed database state")
    return position


async def persisted_snapshot(store: Store, symbols: list[str], limit: int) -> dict[str, Any]:
    async with store.connection() as c, c.transaction(isolation="repeatable_read", readonly=True):
        clock = await c.fetchrow("SELECT * FROM marketdata_event_clock WHERE singleton")
        rows = await store.source_records(
            symbols,
            None,
            None,
            None,
            limit + 1,
            connection=c,
        )
        if set(symbols) - {r["instrument"].canonical_symbol for r in rows}:
            raise HTTPException(404, "unknown canonical symbol")
        if len(rows) > limit:
            raise HTTPException(422, "snapshot source capacity exceeded")
        return dict(rows=rows, cursor=encode_watermark(clock["epoch"], clock["head"]))
