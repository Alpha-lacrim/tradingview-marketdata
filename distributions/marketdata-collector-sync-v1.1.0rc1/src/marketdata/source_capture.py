"""Local durability only. No network, credentials, provider transport or remote receipts.

Lock order: sorted Store source locks, source clock, frontend journal clock. Every
allocation is a transactional row UPDATE, held to commit; sequences are not cursors.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid5

import asyncpg

from marketdata.candles import MarketCandle
from marketdata.domain import Tick
from marketdata.quote_fields import PRICE_FIELDS, field_order
from marketdata.replication_models import CandleRecord, QuoteRecord, canonical_bytes

if TYPE_CHECKING:
    from marketdata.persistence import Store


def wire_observation(observation: Tick | MarketCandle) -> dict[str, Any]:
    data = observation.model_dump(mode="json")
    names = (
        PRICE_FIELDS
        if isinstance(observation, Tick)
        else ("open", "high", "low", "close", "volume")
    )
    for name in names:
        value = getattr(observation, name)
        data[name] = format(value, "f") if value is not None else None
    if isinstance(observation, Tick) and observation.event_timestamp_ns is not None:
        data["event_timestamp_ns"] = str(observation.event_timestamp_ns)
    return data


class CaptureBackpressure(RuntimeError):
    """Nothing in the owning market transaction may commit on capacity exhaustion."""


class CaptureConflict(RuntimeError):
    """Epoch/checkpoint mismatch needs reconciliation, never an implicit reset."""


async def clock(c: Any, *, restricted: bool = False) -> Any:
    if not c.is_in_transaction():
        raise ValueError("capture requires caller-owned transaction")
    # Also make source commits durable when a caller changed its session default.
    await c.execute("SET LOCAL synchronous_commit=on")
    if restricted:
        value = json.loads(await c.fetchval("SELECT source_sync_capture('clock','{}')"))
        value["source_epoch"] = UUID(value["source_epoch"])
        return value
    return await c.fetchrow("SELECT * FROM source_capture_clock WHERE singleton FOR UPDATE")


async def validate_identities(c: Any, observations: list[Tick] | list[MarketCandle]) -> None:
    identities = {
        (
            o.instrument_id,
            o.provider,
            o.provider_symbol,
            o.canonical_symbol,
            o.asset_class if isinstance(o, Tick) else None,
            o.instrument_type if isinstance(o, Tick) else None,
            o.exchange if isinstance(o, Tick) else None,
        )
        for o in observations
    }
    for instrument, provider, symbol, canonical, asset, kind, exchange in identities:
        row = await c.fetchrow(
            """SELECT i.asset_class,i.instrument_type,i.exchange
            FROM provider_instruments p JOIN instruments i
            ON i.id=p.instrument_id WHERE p.instrument_id=$1 AND p.provider=$2
                AND p.provider_symbol=$3 AND i.canonical_symbol=$4""",
            instrument,
            provider,
            symbol,
            canonical,
        )
        if row is None or (
            asset is not None
            and (asset, kind, exchange)
            != (row["asset_class"], row["instrument_type"], row["exchange"])
        ):
            raise ValueError("source capture identity does not match registered mapping")


async def append_candles(
    c: Any,
    store: Store,
    candles: list[MarketCandle],
    *,
    corrected: set[tuple[UUID, str, datetime]] | None = None,
    seed: bool = False,
    restricted: bool = False,
    repair: bool = False,
) -> None:
    if not candles:
        return
    await validate_identities(c, candles)
    state = await clock(c, restricted=restricted)
    rows = []
    for position, bar in enumerate(
        sorted(candles, key=lambda b: (str(b.instrument_id), b.provider, b.bucket_start)),
        state["head"] + 1,
    ):
        observation_id = uuid5(state["source_epoch"], f"candle:{position}")
        record = dict(
            observation_id=str(observation_id),
            source_position=str(position),
            kind="candle",
            observation=wire_observation(bar),
        )
        CandleRecord.model_validate(record)
        if len(canonical_bytes(bar.metadata)) > 16384:
            raise CaptureBackpressure("candle metadata exceeds protocol's 16 KiB bound")
        raw = canonical_bytes(record)
        if len(raw) > 65536:
            raise CaptureBackpressure("candle record exceeds 64 KiB capture bound")
        rows.append(
            (
                position,
                observation_id,
                bar.instrument_id,
                bar.provider,
                raw.decode(),
                len(raw),
                "history_seed"
                if seed
                else "candle_corrected"
                if corrected and (bar.instrument_id, bar.provider, bar.bucket_start) in corrected
                else "candle_completed",
            )
        )
    size = sum(r[5] for r in rows)
    if (
        state["pending_rows"] + len(rows) > store.settings.source_capture_max_rows
        or state["pending_bytes"] + size > store.settings.source_capture_max_bytes
    ):
        raise CaptureBackpressure(
            "candle outbox capacity exhausted; transaction retained for retry"
        )
    if restricted:
        await c.execute(
            "SELECT source_sync_capture('seed_candles',$1::jsonb)",
            json.dumps(
                dict(
                    head=state["head"],
                    repair=repair,
                    max_rows=store.settings.source_capture_max_rows,
                    max_bytes=store.settings.source_capture_max_bytes,
                    rows=[dict(record=json.loads(r[4]), canonical=r[4]) for r in rows],
                )
            ),
        )
        return
    await c.executemany(
        """INSERT INTO source_candle_outbox
            (position,observation_id,instrument_id,provider,record,byte_count,change_kind)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7)""",
        rows,
    )
    await c.executemany(
        """INSERT INTO source_candle_exports VALUES ($1,$2,$3,$4)
            ON CONFLICT (instrument_id,provider,bucket_start) DO UPDATE SET
                position=EXCLUDED.position""",
        [
            (b.instrument_id, b.provider, b.bucket_start, r[0])
            for b, r in zip(
                sorted(candles, key=lambda b: (str(b.instrument_id), b.provider, b.bucket_start)),
                rows,
                strict=True,
            )
        ],
    )
    await c.execute(
        """UPDATE source_capture_clock SET head=$1,pending_rows=pending_rows+$2,
            pending_bytes=pending_bytes+$3 WHERE singleton""",
        rows[-1][0],
        len(rows),
        size,
    )


def field_observation(tick: Tick, name: str) -> dict[str, Any]:
    # Adapters may state which fields actually have the Tick source timestamp.
    # No provider-specific symbol/protocol handling belongs in persistence.
    timed = tick.metadata.get("timestamped_fields", list(PRICE_FIELDS))
    timestamped = name in timed and tick.event_timestamp is not None
    return dict(
        value=str(getattr(tick, name)),
        observation_id=str(tick.tick_id),
        event_timestamp=tick.event_timestamp.isoformat()
        if timestamped and tick.event_timestamp is not None
        else None,
        event_timestamp_ns=str(tick.event_timestamp_ns)
        if timestamped and tick.event_timestamp_ns is not None
        else None,
        received_timestamp=tick.received_timestamp.isoformat(),
        realtime_status=tick.realtime_status.value,
        metadata=tick.metadata,
        sequence_number=tick.sequence_number,
    )


def merge_fields(fields: dict[str, Any], tick: Tick) -> bool:
    changed = False
    for name in PRICE_FIELDS:
        if getattr(tick, name) is None:
            continue  # Null/omitted means no observation, never a price-field clear.
        candidate = field_observation(tick, name)
        old = fields.get(name)
        if old is not None and field_order(candidate) <= field_order(old):
            continue
        fields[name] = candidate
        changed = True
    return changed


async def prepare_quotes(
    c: Any, ticks: list[Tick], *, restricted: bool = False, resnapshot: bool = False
) -> list[Tick]:
    """One coalescible full snapshot/stream/transaction; originals retained per field.

    Prices use the last field's source clock when last exists, otherwise bid then
    ask. Receipt is the actual newest accepted delta receipt, never upload time.
    Field timestamps/status remain authoritative for individual-field freshness.
    """
    if not ticks:
        return []
    await validate_identities(c, ticks)
    state = await clock(c, restricted=restricted)
    grouped: dict[tuple[UUID, str], list[Tick]] = defaultdict(list)
    for tick in ticks:
        grouped[tick.instrument_id, tick.provider].append(tick)
    snapshots = []
    head = state["head"]
    for (instrument, provider), items in sorted(grouped.items(), key=lambda item: str(item[0])):
        row = await c.fetchrow(
            "SELECT fields FROM source_quote_state WHERE instrument_id=$1 AND provider=$2",
            instrument,
            provider,
        )
        fields = json.loads(row["fields"]) if row else {}
        if row is None:
            prior = await c.fetchval(
                "SELECT payload FROM latest_prices WHERE instrument_id=$1 AND provider=$2",
                instrument,
                provider,
            )
            if prior:
                merge_fields(fields, Tick.model_validate_json(prior))
        changed = (row is None or resnapshot) and bool(fields)
        for tick in sorted(items, key=lambda t: (t.received_timestamp, str(t.tick_id))):
            if not resnapshot:
                changed = merge_fields(fields, tick) or changed
        if not changed:
            continue
        head += 1
        observation_id = uuid5(state["source_epoch"], f"quote:{head}")
        primary = next(fields[n] for n in ("last", "bid", "ask") if n in fields)
        newest = max(fields.values(), key=lambda f: datetime.fromisoformat(f["received_timestamp"]))
        data = items[-1].model_dump(mode="json")
        data.update(
            tick_id=str(observation_id),
            event_timestamp=primary["event_timestamp"],
            event_timestamp_ns=int(primary["event_timestamp_ns"])
            if primary["event_timestamp_ns"] is not None
            else None,
            received_timestamp=newest["received_timestamp"],
            realtime_status=primary["realtime_status"],
            sequence_number=None,
            metadata={
                **items[-1].metadata,
                "quote_semantics": "coalesced_partial_fields",
                "field_observations": fields,
                "source_capture_primary": {
                    "received_timestamp": primary["received_timestamp"],
                    "event_timestamp": primary["event_timestamp"],
                },
            },
            **{name: fields[name]["value"] if name in fields else None for name in PRICE_FIELDS},
        )
        snapshot = Tick.model_validate(data)
        record = dict(
            observation_id=str(observation_id),
            source_position=str(head),
            kind="quote",
            observation=wire_observation(snapshot),
        )
        QuoteRecord.model_validate(record)
        if len(canonical_bytes(record)) > 16384:
            raise CaptureBackpressure("quote snapshot exceeds 16 KiB capture bound")
        if restricted:
            await c.execute(
                "SELECT source_sync_capture($1,$2::jsonb)",
                "resnapshot_quote" if resnapshot else "seed_quote",
                json.dumps(dict(record=record)),
            )
            snapshots.append(snapshot)
            continue
        await c.execute(
            """INSERT INTO source_quote_state(instrument_id,provider,version,fields,record)
                VALUES ($1,$2,$3,$4::jsonb,$5::jsonb)
                ON CONFLICT (instrument_id,provider) DO UPDATE SET version=EXCLUDED.version,
                    fields=EXCLUDED.fields,record=EXCLUDED.record""",
            instrument,
            provider,
            head,
            json.dumps(fields),
            json.dumps(record),
        )
        snapshots.append(snapshot)
    if not restricted:
        await c.execute("UPDATE source_capture_clock SET head=$1 WHERE singleton", head)
    return snapshots


class SourceCapture:
    """Bounded local consumer interface. Caller must durably stage bytes before HTTP.

    Acks are trusted local operations after verified remote committed receipts;
    this module does not perform HTTP or claim receipt verification.
    """

    def __init__(self, store: Store):
        if not store.settings.source_capture_enabled:
            raise ValueError("source capture is disabled")
        self.store = store
        self.worker_fence: tuple[UUID, int] | None = None
        self.ack_batch_id: UUID | None = None

    async def enter(self, c: Any) -> None:
        if self.worker_fence:
            await c.execute("SELECT source_sync_enter($1,$2)", *self.worker_fence)

    @asynccontextmanager
    async def consumer_lock(self) -> AsyncIterator[Any]:
        """One local worker holds this dedicated DB session throughout its lifecycle.

        Losing this connection requires stopping staging/recovery/acks. A future
        worker must supervise it, plus fence its own durable staged-state writes.
        """
        async with self.store.connection() as c:
            acquired = await c.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended('marketdata:source-sync',0))"
            )
            if not acquired:
                raise CaptureConflict(
                    "another local synchronization consumer owns the session lock"
                )
            try:
                yield c
            finally:
                try:
                    if not c.is_closed():
                        await c.execute(
                            "SELECT pg_advisory_unlock("
                            "hashtextextended('marketdata:source-sync',0))"
                        )
                except (
                    asyncpg.InterfaceError,
                    asyncpg.ConnectionDoesNotExistError,
                    asyncpg.InternalClientError,
                    TimeoutError,
                ):
                    # Cancel a busy query before closing: a bare socket abort may
                    # leave its server backend sleeping with the advisory lock.
                    # Never return this unusable connection to the pool. No delivery
                    # state is mutated, and the original work/fencing error survives.
                    try:
                        await c.close(timeout=2)
                    except (
                        asyncpg.PostgresError,
                        asyncpg.InterfaceError,
                        asyncpg.InternalClientError,
                        TimeoutError,
                    ):
                        with suppress(asyncpg.InterfaceError):
                            c.terminate()

    async def page(self, limit: int = 100) -> dict[str, Any]:
        if not 1 <= limit <= 500:
            raise ValueError("page limit must be 1..500")
        async with (
            self.store.connection() as c,
            c.transaction(isolation="repeatable_read", readonly=True),
        ):
            state = await c.fetchrow("SELECT * FROM source_capture_clock WHERE singleton")
            rows = await c.fetch(
                "SELECT record,change_kind FROM source_candle_outbox WHERE position>$1 "
                "ORDER BY position LIMIT $2",
                state["candle_ack"],
                limit,
            )
            records = [json.loads(r["record"]) for r in rows]
            return dict(
                source_epoch=state["source_epoch"],
                after=state["candle_ack"],
                through=int(records[-1]["source_position"]) if records else state["head"],
                records=records,
                event_kinds={
                    r["observation_id"]: row["change_kind"]
                    for r, row in zip(records, rows, strict=True)
                },
            )

    async def pending_quotes(self, limit: int = 100) -> dict[str, Any]:
        if not 1 <= limit <= 500:
            raise ValueError("page limit must be 1..500")
        async with (
            self.store.connection() as c,
            c.transaction(isolation="repeatable_read", readonly=True),
        ):
            epoch = await c.fetchval(
                "SELECT source_epoch FROM source_capture_clock WHERE singleton"
            )
            rows = await c.fetch(
                "SELECT record FROM source_quote_state WHERE acked_version<version "
                "ORDER BY version LIMIT $1",
                limit,
            )
            return dict(source_epoch=epoch, records=[json.loads(r["record"]) for r in rows])

    async def acknowledge_candles(self, epoch: UUID, after: int, through: int) -> None:
        async with self.store.connection() as c, c.transaction():
            if self.worker_fence:
                await self.enter(c)
                await c.execute(
                    "SELECT source_sync_capture($1,$2::jsonb)",
                    "ack_candles" if self.ack_batch_id else "empty_ack",
                    json.dumps(
                        dict(
                            epoch=str(epoch),
                            after=after,
                            through=through,
                            batch_id=str(self.ack_batch_id),
                        )
                    ),
                )
                return
            state = await clock(c)
            if state["source_epoch"] != epoch or not 0 <= after <= through <= state["head"]:
                raise CaptureConflict("invalid capture epoch/cursor")
            if state["candle_ack"] == through:
                return
            if state["candle_ack"] != after:
                raise CaptureConflict("candle acknowledgment was concurrently advanced")
            await c.execute(
                "UPDATE source_capture_clock SET candle_ack=$1 WHERE singleton", through
            )

    async def acknowledge_quote(
        self, epoch: UUID, instrument: UUID, provider: str, version: int
    ) -> bool:
        async with self.store.connection() as c, c.transaction():
            if self.worker_fence:
                await self.enter(c)
                await c.execute(
                    "SELECT source_sync_capture('ack_quote',$1::jsonb)",
                    json.dumps(
                        dict(
                            epoch=str(epoch),
                            instrument=str(instrument),
                            provider=provider,
                            version=version,
                            batch_id=str(self.ack_batch_id),
                        )
                    ),
                )
                return bool(
                    await c.fetchval(
                        "SELECT version=acked_version FROM source_quote_state "
                        "WHERE instrument_id=$1 AND provider=$2",
                        instrument,
                        provider,
                    )
                )
            state = await clock(c)
            if state["source_epoch"] != epoch:
                raise CaptureConflict("quote acknowledgment epoch changed")
            row = await c.fetchrow(
                """UPDATE source_quote_state SET acked_version=greatest(acked_version,$3)
                    WHERE instrument_id=$1 AND provider=$2 AND $3>0 AND $3<=version
                    RETURNING version=acked_version AS delivered""",
                instrument,
                provider,
                version,
            )
            if row is None:
                raise CaptureConflict("unknown quote version")
            return bool(row["delivered"])

    async def cleanup(self, limit: int = 500) -> int:
        if not 1 <= limit <= 10000:
            raise ValueError("cleanup limit must be 1..10000")
        async with self.store.connection() as c, c.transaction():
            if self.worker_fence:
                await self.enter(c)
                return int(
                    json.loads(
                        await c.fetchval(
                            "SELECT source_sync_capture('cleanup',$1::jsonb)",
                            json.dumps(dict(limit=limit)),
                        )
                    )
                )
            await clock(c)
            row = await c.fetchrow(
                """WITH removed AS (DELETE FROM source_candle_outbox WHERE position IN
                    (SELECT position FROM source_candle_outbox WHERE position<=
                        (SELECT candle_ack FROM source_capture_clock WHERE singleton)
                        ORDER BY position LIMIT $1) RETURNING byte_count)
                    UPDATE source_capture_clock SET pending_rows=pending_rows-r.n,
                        pending_bytes=pending_bytes-r.bytes FROM
                        (SELECT count(*) AS n,coalesce(sum(byte_count),0) AS bytes FROM removed) r
                    WHERE singleton RETURNING r.n""",
                limit,
            )
            return int(row["n"])

    async def seed_candles(self, instrument: UUID, provider: str, limit: int = 100) -> int:
        """Resumable enrollment of pre-capture history, with no bucket-time cursor."""
        if not 1 <= limit <= 500:
            raise ValueError("seed limit must be 1..500")
        async with self.store.connection() as c, c.transaction():
            await self.enter(c)
            await self.store.lock_sources(c, [(instrument, provider)])
            rows = await c.fetch(
                """SELECT m.payload FROM market_candles m WHERE instrument_id=$1 AND provider=$2
                    AND interval='1m' AND price_basis='native' AND NOT EXISTS
                    (SELECT 1 FROM source_candle_exports e WHERE
                        (e.instrument_id,e.provider,e.bucket_start)=
                        (m.instrument_id,m.provider,m.bucket_start))
                    ORDER BY bucket_start LIMIT $3""",
                instrument,
                provider,
                limit,
            )
            bars = [MarketCandle.model_validate_json(r["payload"]) for r in rows]
            await append_candles(c, self.store, bars, seed=True, restricted=bool(self.worker_fence))
            return len(bars)

    async def seed_quotes(self, limit: int = 100) -> int:
        if not 1 <= limit <= 500:
            raise ValueError("seed limit must be 1..500")
        async with self.store.connection() as c, c.transaction():
            await self.enter(c)
            rows = await c.fetch(
                """SELECT p.payload FROM latest_prices p WHERE NOT EXISTS
                    (SELECT 1 FROM source_quote_state s WHERE
                        (s.instrument_id,s.provider)=(p.instrument_id,p.provider))
                    ORDER BY p.instrument_id,p.provider LIMIT $1""",
                limit,
            )
            ticks = [Tick.model_validate_json(r["payload"]) for r in rows]
            await self.store.lock_sources(c, [(t.instrument_id, t.provider) for t in ticks])
            return len(await prepare_quotes(c, ticks, restricted=bool(self.worker_fence)))

    async def resnapshot_candles(
        self, instrument: UUID, provider: str, buckets: list[datetime]
    ) -> int:
        """Explicit bounded reconciliation repair using current financial rows and allocator."""
        if not 1 <= len(buckets) <= 100:
            raise ValueError("repair requires 1..100 explicit buckets")
        async with self.store.connection() as c, c.transaction():
            await self.enter(c)
            await self.store.lock_sources(c, [(instrument, provider)])
            rows = await c.fetch(
                """SELECT payload FROM market_candles WHERE
                instrument_id=$1 AND provider=$2 AND interval='1m' AND price_basis='native'
                AND bucket_start=ANY($3::timestamptz[]) ORDER BY bucket_start""",
                instrument,
                provider,
                buckets,
            )
            bars = [MarketCandle.model_validate_json(r["payload"]) for r in rows]
            await append_candles(
                c, self.store, bars, seed=True, restricted=bool(self.worker_fence), repair=True
            )
            return len(bars)

    async def resnapshot_quote(self, instrument: UUID, provider: str) -> int:
        """Reviewed reconciliation only: new identity, unchanged field provenance."""
        async with self.store.connection() as c, c.transaction():
            await self.enter(c)
            await self.store.lock_sources(c, [(instrument, provider)])
            value = await c.fetchval(
                "SELECT record FROM source_quote_state WHERE instrument_id=$1 AND provider=$2",
                instrument,
                provider,
            )
            if value is None:
                return 0
            tick = Tick.model_validate(json.loads(value)["observation"])
            return len(
                await prepare_quotes(c, [tick], restricted=bool(self.worker_fence), resnapshot=True)
            )
