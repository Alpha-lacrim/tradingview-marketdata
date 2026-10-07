from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import Tick
from marketdata.registry import Registry
from marketdata.sessions import classify_ranges

INTERVALS = {
    "1s": "1 second",
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
    "1d": "1 day",
}
TICK_COLUMNS = (
    "storage_timestamp",
    "tick_id",
    "instrument_id",
    "provider",
    "provider_symbol",
    "realtime_status",
    "event_timestamp",
    "event_timestamp_ns",
    "received_timestamp",
    "order_ns",
    "bid",
    "ask",
    "last",
    "bid_size",
    "ask_size",
    "last_size",
    "volume",
    "sequence_number",
    "metadata",
    "payload",
)


def order_ns(tick: Tick) -> int:
    if tick.event_timestamp_ns is not None:
        return tick.event_timestamp_ns
    delta = tick.storage_timestamp - datetime(1970, 1, 1, tzinfo=UTC)
    return ((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds) * 1000


def tick_record(tick: Tick) -> tuple[Any, ...]:
    data = tick.model_dump()
    data.update(
        storage_timestamp=tick.storage_timestamp,
        order_ns=order_ns(tick),
        metadata=json.dumps(tick.metadata),
        payload=tick.model_dump_json(),
    )
    return tuple(data[c] for c in TICK_COLUMNS)


class Store:
    def __init__(self, settings: Settings):
        self.settings = settings
        # Serving config chooses active capabilities; offline queries prefer stored candles.
        self.candle_providers: list[str] | None = None
        self.pool: asyncpg.Pool | None = None
        self.has_aggregates = False
        self.last_candle_write: dict[str, Any] = {}

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            self.settings.database_url.get_secret_value(),
            min_size=self.settings.db_pool_min,
            max_size=self.settings.db_pool_max,
            command_timeout=self.settings.db_timeout_seconds,
            timeout=self.settings.db_timeout_seconds,
            server_settings={"timezone": "UTC", "application_name": "marketdata"},
        )
        async with self.connection() as connection:
            version = await connection.fetchval("SELECT version_num FROM alembic_version")
            if version != "0004":
                raise RuntimeError("database migrations are not at head; run alembic upgrade head")
            self.has_aggregates = bool(
                await connection.fetchval("SELECT to_regclass('candles_1m_last')")
            )

    def connection(self) -> Any:
        if self.pool is None:
            raise RuntimeError("store is disconnected")
        return self.pool.acquire(timeout=self.settings.db_timeout_seconds)

    async def close(self) -> None:
        if self.pool is not None:
            try:
                async with asyncio.timeout(self.settings.db_timeout_seconds):
                    await self.pool.close()
            except TimeoutError:
                self.pool.terminate()
            self.pool = None

    async def ping(self) -> bool:
        async with self.connection() as connection:
            return bool(await connection.fetchval("SELECT 1"))

    async def register(self, registry: Registry) -> None:
        async with self.connection() as connection, connection.transaction():
            for entry in registry.entries:
                i = entry.instrument
                fields = i.model_dump()
                fields.update(id=i.id, metadata=json.dumps(i.metadata), payload=i.model_dump_json())
                columns = ["id", *type(i).model_fields.keys(), "payload"]
                assignments = ",".join(f"{c}=EXCLUDED.{c}" for c in columns if c != "id")
                placeholders = ",".join(f"${n}" for n in range(1, len(columns) + 1))
                identity = (
                    "asset_class",
                    "instrument_type",
                    "exchange",
                    "currency",
                    "base_asset",
                    "quote_asset",
                    "root_symbol",
                    "contract_code",
                    "contract_month",
                )
                unchanged = " AND ".join(
                    f"instruments.{c} IS NOT DISTINCT FROM EXCLUDED.{c}" for c in identity
                )
                registered_id = await connection.fetchval(
                    f"INSERT INTO instruments ({','.join(columns)}) VALUES ({placeholders}) "
                    f"ON CONFLICT (id) DO UPDATE SET {assignments},updated_at=now() "
                    f"WHERE {unchanged} RETURNING id",
                    *(fields[c] for c in columns),
                )
                if registered_id is None:
                    raise ValueError(
                        "canonical identity cannot be reassigned to a different instrument"
                    )
                for m in entry.mappings:
                    await connection.execute(
                        """
                        INSERT INTO provider_instruments
                        (instrument_id,provider,provider_symbol,realtime_status,data_delay_seconds,metadata)
                        VALUES ($1,$2,$3,$4,$5,$6)
                        ON CONFLICT (instrument_id,provider) DO UPDATE SET
                        provider_symbol=EXCLUDED.provider_symbol,
                        realtime_status=EXCLUDED.realtime_status,
                        data_delay_seconds=EXCLUDED.data_delay_seconds,
                        metadata=EXCLUDED.metadata,updated_at=now()
                    """,
                        i.id,
                        m.provider,
                        m.provider_symbol,
                        m.realtime_status,
                        m.data_delay_seconds,
                        json.dumps(m.metadata),
                    )

    async def write(self, ticks: list[Tick]) -> int:
        if not ticks:
            return 0
        columns = ",".join(TICK_COLUMNS)
        async with self.connection() as connection, connection.transaction():
            await connection.execute("""CREATE TEMP TABLE IF NOT EXISTS tick_stage
                (LIKE market_ticks INCLUDING DEFAULTS) ON COMMIT DELETE ROWS""")
            await connection.copy_records_to_table(
                "tick_stage",
                records=[tick_record(t) for t in ticks],
                columns=TICK_COLUMNS,
            )
            # Both history and latest state commit atomically. Retry the same UUIDs after
            # uncertain commits; only rows actually inserted can update latest state.
            count = await connection.fetchval(f"""
                WITH inserted AS (
                    INSERT INTO market_ticks ({columns}) SELECT {columns} FROM tick_stage
                    ON CONFLICT (storage_timestamp,tick_id) DO NOTHING RETURNING *
                ), latest AS (
                    INSERT INTO latest_prices
                        (instrument_id,provider,order_ns,received_timestamp,tick_id,payload)
                    SELECT DISTINCT ON (instrument_id,provider)
                        instrument_id,provider,order_ns,received_timestamp,tick_id,payload
                    FROM inserted
                    ORDER BY instrument_id,provider,order_ns DESC,
                        received_timestamp DESC,tick_id DESC
                    ON CONFLICT (instrument_id,provider) DO UPDATE SET
                        order_ns=EXCLUDED.order_ns,received_timestamp=EXCLUDED.received_timestamp,
                        tick_id=EXCLUDED.tick_id,payload=EXCLUDED.payload
                    WHERE (EXCLUDED.order_ns,EXCLUDED.received_timestamp,EXCLUDED.tick_id) >
                        (latest_prices.order_ns,latest_prices.received_timestamp,latest_prices.tick_id)
                    RETURNING 1
                ) SELECT count(*) FROM inserted
            """)
            return int(count)

    async def instruments(self, limit: int, after: str = "") -> list[dict[str, Any]]:
        async with self.connection() as c:
            rows = await c.fetch(
                """SELECT payload, id FROM instruments
                WHERE canonical_symbol > $1 ORDER BY canonical_symbol LIMIT $2""",
                after,
                limit,
            )
        return [dict(json.loads(r["payload"]), id=str(r["id"])) for r in rows]

    async def instrument(self, symbol: str) -> dict[str, Any] | None:
        async with self.connection() as c:
            row = await c.fetchrow(
                "SELECT id,payload FROM instruments WHERE canonical_symbol=$1", symbol
            )
            if row is None:
                return None
            mappings = await c.fetch(
                """SELECT provider,provider_symbol,realtime_status,
                data_delay_seconds,metadata FROM provider_instruments WHERE instrument_id=$1""",
                row["id"],
            )
        return dict(
            json.loads(row["payload"]),
            id=str(row["id"]),
            mappings=[dict(r, metadata=json.loads(r["metadata"])) for r in mappings],
        )

    async def latest(
        self, symbol: str | None, provider: str | None, limit: int, after: str = ""
    ) -> list[Tick | MarketCandle]:
        async with self.connection() as c:
            rows = await c.fetch(
                """WITH current AS (
                SELECT instrument_id,provider,payload,'tick' AS kind FROM latest_prices t
                WHERE ($5::text[] IS NULL AND NOT EXISTS (
                    SELECT 1 FROM latest_candles c
                    WHERE c.instrument_id=t.instrument_id AND c.provider=t.provider))
                    OR ($5::text[] IS NOT NULL AND NOT provider=ANY($5))
                UNION ALL
                SELECT instrument_id,provider,payload,'candle' AS kind FROM latest_candles
                WHERE $5::text[] IS NULL OR provider=ANY($5)
                ) SELECT l.payload,l.kind FROM current l
                JOIN instruments i ON i.id=l.instrument_id
                WHERE ($1::text IS NULL OR i.canonical_symbol=$1)
                AND ($2::text IS NULL OR l.provider=$2)
                AND (i.canonical_symbol || '/' || l.provider) > $4
                ORDER BY i.canonical_symbol,l.provider LIMIT $3""",
                symbol,
                provider,
                limit,
                after,
                self.candle_providers,
            )
        return [
            (MarketCandle if r["kind"] == "candle" else Tick).model_validate_json(r["payload"])
            for r in rows
        ]

    async def candle_progress(self, instrument_id: UUID, provider: str) -> dict[str, Any]:
        async with self.connection() as c:
            row = await c.fetchrow(
                """
                SELECT coverage_start,coverage_end,missing,not_expected,unknown,session_info,
                    (SELECT min(lower(r)) FROM unnest(unknown) r) AS earliest_unknown,
                    (SELECT coalesce(sum(extract(epoch FROM upper(r)-lower(r))/60),0) FROM
                        unnest(unknown) r)::bigint AS unknown_count,
                    (SELECT coalesce(sum(extract(epoch FROM upper(r)-lower(r))/60),0) FROM
                        unnest(not_expected) r)::bigint AS not_expected_count,
                    (SELECT min(lower(r)) FROM unnest(missing) r) AS earliest_gap,
                    (SELECT coalesce(sum(extract(epoch FROM upper(r)-lower(r))/60),0)
                     FROM unnest(missing) r)::bigint AS missing_count,
                    l.bucket_start AS last_persisted_minute,
                    (SELECT max(discovered_at) FROM candle_revisions r
                        WHERE r.instrument_id=c.instrument_id AND r.provider=c.provider)
                        AS last_correction_at
                FROM candle_coverage c LEFT JOIN latest_candles l
                    USING(instrument_id,provider,price_basis)
                WHERE instrument_id=$1 AND provider=$2 AND price_basis='native'
            """,
                instrument_id,
                provider,
            )
        if row is None:
            return {}
        state = dict(row)
        state["session_info"] = json.loads(state["session_info"])
        state["unknown_ranges"] = [(r.lower, r.upper) for r in state.pop("unknown")]
        state["not_expected_ranges"] = [(r.lower, r.upper) for r in state.pop("not_expected")]
        state["missing_ranges"] = [(r.lower, r.upper) for r in state.pop("missing")]
        return state

    async def write_candles(
        self,
        candles: list[MarketCandle],
        coverage: list[
            tuple[UUID, str, datetime, datetime]
            | tuple[UUID, str, datetime, datetime, dict[str, Any]]
        ],
    ) -> int:
        """Atomic candles/latest/coverage. Newer receipts may correct the same minute.

        Coverage is a compact UTC multirange, so even long outages have bounded gap rows.
        An uncertain-commit retry changes neither identity nor coverage.
        """
        candles = list(
            {
                (c.instrument_id, c.provider, c.interval, c.bucket_start, c.price_basis): c
                for c in sorted(candles, key=lambda c: c.received_timestamp)
            }.values()
        )
        changed = 0
        revisions: list[dict[str, Any]] = []
        changed_keys: list[tuple[UUID, str, datetime]] = []
        changed_candles: list[MarketCandle] = []
        async with self.connection() as c, c.transaction():
            for plan in coverage:
                instrument_id, provider, start, end = plan[:4]
                previous = await c.fetchrow(
                    """SELECT coverage_end,session_info FROM candle_coverage WHERE instrument_id=$1
                        AND provider=$2 AND price_basis='native' FOR UPDATE""",
                    instrument_id,
                    provider,
                )
                await c.execute(
                    """
                    INSERT INTO candle_coverage
                        (instrument_id,provider,price_basis,coverage_start,coverage_end,missing)
                    VALUES ($1,$2,'native',$3,$4,tstzmultirange(tstzrange($3,$4,'[)')))
                    ON CONFLICT (instrument_id,provider,price_basis) DO UPDATE SET
                        missing=candle_coverage.missing +
                            tstzmultirange(tstzrange(candle_coverage.coverage_end,
                                greatest(candle_coverage.coverage_end,EXCLUDED.coverage_end),'[)')),
                        coverage_end=greatest(candle_coverage.coverage_end,EXCLUDED.coverage_end)
                """,
                    instrument_id,
                    provider,
                    start,
                    end,
                )
                if len(plan) == 5:
                    info = plan[4]
                    classify_start = (
                        start
                        if previous is None or json.loads(previous["session_info"]) != info
                        else previous["coverage_end"]
                    )
                    if classify_start < end:
                        classified = classify_ranges(info, classify_start, end)

                        def multirange(
                            ranges: list[tuple[datetime, datetime]],
                        ) -> list[asyncpg.Range[datetime]]:
                            return [
                                asyncpg.Range(lo, hi, lower_inc=True, upper_inc=False)
                                for lo, hi in ranges
                            ]

                        # Reclassification is atomic with persistence. Existing native bars
                        # are positive evidence even when a calendar cannot classify them.
                        await c.execute(
                            """
                            UPDATE candle_coverage SET
                                not_expected=(not_expected -
                                    tstzmultirange(tstzrange($3,$4,'[)'))) + $5::tstzmultirange,
                                unknown=(unknown - tstzmultirange(tstzrange($3,$4,'[)'))) +
                                    $6::tstzmultirange,
                                session_info=$7::jsonb
                            WHERE instrument_id=$1 AND provider=$2 AND price_basis='native'
                        """,
                            instrument_id,
                            provider,
                            classify_start,
                            end,
                            multirange(classified["NOT_EXPECTED"]),
                            multirange(classified["UNKNOWN"]),
                            json.dumps(info),
                        )
                    await c.execute(
                        """
                        WITH supplied AS (SELECT
                            coalesce(range_agg(tstzrange(bucket_start,bucket_end,'[)')),'{}'::tstzmultirange)
                            AS ranges
                            FROM market_candles WHERE instrument_id=$1 AND provider=$2 AND
                                interval='1m' AND price_basis='native' AND bucket_start >= $3 AND
                                bucket_start < $4)
                        UPDATE candle_coverage SET missing=CASE WHEN $5 THEN
                            tstzmultirange(tstzrange(coverage_start,coverage_end,'[)')) ELSE
                            missing END
                                -not_expected-unknown-supplied.ranges,
                            not_expected=not_expected-supplied.ranges,
                                unknown=unknown-supplied.ranges
                        FROM supplied WHERE instrument_id=$1 AND provider=$2 AND
                            price_basis='native'
                    """,
                        instrument_id,
                        provider,
                        classify_start,
                        end,
                        previous is None or json.loads(previous["session_info"]) != info,
                    )
            if candles:
                columns = (
                    "instrument_id",
                    "provider",
                    "provider_symbol",
                    "interval",
                    "bucket_start",
                    "bucket_end",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "price_basis",
                    "realtime_status",
                    "source_status",
                    "received_timestamp",
                    "metadata",
                    "payload",
                )
                records = []
                for candle in candles:
                    fields = candle.model_dump()
                    fields.update(
                        metadata=json.dumps(candle.metadata), payload=candle.model_dump_json()
                    )
                    records.append(tuple(fields[name] for name in columns))
                await c.execute("""CREATE TEMP TABLE IF NOT EXISTS candle_stage
                    (LIKE market_candles INCLUDING DEFAULTS) ON COMMIT DELETE ROWS""")
                await c.copy_records_to_table("candle_stage", records=records, columns=columns)
                # Single owned writer; lock matched rows before audit + UPSERT.
                await c.fetch("""SELECT m.bucket_start FROM market_candles m JOIN candle_stage s
                    USING(instrument_id,provider,interval,bucket_start,price_basis) FOR UPDATE OF
                        m""")
                changes = await c.fetch("""
                    INSERT INTO
                        candle_revisions(instrument_id,provider,bucket_start,changed_fields,old_values,new_values)
                    SELECT m.instrument_id,m.provider,m.bucket_start,
                        ARRAY(SELECT key FROM
                            jsonb_each(jsonb_build_object('open',s.open,'high',s.high,'low',s.low,'close',s.close,'volume',s.volume,'source_status',s.source_status,'realtime_status',s.realtime_status))
                            e
                            WHERE e.value IS DISTINCT FROM
                                jsonb_build_object('open',m.open,'high',m.high,'low',m.low,'close',m.close,'volume',m.volume,'source_status',m.source_status,'realtime_status',m.realtime_status)->e.key),
                        jsonb_build_object('open',m.open::text,'high',m.high::text,'low',m.low::text,'close',m.close::text,'volume',m.volume::text,'source_status',m.source_status,'realtime_status',m.realtime_status),
                        jsonb_build_object('open',s.open::text,'high',s.high::text,'low',s.low::text,'close',s.close::text,'volume',s.volume::text,'source_status',s.source_status,'realtime_status',s.realtime_status)
                    FROM market_candles m JOIN candle_stage s
                        USING(instrument_id,provider,interval,bucket_start,price_basis)
                    WHERE s.received_timestamp > m.received_timestamp AND
                        (s.open,s.high,s.low,s.close,s.volume,s.source_status,s.realtime_status)
                            IS DISTINCT FROM
                        (m.open,m.high,m.low,m.close,m.volume,m.source_status,m.realtime_status)
                    RETURNING instrument_id,provider,bucket_start,changed_fields,discovered_at
                """)
                revisions = [dict(r) for r in changes]
                # Backfill provenance is historical fact; later corrections must
                # not erase it when an overlap bar is no longer a recovery insert.
                await c.execute("""
                    UPDATE candle_stage s SET
                        metadata=s.metadata || '{"backfilled":true}'::jsonb,
                        payload=jsonb_set(s.payload,'{metadata,backfilled}','true'::jsonb,true)
                    FROM market_candles m
                    WHERE (s.instrument_id,s.provider,s.interval,s.bucket_start,s.price_basis)=
                        (m.instrument_id,m.provider,m.interval,m.bucket_start,m.price_basis)
                        AND coalesce((m.metadata->>'backfilled')::boolean,false)
                """)
                names = ",".join(columns)
                written = await c.fetch(f"""
                    WITH written AS (
                    INSERT INTO market_candles ({names})
                    SELECT DISTINCT ON (instrument_id,provider,interval,bucket_start,price_basis)
                        {names} FROM candle_stage
                    ORDER BY instrument_id,provider,interval,bucket_start,price_basis,
                        received_timestamp DESC
                    ON CONFLICT (instrument_id,provider,interval,bucket_start,price_basis)
                    DO UPDATE SET open=EXCLUDED.open,high=EXCLUDED.high,low=EXCLUDED.low,
                        close=EXCLUDED.close,volume=EXCLUDED.volume,
                        realtime_status=EXCLUDED.realtime_status,source_status=EXCLUDED.source_status,
                        received_timestamp=EXCLUDED.received_timestamp,
                        metadata=EXCLUDED.metadata,payload=EXCLUDED.payload,updated_at=now()
                    WHERE EXCLUDED.received_timestamp > market_candles.received_timestamp
                        AND
                            (EXCLUDED.open,EXCLUDED.high,EXCLUDED.low,EXCLUDED.close,EXCLUDED.volume,EXCLUDED.source_status,EXCLUDED.realtime_status)
                            IS DISTINCT FROM
                            (market_candles.open,market_candles.high,market_candles.low,market_candles.close,market_candles.volume,market_candles.source_status,market_candles.realtime_status)
                    RETURNING instrument_id,provider,bucket_start,payload) SELECT * FROM written
                """)
                changed = len(written)
                changed_candles = [MarketCandle.model_validate_json(r["payload"]) for r in written]
                changed_keys = [
                    (r["instrument_id"], r["provider"], r["bucket_start"]) for r in written
                ]
                await c.execute("""
                    INSERT INTO latest_candles
                        (instrument_id,provider,price_basis,bucket_start,received_timestamp,payload)
                    SELECT DISTINCT ON (instrument_id,provider,price_basis)
                        instrument_id,provider,price_basis,bucket_start,received_timestamp,payload
                    FROM candle_stage ORDER BY instrument_id,provider,price_basis,
                        bucket_start DESC,received_timestamp DESC
                    ON CONFLICT (instrument_id,provider,price_basis) DO UPDATE SET
                        bucket_start=EXCLUDED.bucket_start,
                        received_timestamp=EXCLUDED.received_timestamp,payload=EXCLUDED.payload
                    WHERE (EXCLUDED.bucket_start,EXCLUDED.received_timestamp) >
                        (latest_candles.bucket_start,latest_candles.received_timestamp)
                """)
                await c.execute("""
                    WITH filled AS (
                        SELECT instrument_id,provider,price_basis,
                            range_agg(tstzrange(bucket_start,bucket_end,'[)')) AS ranges
                        FROM candle_stage GROUP BY instrument_id,provider,price_basis
                    ) UPDATE candle_coverage c SET missing=c.missing-f.ranges,
                        unknown=c.unknown-f.ranges,not_expected=c.not_expected-f.ranges
                    FROM filled f WHERE (c.instrument_id,c.provider,c.price_basis)=
                        (f.instrument_id,f.provider,f.price_basis)
                """)
        self.last_candle_write = {
            "corrections": revisions,
            "changed": changed,
            "changed_keys": changed_keys,
            "changed_candles": changed_candles,
        }
        return changed

    async def native_candles(
        self,
        instrument_id: UUID,
        provider: str,
        start: datetime,
        end: datetime,
        interval: str,
        limit: int,
        after_status: str | None = None,
    ) -> list[dict[str, Any]]:
        if interval not in INTERVALS or interval == "1s":
            raise ValueError("native candles support 1m or larger intervals")
        async with self.connection() as c:
            rows = await c.fetch(
                """
                WITH rolled AS (
                SELECT date_bin($6::text::interval,bucket_start,'1970-01-01T00:00:00Z') AS bucket,
                    provider,realtime_status,
                    (array_agg(open ORDER BY bucket_start))[1] AS open,
                    max(high) AS high,min(low) AS low,
                    (array_agg(close ORDER BY bucket_start DESC))[1] AS close,
                    CASE WHEN bool_and(coalesce((metadata->>'volume_additive')::boolean,false))
                        AND count(volume)=count(*) AND count(DISTINCT metadata->>'volume_type')=1
                        THEN sum(volume) ELSE NULL END AS volume,
                    count(*) AS observations,
                    CASE WHEN count(DISTINCT provider_symbol)=1 THEN min(provider_symbol)
                        ELSE NULL END AS provider_symbol,
                    max(received_timestamp) AS received_timestamp,
                    (array_agg(metadata ORDER BY bucket_start))[1] AS metadata,
                    'native'::text AS price_basis,
                    CASE WHEN count(DISTINCT source_status)=1 THEN min(source_status)
                        ELSE 'mixed' END AS source_status
                FROM market_candles WHERE instrument_id=$1 AND provider=$2
                    AND interval='1m' AND price_basis='native'
                    AND bucket_start >= $3 AND bucket_start < $4
                GROUP BY bucket,provider,realtime_status
                ) SELECT * FROM rolled
                WHERE ($7::text IS NULL OR bucket > $3 OR realtime_status > $7)
                ORDER BY bucket,realtime_status LIMIT $5
            """,
                instrument_id,
                provider,
                start,
                end,
                limit,
                INTERVALS[interval],
                after_status,
            )
        return [
            dict(r, instrument_id=instrument_id, metadata=json.loads(r["metadata"])) for r in rows
        ]

    async def ticks(
        self,
        instrument_id: UUID,
        provider: str | None,
        start: datetime,
        end: datetime,
        limit: int,
        cursor: tuple[datetime, UUID] | None,
    ) -> list[Tick]:
        async with self.connection() as c:
            rows = await c.fetch(
                """SELECT payload FROM market_ticks
                WHERE instrument_id=$1 AND ($2::text IS NULL OR provider=$2)
                AND storage_timestamp >= $3 AND storage_timestamp < $4
                AND ($6::timestamptz IS NULL OR (storage_timestamp,tick_id) > ($6,$7::uuid))
                ORDER BY storage_timestamp,tick_id LIMIT $5""",
                instrument_id,
                provider,
                start,
                end,
                limit,
                cursor[0] if cursor else None,
                cursor[1] if cursor else None,
            )
        return [Tick.model_validate_json(r["payload"]) for r in rows]

    async def candles(
        self,
        instrument_id: UUID,
        provider: str,
        start: datetime,
        end: datetime,
        interval: str,
        basis: Literal["last", "mid"],
        limit: int,
        after_status: str | None = None,
    ) -> list[dict[str, Any]]:
        if interval not in INTERVALS:
            raise ValueError("unsupported interval")
        async with self.connection() as c:
            if self.has_aggregates:
                rows = await c.fetch(
                    f"""SELECT * FROM candles_{interval}_{basis}
                    WHERE instrument_id=$1 AND provider=$2 AND bucket >= $3 AND bucket < $4
                    AND ($6::text IS NULL OR bucket > $3 OR realtime_status > $6)
                    ORDER BY bucket,realtime_status LIMIT $5""",
                    instrument_id,
                    provider,
                    start,
                    end,
                    limit,
                    after_status,
                )
            else:
                price = "last" if basis == "last" else "((bid+ask)/2)"
                condition = (
                    "last IS NOT NULL" if basis == "last" else "bid IS NOT NULL AND ask IS NOT NULL"
                )
                volume = "sum(last_size)" if basis == "last" else "NULL::numeric"
                rows = await c.fetch(
                    f"""WITH aggregated AS (SELECT
                    date_bin($6::text::interval,storage_timestamp,'1970-01-01T00:00:00Z') AS bucket,
                    provider,realtime_status,
                    (array_agg({price} ORDER BY order_ns,received_timestamp,tick_id))[1] AS open,
                    max({price}) AS high,min({price}) AS low,
                    (array_agg({price} ORDER BY order_ns DESC,
                        received_timestamp DESC,tick_id DESC))[1] AS close,
                    {volume} AS volume,count(*) AS observations
                    FROM market_ticks WHERE instrument_id=$1 AND provider=$2
                    AND storage_timestamp >= $3 AND storage_timestamp < $4 AND {condition}
                    GROUP BY bucket,provider,realtime_status
                    ) SELECT * FROM aggregated
                    WHERE ($7::text IS NULL OR bucket > $3 OR realtime_status > $7)
                    ORDER BY bucket,realtime_status LIMIT $5
                """,
                    instrument_id,
                    provider,
                    start,
                    end,
                    limit,
                    INTERVALS[interval],
                    after_status,
                )
        return [dict(r) for r in rows]
