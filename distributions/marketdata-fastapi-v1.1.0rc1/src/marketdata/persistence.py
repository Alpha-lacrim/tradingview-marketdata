from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, nullcontext, suppress
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast
from uuid import UUID

import asyncpg

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import Instrument, ProviderInstrument, RegistryEntry, Tick, utc_now
from marketdata.observability import Metrics
from marketdata.price_selection import select_price
from marketdata.providers.base import ProviderHealth
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
    def __init__(self, settings: Settings, *, read_only: bool = False):
        self.settings = settings
        self.read_only = read_only
        # Serving config chooses active capabilities; offline queries prefer stored candles.
        self.candle_providers: list[str] | None = None
        self.pool: asyncpg.Pool | None = None
        self.has_aggregates = False
        self.last_candle_write: dict[str, Any] = {}
        self.metrics: Metrics | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            self.settings.database_url.get_secret_value(),
            min_size=self.settings.db_pool_min,
            max_size=self.settings.db_pool_max,
            command_timeout=self.settings.db_timeout_seconds,
            timeout=self.settings.db_timeout_seconds,
            server_settings={
                "timezone": "UTC",
                "statement_timeout": str(int(self.settings.db_timeout_seconds * 1000)),
                "application_name": "marketdata-api" if self.read_only else "marketdata-collector",
                **({"default_transaction_read_only": "on"} if self.read_only else {}),
            },
        )
        async with self.connection() as connection:
            version = await connection.fetchval("SELECT version_num FROM alembic_version")
            if self.settings.source_capture_enabled:
                extension = await connection.fetchval(
                    "SELECT to_regclass('source_capture_version')"
                )
                if (
                    self.read_only
                    or version != "0006"
                    or not extension
                    or await connection.fetchval("SELECT version_num FROM source_capture_version")
                    not in {"sc0001", "sc0002"}
                ):
                    raise RuntimeError("source capture requires base 0006 plus source sc0001")
            elif version != "0010":
                if version == "0006" and await connection.fetchval(
                    "SELECT to_regclass('source_capture_version')"
                ):
                    raise RuntimeError(
                        "source capture requires explicit collector opt-in; "
                        "do not run destination upgrade head on this source"
                    )
                raise RuntimeError("database migrations are not at head; run alembic upgrade head")
            self.has_aggregates = bool(
                await connection.fetchval("SELECT to_regclass('candles_1m_last')")
            )
            if self.read_only and self.settings.api_environment == "production":
                unsafe = await connection.fetchval("""
                    SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=current_user
                        AND (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication
                            OR rolbypassrls))
                        OR has_schema_privilege(current_user,'public','CREATE')
                        OR has_database_privilege(current_user,current_database(),'TEMP')
                        OR EXISTS(SELECT 1 FROM unnest(ARRAY[
                            'instruments','provider_instruments','market_ticks','latest_prices',
                            'market_candles','latest_candles','candle_coverage','candle_revisions',
                            'collector_status','marketdata_events','marketdata_event_clock',
                            'alembic_version','ingest_producers','ingest_source_owners','ingest_batches',
                            'ingest_observations','ingest_candle_heads','ingest_coverage_heads',
                            'ingest_source_status','candle_observation_heads',
                            'ingest_request_budgets','ingest_read_status',
                            'ingest_session_recoveries','ingest_quote_heads',
                            'ingest_destination_state']) AS t(name)
                            WHERE has_table_privilege(current_user,name,
                                'INSERT,UPDATE,DELETE,TRUNCATE'))
                        OR has_function_privilege(current_user,
                            'ingest_renew_session(uuid,text,uuid,bigint)','EXECUTE')
                        OR has_function_privilege(current_user,
                            'ingest_admit_request(uuid,text,integer)','EXECUTE')
                        OR has_function_privilege(current_user,
                            'ingest_recover_session(uuid,text,jsonb)','EXECUTE')
                """)
                if unsafe:
                    raise RuntimeError(
                        "production API requires a dedicated SELECT-only database role"
                    )

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        if self.pool is None:
            raise RuntimeError("store is disconnected")

        def measured(query: Any) -> None:
            if self.metrics is not None:
                self.metrics.api_query_latency.observe(query.elapsed)

        async with self.pool.acquire(timeout=self.settings.db_timeout_seconds) as connection:
            if self.metrics is not None:
                connection.add_query_logger(measured)
            try:
                yield connection
            finally:
                if self.metrics is not None:
                    # Terminated backends detach their pool proxy before context exit.
                    with suppress(asyncpg.InterfaceError):
                        connection.remove_query_logger(measured)

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

    async def registry(self) -> Registry:
        """Load persisted identities and roll schedules without registering anything."""
        async with self.connection() as connection:
            rows = await connection.fetch("""
                SELECT i.payload, coalesce(jsonb_agg(jsonb_build_object(
                    'provider', p.provider, 'provider_symbol', p.provider_symbol,
                    'realtime_status', p.realtime_status,
                    'data_delay_seconds', p.data_delay_seconds, 'metadata', p.metadata
                ) ORDER BY p.provider) FILTER (WHERE p.provider IS NOT NULL), '[]') AS mappings
                FROM instruments i LEFT JOIN provider_instruments p ON p.instrument_id=i.id
                GROUP BY i.id ORDER BY i.canonical_symbol
            """)
        return Registry(
            [
                RegistryEntry(
                    instrument=Instrument.model_validate_json(r["payload"]),
                    mappings=[
                        ProviderInstrument.model_validate(m) for m in json.loads(r["mappings"])
                    ],
                )
                for r in rows
            ]
        )

    async def native_provider(self, provider: str) -> bool:
        async with self.connection() as connection:
            return bool(
                await connection.fetchval(
                    """
                SELECT EXISTS(SELECT 1 FROM candle_coverage
                    WHERE provider=$1 AND price_basis='native')
                    OR EXISTS(SELECT 1 FROM latest_candles WHERE provider=$1)
            """,
                    provider,
                )
            )

    async def collector_status(self) -> dict[str, Any] | None:
        async with self.connection() as connection:
            remote = await connection.fetch("SELECT * FROM ingest_read_status")
            if remote:
                from marketdata.remote_status import project_status

                return project_status([dict(row) for row in remote])
            row = await connection.fetchrow("""
                SELECT *, expires_at > clock_timestamp() AS fresh FROM collector_status
                WHERE singleton
            """)
        if row is None:
            return None
        status = dict(row)
        status["providers"] = [
            ProviderHealth.model_validate(p) for p in json.loads(row["providers"])
        ]
        if not status["fresh"] or not status["running"]:
            for health in status["providers"]:
                health.state = "disconnected"
                health.transport = "disconnected"
                health.subscription_restored = False
                health.fresh_symbol_count = 0
                health.stale_symbol_count = health.configured_symbol_count
                health.stale_symbols = sorted(health.symbols)
                health.degradation_reasons = [
                    "collector_stopped" if status["fresh"] else "collector_status_expired"
                ]
                for symbol in health.symbols.values():
                    symbol.stale = True
                    symbol.observed_in_session = False
        return status

    async def publish_collector_status(
        self,
        run_id: UUID,
        running: bool,
        database_healthy: bool,
        queue_depth: int,
        providers: list[ProviderHealth],
    ) -> None:
        async with self.connection() as connection:
            await connection.execute(
                """
                INSERT INTO collector_status
                    (singleton,run_id,expires_at,running,database_healthy,queue_depth,providers)
                VALUES (TRUE,$1,clock_timestamp()+$2*interval '1 second',$3,$4,$5,$6)
                ON CONFLICT (singleton) DO UPDATE SET run_id=EXCLUDED.run_id,
                    updated_at=clock_timestamp(),expires_at=EXCLUDED.expires_at,
                    running=EXCLUDED.running,database_healthy=EXCLUDED.database_healthy,
                    queue_depth=EXCLUDED.queue_depth,providers=EXCLUDED.providers
            """,
                run_id,
                self.settings.collector_status_ttl_seconds,
                running,
                database_healthy,
                queue_depth,
                json.dumps([p.model_dump(mode="json") for p in providers]),
            )

    async def register(
        self, registry: Registry, *, candle_providers: list[str] | None = None
    ) -> None:
        # The next full UTC minute is the first collection bucket for a new source.
        now = utc_now()
        activation = now.replace(second=0, microsecond=0)
        if activation < now:
            activation += timedelta(minutes=1)
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
                    registered_source = await connection.fetchval(
                        """
                        INSERT INTO provider_instruments
                        (instrument_id,provider,provider_symbol,realtime_status,data_delay_seconds,metadata)
                        VALUES ($1,$2,$3,$4,$5,$6)
                        ON CONFLICT (instrument_id,provider) DO UPDATE SET
                        realtime_status=EXCLUDED.realtime_status,
                        data_delay_seconds=EXCLUDED.data_delay_seconds,
                        metadata=EXCLUDED.metadata,updated_at=now()
                        WHERE provider_instruments.provider_symbol=EXCLUDED.provider_symbol
                        RETURNING provider_symbol
                    """,
                        i.id,
                        m.provider,
                        m.provider_symbol,
                        m.realtime_status,
                        m.data_delay_seconds,
                        json.dumps(m.metadata),
                    )
                    if registered_source is None:
                        raise ValueError("provider source identity cannot be reassigned")
                    if m.provider in (candle_providers or []):
                        # Existing coverage is immutable on registration. For legacy
                        # stored-only history without coverage, preserve its first bar.
                        await connection.execute(
                            """
                            INSERT INTO candle_coverage
                                (instrument_id,provider,price_basis,coverage_start,coverage_end)
                            SELECT $1,$2,'native',coalesce(min(bucket_start),$3),
                                coalesce(min(bucket_start),$3)
                            FROM market_candles WHERE instrument_id=$1 AND provider=$2
                                AND price_basis='native'
                            ON CONFLICT (instrument_id,provider,price_basis) DO NOTHING
                            """,
                            i.id,
                            m.provider,
                            activation,
                        )

    async def write(self, ticks: list[Tick]) -> int:
        return await self._write_observations(ticks, history=True)

    async def write_quotes(self, ticks: list[Tick]) -> int:
        """Commit latest genuine observations and journal events without tick history.

        Each batch retains the newest observation per source. Stable observation UUIDs
        make uncertain-commit retries idempotent via the latest-state ordering guard.
        """
        return await self._write_observations(ticks, history=False)

    async def _write_observations(self, ticks: list[Tick], *, history: bool) -> int:
        if not ticks:
            return 0
        async with self.connection() as connection, connection.transaction():
            return await self.write_observations_in_transaction(connection, ticks, history=history)

    @staticmethod
    def require_transaction(connection: Any) -> None:
        if not connection.is_in_transaction():
            raise ValueError("caller-owned database transaction required")

    @staticmethod
    async def lock_sources(connection: Any, sources: list[tuple[UUID, str]]) -> None:
        """All writers use the same lock order; multi-helper callers prelock their union."""
        Store.require_transaction(connection)
        for instrument_id, provider in sorted(set(sources), key=lambda key: (str(key[0]), key[1])):
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"marketdata:source:{instrument_id}:{provider}",
            )

    async def write_observations_in_transaction(
        self,
        connection: Any,
        ticks: list[Tick],
        *,
        history: bool = True,
        update_latest: bool = True,
        ordered_snapshots: list[Tick] | None = None,
    ) -> int:
        """Use the caller's connection; never commit or acquire another connection."""
        self.require_transaction(connection)
        if self.settings.source_capture_enabled:
            # A caller may catch Python validation/backpressure errors inside its
            # transaction. Keep source state/data/events indivisible in that case.
            async with connection.transaction():
                return await self._write_observations_in_transaction(
                    connection,
                    ticks,
                    history=history,
                    update_latest=update_latest,
                    ordered_snapshots=ordered_snapshots,
                )
        return await self._write_observations_in_transaction(
            connection,
            ticks,
            history=history,
            update_latest=update_latest,
            ordered_snapshots=ordered_snapshots,
        )

    async def _write_observations_in_transaction(
        self,
        connection: Any,
        ticks: list[Tick],
        *,
        history: bool,
        update_latest: bool,
        ordered_snapshots: list[Tick] | None = None,
    ) -> int:
        self.require_transaction(connection)
        if not ticks:
            return 0
        await self.lock_sources(connection, [(t.instrument_id, t.provider) for t in ticks])
        captured = self.settings.source_capture_enabled and update_latest
        if ordered_snapshots is not None and captured:
            raise ValueError("destination snapshots cannot bypass source capture")
        if captured or ordered_snapshots is not None:
            snapshots = ordered_snapshots
            if captured:
                from marketdata.source_capture import prepare_quotes

                snapshots = await prepare_quotes(connection, ticks)
            await connection.execute("""CREATE TEMP TABLE IF NOT EXISTS source_quote_stage
                (LIKE market_ticks INCLUDING DEFAULTS) ON COMMIT DELETE ROWS""")
            await connection.execute("TRUNCATE pg_temp.source_quote_stage")
            await connection.copy_records_to_table(
                "source_quote_stage",
                records=[tick_record(t) for t in snapshots or []],
                columns=TICK_COLUMNS,
            )
        columns = ",".join(TICK_COLUMNS)
        await connection.execute("""CREATE TEMP TABLE IF NOT EXISTS tick_stage
            (LIKE market_ticks INCLUDING DEFAULTS) ON COMMIT DELETE ROWS""")
        await connection.execute("TRUNCATE pg_temp.tick_stage")
        await connection.copy_records_to_table(
            "tick_stage",
            records=[tick_record(t) for t in ticks],
            columns=TICK_COLUMNS,
        )
        # History mode updates latest only from newly inserted stable UUIDs.
        # Latest-only mode uses the same ordering guard below for safe retries.
        # State and its journal events always share the owning transaction.
        observations = (
            f"INSERT INTO market_ticks ({columns}) SELECT {columns} FROM tick_stage "
            "ON CONFLICT (storage_timestamp,tick_id) DO NOTHING RETURNING *"
            if history
            else "SELECT DISTINCT ON (storage_timestamp,tick_id) * FROM tick_stage"
        )
        counted = "inserted" if history else "latest"
        latest_stage = (
            "source_quote_stage" if captured or ordered_snapshots is not None else "inserted"
        )
        result = await connection.fetchrow(f"""
            WITH inserted AS (
                {observations}
            ), latest AS (
                INSERT INTO latest_prices
                    (instrument_id,provider,order_ns,received_timestamp,tick_id,payload)
                SELECT DISTINCT ON (instrument_id,provider)
                    instrument_id,provider,order_ns,received_timestamp,tick_id,payload
                FROM {latest_stage}
                WHERE {"TRUE" if update_latest else "FALSE"}
                ORDER BY instrument_id,provider,order_ns DESC,
                    received_timestamp DESC,tick_id DESC
                ON CONFLICT (instrument_id,provider) DO UPDATE SET
                    order_ns=EXCLUDED.order_ns,received_timestamp=EXCLUDED.received_timestamp,
                    tick_id=EXCLUDED.tick_id,payload=EXCLUDED.payload
                WHERE {"TRUE OR" if captured or ordered_snapshots is not None else ""}
                    (EXCLUDED.order_ns,EXCLUDED.received_timestamp,EXCLUDED.tick_id) >
                    (latest_prices.order_ns,latest_prices.received_timestamp,latest_prices.tick_id)
                RETURNING payload
            ) SELECT (SELECT count(*) FROM {counted}) AS count,
                coalesce((SELECT jsonb_agg(payload) FROM latest),'[]') AS latest
        """)
        from marketdata.events import append_events

        await append_events(
            connection,
            [("price_update", Tick.model_validate(p)) for p in json.loads(result["latest"])],
            self.settings.marketdata_event_retention_count,
        )
        return int(result["count"])

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
        if self.candle_providers is None:
            rows = await self.source_records(
                [symbol] if symbol else None,
                provider,
                None,
                None,
                limit,
                after,
                observed_only=True,
            )
            now = rows[0]["as_of"] if rows else utc_now()
            return [
                observation
                for row in rows
                if (observation := select_price(row["tick"], row["candle"], now, self.settings))
                is not None
            ]
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

    async def known_symbols(self, symbols: list[str]) -> set[str]:
        async with self.connection() as c:
            rows = await c.fetch(
                "SELECT canonical_symbol FROM instruments WHERE canonical_symbol=ANY($1::text[])",
                symbols,
            )
        return {r["canonical_symbol"] for r in rows}

    async def source_records(
        self,
        symbols: list[str] | None,
        provider: str | None,
        provider_symbol: str | None,
        asset_class: str | None,
        limit: int,
        after: str = "",
        *,
        observed_only: bool = False,
        connection: Any | None = None,
    ) -> list[dict[str, Any]]:
        """One bounded bulk lookup of mappings and dedicated latest-state tables."""
        async with self.connection() if connection is None else nullcontext(connection) as c:
            rows = await c.fetch(
                """
                SELECT statement_timestamp() AS as_of,
                    i.payload AS instrument,p.provider,p.provider_symbol,p.realtime_status,
                    p.data_delay_seconds,t.payload AS tick,lc.payload AS candle,
                    EXISTS(SELECT 1 FROM candle_coverage cc WHERE cc.instrument_id=i.id
                        AND cc.provider=p.provider AND cc.price_basis='native')
                        OR lc.payload IS NOT NULL AS native_history
                FROM instruments i JOIN provider_instruments p ON p.instrument_id=i.id
                LEFT JOIN latest_prices t ON t.instrument_id=i.id AND t.provider=p.provider
                    AND t.payload->>'provider_symbol'=p.provider_symbol
                LEFT JOIN latest_candles lc ON lc.instrument_id=i.id AND lc.provider=p.provider
                    AND lc.price_basis='native' AND lc.payload->>'provider_symbol'=p.provider_symbol
                WHERE ($1::text[] IS NULL OR i.canonical_symbol=ANY($1))
                    AND ($2::text IS NULL OR p.provider=$2)
                    AND ($3::text IS NULL OR p.provider_symbol=$3)
                    AND ($4::text IS NULL OR i.asset_class=$4)
                    AND (i.canonical_symbol,p.provider) >
                        (split_part($5,'/',1),split_part($5,'/',2))
                    AND (NOT $7 OR t.payload IS NOT NULL OR lc.payload IS NOT NULL)
                ORDER BY i.canonical_symbol,p.provider LIMIT $6
                """,
                symbols,
                provider,
                provider_symbol,
                asset_class,
                after,
                limit,
                observed_only,
            )
        return [
            dict(
                r,
                instrument=Instrument.model_validate_json(r["instrument"]),
                tick=Tick.model_validate_json(r["tick"]) if r["tick"] else None,
                candle=MarketCandle.model_validate_json(r["candle"]) if r["candle"] else None,
            )
            for r in rows
        ]

    async def coverage_summary(
        self,
        identities: list[tuple[UUID, str]],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[dict[str, Any]]:
        """Clip stored tri-state ranges, treating untracked minutes as UNKNOWN.

        Counts are calculated before the bounded gap-detail projection. No historical
        minutes or recovered provenance are synthesized.
        """
        async with self.connection() as c:
            rows = await c.fetch(
                """
                WITH requested AS (
                    SELECT * FROM unnest($1::uuid[], $2::text[]) AS r(instrument_id,provider)
                ), clipped AS (
                    SELECT r.*,c.coverage_start,c.coverage_end,
                        tstzmultirange(tstzrange($3,$4,'[)')) AS window,
                        coalesce(c.missing,'{}'::tstzmultirange) *
                            tstzmultirange(tstzrange($3,$4,'[)')) AS missing,
                        coalesce(c.not_expected,'{}'::tstzmultirange) *
                            tstzmultirange(tstzrange($3,$4,'[)')) AS not_expected,
                        (coalesce(c.unknown,'{}'::tstzmultirange) *
                            tstzmultirange(tstzrange($3,$4,'[)'))) +
                            (tstzmultirange(tstzrange($3,$4,'[)')) -
                                CASE WHEN c.coverage_start IS NULL THEN '{}'::tstzmultirange
                                ELSE tstzmultirange(tstzrange(c.coverage_start,c.coverage_end,'[)'))
                                END) AS unknown
                    FROM requested r LEFT JOIN candle_coverage c USING(instrument_id,provider)
                    WHERE c.price_basis='native' OR c.price_basis IS NULL
                )
                SELECT x.instrument_id,x.provider,x.coverage_start,x.coverage_end,
                    (SELECT coalesce(sum(extract(epoch FROM upper(g)-lower(g))/60),0)::bigint
                        FROM unnest(x.missing) g) AS missing_count,
                    (SELECT coalesce(sum(extract(epoch FROM upper(g)-lower(g))/60),0)::bigint
                        FROM unnest(x.not_expected) g) AS not_expected_count,
                    (SELECT coalesce(sum(extract(epoch FROM upper(g)-lower(g))/60),0)::bigint
                        FROM unnest(x.unknown) g) AS unknown_count,
                    (SELECT count(*) FROM unnest(x.missing)) AS missing_range_count,
                    ARRAY(SELECT g FROM unnest(x.missing) g ORDER BY lower(g) LIMIT 100)
                        AS missing_ranges,
                    stats.persisted,stats.recovered,lc.bucket_start AS last_persisted,
                    CASE WHEN (lc.payload->>'bucket_end')::timestamptz <= $5
                        THEN lc.bucket_start END AS last_completed
                FROM clipped x LEFT JOIN LATERAL (
                    SELECT count(*) AS persisted,
                        count(*) FILTER (WHERE metadata->>'backfilled'='true') AS recovered
                    FROM market_candles m WHERE m.instrument_id=x.instrument_id
                        AND m.provider=x.provider AND m.interval='1m' AND m.price_basis='native'
                        AND m.bucket_start >= $3 AND m.bucket_start < $4 AND m.bucket_end <= $5
                ) stats ON TRUE
                LEFT JOIN latest_candles lc USING(instrument_id,provider)
                WHERE lc.price_basis='native' OR lc.price_basis IS NULL
                """,
                [i for i, _ in identities],
                [p for _, p in identities],
                start,
                end,
                as_of,
            )
        return [dict(r) for r in rows]

    async def coverage_health(self) -> Literal["complete", "gaps", "unknown"]:
        async with self.connection() as c:
            return cast(
                Literal["complete", "gaps", "unknown"],
                await c.fetchval("""
                SELECT CASE
                    WHEN EXISTS(SELECT 1 FROM candle_coverage WHERE NOT isempty(missing))
                        THEN 'gaps'
                    WHEN EXISTS(SELECT 1 FROM candle_coverage WHERE NOT isempty(unknown))
                        OR NOT EXISTS(SELECT 1 FROM candle_coverage)
                        OR EXISTS(SELECT 1 FROM candle_coverage
                            WHERE coverage_start >= coverage_end)
                        OR EXISTS(SELECT 1 FROM provider_instruments p
                            WHERE NOT EXISTS(SELECT 1 FROM candle_coverage c
                                WHERE c.instrument_id=p.instrument_id AND c.provider=p.provider
                                    AND c.price_basis='native')) THEN 'unknown'
                    ELSE 'complete' END
            """),
            )

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
        """Commit candles, latest, coverage, revisions and events atomically as before."""
        async with self.connection() as c, c.transaction():
            result = await self.write_candles_in_transaction(c, candles, coverage)
        # Compatibility for existing single-owner collector callers, only after commit.
        self.last_candle_write = result
        return int(result["changed"])

    async def write_candles_in_transaction(
        self,
        c: Any,
        candles: list[MarketCandle],
        coverage: list[
            tuple[UUID, str, datetime, datetime]
            | tuple[UUID, str, datetime, datetime, dict[str, Any]]
        ],
        *,
        restore_missing: bool = False,
    ) -> dict[str, Any]:
        """Return request-local results; the caller owns commit/rollback and all source locks."""
        self.require_transaction(c)
        if self.settings.source_capture_enabled:
            async with c.transaction():
                return await self._write_candles_in_transaction(c, candles, coverage)
        return await self._write_candles_in_transaction(
            c, candles, coverage, restore_missing=restore_missing
        )

    async def _write_candles_in_transaction(
        self,
        c: Any,
        candles: list[MarketCandle],
        coverage: list[
            tuple[UUID, str, datetime, datetime]
            | tuple[UUID, str, datetime, datetime, dict[str, Any]]
        ],
        *,
        restore_missing: bool = False,
    ) -> dict[str, Any]:
        self.require_transaction(c)
        await self.lock_sources(
            c,
            [(bar.instrument_id, bar.provider) for bar in candles]
            + [(plan[0], plan[1]) for plan in coverage],
        )
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
            await c.execute("TRUNCATE pg_temp.candle_stage")
            await c.copy_records_to_table("candle_stage", records=records, columns=columns)
            # Source advisory locks also protect previously absent business keys.
            await c.fetch("""SELECT m.bucket_start FROM market_candles m JOIN candle_stage s
                USING(instrument_id,provider,interval,bucket_start,price_basis) FOR UPDATE OF
                    m""")
            changes = await c.fetch("""
                INSERT INTO
                    candle_revisions(instrument_id,provider,bucket_start,changed_fields,old_values,new_values)
                SELECT m.instrument_id,m.provider,m.bucket_start,
                    ARRAY(SELECT key FROM
                        jsonb_each(jsonb_build_object('open',s.open,'high',s.high,'low',s.low,'close',s.close,'volume',s.volume,'source_status',s.source_status,'realtime_status',s.realtime_status,'volume_type',s.metadata->'volume_type','volume_additive',s.metadata->'volume_additive'))
                        e
                        WHERE e.value IS DISTINCT FROM
                            jsonb_build_object('open',m.open,'high',m.high,'low',m.low,'close',m.close,'volume',m.volume,'source_status',m.source_status,'realtime_status',m.realtime_status,'volume_type',m.metadata->'volume_type','volume_additive',m.metadata->'volume_additive')->e.key),
                    jsonb_build_object('open',m.open::text,'high',m.high::text,'low',m.low::text,'close',m.close::text,'volume',m.volume::text,'source_status',m.source_status,'realtime_status',m.realtime_status,'volume_type',m.metadata->'volume_type','volume_additive',m.metadata->'volume_additive'),
                    jsonb_build_object('open',s.open::text,'high',s.high::text,'low',s.low::text,'close',s.close::text,'volume',s.volume::text,'source_status',s.source_status,'realtime_status',s.realtime_status,'volume_type',s.metadata->'volume_type','volume_additive',s.metadata->'volume_additive')
                FROM market_candles m JOIN candle_stage s
                    USING(instrument_id,provider,interval,bucket_start,price_basis)
                WHERE s.received_timestamp > m.received_timestamp
                    AND NOT EXISTS (SELECT 1 FROM candle_observation_heads h
                        WHERE (h.instrument_id,h.provider,h.interval,h.bucket_start,h.price_basis)=
                            (s.instrument_id,s.provider,s.interval,s.bucket_start,s.price_basis)
                        AND h.received_timestamp >= s.received_timestamp) AND
                    (s.open,s.high,s.low,s.close,s.volume,s.source_status,s.realtime_status,s.metadata->'volume_type',s.metadata->'volume_additive')
                        IS DISTINCT FROM
                    (m.open,m.high,m.low,m.close,m.volume,m.source_status,m.realtime_status,m.metadata->'volume_type',m.metadata->'volume_additive')
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
                    {names} FROM candle_stage s
                WHERE NOT EXISTS (SELECT 1 FROM candle_observation_heads h
                    WHERE (h.instrument_id,h.provider,h.interval,h.bucket_start,h.price_basis)=
                        (s.instrument_id,s.provider,s.interval,s.bucket_start,s.price_basis)
                    AND h.received_timestamp >= s.received_timestamp
                    AND NOT ({restore_missing} AND NOT EXISTS (
                        SELECT 1 FROM market_candles existing WHERE
                        (existing.instrument_id,existing.provider,existing.interval,
                         existing.bucket_start,existing.price_basis)=
                        (s.instrument_id,s.provider,s.interval,s.bucket_start,s.price_basis))))
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
                        (EXCLUDED.open,EXCLUDED.high,EXCLUDED.low,EXCLUDED.close,EXCLUDED.volume,EXCLUDED.source_status,EXCLUDED.realtime_status,EXCLUDED.metadata->'volume_type',EXCLUDED.metadata->'volume_additive')
                        IS DISTINCT FROM
                        (market_candles.open,market_candles.high,market_candles.low,market_candles.close,market_candles.volume,market_candles.source_status,market_candles.realtime_status,market_candles.metadata->'volume_type',market_candles.metadata->'volume_additive')
                RETURNING instrument_id,provider,bucket_start,payload) SELECT * FROM written
            """)
            changed = len(written)
            changed_candles = [MarketCandle.model_validate_json(r["payload"]) for r in written]
            changed_keys = [(r["instrument_id"], r["provider"], r["bucket_start"]) for r in written]
            await c.execute("""
                INSERT INTO latest_candles
                    (instrument_id,provider,price_basis,bucket_start,received_timestamp,payload)
                SELECT DISTINCT ON (instrument_id,provider,price_basis)
                    instrument_id,provider,price_basis,bucket_start,received_timestamp,payload
                FROM candle_stage s
                WHERE NOT EXISTS (SELECT 1 FROM candle_observation_heads h
                    WHERE (h.instrument_id,h.provider,h.interval,h.bucket_start,h.price_basis)=
                        (s.instrument_id,s.provider,s.interval,s.bucket_start,s.price_basis)
                    AND h.received_timestamp >= s.received_timestamp)
                ORDER BY instrument_id,provider,price_basis,
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
            # Identical observations advance ordering without changing historical receipts,
            # revisions or financial events. Older corrections cannot exploit a no-op gap.
            await c.execute("""
                INSERT INTO candle_observation_heads
                    (instrument_id,provider,interval,bucket_start,price_basis,received_timestamp)
                SELECT instrument_id,provider,interval,bucket_start,price_basis,received_timestamp
                FROM candle_stage
                ON CONFLICT (instrument_id,provider,interval,bucket_start,price_basis)
                DO UPDATE SET received_timestamp=greatest(
                    candle_observation_heads.received_timestamp,EXCLUDED.received_timestamp)
            """)
            from marketdata.events import append_events

            corrected = {(r["instrument_id"], r["provider"], r["bucket_start"]) for r in revisions}
            if self.settings.source_capture_enabled:
                from marketdata.source_capture import append_candles

                await append_candles(c, self, changed_candles, corrected=corrected)

            await append_events(
                c,
                [
                    (
                        "candle_corrected"
                        if (bar.instrument_id, bar.provider, bar.bucket_start) in corrected
                        else "candle_completed",
                        bar,
                    )
                    for bar in changed_candles
                ],
                self.settings.marketdata_event_retention_count,
            )
        return {
            "corrections": revisions,
            "changed": changed,
            "changed_keys": changed_keys,
            "changed_candles": changed_candles,
        }

    async def apply_coverage_in_transaction(
        self,
        c: Any,
        instrument_id: UUID,
        provider: str,
        start: datetime,
        end: datetime,
        unknown: list[tuple[datetime, datetime]],
        not_expected: list[tuple[datetime, datetime]],
        session_info: dict[str, Any],
    ) -> None:
        """Merge a declared source window; destination bars, not source claims, fill gaps.

        The replication layer validates/orders this snapshot. Untracked bridges stay unknown.
        Ordinary collector callers continue using write_candles' existing coverage plans.
        """
        self.require_transaction(c)
        await self.lock_sources(c, [(instrument_id, provider)])
        await c.execute(
            """INSERT INTO candle_coverage
                (instrument_id,provider,price_basis,coverage_start,coverage_end)
                VALUES ($1,$2,'native',$3,$4) ON CONFLICT DO NOTHING""",
            instrument_id,
            provider,
            start,
            end,
        )
        await c.execute(
            """
            WITH input AS (
                SELECT tstzmultirange(tstzrange($3,$4,'[)')) AS window,
                    $5::tstzmultirange AS unknown,$6::tstzmultirange AS not_expected
            ), supplied AS (
                SELECT coalesce(range_agg(tstzrange(bucket_start,bucket_end,'[)')),
                    '{}'::tstzmultirange) AS bars FROM market_candles
                WHERE instrument_id=$1 AND provider=$2 AND interval='1m' AND price_basis='native'
            ) UPDATE candle_coverage cc SET
                unknown=((cc.unknown-input.window)+input.unknown+
                    (tstzmultirange(tstzrange(least(cc.coverage_start,$3),
                        greatest(cc.coverage_end,$4),'[)'))-
                     tstzmultirange(tstzrange(cc.coverage_start,cc.coverage_end,'[)'))-
                     input.window))-supplied.bars,
                not_expected=((cc.not_expected-input.window)+input.not_expected)-supplied.bars,
                missing=((cc.missing-input.window)+
                    (input.window-input.unknown-input.not_expected))-supplied.bars,
                coverage_start=least(cc.coverage_start,$3),
                coverage_end=greatest(cc.coverage_end,$4),session_info=$7::jsonb
            FROM input,supplied WHERE cc.instrument_id=$1 AND cc.provider=$2
                AND cc.price_basis='native'
            """,
            instrument_id,
            provider,
            start,
            end,
            [asyncpg.Range(lo, hi, lower_inc=True, upper_inc=False) for lo, hi in unknown],
            [asyncpg.Range(lo, hi, lower_inc=True, upper_inc=False) for lo, hi in not_expected],
            json.dumps(session_info),
        )

    async def native_candles(
        self,
        instrument_id: UUID,
        provider: str,
        start: datetime,
        end: datetime,
        interval: str,
        limit: int,
        after_status: str | None = None,
        *,
        as_of: datetime | None = None,
        provider_symbol: str | None = None,
        cursor: tuple[datetime, str] | None = None,
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
                    CASE WHEN ($6::text::interval=interval '1 minute') OR
                        (bool_and(coalesce((metadata->>'volume_additive')::boolean,false))
                        AND count(volume)=count(*) AND count(DISTINCT metadata->>'volume_type')=1
                        )
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
                    AND ($8::timestamptz IS NULL OR bucket_end <= $8)
                    AND ($9::text IS NULL OR provider_symbol=$9)
                GROUP BY bucket,provider,realtime_status
                ) SELECT * FROM rolled
                WHERE ($7::text IS NULL OR bucket > $3 OR realtime_status > $7)
                    AND ($8::timestamptz IS NULL OR bucket+$6::text::interval <= $8)
                    AND ($10::timestamptz IS NULL OR (bucket,realtime_status) > ($10,$11::text))
                ORDER BY bucket,realtime_status LIMIT $5
            """,
                instrument_id,
                provider,
                start,
                end,
                limit,
                INTERVALS[interval],
                after_status,
                as_of,
                provider_symbol,
                cursor[0] if cursor else None,
                cursor[1] if cursor else None,
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
