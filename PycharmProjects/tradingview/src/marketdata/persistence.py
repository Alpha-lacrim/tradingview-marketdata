from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg

from marketdata.config import Settings
from marketdata.domain import Tick
from marketdata.registry import Registry

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
        self.pool: asyncpg.Pool | None = None
        self.has_aggregates = False

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
            if version != "0002":
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
    ) -> list[Tick]:
        async with self.connection() as c:
            rows = await c.fetch(
                """SELECT l.payload FROM latest_prices l
                JOIN instruments i ON i.id=l.instrument_id
                WHERE ($1::text IS NULL OR i.canonical_symbol=$1)
                AND ($2::text IS NULL OR l.provider=$2)
                AND (i.canonical_symbol || '/' || l.provider) > $4
                ORDER BY i.canonical_symbol,l.provider LIMIT $3""",
                symbol,
                provider,
                limit,
                after,
            )
        return [Tick.model_validate_json(r["payload"]) for r in rows]

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
