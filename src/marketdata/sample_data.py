"""Bounded, simulated DB writer for disposable developer databases; no provider transport."""

import argparse
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from pydantic_settings import SettingsConfigDict

from marketdata.candles import MarketCandle, minute_start
from marketdata.config import Settings
from marketdata.domain import (
    AssetClass,
    DataStatus,
    Instrument,
    InstrumentType,
    ProviderInstrument,
    RegistryEntry,
    normalize,
    utc_now,
)
from marketdata.persistence import Store
from marketdata.registry import Registry

READER = "sample_api"
RELATIONS = (
    "instruments,provider_instruments,market_ticks,latest_prices,market_candles,"
    "latest_candles,candle_coverage,candle_revisions,collector_status,alembic_version,"
    "marketdata_event_clock,marketdata_events"
)


class SampleSettings(Settings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", hide_input_in_errors=True)


def validate_target(settings: Settings) -> None:
    if settings.api_environment == "production" or not urlsplit(
        settings.database_url.get_secret_value()
    ).path.endswith("_test"):
        raise ValueError("sample writer requires a disposable database ending in _test")


def sample_registry() -> Registry:
    entries = []
    for symbol, asset, kind in (
        ("BTCUSD", AssetClass.CRYPTO, InstrumentType.SPOT),
        ("EURUSD", AssetClass.FOREX, InstrumentType.OTC),
    ):
        entries.append(
            RegistryEntry(
                instrument=Instrument(
                    canonical_symbol="DEMO:" + symbol,
                    display_name=symbol + " simulated developer sample",
                    asset_class=asset,
                    instrument_type=kind,
                    exchange="DEMO",
                    currency="USD",
                    metadata={"simulated": True},
                ),
                mappings=[
                    ProviderInstrument(
                        provider="sample",
                        provider_symbol=symbol,
                        realtime_status=DataStatus.SIMULATED,
                        metadata={"simulated": True},
                    )
                ],
            )
        )
    return Registry(entries)


async def prepare_reader(store: Store) -> None:
    """Explicit test-only role setup, after migrations; not part of API startup."""
    validate_target(store.settings)
    async with store.connection() as c, c.transaction():
        database = await c.fetchval("SELECT current_database()")
        if not database.endswith("_test") or not database.replace("_", "").isalnum():
            raise ValueError("unsafe disposable database name")
        await c.execute(f"REVOKE CREATE, TEMPORARY ON DATABASE {database} FROM PUBLIC")
        await c.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        if not await c.fetchval("SELECT 1 FROM pg_roles WHERE rolname=$1", READER):
            # Public disposable fixture password, never a production credential.
            await c.execute(
                f"CREATE ROLE {READER} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOREPLICATION PASSWORD 'sample_read_only'"
            )
        await c.execute(f"GRANT CONNECT ON DATABASE {database} TO {READER}")
        await c.execute(f"GRANT USAGE ON SCHEMA public TO {READER}")
        await c.execute(f"GRANT SELECT ON {RELATIONS} TO {READER}")
        # Optional legacy tick aggregates on TimescaleDB, scoped to existing views.
        for interval in ("1s", "1m", "5m", "15m", "1h", "1d"):
            for basis in ("last", "mid"):
                relation = f"candles_{interval}_{basis}"
                if await c.fetchval("SELECT to_regclass($1)", "public." + relation):
                    await c.execute(f"GRANT SELECT ON public.{relation} TO {READER}")
        await c.execute(f"ALTER ROLE {READER} SET default_transaction_read_only = on")


async def seed_history(store: Store, minutes: int = 120, *, now: datetime | None = None) -> int:
    validate_target(store.settings)
    if not 1 <= minutes <= 1440:
        raise ValueError("sample history must contain 1..1440 minutes")
    now = now or utc_now()
    end = minute_start(now)
    start = end - timedelta(minutes=minutes)
    registry = sample_registry()
    # Coverage starts at the first supplied historical bar, not future activation.
    await store.register(registry)
    bars = []
    coverage: list[
        tuple[UUID, str, datetime, datetime] | tuple[UUID, str, datetime, datetime, dict[str, Any]]
    ] = []
    for index, (instrument, mapping) in enumerate(registry.subscriptions("sample")):
        base = Decimal("60000") if index == 0 else Decimal("1.08")
        step = Decimal("0.5") if index == 0 else Decimal("0.0001")
        for n in range(minutes):
            bucket = start + timedelta(minutes=n)
            price = base + step * (n % 20)
            bars.append(
                MarketCandle(
                    instrument_id=instrument.id,
                    canonical_symbol=instrument.canonical_symbol,
                    provider=mapping.provider,
                    provider_symbol=mapping.provider_symbol,
                    bucket_start=bucket,
                    bucket_end=bucket + timedelta(minutes=1),
                    open=price,
                    high=price + step * 2,
                    low=price - step,
                    close=price + step,
                    volume=None,
                    realtime_status=DataStatus.SIMULATED,
                    source_status="streaming",
                    received_timestamp=now,
                    metadata={"simulated": True, "volume_type": "unknown"},
                )
            )
        coverage.append((instrument.id, mapping.provider, start, end))
    return await store.write_candles(bars, coverage)


async def publish_quotes(store: Store, sequence: int) -> int:
    validate_target(store.settings)
    now = utc_now()
    ticks = []
    for index, pair in enumerate(sample_registry().subscriptions("sample")):
        base = Decimal("60000") if index == 0 else Decimal("1.08")
        step = Decimal("0.5") if index == 0 else Decimal("0.0001")
        price = base + step * (sequence % 20)
        ticks.append(
            normalize(
                *pair,
                now,
                event_timestamp=now,
                bid=price - step,
                ask=price + step,
                last=price,
                metadata={"simulated": True},
            )
        )
    return await store.write(ticks)


async def run(minutes: int, events: int, interval_seconds: float, reader: bool) -> None:
    # Ignore repository .env: this utility requires an explicitly supplied disposable DSN.
    settings = SampleSettings()
    validate_target(settings)
    store = Store(settings)
    try:
        await store.connect()
        if reader:
            await prepare_reader(store)
        count = await seed_history(store, minutes)
        await publish_quotes(store, 0)
        print(f"Committed {count} simulated candles and two sample quotes", flush=True)
        for sequence in range(1, events + 1):
            await asyncio.sleep(interval_seconds)
            await publish_quotes(store, sequence)
    finally:
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=int, default=120)
    parser.add_argument("--events", type=int, default=0, help="additional bounded quote batches")
    parser.add_argument("--interval-seconds", type=float, default=2)
    parser.add_argument("--prepare-reader", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.minutes <= 1440 or not 0 <= args.events <= 10000:
        parser.error("minutes must be 1..1440 and events 0..10000")
    if not 0.1 <= args.interval_seconds <= 60:
        parser.error("interval-seconds must be 0.1..60")
    asyncio.run(run(args.minutes, args.events, args.interval_seconds, args.prepare_reader))


if __name__ == "__main__":
    main()
