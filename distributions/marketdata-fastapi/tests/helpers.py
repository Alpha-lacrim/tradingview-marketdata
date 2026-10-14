from datetime import UTC, datetime, timedelta

from marketdata.candles import MarketCandle

NOW = datetime(2026, 10, 7, 10, 30, 2, tzinfo=UTC)


def make_candle(pair, start=NOW - timedelta(minutes=1, seconds=2), **changes):
    data = dict(
        instrument_id=pair[0].id,
        canonical_symbol=pair[0].canonical_symbol,
        provider=pair[1].provider,
        provider_symbol=pair[1].provider_symbol,
        bucket_start=start,
        bucket_end=start + timedelta(minutes=1),
        open="100.123456789012345678901",
        high="102",
        low="99",
        close="101",
        volume="10",
        realtime_status="realtime",
        source_status="streaming",
        received_timestamp=NOW + timedelta(days=1),
        metadata={"volume_type": "base", "volume_additive": True},
    )
    data.update(changes)
    return MarketCandle(**data)


TABLES = (
    "instruments",
    "provider_instruments",
    "market_ticks",
    "latest_prices",
    "market_candles",
    "latest_candles",
    "candle_coverage",
    "candle_revisions",
)


async def snapshot(store):
    async with store.connection() as connection:
        return {
            table: tuple(
                await connection.fetchrow(
                    f"SELECT count(*), md5(coalesce(string_agg(row_value, '' ORDER BY "
                    f"row_value), '')) FROM (SELECT to_jsonb(t)::text row_value FROM "
                    f"{table} t) rows"
                )
            )
            for table in TABLES
        }


def forbidden(*args, **kwargs):
    raise AssertionError("API invoked collector responsibilities")
