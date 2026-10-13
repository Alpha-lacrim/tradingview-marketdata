"""Local fixtures prove broker separation and additive 0004/0005 compatibility."""

import asyncio
import os
import subprocess
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from test_native_candles import NOW, make_candle
from test_process_separation import forbidden

from marketdata.api import create_app
from marketdata.config import Settings
from marketdata.domain import normalize
from marketdata.registry import Registry

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


async def seed_brokers(store):
    registry = Registry.load(Path("config/instruments.tradingview-business-proposal.json"))
    await store.register(registry, candle_providers=["tradingview_guest"])
    # Preserve the original 17 as well as the 99 proposal; this is a test database only.
    await store.register(
        Registry.load(Path("config/instruments.tradingview.json")),
        candle_providers=["tradingview_guest"],
    )
    pairs = [registry.by_provider["tradingview_guest", s] for s in ("OANDA:EURUSD", "FX:EURUSD")]
    start = NOW.replace(second=0) - timedelta(minutes=3)
    candles = [
        make_candle(pairs[0], start, close="101.123456789012345678901"),
        make_candle(
            pairs[1],
            start + timedelta(minutes=1),
            close="100.987654321098765432109",
            volume=None,
            metadata={"source_volume_raw": "20.123", "volume_additive": False},
        ),
    ]
    # Fixed legacy activation starts plus different missing minutes, never erased on upgrade.
    async with store.connection() as connection:
        for instrument, mapping in pairs:
            await connection.execute(
                "UPDATE candle_coverage SET coverage_start=$1,coverage_end=$1 "
                "WHERE instrument_id=$2 AND provider=$3",
                start,
                instrument.id,
                mapping.provider,
            )
    plans = [
        (
            i.id,
            m.provider,
            start,
            start + timedelta(minutes=3),
            {"session": "24x7", "timezone": "Etc/UTC"},
        )
        for i, m in pairs
    ]
    await store.write_candles(candles, plans)
    # Persist correction provenance and independent quote/latest identities.
    corrected = candles[0].model_copy(
        update={
            "close": candles[0].close + 1,
            "high": Decimal("103"),
            "received_timestamp": candles[0].received_timestamp + timedelta(seconds=1),
        }
    )
    await store.write_candles([corrected], plans)
    await store.write(
        [
            normalize(*pair, NOW, event_timestamp=NOW, last=str(100 + index), volume=None)
            for index, pair in enumerate(pairs)
        ]
    )
    return pairs, [corrected, candles[1]], plans


@pytest.mark.integration
async def test_0004_upgrade_and_status_only_rollback_preserve_all_market_rows(store, monkeypatch):
    pairs, candles, plans = await seed_brokers(store)
    before = await snapshot(store)
    assert all(count > 0 for count, _ in before.values())
    env = dict(os.environ, DATABASE_URL=store.settings.database_url.get_secret_value())

    async def migrate(*arguments):
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, "-m", "alembic", *arguments],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    monkeypatch.setattr("marketdata.collector.build_providers", forbidden)
    try:
        await migrate("downgrade", "0004")
        async with store.connection() as connection:
            assert await connection.fetchval("SELECT version_num FROM alembic_version") == "0004"
            assert await connection.fetchval("SELECT to_regclass('collector_status')") is None
        assert await snapshot(store) == before
        app = create_app(Settings(_env_file=None, database_url=store.settings.database_url))
        with pytest.raises(RuntimeError, match="migrations are not at head"):
            async with app.router.lifespan_context(app):
                pytest.fail("unmigrated API started")
        assert app.state.store.pool is None
    finally:
        await migrate("upgrade", "head")
    assert await snapshot(store) == before
    app = create_app(Settings(_env_file=None, database_url=store.settings.database_url))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client,
    ):
        assert (await client.get("/health")).json()["ingestion_state"] == "unknown"
        for pair, candle in zip(pairs, candles, strict=True):
            response = await client.get(
                f"/candles/{pair[0].canonical_symbol}",
                params={
                    "provider": "tradingview_guest",
                    "start": plans[0][2].isoformat(),
                    "end": plans[0][3].isoformat(),
                },
            )
            assert response.status_code == 200, response.text
            assert response.json()["items"][0]["close"] == str(candle.close)
            assert response.json()["items"][0]["provider_symbol"] == pair[1].provider_symbol
    assert await snapshot(store) == before


@pytest.mark.integration
async def test_broker_api_reads_during_independent_recovery_keep_quotes_and_gaps_separate(
    store, monkeypatch
):
    pairs, candles, plans = await seed_brokers(store)
    before = [await store.candle_progress(i.id, m.provider) for i, m in pairs]
    assert [p["missing_count"] for p in before] == [2, 2]
    monkeypatch.setattr("marketdata.collector.build_providers", forbidden)
    app = create_app(Settings(_env_file=None, database_url=store.settings.database_url))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client,
    ):
        recovery = candles[0].model_copy(
            update={
                "bucket_start": candles[0].bucket_start + timedelta(minutes=2),
                "bucket_end": candles[0].bucket_end + timedelta(minutes=2),
            }
        )
        written, *responses = await asyncio.gather(
            store.write_candles([recovery], plans),
            *(client.get(f"/prices/latest/{i.canonical_symbol}") for i, _ in pairs),
        )
        assert written == 1
        for index, response in enumerate(responses):
            assert response.status_code == 200, response.text
            latest = response.json()[0]
            assert latest["provider_symbol"] == pairs[index][1].provider_symbol
            assert latest["last"] == str(candles[index].close) and latest["stale"]
            assert latest["price_basis"] == "candle_close"
            # The existing latest route prefers native candles when both are stored.
            # Quote history must still remain independently queryable for each broker.
            ticks = await client.get(
                f"/ticks/{pairs[index][0].canonical_symbol}",
                params={
                    "provider": "tradingview_guest",
                    "start": (NOW - timedelta(minutes=1)).isoformat(),
                    "end": (NOW + timedelta(minutes=1)).isoformat(),
                },
            )
            assert ticks.status_code == 200, ticks.text
            assert ticks.json()["items"][0]["last"] == str(100 + index)
            assert ticks.json()["items"][0]["provider_symbol"] == pairs[index][1].provider_symbol
        for pair in pairs:
            response = await client.get(
                f"/candles/{pair[0].canonical_symbol}",
                params={
                    "provider": "tradingview_guest",
                    "start": plans[0][2].isoformat(),
                    "end": plans[0][3].isoformat(),
                },
            )
            assert response.status_code == 200, response.text
            assert all(
                c["provider_symbol"] == pair[1].provider_symbol for c in response.json()["items"]
            )
            if pair == pairs[1]:
                assert response.json()["items"][0]["volume"] is None
                assert response.json()["items"][0]["metadata"]["source_volume_raw"] == "20.123"
    after = [await store.candle_progress(i.id, m.provider) for i, m in pairs]
    assert [p["missing_count"] for p in after] == [1, 2]
    assert [p["coverage_start"] for p in after] == [p["coverage_start"] for p in before]
