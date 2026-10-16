"""Fresh installation and populated 0006 upgrade on disposable databases only."""

import asyncio
import os
import subprocess
import sys
from datetime import timedelta
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
from test_integration_compatibility import snapshot
from test_native_candles import make_candle

from marketdata.domain import normalize, utc_now

pytestmark = pytest.mark.integration


async def migrate(url, *arguments):
    assert urlsplit(url).path.endswith("_test")
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-m", "alembic", *arguments],
        env=dict(os.environ, DATABASE_URL=url),
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr


async def test_populated_0006_upgrade_preserves_market_events_and_seeds_noop_heads(store, pair):
    bar = make_candle(pair, realtime_status="simulated")
    await store.write_candles([bar], [])
    later = bar.model_copy(
        update={"received_timestamp": bar.received_timestamp + timedelta(seconds=3)}
    )
    await store.write_candles([later], [])
    await store.write([normalize(*pair, utc_now(), last="100.1234567890123456789")])
    before = await snapshot(store)
    async with store.connection() as c:
        events = await c.fetch("SELECT * FROM marketdata_events ORDER BY id")
        clock = await c.fetchrow("SELECT * FROM marketdata_event_clock")
    url = store.settings.database_url.get_secret_value()
    try:
        await migrate(url, "downgrade", "0006")
        assert await snapshot(store) == before
        async with store.connection() as c:
            assert await c.fetchval("SELECT version_num FROM alembic_version") == "0006"
            assert await c.fetchval("SELECT to_regclass('ingest_batches')") is None
            assert await c.fetch("SELECT * FROM marketdata_events ORDER BY id") == events
        await migrate(url, "upgrade", "head")
        assert await snapshot(store) == before
        async with store.connection() as c:
            assert await c.fetchrow("SELECT * FROM marketdata_event_clock") == clock
            assert await c.fetch("SELECT * FROM marketdata_events ORDER BY id") == events
            assert (
                await c.fetchval("SELECT received_timestamp FROM candle_observation_heads")
                == later.received_timestamp
            )
            assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 0
        stale = bar.model_copy(
            update={
                "close": bar.open,
                "received_timestamp": bar.received_timestamp + timedelta(seconds=2),
            }
        )
        assert await store.write_candles([stale], []) == 0
        assert await snapshot(store) == before
    finally:
        await migrate(url, "upgrade", "head")


async def test_fresh_database_installs_current_head_and_optional_timescale(store):
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    database = "m1_fresh_" + uuid4().hex + "_test"
    assert (
        database.startswith("m1_fresh_")
        and database.endswith("_test")
        and database.replace("_", "").isalnum()
    )
    url = urlunsplit(parsed._replace(path="/" + database))
    async with store.connection() as admin:
        await admin.execute(f'CREATE DATABASE "{database}"')
    try:
        args = ["-x", "timescale=true"] if store.has_aggregates else []
        await migrate(url, *args, "upgrade", "head")
        c = await asyncpg.connect(url)
        try:
            assert await c.fetchval("SELECT version_num FROM alembic_version") == "0010"
            assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 0
            assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 0
            assert (
                bool(await c.fetchval("SELECT to_regclass('candles_1m_last')"))
                == store.has_aggregates
            )
            assert (
                await c.fetchval(
                    "SELECT count(*) FROM pg_tables WHERE schemaname='public' "
                    "AND tablename LIKE 'sync_%'"
                )
                == 0
            )
        finally:
            await c.close()
    finally:
        async with store.connection() as admin:
            # Timescale's scheduler can reconnect after our client closes. This UUID
            # database was created above by this test, never supplied by the operator.
            await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
