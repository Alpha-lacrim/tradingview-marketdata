"""0006 preservation and actual SELECT-only cross-process socket permissions."""

import asyncio
import os
import subprocess
import sys
from datetime import timedelta
from decimal import Decimal
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
from helpers import snapshot
from test_market_stream import HEADERS, ORIGIN, bar, receive, server, subscribe, ws_url
from websockets.asyncio.client import connect

from marketdata.config import Settings
from marketdata.events import event_page
from marketdata.persistence import Store

pytestmark = pytest.mark.integration


async def test_select_only_role_receives_journal_events_without_write_permissions(store, pair):
    role = "batch3_read_" + uuid4().hex
    tables = (
        "instruments,provider_instruments,market_ticks,latest_prices,market_candles,"
        "latest_candles,candle_coverage,candle_revisions,collector_status,alembic_version,"
        "marketdata_event_clock,marketdata_events"
    )
    async with store.connection() as admin:
        await admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'disposable_test'")
        await admin.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        await admin.execute(f"GRANT SELECT ON {tables} TO {role}")
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    url = urlunsplit(
        parsed._replace(netloc=f"{role}:disposable_test@{parsed.hostname}:{parsed.port}")
    )
    reader = Store(Settings(_env_file=None, database_url=url), read_only=True)
    try:
        await reader.connect()
        async with (
            server(reader) as (_, base),
            connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws,
        ):
            await subscribe(ws, [pair[0].canonical_symbol])
            await store.write_candles([bar(pair)], [])
            assert (await receive(ws, "candle_completed"))["persisted"]
        async with reader.connection() as c:
            await c.execute("SET default_transaction_read_only=off")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await c.execute("UPDATE marketdata_event_clock SET head=head")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await c.execute("DELETE FROM marketdata_events")
    finally:
        await reader.close()
        async with store.connection() as admin:
            await admin.execute(f"REVOKE SELECT ON {tables} FROM {role}")
            await admin.execute(f"REVOKE USAGE ON SCHEMA public FROM {role}")
            await admin.execute(f"DROP ROLE {role}")


async def test_0006_roundtrip_preserves_market_and_health_rows_with_new_epoch(store, pair):
    candle = bar(pair)
    await store.write_candles([candle], [])
    await store.write_candles(
        [
            candle.model_copy(
                update={
                    "close": Decimal("100.5"),
                    "received_timestamp": candle.received_timestamp + timedelta(seconds=1),
                }
            )
        ],
        [],
    )
    await store.publish_collector_status(uuid4(), False, True, 0, [])
    market = await snapshot(store)
    previous = await event_page(store, None, 100)
    async with store.connection() as c:
        status = await c.fetch("SELECT * FROM collector_status")
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

    try:
        await migrate("downgrade", "0005")
        assert await snapshot(store) == market
        async with store.connection() as c:
            assert await c.fetch("SELECT * FROM collector_status") == status
            assert await c.fetchval("SELECT to_regclass('marketdata_events')") is None
    finally:
        await migrate("upgrade", "head")
    assert await snapshot(store) == market
    current = await event_page(store, None, 100)
    assert current["epoch"] != previous["epoch"]
    assert current["rows"] == []
