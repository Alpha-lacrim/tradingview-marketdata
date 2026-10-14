"""Standalone packaging and a sample writer independent of all upstream adapters."""

import subprocess
import sys
from datetime import timedelta
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, receive, server, subscribe, ws_url
from websockets.asyncio.client import connect

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.persistence import Store
from marketdata.sample_data import (
    prepare_reader,
    publish_quotes,
    sample_registry,
    seed_history,
    validate_target,
)


def test_api_import_and_factory_without_collector_or_optional_dependencies():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import importlib.abc
import sys
class Forbidden(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in ('marketdata.collector', 'marketdata.ingestion',
                        'marketdata.candle_ingestion') or fullname.split('.')[0] in (
                'httpx', 'alembic', 'sqlalchemy', 'psycopg', 'databento') or (
                fullname.startswith('marketdata.providers.') and
                fullname != 'marketdata.providers.base'):
            raise AssertionError('API imports optional transport/tooling: ' + fullname)
sys.meta_path.insert(0, Forbidden())
from marketdata.api import create_app
from marketdata.config import Settings
app = create_app(Settings(_env_file=None, registry_file='absent.json'))
assert app.state.service is None
assert '/api/v1/candles' in app.openapi()['paths']
""",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "url,environment",
    [
        ("postgresql://unused@localhost/marketdata", "test"),
        ("postgresql://unused@localhost/demo_test", "production"),
    ],
)
def test_sample_writer_rejects_non_disposable_targets_before_connect(url, environment):
    with pytest.raises(ValueError, match="disposable"):
        validate_target(
            Settings(
                _env_file=None, database_url=url, api_environment=environment, api_key="x" * 32
            )
        )


@pytest.mark.integration
async def test_sample_history_readonly_api_and_live_replay_without_collector(store, monkeypatch):
    import marketdata.collector

    def forbidden(*args, **kwargs):
        raise AssertionError("standalone API started a provider")

    monkeypatch.setattr(marketdata.collector, "build_providers", forbidden)
    await prepare_reader(store)
    # Include previous UTC-day buckets too, to exercise every derived interval.
    assert await seed_history(store, 1440) == 2880
    assert await publish_quotes(store, 0) == 2
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    url = urlunsplit(
        parsed._replace(netloc=f"sample_api:sample_read_only@{parsed.hostname}:{parsed.port}")
    )
    reader = Store(Settings(_env_file=None, database_url=url), read_only=True)
    symbol = sample_registry().entries[0].instrument.canonical_symbol
    try:
        await reader.connect()
        async with reader.connection() as c:
            await c.execute("SET default_transaction_read_only=off")
            for statement in (
                "UPDATE instruments SET active=active",
                "DELETE FROM marketdata_events",
                "CREATE TABLE public.forbidden_test(id int)",
                "CREATE TEMP TABLE forbidden_test(id int)",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await c.execute(statement)
        async with server(reader) as (app, base):
            assert app.state.service is None
            async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
                assert (await client.get("/health")).status_code == 200
                health = (await client.get("/api/v1/health")).json()
                assert health["collector"] != "running"
                sources = (await client.get("/api/v1/instruments")).json()["items"]
                assert len([s for s in sources if s["provider"] == "sample"]) == 2
                for interval in ("1m", "5m", "15m", "1h", "1d"):
                    history = await client.get(
                        "/api/v1/candles",
                        params={"symbol": symbol, "provider": "sample", "interval": interval},
                    )
                    assert history.status_code == 200, history.text
                    assert history.json()["items"]
                    assert {c["realtime_status"] for c in history.json()["items"]} == {"simulated"}
                prices = (
                    await client.get("/api/v1/prices/latest", params={"symbols": symbol})
                ).json()["items"]
                assert prices[0]["price_type"] == "quote"
                assert not prices[0]["realtime_quote_available"]
                assert prices[0]["freshness"] == "fresh"
                with monkeypatch.context() as clock:
                    later = utc_now() + timedelta(minutes=3)
                    clock.setattr("marketdata.api_v1.utc_now", lambda: later)
                    aged = (
                        await client.get("/api/v1/prices/latest", params={"symbols": symbol})
                    ).json()["items"][0]
                    assert aged["freshness"] == "stale"
                assert (await client.get("/api/v1/coverage", params={"symbols": symbol})).json()[
                    "items"
                ]
                async with connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws:
                    ack, snapshots = await subscribe(ws, [symbol])
                    assert snapshots[0]["realtime_status"] == "simulated"
                    await publish_quotes(store, 1)
                    live = await receive(ws, "price_update")
                    assert live["data"]["last"] == "60000.5"
                async with connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws:
                    await subscribe(ws, [symbol])
                    replay = (
                        await client.get(
                            "/api/v1/marketdata/changes", params={"after": ack["snapshot_cursor"]}
                        )
                    ).json()
                    assert live["event_id"] in {e["event_id"] for e in replay["items"]}
    finally:
        await reader.close()
        async with store.connection() as c:
            await c.execute("DROP OWNED BY sample_api")
            await c.execute("DROP ROLE sample_api")
