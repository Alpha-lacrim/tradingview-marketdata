import asyncio
import os
import socket
import subprocess
import sys
from datetime import timedelta

import asyncpg
import httpx
import pytest
from test_api_v1 import NOW, START, TOKEN, api_client, persist
from test_api_v1 import brokers as brokers
from test_integration_compatibility import snapshot
from test_native_candles import make_candle

from marketdata.config import Settings
from marketdata.domain import DataStatus, RegistryEntry, normalize
from marketdata.price_selection import select_price
from marketdata.registry import Registry

pytestmark = pytest.mark.integration


async def test_keyset_prefix_symbols_with_period_and_hyphen_are_not_skipped(store, pair):
    symbols = ["CRYPTO:ABC", "CRYPTO:ABC.DEF", "CRYPTO:ABC-DEF"]
    entries = [
        RegistryEntry(
            instrument=pair[0].model_copy(update={"canonical_symbol": symbol}),
            mappings=[pair[1].model_copy(update={"provider_symbol": f"PAGING_{index}"})],
        )
        for index, symbol in enumerate(symbols)
    ]
    await store.register(Registry(entries))
    async with api_client(store) as (_, client):
        params = [("symbols", symbol) for symbol in symbols] + [("limit", "1")]
        seen = []
        while True:
            response = await client.get("/api/v1/prices/latest", params=params)
            assert response.status_code == 200, response.text
            page = response.json()
            seen.extend(row["canonical_symbol"] for row in page["items"])
            if page["next_after"] is None:
                break
            params = [(k, v) for k, v in params if k != "after"] + [("after", page["next_after"])]
        assert seen == sorted(symbols)


async def test_default_legacy_selection_prefers_fresh_quote_and_newer_stale_candle(store, brokers):
    pair = brokers[0]
    async with store.connection() as c:
        now = await c.fetchval("SELECT statement_timestamp()")
    tick = normalize(
        pair[0],
        pair[1].model_copy(update={"realtime_status": DataStatus.REALTIME}),
        now,
        event_timestamp=now,
        last="1.123456789012345678901",
    )
    bar = make_candle(
        pair,
        now.replace(second=0, microsecond=0) - timedelta(minutes=1),
        received_timestamp=now,
        volume=None,
    )
    await store.write_candles([bar], [])
    await store.write([tick])
    async with api_client(store) as (_, client):
        row = (await client.get("/prices/latest/" + pair[0].canonical_symbol)).json()[0]
        assert row["price_type"] == "quote" and row["last"] == str(tick.last)
        # Legacy freshness intentionally still includes collector snapshot health.
        assert row["stale"]
    later = make_candle(
        pair,
        now.replace(second=0, microsecond=0),
        received_timestamp=now + timedelta(minutes=2),
        volume=None,
    )
    assert select_price(tick, later, now + timedelta(minutes=2), Settings(_env_file=None)) == later


async def test_bounded_pages_on_bulk_routes_and_utc_offsets(store, brokers):
    pair = brokers[0]
    await persist(store, pair, [START])
    async with api_client(store, api_max_rows=2) as (_, client):
        for path in (
            "/api/v1/instruments",
            "/api/v1/prices/latest",
            "/api/v1/candles/latest",
            "/api/v1/coverage",
        ):
            params = {"provider": "tradingview_guest", "limit": 1}
            first = (await client.get(path, params=params)).json()
            assert len(first["items"]) == 1 and first["next_after"]
            params["after"] = first["next_after"]
            second = (await client.get(path, params=params)).json()
            assert len(second["items"]) == 1 and second["next_after"] is None
            assert first["items"][0]["instrument_id"] != second["items"][0]["instrument_id"]
            assert (await client.get(path, params={"limit": 3})).status_code == 422
        result = await client.get(
            "/api/v1/candles",
            params={
                "symbol": pair[0].canonical_symbol,
                "provider": "tradingview_guest",
                "limit": 1,
                "start": "2026-10-12T03:30:00+03:30",
                "end": "2026-10-12T03:31:00+03:30",
            },
        )
        assert result.status_code == 200 and len(result.json()["items"]) == 1
        assert result.json()["start"] == START.isoformat().replace("+00:00", "Z")


async def test_native_tick_volume_keeps_its_units_and_rollup_does_not_sum_it(store, brokers):
    pair = brokers[0]
    bars = [
        make_candle(
            pair,
            START + timedelta(minutes=i),
            received_timestamp=NOW,
            volume="7.5",
            metadata={"volume_type": "tick", "volume_additive": False},
        )
        for i in range(5)
    ]
    await store.write_candles(bars, [])
    async with api_client(store) as (_, client):
        params = {
            "symbol": pair[0].canonical_symbol,
            "provider": "tradingview_guest",
            "start": START.isoformat(),
            "end": (START + timedelta(minutes=5)).isoformat(),
        }
        native = (await client.get("/api/v1/candles", params=params)).json()["items"]
        assert all(r["volume"] == "7.5" and r["volume_type"] == "tick" for r in native)
        params["interval"] = "5m"
        derived = (await client.get("/api/v1/candles", params=params)).json()["items"][0]
        assert derived["volume"] is None and derived["volume_type"] is None


async def test_real_connection_failure_and_server_statement_timeout(store, monkeypatch):
    async with api_client(store, db_pool_min=1, db_pool_max=1, db_timeout_seconds=0.5) as (
        app,
        client,
    ):
        async with app.state.store.connection() as c:
            assert await c.fetchval("SHOW statement_timeout") == "500ms"
            with pytest.raises((asyncpg.QueryCanceledError, TimeoutError)):
                await c.fetchval("SELECT pg_sleep(1)")
            disconnected = asyncio.Event()
            c.add_termination_listener(lambda _: disconnected.set())
            pid = await c.fetchval("SELECT pg_backend_pid()")
            async with store.connection() as admin:
                assert await admin.fetchval("SELECT pg_terminate_backend($1)", pid)
            await asyncio.wait_for(disconnected.wait(), 2)
            original = app.state.store.source_records

            async def lost(*args, **kwargs):
                return await c.fetch("SELECT 1")

            monkeypatch.setattr(app.state.store, "source_records", lost)
            assert (await client.get("/api/v1/prices/latest")).status_code == 503
            monkeypatch.setattr(app.state.store, "source_records", original)
        assert (await client.get("/api/v1/prices/latest")).status_code == 200


async def test_real_http_workers_read_stored_broker_candles_without_collector(
    store, brokers, tmp_path, readonly_database_url
):
    bars = [(await persist(store, p, [START]))[0] for p in brokers]
    before = await snapshot(store)
    owned = []
    try:
        for i in range(2):
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            stop_file = tmp_path / f"api-v1-{i}.stop"
            log_path = tmp_path / f"api-v1-{i}.log"
            env = dict(
                os.environ,
                DATABASE_URL=readonly_database_url,
                API_ENVIRONMENT="production",
                API_KEY=TOKEN,
            )
            with log_path.open("w") as log:
                process = await asyncio.to_thread(
                    subprocess.Popen,
                    [
                        sys.executable,
                        "tests/process_isolation_worker.py",
                        "api",
                        "--port",
                        str(port),
                        "--stop-file",
                        str(stop_file),
                    ],
                    env=env,
                    stdout=log,
                    stderr=log,
                    creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
                )
            owned.append((process, stop_file, log_path, f"http://127.0.0.1:{port}"))
        async with httpx.AsyncClient(
            timeout=2, headers={"Authorization": "Bearer " + TOKEN}
        ) as client:
            for process, _, log_path, url in owned:
                async with asyncio.timeout(15):
                    while True:  # noqa: ASYNC110 - polls a test-owned HTTP process
                        assert process.poll() is None, log_path.read_text()
                        try:
                            if (await client.get(url + "/health")).status_code == 200:
                                break
                        except httpx.HTTPError:
                            pass
                        await asyncio.sleep(0.05)
                assert (await client.get(url + "/api/v1/health")).json()["collector"] == "unknown"
                latest = (
                    await client.get(
                        url + "/api/v1/candles/latest", params={"provider": "tradingview_guest"}
                    )
                ).json()["items"]
                assert {r["provider_symbol"] for r in latest} == {"FX:EURUSD", "OANDA:EURUSD"}
                for pair, bar in zip(brokers, bars, strict=True):
                    response = await client.get(
                        url + "/api/v1/candles",
                        params={
                            "symbol": pair[0].canonical_symbol,
                            "provider": pair[1].provider,
                            "start": START.isoformat(),
                            "end": (START + timedelta(minutes=1)).isoformat(),
                        },
                    )
                    assert response.status_code == 200, response.text
                    row = response.json()["items"][0]
                    assert row["close"] == str(bar.close) and row["volume"] is None
                    assert row["provider_symbol"] == bar.provider_symbol
        assert await snapshot(store) == before
    finally:
        for process, stop_file, _log, _url in owned:
            stop_file.touch()
            try:
                await asyncio.to_thread(process.wait, 10)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait, 5)
            assert process.returncode == 0
