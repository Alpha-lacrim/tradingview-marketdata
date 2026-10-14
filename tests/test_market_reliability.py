"""Bounded delivery, 100-instrument simulated read-back and real mock collector restart."""

import asyncio
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, bar, receive, server, settings, subscribe, ws_url
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from marketdata.domain import RegistryEntry, normalize
from marketdata.events import decode_watermark, event_page
from marketdata.market_stream import MarketHub
from marketdata.observability import Metrics
from marketdata.registry import Registry
from marketdata.ws_models import MarketCommand

pytestmark = pytest.mark.integration


async def test_100_instruments_snapshot_and_committed_candles_match_rest(store, pair):
    registry = Registry(
        [
            RegistryEntry(
                instrument=pair[0].model_copy(update={"canonical_symbol": f"CRYPTO:WS{i:03}"}),
                mappings=[pair[1].model_copy(update={"provider_symbol": f"WS{i:03}"})],
            )
            for i in range(100)
        ]
    )
    await store.register(registry)
    pairs = registry.subscriptions("mock")
    symbols = [i.canonical_symbol for i, _ in pairs]
    async with (
        server(store) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        ack, snapshots = await subscribe(ws, symbols)
        assert len(ack["symbols"]) == len(snapshots) == 100
        candles = [bar(p) for p in pairs]
        await store.write_candles(candles, [])
        delivered = [await receive(ws, "candle_completed") for _ in range(100)]
        assert {e["data"]["canonical_symbol"] for e in delivered} == set(symbols)
        async with httpx.AsyncClient(base_url=url, headers=HEADERS) as client:
            latest = (
                await client.get(
                    "/api/v1/candles/latest",
                    params=[("symbols", s) for s in symbols] + [("limit", "100")],
                )
            ).json()
            by_symbol = {r["canonical_symbol"]: r["candle"] for r in latest["items"]}
            assert all(by_symbol[e["data"]["canonical_symbol"]] == e["data"] for e in delivered)


async def test_client_admission_closes_1013_without_leaking_a_subscription(store):
    async with (
        server(store, websocket_max_clients=1) as (app, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS),
    ):
        async with connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as second:
            with pytest.raises(ConnectionClosed) as closed:
                await second.recv()
            assert closed.value.rcvd.code == 1013
            assert len(app.state.market_hub.clients) == 1


async def test_worker_cursor_expiry_fails_explicitly_and_keeps_history(store, pair):
    hub = MarketHub(store, settings(store), Metrics())
    client = hub.register()
    await hub.command(client, MarketCommand(action="subscribe", symbols=[pair[0].canonical_symbol]))
    baseline = hub.cursor
    store.settings.marketdata_event_retention_count = 1
    candles = [bar(pair, i) for i in (5, 4, 3)]
    await store.write_candles(candles, [])
    hub.start()
    try:
        await asyncio.wait_for(client.failed.wait(), 3)
        assert "cursor expired" in client.close_reason
    finally:
        await hub.stop()
    async with server(store) as (_, url), httpx.AsyncClient(base_url=url, headers=HEADERS) as rest:
        response = await rest.get("/api/v1/marketdata/changes", params={"after": baseline})
        assert response.status_code == 409 and response.json()["error"]["code"] == "resync_required"
        assert (
            len(
                (
                    await rest.get(
                        "/api/v1/candles",
                        params=dict(
                            symbol=pair[0].canonical_symbol,
                            provider=pair[1].provider,
                            start=candles[0].bucket_start.isoformat(),
                            end=candles[-1].bucket_end.isoformat(),
                        ),
                    )
                ).json()["items"]
            )
            == 3
        )


async def test_fresh_observation_becomes_stale_without_new_quote(store, pair, monkeypatch):
    now = datetime.now(UTC)
    await store.write([normalize(*pair, now, event_timestamp=now, last="100")])
    hub = MarketHub(store, settings(store), Metrics())
    client = hub.register()
    await hub.command(client, MarketCommand(action="subscribe", symbols=[pair[0].canonical_symbol]))
    while not client.queue.empty():
        client.queue.get_nowait()
    monkeypatch.setattr("marketdata.market_stream.utc_now", lambda: now + timedelta(seconds=31))
    await hub.statuses()
    status = json.loads(client.queue.get_nowait())
    assert status["type"] == "source_status" and status["items"][0]["freshness"] == "stale"
    assert status["items"][0]["quote_freshness"] == "stale"
    assert not status["items"][0]["realtime_quote_available"]


async def test_real_collector_restart_resumes_committed_quotes_on_same_api(store, pair, tmp_path):
    owned = []

    async def start():
        stop_file = tmp_path / f"collector-{len(owned)}.stop"
        log_path = tmp_path / f"collector-{len(owned)}.log"
        with log_path.open("w") as log:
            process = await asyncio.to_thread(
                subprocess.Popen,
                [
                    sys.executable,
                    "tests/process_isolation_worker.py",
                    "collector",
                    "--stop-file",
                    str(stop_file),
                ],
                env=dict(os.environ, DATABASE_URL=store.settings.database_url.get_secret_value()),
                stdout=log,
                stderr=log,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
        owned.append((process, stop_file, log_path))
        return owned[-1]

    async def stop(worker):
        process, stop_file, log_path = worker
        stop_file.touch()
        await asyncio.to_thread(process.wait, 10)
        assert process.returncode == 0, log_path.read_text()

    try:
        async with (
            server(store) as (_, url),
            connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
        ):
            await subscribe(ws, [pair[0].canonical_symbol])
            collector = await start()
            first = await receive(ws, "price_update")
            assert first["data"]["realtime_status"] == "simulated"
            await stop(collector)
            _, stopped_position = decode_watermark((await event_page(store, None, 1))["cursor"])
            async with httpx.AsyncClient(base_url=url, headers=HEADERS) as rest:
                assert (await rest.get(f"/ticks/{pair[0].canonical_symbol}")).json()["items"]
            collector = await start()
            async with asyncio.timeout(5):
                while True:
                    update = await receive(ws, "price_update")
                    if int(update["event_id"]) > stopped_position:
                        break
            assert int(update["event_id"]) > int(first["event_id"])
            await stop(collector)
    finally:
        for process, stop_file, _ in owned:
            if process.poll() is None:
                stop_file.touch()
                try:
                    await asyncio.to_thread(process.wait, 10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait, 5)
                    pytest.fail("test-owned collector failed graceful shutdown")
