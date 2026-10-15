"""Deterministic TradingView fixtures; no live upstream connections."""

import asyncio
import hashlib
import importlib.util
import json
import os
import socket
import subprocess
import sys
import zipfile
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, TOKEN, receive, subscribe, ws_url
from test_native_candles import Replay, make_candle
from websockets.asyncio.client import connect
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from marketdata.collector import collector_lifespan
from marketdata.config import Settings
from marketdata.domain import normalize, utc_now
from marketdata.observability import Metrics
from marketdata.providers.candles import CandleProvider
from marketdata.providers.factory import build_providers
from marketdata.providers.tradingview import FrameDecoder, TradingViewGuestProvider, encode_message
from marketdata.registry import Registry

RELEASE_SHA256 = "3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a"


def observer_module():
    spec = importlib.util.spec_from_file_location(
        "dual_observer", "scripts/observe_dual_ingestion.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_observer_separates_source_time_delivery_and_missing_timestamps():
    events = [
        {
            "type": "price_update",
            "recorded_at": "2026-10-14T10:00:01.350Z",
            "client_received_at": "2026-10-14T10:00:01.450Z",
            "data": {
                "canonical_symbol": "CRYPTO:BTCUSDT",
                "source_timestamp": "2026-10-14T10:00:01Z",
                "received_timestamp": "2026-10-14T10:00:01.300Z",
            },
        },
        {
            "type": "price_update",
            "recorded_at": "2026-10-14T10:00:02.500Z",
            "client_received_at": "2026-10-14T10:00:02.600Z",
            "data": {
                "canonical_symbol": "CRYPTO:BTCUSDT",
                "source_timestamp": None,
                "received_timestamp": "2026-10-14T10:00:02.400Z",
            },
        },
    ]
    row = observer_module().summarize({"CRYPTO:BTCUSDT": "BINANCE:BTCUSDT"}, events, 10)[
        "CRYPTO:BTCUSDT"
    ]
    assert (
        row["persisted_quote_updates"] == 2 and row["persisted_delivery_updates_per_second"] == 0.2
    )
    assert row["receipt_intervals_seconds"] == [1.1]
    assert row["source_to_journal_append_seconds"] == [0.35, None]
    assert row["journal_append_to_client_seconds"] == [0.1, 0.1]


@pytest.mark.integration
async def test_observer_never_qualifies_simulated_tradingview_prices(store, tmp_path):
    from test_market_stream import server

    registry = Registry.load(Path("config/instruments.tradingview.json"))
    registry = Registry(registry.entries[:1])
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    output = tmp_path / "simulated-report.json"
    async with server(store) as (app, base):
        task = asyncio.create_task(
            observer_module().observe(base, TOKEN, ORIGIN, registry, 1, output)
        )

        async def subscribed():
            return any(pair[0].canonical_symbol in c.symbols for c in app.state.market_hub.clients)

        try:
            await eventually(subscribed)
            await store.write_quotes(
                [
                    normalize(
                        pair[0],
                        pair[1].model_copy(update={"realtime_status": "simulated"}),
                        utc_now(),
                        last="123",
                    )
                ]
            )
            report = await asyncio.wait_for(task, 5)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    assert not report["passed"] and report["failure_category"] == "AssertionError"
    assert TOKEN not in await asyncio.to_thread(output.read_text)


@pytest.mark.parametrize("filename", ["100", "business-proposal"])
async def test_dual_factory_keeps_both_registries_and_independent_health(filename):
    registry = Registry.load(Path(f"config/instruments.tradingview-{filename}.json"))
    settings = Settings(
        _env_file=None,
        tradingview_storage_mode="dual",
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=4,
        candle_pool_concurrency=8,
    )
    candles, quotes = await build_providers(settings, registry, Metrics())
    assert isinstance(candles, CandleProvider)
    assert isinstance(quotes, TradingViewGuestProvider)
    assert (
        candles.subscriptions == quotes.subscriptions == registry.subscriptions("tradingview_guest")
    )
    assert candles.health.provider != quotes.health.provider
    assert quotes.health.storage_mode == "quotes" and not quotes.persist_tick_history
    assert all(mapping.provider == "tradingview_guest" for _, mapping in quotes.subscriptions)
    assert quotes.socket is None and not candles.opened


async def test_dual_distribution_and_total_connection_bound():
    registry = Registry.load(Path("config/instruments.tradingview-256.json"))
    symbol = registry.subscriptions("tradingview_guest")[-1][1].provider_symbol
    settings = Settings(
        _env_file=None,
        tradingview_storage_mode="dual",
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=3,
        candle_pool_concurrency=6,
        candle_pool_max_instruments=256,
        tradingview_symbol_stale_seconds={symbol: 120},
    )
    providers = await build_providers(settings, registry, Metrics())
    assert [len(p.subscriptions) for p in providers] == [256, 100, 100, 56]
    assert sum(symbol in p.symbol_stale_seconds for p in providers[1:]) == 1
    assert len({p.name for p in providers}) == 4
    with pytest.raises(ValueError, match="six total"):
        await build_providers(
            settings.model_copy(update={"candle_pool_connections": 4}), registry, Metrics()
        )
    with pytest.raises(ValueError, match="unconfigured"):
        await build_providers(
            settings.model_copy(update={"tradingview_symbol_stale_seconds": {"NO:SOURCE": 30}}),
            registry,
            Metrics(),
        )


async def test_quote_429_honors_retry_after_without_access_bypass(monkeypatch):
    provider = TradingViewGuestProvider(Settings(_env_file=None), Metrics())
    await provider.subscribe(
        Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
            "tradingview_guest"
        )[:1]
    )

    async def rejected(*args, **kwargs):
        raise InvalidStatus(Response(429, "Too Many Requests", Headers({"Retry-After": "180"})))

    monkeypatch.setattr("marketdata.providers.tradingview.connect", rejected)
    monkeypatch.setattr("marketdata.providers.tradingview.time.monotonic", lambda: 10)
    with pytest.raises(InvalidStatus):
        await provider.connect()
    assert provider.retry_at == 190


async def test_reconnect_does_not_refresh_previous_session_observation():
    pair = Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
        "tradingview_guest"
    )[0]
    provider = TradingViewGuestProvider(Settings(_env_file=None), Metrics())
    await provider.subscribe([pair])
    provider.transport_connected()
    now = utc_now()
    old = now - timedelta(minutes=5)
    provider.observed(normalize(*pair, old, event_timestamp=old, last="100"))
    provider.connection_started(reconnecting=True)
    provider.transport_connected(now)
    assert provider.health.symbols[pair[0].canonical_symbol].stale
    assert provider.health.fresh_symbol_count == 0
    assert provider.health.last_success_at == old
    assert provider.snapshots[pair[0].canonical_symbol].received_timestamp == old


@pytest.mark.integration
async def test_latest_only_atomic_retry_ordering_precision_and_no_history(store, pair):
    now = utc_now()
    first = normalize(*pair, now, event_timestamp=now, last="123.1234567890123456789012345678")
    later = normalize(
        *pair,
        now + timedelta(microseconds=1),
        event_timestamp=now,
        bid="123.1234567890123456789012345679",
    )
    assert await store.write_quotes([first, later]) == 1
    assert await store.write_quotes([first, later]) == 0
    assert await store.write_quotes([first]) == 0
    assert (await store.latest(pair[0].canonical_symbol, "mock", 10))[0] == later
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
        events = await c.fetch("SELECT payload FROM marketdata_events ORDER BY id")
        assert len(events) == 1
        assert json.loads(events[0]["payload"])["bid"] == str(later.bid)
    # Concurrent candle and quote writers use the same committed cursor lock.
    bar = make_candle(
        pair,
        now.replace(second=0, microsecond=0) - timedelta(minutes=1),
        received_timestamp=now,
        realtime_status="simulated",
    )
    equal_price = normalize(*pair, now + timedelta(seconds=1), last=later.bid)
    await asyncio.gather(store.write_candles([bar], []), store.write_quotes([equal_price]))
    async with store.connection() as c:
        events = await c.fetch("SELECT id,kind FROM marketdata_events ORDER BY id")
        assert [r["id"] for r in events] == list(range(events[0]["id"], events[0]["id"] + 3))
        assert {r["kind"] for r in events[1:]} == {"price_update", "candle_completed"}
    assert await store.write_quotes([equal_price]) == 0


@pytest.mark.integration
async def test_quote_latest_and_event_rollback_together(store, pair, monkeypatch):
    async def failed_journal(*args):
        raise ConnectionError("fixture_journal_failure")

    monkeypatch.setattr("marketdata.events.append_events", failed_journal)
    with pytest.raises(ConnectionError):
        await store.write_quotes([normalize(*pair, utc_now(), last="1")])
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM latest_prices") == 0
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 0


class QuoteSocket:
    def __init__(self):
        self.frames = asyncio.Queue()
        self.session = None

    async def send(self, raw):
        for body in FrameDecoder().feed(raw):
            if body.startswith("~h~"):
                continue
            command = json.loads(body)
            if command["m"] == "quote_create_session":
                self.session = command["p"][0]

    def __aiter__(self):
        return self

    async def __anext__(self):
        raw = await self.frames.get()
        if isinstance(raw, Exception):
            raise raw
        return raw

    async def close(self):
        await self.frames.put(ConnectionError("fixture_disconnect"))

    async def quote(self, pair, price, **changes):
        values = dict(
            lp=price,
            lp_time=str(utc_now().timestamp()),
            type=pair[1].metadata["source_type"],
            exchange="FXCM" if pair[0].exchange == "FX" else pair[0].exchange,
            currency_code=pair[0].currency,
            update_mode="streaming",
            update_mode_seconds=0,
        )
        values.update(changes)
        await self.frames.put(
            encode_message("qsd", [self.session, dict(n=pair[1].provider_symbol, s="ok", v=values)])
        )


class BlockingCandles(Replay):
    def __init__(self):
        super().__init__()
        self.gate = asyncio.Event()
        self.fetch_started = asyncio.Event()

    async def fetch_completed(self, pair, start, end, count):
        self.fetch_started.set()
        await self.gate.wait()
        self.calls.append((start, end, count))
        if self.fail:
            raise ConnectionError("fixture_candle_disconnect")
        return [
            make_candle(pair, start + timedelta(minutes=k), received_timestamp=utc_now())
            for k in range(int((end - start).total_seconds() // 60))
            if start + timedelta(minutes=k) not in self.omit
        ]


async def eventually(predicate):
    async with asyncio.timeout(8):
        while not await predicate():  # noqa: ASYNC110 - bounded fixture reconciliation
            await asyncio.sleep(0.02)


@asynccontextmanager
async def released_api(tmp_path, database_url):
    release = Path("artifacts/marketdata-fastapi-v1.0.zip")
    if not await asyncio.to_thread(release.is_file):
        pytest.skip("exact released v1.0 ZIP is required for independent consumer qualification")
    assert hashlib.sha256(await asyncio.to_thread(release.read_bytes)).hexdigest() == RELEASE_SHA256
    extraction = tmp_path / "released-api"
    with zipfile.ZipFile(release) as archive:
        archive.extractall(extraction)
    target = extraction / "marketdata-fastapi"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = {
        **os.environ,
        "PYTHONPATH": str(target / "src"),
        "DATABASE_URL": database_url,
        "API_KEY": TOKEN,
        "API_ENVIRONMENT": "production",
        "APPLICATION_ROLE": "api",
        "WEBSOCKET_ALLOWED_ORIGINS": json.dumps([ORIGIN]),
        "MARKETDATA_POLL_SECONDS": "0.02",
        "WEBSOCKET_HEARTBEAT_SECONDS": "0.1",
        "DB_TIMEOUT_SECONDS": "0.5",
    }
    code = (
        "from pathlib import Path; import marketdata.api as api; "
        "assert Path(api.__file__).resolve().is_relative_to(Path.cwd()); "
        "import uvicorn; uvicorn.run(api.create_app(),host='127.0.0.1',port="
        + str(port)
        + ",log_level='error')"
    )
    log = (tmp_path / "released-api.log").open("w")
    process = await asyncio.to_thread(
        subprocess.Popen, [sys.executable, "-c", code], cwd=target, env=env, stdout=log, stderr=log
    )
    base = f"http://127.0.0.1:{port}"
    try:
        async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:

            async def ready():
                assert process.poll() is None, (tmp_path / "released-api.log").read_text()
                try:
                    return (await client.get("/health")).status_code == 200
                except httpx.ConnectError:
                    return False

            await eventually(ready)
        yield base
    finally:
        process.terminate()
        await asyncio.to_thread(process.wait, 10)
        log.close()
    assert hashlib.sha256(await asyncio.to_thread(release.read_bytes)).hexdigest() == RELEASE_SHA256


@asynccontextmanager
async def paused_test_database(store):
    from urllib.parse import urlsplit

    container = os.environ["DUAL_TEST_CONTAINER"]
    url = urlsplit(store.settings.database_url.get_secret_value())
    assert container in {"marketdata-dual-pg16-test", "marketdata-dual-ts-test"}
    assert url.hostname == "127.0.0.1" and url.port in {55935, 55936}
    assert url.path == "/marketdata_dual_test"

    async def docker(*args):
        result = await asyncio.to_thread(
            subprocess.run, ["docker", *args], capture_output=True, text=True, timeout=10
        )
        assert result.returncode == 0, result.stderr
        return result.stdout.strip()

    assert (
        await docker("inspect", "--format", '{{index .Config.Labels "marketdata.task"}}', container)
        == "dual-ingestion"
    )
    assert await docker("port", container, "5432/tcp") == f"127.0.0.1:{url.port}"
    paused = False
    try:
        await docker("pause", container)
        paused = True
        yield
    finally:
        if paused:
            await docker("unpause", container)


@pytest.mark.integration
async def test_old_quote_stays_stale_across_released_api_restarts(
    store, readonly_database_url, tmp_path
):
    registry = Registry.load(Path("config/instruments.tradingview.json"))
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    old = utc_now() - timedelta(minutes=5)
    tick = normalize(
        pair[0],
        pair[1].model_copy(update={"realtime_status": "realtime"}),
        old,
        event_timestamp=old,
        last="123",
    )
    await store.write_quotes([tick])
    observed = []
    for index in range(2):
        async with released_api(tmp_path / str(index), readonly_database_url) as base:
            async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
                item = (
                    await client.get(
                        "/api/v1/prices/latest",
                        params={
                            "symbols": pair[0].canonical_symbol,
                            "provider": "tradingview_guest",
                        },
                    )
                ).json()["items"][0]
                assert item["freshness"] == item["quote_freshness"] == "stale"
                assert not item["realtime_quote_available"]
                observed.append(
                    (item["source_timestamp"], item["received_timestamp"], item["last"])
                )
    assert observed[0] == observed[1]


@pytest.mark.integration
async def test_dual_collector_to_exact_released_api_outage_recovery_replay(
    store, readonly_database_url, tmp_path, monkeypatch
):
    proposed = Registry.load(Path("config/instruments.tradingview-business-proposal.json"))
    monkeypatch.setattr("marketdata.collector.configure_logging", lambda _: None)
    entries = [
        next(e for e in proposed.entries if e.mappings[0].provider_symbol == symbol)
        for symbol in ("BINANCE:BTCUSDT", "OANDA:EURUSD", "FX:EURUSD")
    ]
    registry = Registry(entries)
    registry_file = tmp_path / "registry.json"
    registry_file.write_text(json.dumps([e.model_dump(mode="json") for e in entries]))
    pairs = registry.subscriptions("tradingview_guest")
    candle_provider = BlockingCandles()
    monkeypatch.setattr(
        "marketdata.providers.factory.TradingViewCandleProvider", lambda *a, **k: candle_provider
    )
    sockets = []

    async def quote_connect(*args, **kwargs):
        socket = QuoteSocket()
        sockets.append(socket)
        return socket

    monkeypatch.setattr("marketdata.providers.tradingview.connect", quote_connect)
    # Freeze native cadence: a real DB pause must not cross an unrelated UTC
    # health boundary and make an otherwise-correct fixture nondeterministic.
    candle_clock = utc_now().replace(second=10, microsecond=0)
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: candle_clock)
    monkeypatch.setattr("marketdata.providers.candles.utc_now", lambda: candle_clock)
    monkeypatch.setattr(
        "marketdata.persistence.utc_now", lambda: candle_clock - timedelta(minutes=3)
    )
    settings = Settings(
        _env_file=None,
        application_role="collector",
        database_url=store.settings.database_url,
        registry_file=registry_file,
        tradingview_storage_mode="dual",
        market_data_provider_crypto="tradingview_guest",
        market_data_provider_forex="tradingview_guest",
        collector_metrics_port=0,
        collector_status_interval_seconds=0.1,
        batch_flush_seconds=0.01,
        db_timeout_seconds=0.5,
        log_level="INFO",
    )
    async with released_api(tmp_path, readonly_database_url) as base:
        async with collector_lifespan(settings) as runtime:
            await asyncio.wait_for(candle_provider.fetch_started.wait(), 5)
            async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
                assert (
                    await client.get(
                        "/api/v1/prices/latest", headers={"Authorization": "Bearer wrong"}
                    )
                ).status_code == 403
                async with connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws:
                    ack, _ = await subscribe(ws, [p[0].canonical_symbol for p in pairs])
                    for pair in pairs:
                        await sockets[-1].quote(pair, "1.1234567890123456789012345678")
                    updates = [await receive(ws, "price_update") for _ in pairs]
                    assert {e["data"]["provider_symbol"] for e in updates} == {
                        p[1].provider_symbol for p in pairs
                    }
                    assert {e["data"]["last"] for e in updates} == {
                        "1.1234567890123456789012345678"
                    }
                    assert len({e["data"]["instrument_id"] for e in updates}) == 3
                    async with store.connection() as c:
                        assert await c.fetchval("SELECT count(*) FROM market_candles") == 0
                        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
                    # Quotes recover from disconnect while the independent candle fetch is blocked.
                    await sockets[-1].close()

                    async def reconnected():
                        return len(sockets) == 2 and sockets[-1].session is not None

                    await eventually(reconnected)
                    await sockets[-1].quote(pairs[0], "1.2")
                    recovered = await receive(ws, "price_update")
                    assert recovered["data"]["last"] == "1.2"
                    assert runtime.service.providers[1].health.reconnect_count == 1
                    # An uncertain quote commit retries the same UUID without duplicate events.
                    writer = runtime.store.write_quotes
                    calls = 0

                    async def uncertain(ticks):
                        nonlocal calls
                        calls += 1
                        result = await writer(ticks)
                        if calls == 1:
                            raise ConnectionError("fixture_uncertain_commit")
                        return result

                    monkeypatch.setattr(runtime.store, "write_quotes", uncertain)
                    await sockets[-1].quote(pairs[0], "1.3")
                    outage = await receive(ws, "price_update")
                    await asyncio.wait_for(runtime.service.queue.join(), 5)
                    assert calls == 2 and runtime.service.database_healthy
                    assert runtime.metrics.db_errors._value.get() == 1
                    latest_price = "1.3"
                    quote_events = 5
                    if os.environ.get("DUAL_TEST_CONTAINER"):
                        async with paused_test_database(store):
                            await sockets[-1].quote(pairs[0], "1.4")
                            async with asyncio.timeout(5):
                                while (await receive(ws, "heartbeat"))["database"] != "unavailable":
                                    pass
                            assert (await client.get("/api/v1/prices/latest")).status_code == 503
                            assert (
                                runtime.service.providers[1]
                                .health.symbols[pairs[0][0].canonical_symbol]
                                .observation_count
                                == 4
                            )
                        persisted = await receive(ws, "price_update")
                        assert persisted["data"]["last"] == "1.4"
                        await asyncio.wait_for(runtime.service.queue.join(), 5)
                        assert runtime.service.database_healthy
                        latest_price, quote_events = "1.4", 6
                    candle_provider.gate.set()
                    await receive(ws, "candle_completed")

                    async def candles_done():
                        async with store.connection() as c:
                            return (
                                await c.fetchval(
                                    "SELECT count(DISTINCT instrument_id) FROM market_candles"
                                )
                                == 3
                            )

                    await eventually(candles_done)
                    params = {
                        "symbol": pairs[0][0].canonical_symbol,
                        "provider": "tradingview_guest",
                    }
                    history = (await client.get("/api/v1/candles", params=params)).json()["items"]
                    assert history and all(c["origin"] == "native" for c in history)
                    chart = (await client.get("/api/v1/dashboard/candles", params=params)).json()
                    assert set(chart) == set("abcdefgh") and chart["f"] == chart["h"] == [0] * len(
                        chart["a"]
                    )
                    assert chart["c"] == [float(c["close"]) for c in history]
                    prices = (
                        await client.get(
                            "/api/v1/prices/latest",
                            params={
                                "symbols": pairs[0][0].canonical_symbol,
                                "provider": "tradingview_guest",
                            },
                        )
                    ).json()["items"]
                    assert prices[0]["price_type"] == "quote" and prices[0]["last"] == latest_price
                    legacy = await runtime.store.latest(
                        pairs[0][0].canonical_symbol, "tradingview_guest", 1
                    )
                    assert str(legacy[0].last) == prices[0]["last"]
                    await runtime.publish_status(True)
                    async with asyncio.timeout(5):
                        while True:
                            status = await receive(ws, "source_status")
                            current = next(
                                (
                                    item
                                    for item in status["items"]
                                    if item["canonical_symbol"] == pairs[0][0].canonical_symbol
                                    and item["provider"] == "tradingview_guest"
                                ),
                                None,
                            )
                            if current and current["candle_state"] == "healthy":
                                assert current["transport"] == "connected"
                                assert current["session_expectation"] == "EXPECTED"
                                break
                    await sockets[-1].quote(
                        pairs[1], "1.5", update_mode="delayed", update_mode_seconds="600"
                    )

                    async def rejected_source():
                        return pairs[1][1].provider_symbol in runtime.service.providers[1].rejected

                    await eventually(rejected_source)
                    await runtime.publish_status(True)
                    healths = (await client.get("/providers")).json()
                    quote_health = next(h for h in healths if h["storage_mode"] == "quotes")
                    assert quote_health["state"] == "degraded"
                    assert (
                        quote_health["symbols"][pairs[1][0].canonical_symbol]["source_update_mode"]
                        == "delayed"
                    )
                    replay = (
                        await client.get(
                            "/api/v1/marketdata/changes", params={"after": ack["snapshot_cursor"]}
                        )
                    ).json()
                    assert outage["event_id"] in {e["event_id"] for e in replay["items"]}
                    assert (
                        len([e for e in replay["items"] if e["type"] == "price_update"])
                        == quote_events
                    )
                async with connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws:
                    _, snapshots = await subscribe(ws, [pairs[0][0].canonical_symbol])
                    assert (
                        next(p for p in snapshots if p["provider"] == "tradingview_guest")["last"]
                        == latest_price
                    )
        # Independent consumer stays available when the collector exits; receipts do not change.
        async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
            assert (await client.get("/health")).status_code == 200
            after = (
                await client.get(
                    "/api/v1/prices/latest",
                    params={
                        "symbols": pairs[0][0].canonical_symbol,
                        "provider": "tradingview_guest",
                    },
                )
            ).json()["items"][0]
            assert after["received_timestamp"] == prices[0]["received_timestamp"]
            assert (await client.get("/api/v1/health")).json()["collector"] == "stopped"
