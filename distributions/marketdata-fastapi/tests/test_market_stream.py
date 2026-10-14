"""Simulated publisher -> real commit -> read-only API -> real WebSocket client."""

import asyncio
import json
import socket
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import asyncpg
import httpx
import pytest
import uvicorn
from fastapi import HTTPException
from fastapi.testclient import TestClient
from helpers import make_candle
from pydantic import TypeAdapter, ValidationError
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from marketdata.api import create_app
from marketdata.config import Settings
from marketdata.domain import DataStatus, normalize
from marketdata.events import decode_watermark, encode_watermark, event_page
from marketdata.market_stream import MarketHub
from marketdata.observability import Metrics
from marketdata.providers.base import ProviderHealth, SymbolHealth
from marketdata.ws_models import Heartbeat, MarketCommand, MarketEvent

ORIGIN = "https://frontend.example.com"
TOKEN = "deterministic-test-token-32-characters"
HEADERS = {"Authorization": "Bearer " + TOKEN}


def settings(store, **kwargs):
    return Settings(
        _env_file=None,
        database_url=store.settings.database_url,
        api_key=TOKEN,
        websocket_allowed_origins=[ORIGIN],
        marketdata_poll_seconds=0.02,
        websocket_heartbeat_seconds=0.05,
        **kwargs,
    )


def bar(pair, minutes_ago=2, **changes):
    now = datetime.now(UTC)
    return make_candle(
        pair,
        now.replace(second=0, microsecond=0) - timedelta(minutes=minutes_ago),
        received_timestamp=now,
        realtime_status=DataStatus.SIMULATED,
        **changes,
    )


@asynccontextmanager
async def server(store, **kwargs):
    app = create_app(settings(store, **kwargs))
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        port = listener.getsockname()[1]
        runner = uvicorn.Server(
            uvicorn.Config(
                app,
                log_level="error",
                lifespan="on",
                ws="websockets",
                ws_max_size=65536,
                ws_max_queue=16,
            )
        )
        task = asyncio.create_task(runner.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not runner.started:  # noqa: ASYNC110 - test-owned server readiness
                    assert not task.done()
                    await asyncio.sleep(0.01)
            yield app, f"http://127.0.0.1:{port}"
        finally:
            runner.should_exit = True
            await asyncio.wait_for(task, 5)
            assert app.state.market_hub.task.done()
            assert not app.state.market_hub.clients


def ws_url(url):
    return url.replace("http://", "ws://") + "/api/v1/ws/marketdata"


async def receive(ws, kind, request_id=None):
    async with asyncio.timeout(5):
        while True:
            event = json.loads(await ws.recv())
            TypeAdapter(MarketEvent).validate_python(event)
            if event["type"] == kind and (
                request_id is None or event.get("request_id") == request_id
            ):
                return event


async def subscribe(ws, symbols, request_id="s"):
    await ws.send(json.dumps(dict(action="subscribe", symbols=symbols, request_id=request_id)))
    ack = await receive(ws, "subscription_ack", request_id)
    snapshots = []
    while True:
        snapshot = await receive(ws, "price_snapshot", request_id)
        snapshots.extend(snapshot["items"])
        if snapshot["final"]:
            return ack, snapshots


@pytest.mark.parametrize(
    "raw",
    [
        "{}",
        "null",
        "[]",
        "false",
        '{"action":"x"}',
        '{"action":"subscribe","symbols":[]}',
        '{"action":"subscribe","symbols":"CRYPTO:BTCUSDT"}',
        '{"action":"ping","symbols":["CRYPTO:BTCUSDT"]}',
        '{"action":"ping","extra":1}',
        '{"action":"subscribe","symbols":["../bad"]}',
        '{"action":"ping","request_id":3}',
    ],
)
def test_commands_reject_malformed_or_unbounded_shapes(raw):
    with pytest.raises(ValidationError):
        MarketCommand.model_validate_json(raw)


@pytest.mark.parametrize(
    "value",
    [
        "a",
        "!",
        "",
        "eA==",
        "A" * 129,
        "MTphOjA=",
        "MTowMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMDA6LTE=",
    ],
)
def test_watermark_validation(value):
    with pytest.raises(HTTPException) as error:
        decode_watermark(value)
    assert error.value.status_code == 422


@pytest.mark.parametrize(
    "headers",
    [
        {},
        HEADERS,
        {"Origin": ORIGIN},
        {**HEADERS, "Origin": "https://evil.example.com"},
        {"Authorization": "Bearer wrong", "Origin": ORIGIN},
    ],
)
def test_authentication_and_origin_fail_before_registration(headers):
    app = create_app(Settings(_env_file=None, api_key=TOKEN, websocket_allowed_origins=[ORIGIN]))
    with pytest.raises(WebSocketDisconnect) as error:
        with TestClient(app).websocket_connect("/api/v1/ws/marketdata", headers=dict(headers)):
            pass
    assert error.value.code == 1008


async def test_queue_overflow_and_clients_are_bounded_and_isolated():
    config = Settings(_env_file=None, websocket_queue_capacity=1, websocket_max_clients=2)
    hub = MarketHub(None, config, Metrics())
    slow, fast = hub.register(), hub.register()
    with pytest.raises(OverflowError):
        hub.register()
    message = Heartbeat(server_timestamp=datetime.now(UTC), database="available", cursor=None)
    hub.enqueue(slow, message)
    hub.enqueue(slow, message)
    hub.enqueue(fast, message)
    assert slow.failed.is_set() and not fast.failed.is_set()
    assert slow.queue.qsize() == fast.queue.qsize() == 1
    assert json.loads(fast.queue.get_nowait())["type"] == "heartbeat"


def test_failed_handshake_releases_the_registered_client_slot(monkeypatch):
    from starlette.websockets import WebSocket

    config = Settings(_env_file=None, api_key=TOKEN, websocket_allowed_origins=[ORIGIN])
    app = create_app(config)
    hub = MarketHub(None, config, Metrics())
    app.state.market_hub = hub

    async def failed_accept(*args, **kwargs):
        raise RuntimeError("simulated handshake transport failure")

    monkeypatch.setattr(WebSocket, "accept", failed_accept)
    with pytest.raises(RuntimeError, match="simulated handshake"):
        with TestClient(app).websocket_connect(
            "/api/v1/ws/marketdata", headers={**HEADERS, "Origin": ORIGIN}
        ):
            pass
    assert not hub.clients


@pytest.mark.integration
async def test_committed_candle_quote_correction_and_rest_agree(store, pair):
    async with (
        server(store) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        ack, snapshots = await subscribe(ws, [pair[0].canonical_symbol])
        assert snapshots[0]["price_type"] == "unavailable"
        candle = bar(pair)
        assert await store.write_candles([candle], []) == 1
        event = await receive(ws, "candle_completed")
        assert event["persisted"] and event["data"]["close"] == str(candle.close)
        assert event["data"]["price_basis"] == "native"
        assert event["data"]["realtime_status"] == "simulated"
        now = datetime.now(UTC)
        tick = normalize(*pair, now, event_timestamp=now, last="101.123456789012345678901")
        await store.write([tick])
        quote = await receive(ws, "price_update")
        assert quote["data"]["last"] == str(tick.last)
        assert quote["data"]["price_type"] == "quote"
        correction = candle.model_copy(
            update={
                "close": Decimal("100.5"),
                "received_timestamp": candle.received_timestamp + timedelta(seconds=1),
            }
        )
        await store.write_candles([correction], [])
        corrected = await receive(ws, "candle_corrected")
        assert corrected["data"]["close"] == "100.5"
        assert int(event["event_id"]) < int(quote["event_id"]) < int(corrected["event_id"])
        async with httpx.AsyncClient(base_url=url, headers=HEADERS) as client:
            history = await client.get(
                "/api/v1/candles",
                params=dict(
                    symbol=candle.canonical_symbol,
                    provider=candle.provider,
                    start=candle.bucket_start.isoformat(),
                    end=candle.bucket_end.isoformat(),
                ),
            )
            assert history.json()["items"] == [corrected["data"]]
            latest = (
                await client.get(
                    "/api/v1/prices/latest", params={"symbols": candle.canonical_symbol}
                )
            ).json()["items"][0]
            for key in (
                "last",
                "provider_symbol",
                "price_type",
                "price_basis",
                "received_timestamp",
            ):
                assert latest[key] == quote["data"][key]
            changes = (
                await client.get(
                    "/api/v1/marketdata/changes", params={"after": ack["snapshot_cursor"]}
                )
            ).json()
            assert [r["event_id"] for r in changes["items"]] == [
                event["event_id"],
                quote["event_id"],
                corrected["event_id"],
            ]


@pytest.mark.integration
async def test_no_precommit_delivery_and_rollback_cannot_publish(store, pair, monkeypatch):
    import marketdata.events as events

    initial = await event_page(store, None, 100)
    appended, release = asyncio.Event(), asyncio.Event()
    original = events.append_events

    async def pause(connection, observations, retention):
        await original(connection, observations, retention)
        appended.set()
        await release.wait()
        raise RuntimeError("injected rollback after journal append")

    monkeypatch.setattr(events, "append_events", pause)
    candle = bar(pair)
    task = asyncio.create_task(store.write_candles([candle], []))
    try:
        await asyncio.wait_for(appended.wait(), 3)
        page = await event_page(store, initial["cursor"], 100)
        assert page["rows"] == [] and page["cursor"] == initial["cursor"]
        assert not await store.latest(candle.canonical_symbol, candle.provider, 100)
    finally:
        release.set()
        with pytest.raises(RuntimeError, match="injected rollback"):
            await task
    assert (await event_page(store, initial["cursor"], 100))["rows"] == []
    monkeypatch.setattr(events, "append_events", original)
    await store.write_candles([candle], [])
    assert len((await event_page(store, initial["cursor"], 100))["rows"]) == 1


@pytest.mark.integration
async def test_retry_identical_prices_and_old_ticks_follow_latest_state(store, pair):
    baseline = (await event_page(store, None, 100))["cursor"]
    candle = bar(pair)
    await store.write_candles([candle], [])
    assert await store.write_candles([candle], []) == 0
    now = datetime.now(UTC)
    tick = normalize(*pair, now, event_timestamp=now, last="100")
    await store.write([tick])
    assert await store.write([tick]) == 0
    # Identical value at a new source observation is a genuine new latest update.
    later = normalize(
        *pair, now + timedelta(seconds=1), event_timestamp=now + timedelta(seconds=1), last="100"
    )
    await store.write([later])
    older = normalize(*pair, now, event_timestamp=now - timedelta(seconds=1), last="99")
    await store.write([older])
    rows = (await event_page(store, baseline, 100))["rows"]
    assert [r["kind"] for r in rows] == ["candle_completed", "price_update", "price_update"]


@pytest.mark.integration
async def test_concurrent_commit_order_never_loses_live_or_reconnected_events(
    store, registry, monkeypatch
):
    import marketdata.events as events

    first, second = registry.subscriptions("mock")[:2]
    paused, release, attempted = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = events.append_events

    async def controlled(connection, observations, retention):
        if observations[0][1].canonical_symbol == first[0].canonical_symbol:
            await original(connection, observations, retention)
            paused.set()  # A has allocated its cursor but has NOT committed.
            await release.wait()
        else:
            attempted.set()  # B reaches the cursor allocator while A is paused.
            await original(connection, observations, retention)

    monkeypatch.setattr(events, "append_events", controlled)
    async with (
        server(store) as (app, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        ack, _ = await subscribe(ws, [first[0].canonical_symbol, second[0].canonical_symbol])
        baseline = ack["snapshot_cursor"]
        a = asyncio.create_task(store.write_candles([bar(first)], []))
        b = None
        try:
            await asyncio.wait_for(paused.wait(), 3)
            b = asyncio.create_task(store.write_candles([bar(second)], []))
            await asyncio.wait_for(attempted.wait(), 3)
            # Wait for actual PostgreSQL lock evidence, rather than an arbitrary sleep.
            async with asyncio.timeout(3):
                while True:  # noqa: ASYNC110 - observes test-owned DB transaction lock
                    async with store.connection() as c:
                        blocked = await c.fetchval("""
                            SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                            WHERE datname=current_database() AND wait_event_type='Lock'
                            AND query LIKE 'UPDATE marketdata_event_clock SET head=head+%')
                        """)
                    if blocked:
                        break
                    await asyncio.sleep(0.01)
            assert not b.done()
            assert (await event_page(store, baseline, 100))["rows"] == []
            assert app.state.market_hub.cursor == baseline
        finally:
            release.set()
            await asyncio.wait_for(a, 3)
            if b is not None:
                await asyncio.wait_for(b, 3)
        live = [await receive(ws, "candle_completed") for _ in range(2)]
        assert [e["data"]["canonical_symbol"] for e in live] == [
            first[0].canonical_symbol,
            second[0].canonical_symbol,
        ]
        assert int(live[1]["event_id"]) == int(live[0]["event_id"]) + 1
        # A reconnect from the pre-transaction checkpoint sees both; reconnect from A
        # sees exactly B. The REST fence is the same committed cursor as live delivery.
        async with httpx.AsyncClient(base_url=url, headers=HEADERS) as client:
            all_events = (
                await client.get(
                    "/api/v1/marketdata/changes", params={"after": baseline, "limit": 1}
                )
            ).json()
            assert (
                all_events["has_more"] and all_events["items"][0]["event_id"] == live[0]["event_id"]
            )
            tail = (
                await client.get(
                    "/api/v1/marketdata/changes",
                    params={"after": all_events["cursor"], "until": all_events["until"]},
                )
            ).json()
            assert [e["event_id"] for e in tail["items"]] == [live[1]["event_id"]]
            assert tail["cursor"] == live[1]["cursor"]


@pytest.mark.integration
async def test_journal_pages_retention_and_epoch_expiry(store, pair):
    baseline = (await event_page(store, None, 100))["cursor"]
    store.settings.marketdata_event_retention_count = 3
    candles = [bar(pair, i) for i in (5, 4, 3)]
    await store.write_candles(candles, [])
    fence = (await event_page(store, None, 100))["cursor"]
    first = await event_page(store, baseline, 1, fence)
    assert first["has_more"] and len(first["rows"]) == 1
    second = await event_page(store, first["cursor"], 10, fence)
    assert not second["has_more"] and len(second["rows"]) == 2
    assert second["cursor"] == fence
    await store.write_candles([bar(pair, 2)], [])
    with pytest.raises(HTTPException) as expired:
        await event_page(store, baseline, 10)
    assert expired.value.status_code == 409
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 3
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 4
    epoch, position = decode_watermark(fence)
    for cursor, expected in [
        (encode_watermark(uuid4(), position), 409),
        (encode_watermark(epoch, position + 1000), 422),
    ]:
        with pytest.raises(HTTPException) as bad:
            await event_page(store, cursor, 100)
        assert bad.value.status_code == expected


@pytest.mark.integration
async def test_disconnect_api_restart_and_late_old_correction_rest_recovery(store, pair):
    async with (
        server(store) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        ack, _ = await subscribe(ws, [pair[0].canonical_symbol])
        cursor = ack["snapshot_cursor"]
    # Collector/test publisher continues while the API process is absent.
    old, recent = bar(pair, 20), bar(pair, 2)
    await store.write_candles([old, recent], [])
    corrected = old.model_copy(
        update={
            "close": Decimal("100.5"),
            "received_timestamp": old.received_timestamp + timedelta(seconds=1),
        }
    )
    await store.write_candles([corrected], [])
    async with (
        server(store) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        ack, snapshots = await subscribe(ws, [pair[0].canonical_symbol])
        assert snapshots[0]["price_type"] == "candle_close"
        assert not snapshots[0]["realtime_quote_available"]
        async with httpx.AsyncClient(base_url=url, headers=HEADERS) as client:
            page = (
                await client.get(
                    "/api/v1/marketdata/changes",
                    params={"after": cursor, "until": ack["snapshot_cursor"]},
                )
            ).json()
            assert [r["type"] for r in page["items"]] == [
                "candle_completed",
                "candle_completed",
                "candle_corrected",
            ]
            assert (
                page["items"][-1]["data"]["bucket_start"]
                == old.model_dump(mode="json")["bucket_start"]
            )
            history = (
                await client.get(
                    "/api/v1/candles",
                    params=dict(
                        symbol=old.canonical_symbol,
                        provider=old.provider,
                        start=old.bucket_start.isoformat(),
                        end=recent.bucket_end.isoformat(),
                    ),
                )
            ).json()["items"]
            assert [r["close"] for r in history] == ["100.5", "101"]
        await store.write_candles([bar(pair, 1)], [])
        assert (await receive(ws, "candle_completed"))["data"]["complete"]


@pytest.mark.integration
async def test_commands_unknowns_repeats_unsubscribe_snapshot_and_disconnect(store, pair):
    symbol = pair[0].canonical_symbol
    async with (
        server(store) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        await ws.send("not-json")
        assert (await receive(ws, "error"))["code"] == "invalid_command"
        await ws.send(json.dumps({"action": "subscribe", "symbols": ["CRYPTO:UNKNOWN"]}))
        assert (await receive(ws, "error"))["code"] == "unknown_symbol"
        for _ in range(2):
            ack, _ = await subscribe(ws, [symbol])
            assert ack["symbols"] == [symbol]
        await ws.send(json.dumps({"action": "snapshot", "request_id": "snap"}))
        assert (await receive(ws, "price_snapshot", "snap"))["final"]
        for _ in range(2):
            await ws.send(json.dumps({"action": "unsubscribe", "symbols": [symbol]}))
            assert (await receive(ws, "subscription_ack"))["symbols"] == []
        await ws.send(json.dumps({"action": "ping", "request_id": "ping"}))
        assert (await receive(ws, "heartbeat", "ping"))["database"] == "available"
        await ws.send(json.dumps({"action": "disconnect"}))
        with pytest.raises(ConnectionClosed) as closed:
            await ws.recv()
        assert closed.value.rcvd.code == 1000


@pytest.mark.integration
async def test_multiple_frontends_snapshot_batches_and_subscription_limits(store, registry):
    symbols = [p[0].canonical_symbol for p in registry.subscriptions("mock")[:3]]
    async with server(
        store, websocket_snapshot_batch_size=1, websocket_max_total_subscriptions=4
    ) as (_, url):
        async with (
            connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as a,
            connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as b,
        ):
            ack, snapshots = await subscribe(a, symbols)
            assert len(snapshots) == 3 and len(ack["symbols"]) == 3
            await subscribe(b, symbols[:1])
            await b.send(json.dumps({"action": "subscribe", "symbols": symbols[1:]}))
            assert (await receive(b, "error"))["code"] == "subscription_limit"
            await store.write_candles([bar(registry.subscriptions("mock")[0])], [])
            first, second = await asyncio.gather(
                receive(a, "candle_completed"), receive(b, "candle_completed")
            )
            assert first["event_id"] == second["event_id"]


@pytest.mark.integration
async def test_stale_quote_closed_market_and_historical_reads_while_source_stopped(store, pair):
    old = datetime.now(UTC) - timedelta(hours=2)
    await store.write([normalize(*pair, old, event_timestamp=old, last="100")])
    candle = bar(pair, 120)
    await store.write_candles([candle], [])
    await store.publish_collector_status(
        uuid4(),
        False,
        True,
        0,
        [
            ProviderHealth(
                provider=pair[1].provider,
                state="disconnected",
                symbols={
                    pair[0].canonical_symbol: SymbolHealth(
                        canonical_symbol=pair[0].canonical_symbol,
                        provider_symbol=pair[1].provider_symbol,
                        session_expectation="NOT_EXPECTED",
                        candle_state="closed",
                    )
                },
            )
        ],
    )
    async with (
        server(store) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        _, snapshots = await subscribe(ws, [pair[0].canonical_symbol])
        assert snapshots[0]["freshness"] == "stale" and not snapshots[0]["realtime_quote_available"]
        status = await receive(ws, "source_status")
        assert status["collector"] == "stopped"
        assert status["items"][0]["session_expectation"] == "NOT_EXPECTED"
        assert status["items"][0]["candle_state"] == "closed"
        async with httpx.AsyncClient(base_url=url, headers=HEADERS) as client:
            assert (
                await client.get(
                    "/api/v1/candles",
                    params=dict(
                        symbol=candle.canonical_symbol,
                        provider=candle.provider,
                        start=candle.bucket_start.isoformat(),
                        end=candle.bucket_end.isoformat(),
                    ),
                )
            ).json()["items"]


@pytest.mark.integration
async def test_reconciliation_outage_recovers_cursor_without_notify_or_source_access(
    store, pair, monkeypatch
):
    import marketdata.market_stream as stream

    async with (
        server(store) as (app, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        await subscribe(ws, [pair[0].canonical_symbol])
        before = app.state.market_hub.cursor
        original = stream.event_page

        async def outage(*args, **kwargs):
            raise asyncpg.CannotConnectNowError("simulated PostgreSQL outage")

        monkeypatch.setattr(stream, "event_page", outage)
        async with asyncio.timeout(3):
            while (await receive(ws, "heartbeat"))["database"] != "unavailable":
                pass
        await store.write_candles([bar(pair)], [])
        assert app.state.market_hub.cursor == before
        monkeypatch.setattr(stream, "event_page", original)
        event = await receive(ws, "candle_completed")
        assert event["data"]["close"] == "101"
        assert app.state.market_hub.cursor != before


@pytest.mark.integration
async def test_slow_network_sender_times_out_and_does_not_block_other_clients(
    store, pair, monkeypatch
):
    from starlette.websockets import WebSocket

    original = WebSocket.send_text
    block = asyncio.Event()

    async def slow(self, raw):
        if self.query_params.get("slow") == "1":
            await block.wait()
        await original(self, raw)

    monkeypatch.setattr(WebSocket, "send_text", slow)
    async with server(store, websocket_send_timeout_seconds=0.1) as (_, url):
        async with (
            connect(ws_url(url) + "?slow=1", origin=ORIGIN, additional_headers=HEADERS) as slow_ws,
            connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as fast_ws,
        ):
            await slow_ws.send(
                json.dumps({"action": "subscribe", "symbols": [pair[0].canonical_symbol]})
            )
            await subscribe(fast_ws, [pair[0].canonical_symbol])
            await store.write_candles([bar(pair)], [])
            assert (await receive(fast_ws, "candle_completed"))["persisted"]
            with pytest.raises(ConnectionClosed) as error:
                await asyncio.wait_for(slow_ws.recv(), 3)
            assert error.value.rcvd.code == 1013


@pytest.mark.integration
@pytest.mark.parametrize("binary", [True, False])
async def test_message_size_and_binary_commands_close_explicitly(store, binary):
    async with (
        server(store, websocket_message_max_bytes=1024) as (_, url),
        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        await ws.send(b"{}" if binary else "x" * 1025)
        with pytest.raises(ConnectionClosed) as closed:
            while True:
                await asyncio.wait_for(ws.recv(), 3)
        assert closed.value.rcvd.code == (1003 if binary else 1009)


@pytest.mark.integration
async def test_new_subscription_snapshot_barrier_does_not_skip_existing_symbol_events(
    store, registry
):
    first, second = registry.subscriptions("mock")[:2]
    hub = MarketHub(store, settings(store), Metrics())
    client = hub.register()
    await hub.command(
        client, MarketCommand(action="subscribe", symbols=[first[0].canonical_symbol])
    )
    while not client.queue.empty():
        client.queue.get_nowait()
    await store.write_candles([bar(first), bar(second)], [])
    await hub.command(
        client, MarketCommand(action="subscribe", symbols=[second[0].canonical_symbol])
    )
    await hub.reconcile()
    messages = [json.loads(client.queue.get_nowait()) for _ in range(client.queue.qsize())]
    completed = [m for m in messages if m["type"] == "candle_completed"]
    assert [m["data"]["canonical_symbol"] for m in completed] == [first[0].canonical_symbol]
    assert [m for m in messages if m["type"] == "price_snapshot"][0]["items"][0][
        "canonical_symbol"
    ] == second[0].canonical_symbol
