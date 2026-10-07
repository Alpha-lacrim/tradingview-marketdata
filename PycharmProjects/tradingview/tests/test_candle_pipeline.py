"""Sliding window routing/lifecycle/scaling regressions, without live connectivity."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from test_native_candles import NOW, Replay, make_candle

from marketdata.candle_benchmark import SyntheticChartSocket, synthetic_pairs
from marketdata.candle_ingestion import CandleIngestion
from marketdata.config import Settings
from marketdata.domain import RegistryEntry
from marketdata.fanout import Fanout
from marketdata.observability import Metrics
from marketdata.providers.tradingview import encode_message
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


def pipeline(count=6, window=4, **socket_options):
    pairs = synthetic_pairs(count)
    p = TradingViewCandleProvider(
        Settings(
            _env_file=None,
            tradingview_candle_concurrency=window,
            candle_request_timeout_seconds=1,
            candle_fetch_grace_seconds=0,
        ),
        Metrics(),
    )
    end = NOW.replace(second=0)
    socket = SyntheticChartSocket(end, latency=0.002, **socket_options)
    return p, socket, pairs, end


def no_receiver_leaks(p):
    assert not p.fetching
    assert p.metrics.candle_inflight.labels(p.name)._value.get() == 0
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "tradingview-candle-receiver"]


@pytest.mark.parametrize("window", [1, 2, 4, 6, 8, 12])
@pytest.mark.parametrize("count", [17, 34, 64, 100])
async def test_bounded_window_large_universe_exact_routing(window, count):
    p, socket, pairs, end = pipeline(count, window)
    await p.subscribe(pairs)
    p.socket = socket
    try:
        result = await p.fetch_many([(pair, end - timedelta(minutes=3), end, 4) for pair in pairs])
        assert socket.max_active == window
        assert len(socket.requests) == count
        assert p.health.candle_retrieval["peak_inflight"] == window
        for index, pair in enumerate(pairs):
            candles = result[pair[0].canonical_symbol]
            assert isinstance(candles, list) and len(candles) == 3
            assert all(c.instrument_id == pair[0].id for c in candles)
            assert all(c.close == Decimal(101 + index) for c in candles)
            assert all(c.bucket_end <= end for c in candles)
        assert not socket.sessions and not socket.tasks
        no_receiver_leaks(p)
    finally:
        await p.disconnect()


async def test_out_of_order_slow_symbol_does_not_block_other_replies():
    p, socket, pairs, end = pipeline(delays={0: 0.08})
    await p.subscribe(pairs)
    p.socket = socket
    result = await p.fetch_many([(pair, end - timedelta(minutes=1), end, 3) for pair in pairs])
    assert socket.completed_order[-1] == 0
    assert set(result) == {pair[0].canonical_symbol for pair in pairs}
    assert all(isinstance(v, list) for v in result.values())
    no_receiver_leaks(p)
    await p.disconnect()


async def test_timeout_isolated_remaining_jobs_continue_and_window_decreases():
    p, socket, pairs, end = pipeline(count=100, drop={0})
    await p.subscribe(pairs)
    p.socket = socket
    result = await p.fetch_many([(pair, end - timedelta(minutes=3), end, 4) for pair in pairs])
    assert isinstance(result[pairs[0][0].canonical_symbol], TimeoutError)
    assert sum(isinstance(v, list) for v in result.values()) == 99
    assert len(socket.requests) == 100
    assert p.effective_concurrency == 2
    assert p.health.candle_retrieval["timeouts"] == 1
    assert p.timings[pairs[0][0].canonical_symbol]["duration_seconds"] >= 1
    no_receiver_leaks(p)
    await p.disconnect()


async def test_receiver_keeps_reading_while_dispatcher_waits_for_cleanup():
    p, socket, pairs, end = pipeline(count=4)
    released, blocked = asyncio.Event(), asyncio.Event()
    original = socket.send

    async def send(raw):
        if '"chart_delete_session"' in raw and not blocked.is_set():
            blocked.set()
            await released.wait()
        await original(raw)

    socket.send = send
    await p.subscribe(pairs)
    p.socket = socket
    task = asyncio.create_task(
        p.fetch_many([(pair, end - timedelta(minutes=1), end, 3) for pair in pairs])
    )
    try:
        await asyncio.wait_for(blocked.wait(), 1)
        await asyncio.sleep(0.02)
        assert len(socket.completed_order) == 4 and socket.messages.empty()
    finally:
        released.set()
    result = await task
    assert all(isinstance(v, list) for v in result.values())
    # Dispatcher cleanup must not inflate another request's measured receive/parse time.
    for timing in p.timings.values():
        parsed_duration = (
            datetime.fromisoformat(timing["parser_completed_at"])
            - datetime.fromisoformat(timing["request_started_at"])
        ).total_seconds()
        assert abs(timing["duration_seconds"] - parsed_duration) < 0.01
    no_receiver_leaks(p)
    await p.disconnect()


async def test_old_session_duplicates_and_unsolicited_frames_cannot_route_to_new_job():
    p, socket, pairs, end = pipeline(count=2, window=2, duplicate=True)
    await p.subscribe(pairs)
    p.socket = socket
    jobs = [(pair, end - timedelta(minutes=1), end, 3) for pair in pairs]
    await p.fetch_many(jobs)
    old = socket.created[0]
    await socket.messages.put(encode_message("series_completed", [old, "s1", "streaming", "s1"]))
    await socket.messages.put(encode_message("unrelated", ["old_session", "s1"]))
    result = await p.fetch_many(jobs)
    assert not set(socket.created[:2]) & set(socket.created[2:])
    assert all(isinstance(v, list) and len(v) == 1 for v in result.values())
    assert p.health.candle_retrieval["ignored_responses"] >= 2
    no_receiver_leaks(p)
    await p.disconnect()


async def test_reconnect_with_outstanding_requests_rejects_old_generation(monkeypatch):
    p, old, pairs, end = pipeline(count=4, drop={0, 1, 2, 3})
    await p.subscribe(pairs)
    sockets = [old, SyntheticChartSocket(end, latency=0.002)]

    async def connect(*args, **kwargs):
        return sockets.pop(0)

    monkeypatch.setattr("marketdata.providers.tradingview_candles.connect", connect)
    jobs = [(pair, end - timedelta(minutes=1), end, 3) for pair in pairs]
    await p.connect()
    generation = p.connection_generation
    task = asyncio.create_task(p.fetch_many(jobs))
    await asyncio.wait_for(old.wait_for_requests(4), 1)
    await p.disconnect()
    result = await task
    assert all(isinstance(v, Exception) for v in result.values())
    await p.connect()
    assert p.connection_generation > generation
    for session in old.created:
        await p.socket.messages.put(
            encode_message("series_completed", [session, "s1", "streaming", "s1"])
        )
    result = await p.fetch_many(jobs)
    assert all(isinstance(v, list) for v in result.values())
    no_receiver_leaks(p)
    await p.disconnect()


async def test_shutdown_cancels_receiver_and_all_waiters_without_future_leaks(monkeypatch):
    p, socket, pairs, end = pipeline(count=100, drop=set(range(100)))
    await p.subscribe(pairs)
    p.socket = socket
    loop = asyncio.get_running_loop()
    create_future = loop.create_future
    futures = []

    def tracked_future():
        future = create_future()
        futures.append(future)
        return future

    monkeypatch.setattr(loop, "create_future", tracked_future)
    task = asyncio.create_task(
        p.fetch_many([(pair, end - timedelta(minutes=3), end, 4) for pair in pairs])
    )
    await asyncio.wait_for(socket.wait_for_requests(4), 1)
    with pytest.raises(RuntimeError, match="already in progress"):
        await p.fetch_many([(pairs[0], end - timedelta(minutes=1), end, 3)])
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not socket.tasks and not socket.sessions
    no_receiver_leaks(p)
    await p.disconnect()
    assert futures and all(f.done() for f in futures)


async def test_wrong_series_cannot_be_silently_mapped_to_expected_instrument():
    p, socket, pairs, end = pipeline(count=2, window=2, delays={0: 0.03})
    original = socket.emit

    async def emit(session, source, index, count):
        if index == 0:
            await socket.messages.put(
                encode_message("timescale_update", [session, {"s999": {"s": []}}])
            )
        else:
            await original(session, source, index, count)

    socket.emit = emit
    await p.subscribe(pairs)
    p.socket = socket
    result = await p.fetch_many([(pair, end - timedelta(minutes=1), end, 3) for pair in pairs])
    assert isinstance(result[pairs[0][0].canonical_symbol], ValueError)
    assert isinstance(result[pairs[1][0].canonical_symbol], list)
    await p.disconnect()


@pytest.mark.integration
async def test_partial_concurrent_cycle_persists_siblings_and_backfills_in_next_cycle(
    store, monkeypatch
):
    p, _, pairs, end = pipeline(count=17, window=4)
    await p.subscribe(pairs)
    await store.register(Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in pairs]))
    clock = [NOW]
    drop = [set()]

    async def connect():
        p.connection_generation += 1
        p.socket = SyntheticChartSocket(clock[0].replace(second=0), latency=0.001, drop=drop[0])
        p.transport_connected()

    monkeypatch.setattr(p, "connect", connect)
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: clock[0])
    monkeypatch.setattr("marketdata.providers.tradingview_pipeline.utc_now", lambda: clock[0])
    monkeypatch.setattr("marketdata.providers.candles.utc_now", lambda: clock[0])
    service = CandleIngestion(p.settings, store, p, Fanout(100, 1, p.metrics))
    await service.cycle(clock[0])
    drop[0] = {3}
    clock[0] += timedelta(minutes=1)
    await service.cycle(clock[0])
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 33
    assert p.progress[pairs[3][0].canonical_symbol]["missing_count"] == 1
    assert p.health.state == "degraded"
    drop[0] = set()
    clock[0] += timedelta(minutes=1)
    service.retry_at = 0  # Deterministic elapsed backoff gate; no wall-clock wait in tests.
    await service.cycle(clock[0])
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 51
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
    assert p.health.state == "healthy"
    assert all(s["missing_count"] == 0 for s in p.progress.values())


async def test_overrun_reported_once_and_does_not_spawn_another_cycle():
    p = Replay()
    service = CandleIngestion(Settings(_env_file=None), None, p, None)
    deadline = NOW + timedelta(minutes=1)
    assert not service.observe_overrun(deadline, NOW)
    assert service.observe_overrun(deadline, deadline)
    assert service.observe_overrun(deadline, deadline + timedelta(minutes=3))
    assert p.metrics.candle_overruns.labels(p.name)._value.get() == 1
    assert not p.calls


async def test_running_cycle_overrun_timer_is_owned_and_cancelled_on_shutdown(monkeypatch):
    p = Replay()
    service = CandleIngestion(Settings(_env_file=None), None, p, None)
    clock = [NOW]
    entered, blocked = asyncio.Event(), asyncio.Event()
    calls, alarms = [], []
    loop = asyncio.get_running_loop()
    original_timer = loop.call_later

    def timer(delay, callback, *args, **kwargs):
        handle = original_timer(3600, callback, *args, **kwargs)
        alarms.append((handle, callback))
        return handle

    async def cycle():
        calls.append(clock[0])
        entered.set()
        await blocked.wait()

    monkeypatch.setattr(loop, "call_later", timer)
    monkeypatch.setattr(service, "cycle", cycle)
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: clock[0])
    task = asyncio.create_task(service.run())
    await entered.wait()
    clock[0] += timedelta(minutes=2)
    alarms[0][1]()
    assert p.metrics.candle_overruns.labels(p.name)._value.get() == 1
    assert len(calls) == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(handle.cancelled() for handle, _ in alarms)


async def test_connect_rejection_backs_off_without_creating_more_connections(monkeypatch):
    from websockets.datastructures import Headers
    from websockets.exceptions import InvalidStatus
    from websockets.http11 import Response

    p, _, pairs, _ = pipeline(count=1, window=4)
    await p.subscribe(pairs)
    calls = []

    async def connect(*args, **kwargs):
        calls.append(1)
        raise InvalidStatus(Response(429, "Too Many Requests", Headers()))

    monkeypatch.setattr("marketdata.providers.tradingview_candles.connect", connect)
    with pytest.raises(InvalidStatus):
        await p.connect()
    assert len(calls) == 1 and p.effective_concurrency == 2
    assert p.metrics.tradingview_throttle.labels("http_429")._value.get() == 1
    assert p.socket is None


async def test_auth_send_disconnect_is_measured_and_reduces_window(monkeypatch):
    from websockets.exceptions import ConnectionClosedOK

    p, socket, pairs, _ = pipeline(count=1, window=4)
    await p.subscribe(pairs)

    async def send(raw):
        raise ConnectionClosedOK(None, None)

    async def connect(*args, **kwargs):
        return socket

    socket.send = send
    monkeypatch.setattr("marketdata.providers.tradingview_candles.connect", connect)
    with pytest.raises(ConnectionClosedOK):
        await p.connect()
    assert p.metrics.tradingview_disconnects._value.get() == 1
    assert p.effective_concurrency == 2
    await p.disconnect()


@pytest.mark.parametrize("count,duplicate", [(101, False), (2, True)])
async def test_invalid_job_universe_rejected_before_any_dispatch(count, duplicate):
    p, socket, pairs, end = pipeline(count=count)
    await p.subscribe(pairs)
    p.socket = socket
    jobs = [(pair, end - timedelta(minutes=1), end, 3) for pair in pairs]
    if duplicate:
        jobs[1] = jobs[0]
    with pytest.raises(ValueError, match="unique candle jobs"):
        await p.fetch_many(jobs)
    assert not socket.requests
    no_receiver_leaks(p)
    await p.disconnect()


@pytest.mark.parametrize(
    "changes",
    [
        {"tradingview_candle_concurrency": 0},
        {"tradingview_candle_concurrency": 13},
        {"candle_cycle_safety_margin_seconds": 0},
        {"candle_cycle_safety_margin_seconds": 30, "candle_fetch_grace_seconds": 30},
    ],
)
def test_window_and_budget_configuration_validation(changes):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **changes)


@pytest.mark.integration
@pytest.mark.parametrize("count", [17, 100, 500])
async def test_large_candle_batch_is_atomic_idempotent_and_correction_safe(store, count):
    pairs = synthetic_pairs(count)
    await store.register(Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in pairs]))
    candles = [make_candle(pair, realtime_status="simulated") for pair in pairs]
    coverage = [
        (
            c.instrument_id,
            c.provider,
            c.bucket_start,
            c.bucket_end,
            {"session": "24x7", "timezone": "Etc/UTC"},
        )
        for c in candles
    ]
    assert await store.write_candles(candles, coverage) == count
    assert await store.write_candles(candles, coverage) == 0
    corrected = candles[0].model_copy(
        update={
            "close": Decimal("100.5"),
            "received_timestamp": candles[0].received_timestamp + timedelta(seconds=1),
        }
    )
    assert await store.write_candles([corrected], coverage) == 1
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == count
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") == 1
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
