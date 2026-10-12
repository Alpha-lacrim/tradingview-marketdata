"""Pool ownership, failures, deadlines, resource limits and durable recovery."""

import asyncio
from datetime import timedelta
from decimal import Decimal

import pytest
from test_native_candles import NOW
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from marketdata.candle_benchmark import synthetic_pairs
from marketdata.candle_ingestion import CandleIngestion
from marketdata.config import Settings
from marketdata.domain import DataStatus, RegistryEntry
from marketdata.fanout import Fanout
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentConnectionError
from marketdata.providers.candle_pool import CandleConnectionPool, PoolLimits
from marketdata.providers.synthetic_candles import SyntheticCandleProvider
from marketdata.providers.tradingview import encode_message
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


def make_pool(count=34, connections=2, window=2, global_window=4, **options):
    deadline = options.pop("deadline", 20)
    latency = options.pop("latency", 0.002)
    request_timeout = options.pop("request_timeout", 5)
    settings = Settings(
        _env_file=None,
        candle_pool_connections=connections,
        candle_pool_worker_capacity=max(1, (count + connections - 1) // connections),
        candle_pool_max_instruments=max(1, count),
        candle_pool_concurrency=global_window,
        candle_pool_deadline_seconds=deadline,
        candle_fetch_grace_seconds=0,
        candle_request_timeout_seconds=request_timeout,
        tradingview_candle_concurrency=window,
    )
    metrics = Metrics()
    end = NOW.replace(second=0)
    pool = CandleConnectionPool(
        "benchmark",
        settings,
        metrics,
        lambda _: SyntheticCandleProvider(
            settings, metrics, end, latency=latency, handshake_seconds=0, **options
        ),
        PoolLimits(32, 4096, 4096, 128),
    )
    return pool, synthetic_pairs(count), end


def jobs(pairs, end, minutes=3):
    return [(pair, end - timedelta(minutes=minutes), end, minutes + 1) for pair in pairs]


async def run(pool, pairs, end):
    await pool.subscribe(pairs)
    await pool.connect()
    try:
        return await pool.fetch_many(jobs(pairs, end))
    finally:
        await pool.disconnect()


def clean(pool):
    assert not pool.tasks
    assert len(asyncio.all_tasks()) == 1  # Includes unnamed capacity-acquisition tasks.
    assert pool.health.candle_retrieval["active_requests"] == 0
    assert not [
        t
        for t in asyncio.all_tasks()
        if t.get_name().startswith(
            ("candle-pool-", "tradingview-candle-receiver", "synthetic-chart-emitter")
        )
    ]
    for w in pool.workers:
        assert w.provider.socket is None and not w.provider.fetching
        assert all(s.closed and not s.sessions and not s.tasks for s in w.provider.sockets)


@pytest.mark.parametrize("count,connections", [(17, 1), (64, 4), (128, 8), (256, 16), (512, 16)])
@pytest.mark.parametrize("global_window", [1, 4, 12])
async def test_large_pool_exact_ownership_and_request_bounds(count, connections, global_window):
    pool, pairs, end = make_pool(
        count, connections, window=4, global_window=global_window, latency=0
    )
    results = await run(pool, pairs, end)
    assert len(results) == count
    sessions = []
    for n, pair in enumerate(pairs):
        candles = results[pair[0].canonical_symbol]
        assert isinstance(candles, list) and len(candles) == 3
        assert all(
            c.instrument_id == pair[0].id
            and c.close == Decimal(101 + n)
            and c.bucket_end <= end
            and c.realtime_status == DataStatus.SIMULATED
            for c in candles
        )
        assert pool.owners[pair[0].canonical_symbol] == n % connections
    for w in pool.workers:
        for s in w.provider.sockets:
            assert s.max_active <= 4
            sessions.extend(s.created)
    assert len(sessions) == len(set(sessions)) == count
    assert pool.health.candle_retrieval["peak_inflight"] <= global_window
    ownership = dict(pool.owners)
    clean(pool)
    await pool.subscribe(list(reversed(pairs)))
    assert pool.owners == ownership


async def test_bounds_reject_duplicate_overflow_and_wrong_identity():
    pool, pairs, end = make_pool(count=4)
    with pytest.raises(ValueError):
        await pool.subscribe([*pairs, pairs[0]])
    with pytest.raises(ValueError):
        await pool.subscribe(synthetic_pairs(5))
    await pool.subscribe(pairs)
    await pool.connect()
    with pytest.raises(RuntimeError):
        await pool.subscribe(pairs)
    with pytest.raises(ValueError):
        await pool.fetch_many(
            jobs(
                [(pairs[0][0], pairs[0][1].model_copy(update={"provider_symbol": "BENCH:S9999"}))],
                end,
            )
        )
    with pytest.raises(ValueError):
        await pool.fetch_many(jobs([pairs[0], pairs[0]], end))
    await pool.disconnect()


async def test_declared_permissions_and_live_guest_guard(monkeypatch):
    pool, pairs, _ = make_pool()
    with pytest.raises(ValueError, match="declared provider limits"):
        CandleConnectionPool(
            "benchmark",
            pool.settings,
            pool.metrics,
            lambda _: pool.workers[0].provider,
            PoolLimits(1, 100, 100, 2),
        )
    provider = TradingViewCandleProvider(pool.settings, Metrics())
    await provider.subscribe(pairs)

    async def forbidden(*args, **kwargs):
        pytest.fail("permission guard must run before network")

    monkeypatch.setattr("marketdata.providers.tradingview_candles.connect", forbidden)
    with pytest.raises(PermanentConnectionError, match="permissions_unverified"):
        await provider.connect()


async def test_handshake_failure_isolated_backoff_and_next_cycle_recovery():
    pool, pairs, end = make_pool(count=8)
    pool.workers[0].provider.fail_handshake = True
    results = await run(pool, pairs, end)
    assert sum(isinstance(v, list) for v in results.values()) == 4
    assert pool.workers[0].retry_at > 0 and pool.workers[1].retry_at == 0
    clean(pool)
    pool.workers[0].provider.fail_handshake = False
    await pool.connect()
    results = await pool.fetch_many(jobs(pairs, end))
    await pool.disconnect()
    assert sum(isinstance(v, list) for v in results.values()) == 4
    assert len(pool.workers[0].provider.sockets) == 0  # no intracycle retry/growth
    pool.workers[0].retry_at = 0
    await pool.connect()
    results = await pool.fetch_many(jobs(pairs, end))
    await pool.disconnect()
    assert all(isinstance(v, list) for v in results.values())
    clean(pool)


async def test_deadline_retains_completed_results_in_same_worker_and_other_workers():
    pool, pairs, end = make_pool(count=20, delays={0: 2}, deadline=0.5)
    result = await run(pool, pairs, end)
    assert isinstance(result[pairs[0][0].canonical_symbol], TimeoutError)
    assert sum(isinstance(v, list) for v in result.values()) >= 10
    assert len(result) == 20
    clean(pool)


async def test_completed_native_missing_bar_is_success_not_transport_error():
    pool, pairs, end = make_pool(count=8, missing={0, 1})
    result = await run(pool, pairs, end)
    assert all(isinstance(v, list) for v in result.values())
    assert len(result[pairs[0][0].canonical_symbol]) == 2
    assert pool.health.candle_retrieval["timeouts"] == 0
    assert all(w.retry_at == 0 for w in pool.workers)
    clean(pool)


async def test_subset_cycle_does_not_recount_idle_worker_diagnostics():
    pool, pairs, end = make_pool(count=8)
    await run(pool, pairs, end)
    pool.workers[1].provider.health.candle_retrieval["timeouts"] = 3
    await pool.connect()
    result = await pool.fetch_many(jobs([pairs[0]], end))
    await pool.disconnect()
    assert len(result) == 1 and isinstance(result[pairs[0][0].canonical_symbol], list)
    assert pool.health.candle_retrieval["timeouts"] == 0
    assert pool.health.candle_retrieval["sockets"] == 1
    clean(pool)


async def test_wrong_worker_and_stale_sessions_ignored(monkeypatch):
    pool, pairs, end = make_pool(count=8, delays={0: 0.04, 1: 0.04})
    await pool.subscribe(pairs)
    await pool.connect()
    task = asyncio.create_task(pool.fetch_many(jobs(pairs, end)))
    await asyncio.gather(*(w.provider.connected.wait() for w in pool.workers))
    a, b = [w.provider.sockets[-1] for w in pool.workers]
    await a.wait_for_requests(1)
    await b.wait_for_requests(1)
    foreign = a.created[0]
    await b.messages.put(encode_message("series_completed", [foreign, "s1", "streaming", "s1"]))
    await b.messages.put(
        encode_message("series_completed", ["cs_w01000000000000", "s1", "streaming", "s1"])
    )
    result = await task
    await pool.disconnect()
    assert all(isinstance(v, list) for v in result.values())
    assert pool.workers[1].provider.health.candle_retrieval["ignored_responses"] >= 2
    clean(pool)


async def test_disconnect_partial_results_and_ownership_survive():
    pool, pairs, end = make_pool(count=12, delays={0: 0.05})
    await pool.subscribe(pairs)
    owners = dict(pool.owners)
    await pool.connect()
    task = asyncio.create_task(pool.fetch_many(jobs(pairs, end)))
    await pool.workers[0].provider.connected.wait()
    socket = pool.workers[0].provider.sockets[-1]
    await socket.wait_for_requests(3)
    await socket.messages.put(ConnectionError("synthetic_disconnect"))
    result = await task
    await pool.disconnect()
    assert sum(isinstance(v, list) for v in result.values()) >= 7
    assert pool.owners == owners
    clean(pool)


@pytest.mark.parametrize("phase", ["handshake", "requests", "global_wait"])
async def test_cancellation_joins_all_tasks_and_releases_capacity(phase):
    pool, pairs, end = make_pool(count=64, connections=4, drop=set(range(64)), global_window=1)
    if phase == "handshake":
        for w in pool.workers:
            w.provider.handshake_seconds = 1
    await pool.subscribe(pairs)
    await pool.connect()
    task = asyncio.create_task(pool.fetch_many(jobs(pairs, end)))
    await asyncio.sleep(0.015)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await pool.disconnect()
    clean(pool)


async def test_large_native_parse_yields_to_shutdown_before_finishing_history(monkeypatch):
    pool, pairs, end = make_pool(count=1, connections=1, latency=0)
    provider = pool.workers[0].provider
    first_bar = asyncio.Event()
    parsed = 0
    original = provider._parse_native_bar

    def observe(*args):
        nonlocal parsed
        parsed += 1
        first_bar.set()
        return original(*args)

    monkeypatch.setattr(provider, "_parse_native_bar", observe)
    await pool.subscribe(pairs)
    await pool.connect()
    task = asyncio.create_task(pool.fetch_many(jobs(pairs, end, minutes=300)))
    await first_bar.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await pool.disconnect()
    assert 0 < parsed < 300
    clean(pool)


async def test_provider_wide_429_backoff_does_not_multiply_connections(monkeypatch):
    pool, pairs, end = make_pool(count=8)

    async def rejected():
        raise InvalidStatus(Response(429, "Too Many Requests", []))

    monkeypatch.setattr(pool.workers[0].provider, "connect", rejected)
    await run(pool, pairs, end)
    assert pool.retry_at > 0 and pool.effective_concurrency == 2
    before = sum(len(w.provider.sockets) for w in pool.workers)
    await pool.connect()
    result = await pool.fetch_many(jobs(pairs, end))
    await pool.disconnect()
    assert all(isinstance(v, ConnectionError) for v in result.values())
    assert sum(len(w.provider.sockets) for w in pool.workers) == before
    clean(pool)


async def test_merge_never_accepts_foreign_worker_keys_or_candles(monkeypatch):
    pool, pairs, end = make_pool(count=8)
    original = pool.workers[0].provider.fetch_many

    async def foreign(jobs):
        result = await original(jobs)
        wrong = result[pairs[0][0].canonical_symbol][0].model_copy(
            update={"instrument_id": pairs[1][0].id}
        )
        result[pairs[0][0].canonical_symbol] = [wrong]
        result[pairs[1][0].canonical_symbol] = [wrong]
        return result

    monkeypatch.setattr(pool.workers[0].provider, "fetch_many", foreign)
    result = await run(pool, pairs, end)
    assert isinstance(result[pairs[0][0].canonical_symbol], ValueError)
    assert isinstance(result[pairs[1][0].canonical_symbol], list)
    assert len(result) == 8
    clean(pool)


async def test_cleanup_deadline_blocks_replacement_connections(monkeypatch):
    pool, pairs, end = make_pool(count=8)
    pool.settings.candle_pool_cleanup_seconds = 0.03
    worker = pool.workers[0].provider
    original = worker.disconnect

    async def blocked():
        await asyncio.Event().wait()

    monkeypatch.setattr(worker, "disconnect", blocked)
    await run(pool, pairs, end)
    assert pool.stopped is not None
    assert pool.health.candle_retrieval["workers"]["w00"]["cleanup_error"]
    with pytest.raises(PermanentConnectionError, match="cleanup_failed"):
        await pool.connect()
    monkeypatch.setattr(worker, "disconnect", original)
    await pool.disconnect()
    clean(pool)


@pytest.mark.integration
async def test_pool_persistence_partial_cycle_recovery_and_idempotency(store, monkeypatch):
    pool, pairs, end = make_pool(count=128, connections=8, global_window=12)
    await pool.subscribe(pairs)
    await store.register(Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in pairs]))
    clock = [NOW]
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: clock[0])
    monkeypatch.setattr("marketdata.providers.candles.utc_now", lambda: clock[0])
    pool.workers[0].provider.fail_handshake = True
    service = CandleIngestion(pool.settings, store, pool, Fanout(256, 1, pool.metrics))
    await service.cycle(clock[0])
    assert service.retry_at == 0  # healthy workers are not held by failed worker's backoff
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 112
    failed = pool.progress[pairs[0][0].canonical_symbol]
    # Failed first handshake has no verified session: UNKNOWN, never invented EXPECTED.
    assert failed["missing_count"] == 0 and failed["unknown_count"] == 1
    assert all(
        pool.progress[pairs[n][0].canonical_symbol]["missing_count"] == 0
        for n in range(128)
        if n % 8
    )
    pool.workers[0].provider.fail_handshake = False
    pool.workers[0].retry_at = 0
    clock[0] += timedelta(minutes=1)
    for w in pool.workers:
        w.provider.end = clock[0].replace(second=0)
    await service.cycle(clock[0])
    assert all(s["missing_count"] == 0 for s in pool.progress.values())
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 256
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
    await pool.connect()
    bars = [
        c
        for v in (await pool.fetch_many(jobs(pairs, end + timedelta(minutes=1)))).values()
        if isinstance(v, list)
        for c in v
        if c.bucket_start >= end - timedelta(minutes=1)
    ]
    await pool.disconnect()
    assert await store.write_candles(bars, []) == 0
    clean(pool)
