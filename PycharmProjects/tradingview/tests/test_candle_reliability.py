"""Deterministic native candle reliability tests; no live market access."""

import copy
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from test_native_candles import NOW, RAW_CAPTURED, REGISTRY, Replay, captured, make_candle, source

from marketdata.candle_ingestion import CandleIngestion
from marketdata.config import Settings
from marketdata.fanout import Fanout
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry
from marketdata.sessions import Expectation, classify_ranges, expectation

FX = {"session": "1700-1700", "timezone": "America/New_York"}
CRYPTO = {"session": "24x7", "timezone": "Etc/UTC"}


@pytest.mark.parametrize(
    "info,bucket,state",
    [
        (CRYPTO, "2026-10-10T12:00Z", "EXPECTED"),
        (FX, "2026-10-10T12:00Z", "NOT_EXPECTED"),
        (FX, "2026-10-09T20:59Z", "EXPECTED"),
        (FX, "2026-10-09T21:00Z", "NOT_EXPECTED"),
        (FX, "2026-10-11T20:59Z", "NOT_EXPECTED"),
        (FX, "2026-10-11T21:00Z", "EXPECTED"),
        ({}, "2026-10-10T12:00Z", "UNKNOWN"),
        ({"timezone": None, "session": "24x7"}, "2026-10-10T12:00Z", "UNKNOWN"),
        ({"timezone": "Etc/UTC", "session": None}, "2026-10-10T12:00Z", "UNKNOWN"),
        ({"session": "unsupported", "timezone": "Etc/UTC"}, "2026-10-07T12:00Z", "UNKNOWN"),
        (
            {"session": "1800-1700", "timezone": "America/New_York"},
            "2026-10-07T21:30Z",
            "NOT_EXPECTED",
        ),
        ({**FX, "uncertain_break": "1659-1805"}, "2026-10-07T21:30Z", "UNKNOWN"),
        ({**FX, "corrections": "1700-1430:20261007"}, "2026-10-07T12:00Z", "UNKNOWN"),
        (
            {
                "session": "0930-1600",
                "timezone": "America/New_York",
                "session_holidays": "20261007",
            },
            "2026-10-07T15:00Z",
            "NOT_EXPECTED",
        ),
        (
            {"session": "0930-1600#20261007/1000-1600", "timezone": "Etc/UTC"},
            "2026-10-08T09:30Z",
            "EXPECTED",
        ),
        (
            {"session": "0930-1600#20261007/1000-1600", "timezone": "Etc/UTC"},
            "2026-10-12T09:30Z",
            "NOT_EXPECTED",
        ),
    ],
)
def test_session_classification(info, bucket, state):
    assert expectation(info, datetime.fromisoformat(bucket)) == state


def test_all_captured_sessions_and_weekend():
    for row in captured():
        info = source(row)
        saturday = datetime(2026, 10, 10, 12, tzinfo=UTC)
        assert expectation(info, saturday) == (
            Expectation.EXPECTED
            if row["symbol"].startswith(("BINANCE:", "OKX:"))
            else Expectation.NOT_EXPECTED
        )
    start = datetime(2026, 10, 10, tzinfo=UTC)
    ranges = classify_ranges(FX, start, start + timedelta(days=1))
    assert ranges["EXPECTED"] == [] and ranges["NOT_EXPECTED"] == [
        (start, start + timedelta(days=1))
    ]


async def setup(store, pairs=1, **settings):
    registry = Registry.load(REGISTRY)
    await store.register(registry)
    p = Replay()
    await p.subscribe(registry.subscriptions("tradingview_guest")[:pairs])
    service = CandleIngestion(
        Settings(_env_file=None, **settings), store, p, Fanout(100, 2, p.metrics)
    )
    return p, service


@pytest.mark.integration
async def test_once_per_minute_overlap_and_latency_stages(store, monkeypatch):
    p, service = await setup(store)
    clock = [NOW]
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: clock[0])
    original = p.fetch_completed

    async def fetch(*args):
        clock[0] += timedelta(seconds=3)
        return await original(*args)

    p.fetch_completed = fetch
    original_write = store.write_candles

    async def write(*args):
        clock[0] += timedelta(seconds=0.2)
        return await original_write(*args)

    store.write_candles = write
    await service.cycle(NOW)
    summary = p.health.last_candle_cycle
    assert summary["scheduler_lateness_seconds"] == 0
    assert summary["close_to_commit_seconds"] == pytest.approx(5.2)
    assert summary["db_write_started_at"] != summary["fetch_cycle_started_at"]
    assert all(v >= 0 for k, v in summary.items() if k.endswith("_seconds"))
    await service.cycle(NOW + timedelta(seconds=20))
    assert len(p.calls) == 1
    clock[0] = NOW + timedelta(minutes=4)
    await service.cycle(clock[0])
    assert p.calls[-1][0] <= NOW + timedelta(minutes=1)
    assert p.metrics.candle_cycle.labels(p.name)._value.get() == 2
    assert p.metrics.received.labels(p.name)._value.get() == 0


@pytest.mark.integration
async def test_weekend_restart_no_false_gaps_and_unknown_exposed(store, monkeypatch):
    p, service = await setup(store)
    pair = p.subscriptions[0]
    symbol = pair[0].canonical_symbol
    p.session_info[symbol] = FX
    friday = datetime(2026, 10, 9, 21, 0, 2, tzinfo=UTC)
    monkeypatch.setattr("test_native_candles.NOW", friday + timedelta(days=3))
    await service.cycle(friday)
    p.omit.update(friday.replace(second=0) + timedelta(minutes=k) for k in range(2 * 1440))
    await service.cycle(friday + timedelta(days=1))
    state = await store.candle_progress(pair[0].id, p.name)
    assert state["missing_count"] == 0 and state["not_expected_count"] == 1440
    resumed = Replay()
    await resumed.subscribe([pair])
    resumed.session_info = {}
    resumed.omit = p.omit
    restart = CandleIngestion(
        Settings(_env_file=None), store, resumed, Fanout(100, 2, resumed.metrics)
    )
    await restart.cycle(friday + timedelta(days=1, minutes=1))
    resumed.refresh_health(friday + timedelta(days=1, minutes=1))
    assert resumed.health.state == "healthy"
    assert resumed.health.symbols[symbol].session_expectation == "NOT_EXPECTED"
    # Missing minutes for unsupported schedules are uncertainty, not asserted loss.
    resumed.session_info[symbol] = {"session": "unverified", "timezone": "Etc/UTC"}
    await restart.cycle(friday + timedelta(days=1, minutes=2))
    state = await store.candle_progress(pair[0].id, p.name)
    assert state["unknown_count"] > 0 and state["missing_count"] == 0
    resumed.refresh_health(friday + timedelta(days=1, minutes=2))
    assert resumed.health.state == "degraded"


@pytest.mark.integration
@pytest.mark.parametrize(
    "change",
    [
        {"close": Decimal("100")},
        {"high": Decimal("103"), "low": Decimal("98")},
        {"volume": Decimal("11")},
        {"source_status": "realtime"},
    ],
)
async def test_corrections_idempotent_audited_and_roll_up(store, change):
    p, service = await setup(store)
    pair = p.subscriptions[0]
    first = make_candle(pair)
    bars = [make_candle(pair, first.bucket_start + timedelta(minutes=k)) for k in range(5)]
    coverage = [(pair[0].id, p.name, first.bucket_start, bars[-1].bucket_end)]
    await store.write_candles(bars, coverage)
    unchanged = first.model_copy(
        update={"received_timestamp": first.received_timestamp + timedelta(seconds=1)}
    )
    assert await store.write_candles([unchanged], coverage) == 0
    correction = first.model_copy(
        update={**change, "received_timestamp": first.received_timestamp + timedelta(seconds=2)}
    )
    assert await store.write_candles([correction], coverage) == 1
    assert set(store.last_candle_write["corrections"][0]["changed_fields"]) == set(change)
    repeat = correction.model_copy(
        update={"received_timestamp": first.received_timestamp + timedelta(seconds=3)}
    )
    assert await store.write_candles([repeat], coverage) == 0
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") == 1
    for interval in ("5m", "15m", "1h", "1d"):
        rows = await store.native_candles(
            pair[0].id,
            p.name,
            first.bucket_start.replace(hour=0, minute=0),
            first.bucket_start.replace(hour=0, minute=0) + timedelta(days=1),
            interval,
            100,
        )
        assert max(r["high"] for r in rows) == change.get("high", Decimal("102"))
        assert min(r["low"] for r in rows) == change.get("low", Decimal("99"))
        assert sum(r["volume"] for r in rows) == (51 if "volume" in change else 50)


@pytest.mark.integration
async def test_overlap_automatically_discovers_correction(store):
    p, service = await setup(store)
    await service.cycle(NOW)
    original = p.fetch_completed

    async def corrected(*args):
        bars = await original(*args)
        return [
            c.model_copy(
                update={
                    "close": Decimal("100"),
                    "received_timestamp": c.received_timestamp + timedelta(seconds=1),
                }
            )
            for c in bars
        ]

    p.fetch_completed = corrected
    await service.cycle(NOW + timedelta(minutes=1))
    assert len(store.last_candle_write["corrections"]) == 1
    await service.cycle(NOW + timedelta(minutes=2))
    assert not store.last_candle_write["corrections"]


@pytest.mark.integration
async def test_network_outage_short_and_outside_horizon(store):
    p, service = await setup(store, candle_backfill_minutes=3)
    await service.cycle(NOW)
    p.fail = True
    await service.cycle(NOW + timedelta(minutes=2))
    assert (await store.candle_progress(p.subscriptions[0][0].id, p.name))["missing_count"] == 2
    p.fail = False
    service.retry_at = 0
    await service.cycle(NOW + timedelta(minutes=3))
    state = await store.candle_progress(p.subscriptions[0][0].id, p.name)
    assert state["missing_count"] == 0
    await service.cycle(NOW + timedelta(minutes=10))
    state = await store.candle_progress(p.subscriptions[0][0].id, p.name)
    assert state["missing_count"] == 4
    p.refresh_health(NOW + timedelta(minutes=10))
    health = p.health.symbols[p.subscriptions[0][0].canonical_symbol]
    assert health.outside_horizon_missing_count == 4 and p.health.state == "degraded"


@pytest.mark.integration
@pytest.mark.parametrize("outage_minutes", [3, 10])
async def test_db_read_outage_recovery_uses_actual_state(store, outage_minutes):
    p, service = await setup(store, candle_backfill_minutes=3)
    await service.cycle(NOW)
    original = store.candle_progress

    async def unavailable(*args):
        raise ConnectionError("controlled DB unavailable")

    store.candle_progress = unavailable
    with pytest.raises(ConnectionError):
        await service.cycle(NOW + timedelta(minutes=1))
    assert not service.database_healthy
    assert p.metrics.candles.labels(p.name, "persisted")._value.get() == 1
    store.candle_progress = original
    await service.cycle(NOW + timedelta(minutes=outage_minutes))
    state = await original(p.subscriptions[0][0].id, p.name)
    assert state["missing_count"] == max(0, outage_minutes - 3)
    assert service.database_healthy


@pytest.mark.integration
async def test_db_write_failure_never_acknowledged_before_success(store, monkeypatch):
    p, service = await setup(store)
    original = store.write_candles
    attempts = []

    async def write(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise ConnectionError("controlled write failure")
        assert not service.database_healthy
        assert p.metrics.candles.labels(p.name, "persisted")._value.get() == 0
        async with store.connection() as c:
            assert await c.fetchval("SELECT count(*) FROM market_candles") == 0
        return await original(*args)

    store.write_candles = write
    monkeypatch.setattr("marketdata.candle_ingestion.random.uniform", lambda *args: 0)
    await service.cycle(NOW)
    assert len(p.calls) == 1 and len(attempts) == 2
    assert p.health.last_candle_cycle["db_failures"] == 1
    assert service.pending == []


@pytest.mark.integration
async def test_restart_different_instrument_progress_and_forming_excluded(store):
    p, service = await setup(store, pairs=2)
    await service.cycle(NOW)
    pair = p.subscriptions[0]
    first = make_candle(pair)
    extra = make_candle(pair, first.bucket_start + timedelta(minutes=1))
    await store.write_candles([extra], [(pair[0].id, p.name, first.bucket_start, extra.bucket_end)])
    resumed = Replay()
    await resumed.subscribe(p.subscriptions)
    recovery = CandleIngestion(
        Settings(_env_file=None), store, resumed, Fanout(100, 2, resumed.metrics)
    )
    await recovery.cycle(NOW + timedelta(minutes=4, seconds=18))
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 10
        assert await c.fetchval("SELECT max(bucket_end) FROM market_candles") == NOW.replace(
            second=0
        ) + timedelta(minutes=4)
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
    assert all(s["missing_count"] == 0 for s in resumed.progress.values())


async def test_pipeline_interleaving_two_requests_routes_exact_symbols():
    from marketdata.observability import Metrics
    from marketdata.providers.tradingview import FrameDecoder, encode_message

    rows = RAW_CAPTURED[:2]
    pairs = Registry.load(REGISTRY).subscriptions("tradingview_guest")[:2]

    class InterleavedSocket:
        def __init__(self):
            self.sessions = []
            self.messages = []
            self.max_active = 0
            self.active = 0

        async def send(self, raw):
            message = json.loads(FrameDecoder().feed(raw)[0])
            kind, params = message["m"], message["p"]
            if kind == "chart_create_session":
                self.sessions.append(params[0])
                self.active += 1
                self.max_active = max(self.active, self.max_active)
            if kind == "chart_delete_session":
                self.active -= 1
            if kind == "create_series" and len(self.sessions) == 2:
                # Complete the second request first, sharing one receive loop.
                for index in (1, 0):
                    for m in rows[index]["messages"]:
                        m = copy.deepcopy(m)
                        m["p"][0] = self.sessions[index]
                        self.messages.append(encode_message(m["m"], m["p"]))

        async def recv(self):
            return self.messages.pop(0)

    socket = InterleavedSocket()
    provider = TradingViewCandleProvider(Settings(_env_file=None), Metrics())
    await provider.subscribe(pairs)
    provider.socket = socket
    start = datetime(2026, 10, 7, 9, 23, tzinfo=UTC)
    result = await provider.fetch_many([(p, start, start + timedelta(minutes=4), 5) for p in pairs])
    assert socket.max_active == 2 and socket.active == 0
    assert all(isinstance(v, list) and len(v) == 4 for v in result.values()), result
    assert all(
        all(c.canonical_symbol == symbol for c in candles) for symbol, candles in result.items()
    )
    assert set(provider.timings) == {p[0].canonical_symbol for p in pairs}


@pytest.mark.integration
async def test_first_last_and_database_timings_are_separate(store, monkeypatch):
    p, service = await setup(store, pairs=2)
    clock = [NOW]
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: clock[0])
    original = p.fetch_many

    async def fetch(requests):
        result = await original(requests)
        for index, (pair, _, _, _) in enumerate(requests):
            available = NOW + timedelta(seconds=index + 1)
            p.timings[pair[0].canonical_symbol] = {
                "completed_candle_available_at": available.isoformat()
            }
        clock[0] = NOW + timedelta(seconds=3)
        return result

    p.fetch_many = fetch
    write = store.write_candles

    async def slow_write(*args):
        clock[0] += timedelta(seconds=1)
        return await write(*args)

    store.write_candles = slow_write
    await service.cycle(NOW + timedelta(seconds=0.5))
    timing = p.health.last_candle_cycle
    assert timing["scheduler_lateness_seconds"] == 0.5
    assert timing["close_to_first_receive_seconds"] == 3
    assert timing["close_to_last_receive_seconds"] == 4
    assert timing["first_to_last_receive_seconds"] == 1
    assert timing["close_to_commit_seconds"] == 6
    assert timing["db_write_started_at"] == (NOW + timedelta(seconds=3)).isoformat()


@pytest.mark.integration
async def test_oil_correction_keeps_null_volume_and_no_duplicate_fanout(store):
    registry = Registry.load(REGISTRY)
    await store.register(registry)
    pair = next(
        p
        for p in registry.subscriptions("tradingview_guest")
        if p[1].provider_symbol == "TVC:USOIL"
    )
    first = make_candle(pair, volume=None, metadata={"volume_additive": False})
    coverage = [(pair[0].id, pair[1].provider, first.bucket_start, first.bucket_end)]
    await store.write_candles([first], coverage)
    corrected = first.model_copy(
        update={
            "high": Decimal("103"),
            "received_timestamp": first.received_timestamp + timedelta(seconds=1),
        }
    )
    await store.write_candles([corrected], coverage)
    rows = await store.native_candles(
        pair[0].id,
        pair[1].provider,
        first.bucket_start.replace(hour=0, minute=0),
        first.bucket_start.replace(hour=0, minute=0) + timedelta(days=1),
        "1d",
        10,
    )
    assert rows[0]["volume"] is None and rows[0]["high"] == 103
    assert store.last_candle_write["corrections"][0]["changed_fields"] == ["high"]
    await store.write_candles([corrected], coverage)
    assert store.last_candle_write["changed_keys"] == []


async def test_timeout_is_visible_per_symbol_and_never_finalized(monkeypatch):
    import asyncio

    from marketdata.observability import Metrics

    class SilentSocket:
        async def send(self, message):
            pass

        async def recv(self):
            await asyncio.Event().wait()

    pair = Registry.load(REGISTRY).subscriptions("tradingview_guest")[0]
    p = TradingViewCandleProvider(
        Settings(_env_file=None, candle_request_timeout_seconds=1), Metrics()
    )
    await p.subscribe([pair])
    p.socket = SilentSocket()
    end = NOW.replace(second=0)
    result = await p.fetch_many([(pair, end - timedelta(minutes=1), end, 3)])
    assert isinstance(result[pair[0].canonical_symbol], TimeoutError)
    assert p.timings[pair[0].canonical_symbol]["duration_seconds"] >= 1
    assert p.timings[pair[0].canonical_symbol]["completed_candle_available_at"] is None
    assert p.health.symbols[pair[0].canonical_symbol].last_successful_fetch_at is None


@pytest.mark.integration
async def test_historical_reconciliation_proves_partitions(store):
    import importlib.util

    spec = importlib.util.spec_from_file_location("reconciliation", "scripts/reconcile_candles.py")
    utility = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(utility)
    p, service = await setup(store, candle_backfill_minutes=3)
    await service.cycle(NOW)
    await service.cycle(NOW + timedelta(minutes=5))
    first = NOW.replace(second=0) - timedelta(minutes=1)
    result = await utility.reconcile(store, first, first + timedelta(minutes=6))
    row = result[0]
    assert row["EXPECTED"] == 6 and row["persisted_native_candles"] == 4
    assert row["missing_expected_minutes"] == 2 and row["invariant_holds"]
    assert row["backfilled_minutes"] == 3


async def test_health_reopening_counts_only_expected_minutes_and_clock_regression():
    p = Replay()
    pair = Registry.load(REGISTRY).subscriptions("tradingview_guest")[0]
    await p.subscribe([pair])
    symbol = pair[0].canonical_symbol
    p.session_info[symbol] = FX
    friday = datetime(2026, 10, 9, 20, 59, tzinfo=UTC)
    p.progress[symbol] = {
        "last_persisted_minute": friday,
        "coverage_end": datetime(2026, 10, 11, 21, tzinfo=UTC),
        "missing_count": 0,
        "missing_ranges": [],
        "unknown_count": 0,
    }
    p.refresh_health(datetime(2026, 10, 11, 21, 1, 2, tzinfo=UTC))
    item = p.health.symbols[symbol]
    assert item.minutes_behind == 1 and item.missing_candle_count == 1
    assert item.recoverable_missing_count == 1 and item.outside_horizon_missing_count == 0
    assert item.oldest_unresolved_expected_gap == datetime(2026, 10, 11, 21, tzinfo=UTC)
    assert item.candle_state == "degraded"
    p.refresh_health(friday - timedelta(hours=1))
    assert item.stale


@pytest.mark.integration
async def test_backfill_provenance_survives_late_correction(store):
    p, service = await setup(store)
    pair = p.subscriptions[0]
    first = make_candle(
        pair, metadata={"backfilled": True, "volume_type": "base", "volume_additive": True}
    )
    coverage = [(pair[0].id, p.name, first.bucket_start, first.bucket_end)]
    await store.write_candles([first], coverage)
    correction = first.model_copy(
        update={
            "close": Decimal("100"),
            "received_timestamp": first.received_timestamp + timedelta(seconds=1),
            "metadata": {"backfilled": False, "volume_type": "base", "volume_additive": True},
        }
    )
    await store.write_candles([correction], coverage)
    async with store.connection() as c:
        row = await c.fetchrow("SELECT metadata,payload FROM market_candles")
    assert json.loads(row["metadata"])["backfilled"] is True
    assert json.loads(row["payload"])["metadata"]["backfilled"] is True
    assert store.last_candle_write["changed_candles"][0].metadata["backfilled"] is True


@pytest.mark.parametrize(
    "symbol", ["BINANCE:BTCUSDT", "OANDA:EURUSD", "OANDA:XAUUSD", "TVC:USOIL", "TVC:UKOIL"]
)
def test_compressed_sessions_cover_all_minutes_without_overlap(symbol):
    info = source(next(r for r in captured() if r["symbol"] == symbol))
    start = datetime(2026, 10, 9, 18, tzinfo=UTC)
    end = start + timedelta(days=3)
    ranges = classify_ranges(info, start, end)
    # Cross-check UTC/DST and weekend boundaries against minute expectations.
    for offset in range(int((end - start).total_seconds() // 60)):
        bucket = start + timedelta(minutes=offset)
        matches = [
            state for state, values in ranges.items() for lo, hi in values if lo <= bucket < hi
        ]
        assert matches == [expectation(info, bucket).value]
