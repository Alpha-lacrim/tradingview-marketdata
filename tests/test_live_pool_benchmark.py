"""Live harness accounting tested offline, including real PostgreSQL transactions."""

import importlib
from argparse import Namespace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from test_native_candles import NOW, captured, source, vectors

from marketdata.config import Settings
from marketdata.domain import RegistryEntry
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentConnectionError
from marketdata.providers.tradingview_candles import TradingViewCandleProvider, parse_bar
from marketdata.registry import Registry


@pytest.fixture
def live_harness(monkeypatch):
    monkeypatch.syspath_prepend(str(Path("scripts").resolve()))
    return importlib.import_module("benchmark_live_pool")


async def test_guest_opt_in_keeps_worker_cap_and_default_guard(monkeypatch):
    settings = Settings(_env_file=None, candle_pool_connections=8)
    pairs = Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
        "tradingview_guest"
    )
    calls = []

    async def connect(*args, **kwargs):
        calls.append(kwargs)
        raise TimeoutError("offline handshake")

    monkeypatch.setattr("marketdata.providers.tradingview_candles.connect", connect)
    ordinary = TradingViewCandleProvider(settings, Metrics())
    await ordinary.subscribe(pairs)
    with pytest.raises(PermanentConnectionError, match="permissions_unverified"):
        await ordinary.connect()
    assert not calls
    experimental = TradingViewCandleProvider(settings, Metrics(), experimental_pool=True)
    await experimental.subscribe(pairs)
    with pytest.raises(TimeoutError, match="offline handshake"):
        await experimental.connect()
    assert len(calls) == 1 and calls[0]["max_queue"] == 16
    experimental.job_limit = 101
    with pytest.raises(PermanentConnectionError, match="permissions_unverified"):
        await experimental.connect()
    assert len(calls) == 1


async def test_live_harness_rejects_production_database_before_network(live_harness, monkeypatch):
    monkeypatch.setenv("TEST_DATABASE_URL", "postgresql://localhost/marketdata")
    with pytest.raises(ValueError, match="isolated _test"):
        await live_harness.run(Namespace(collector_stopped=True))


def test_missing_native_candles_are_not_counted_from_network_success(live_harness):
    bucket = NOW.replace(second=0)
    stage = dict(
        expected=[("A", bucket.isoformat()), ("B", bucket.isoformat())],
        cycles=[dict(durable_current=1, wall_seconds=2, phase="baseline")],
        writes=[dict(seconds=0.1)],
    )
    commits = {("A", bucket): dict(close_to_commit_seconds=3)}
    summary = live_harness.stage_summary(stage, commits)
    assert summary["immediate_candles"] == summary["eventual_candles"] == 1
    assert summary["expected_candles"] == 2 and summary["verdict"] == "LIVE INCOMPLETE"
    commits[("B", bucket)] = dict(close_to_commit_seconds=63)
    summary = live_harness.stage_summary(stage, commits)
    assert summary["immediate_candles"] == 1 and summary["eventual_candles"] == 2
    assert summary["close_to_commit_seconds"]["max"] == 63


@pytest.mark.integration
async def test_recorded_commits_isolate_namespace_verify_decimals_and_replay(live_harness, store):
    registry = Registry.load(Path("config/instruments.tradingview.json"))
    pair = registry.subscriptions("tradingview_guest")[0]
    row = next(r for r in captured() if r["symbol"] == pair[1].provider_symbol)
    candle = parse_bar(vectors(row)[0]["v"], pair, source(row), NOW, "streaming")
    namespace = "benchmark_live_pool_" + uuid4().hex[:12]
    await store.register(
        Registry(
            [
                RegistryEntry(
                    instrument=pair[0],
                    mappings=[pair[1].model_copy(update={"provider": namespace})],
                )
            ]
        )
    )
    recording = live_harness.RecordingStore(store, namespace)
    coverage = [(candle.instrument_id, candle.provider, candle.bucket_start, candle.bucket_end)]
    recording.fail_once = True
    with pytest.raises(ConnectionError, match="pre-transaction"):
        await recording.write_candles([candle], coverage)
    assert not recording.first_commits
    assert await recording.write_candles([candle], coverage) == 1
    first = dict(recording.first_commits)
    replay = candle.model_copy(update={"received_timestamp": NOW + timedelta(seconds=10)})
    assert await recording.write_candles([replay], coverage) == 0
    assert recording.first_commits == first
    assert recording.writes[-1]["durable_rows"] == 1
    progress = await recording.candle_progress(candle.instrument_id, candle.provider)
    assert progress["last_persisted_minute"] == candle.bucket_start
    async with store.connection() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM market_candles WHERE provider='tradingview_guest'"
            )
            == 0
        )
