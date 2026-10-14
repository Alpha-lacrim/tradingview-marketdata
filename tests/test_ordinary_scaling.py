"""Ordinary scaling identity and evidence-accounting invariants, without live I/O."""

import asyncio
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from marketdata.config import Settings
from marketdata.observability import Metrics
from marketdata.providers.candle_pool import CandleConnectionPool
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry


@pytest.mark.parametrize("count,connections", [(64, 2), (128, 4), (256, 4)])
async def test_scaled_registries_preserve_original_sources_and_ordinary_ownership(
    count, connections
):
    registry = Registry.load(Path(f"config/instruments.tradingview-{count}.json"))
    original = Registry.load(Path("config/instruments.tradingview-pilot.json"))
    assert registry.entries[:34] == original.entries
    assert len(registry.entries) == count
    if count > 64:
        preceding = Registry.load(Path(f"config/instruments.tradingview-{count // 2}.json"))
        assert registry.entries[: count // 2] == preceding.entries
    settings = Settings(
        _env_file=None,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=connections,
        candle_pool_concurrency=connections * 2,
        candle_pool_max_instruments=count,
    )
    pool = (await build_providers(settings, registry, Metrics()))[0]
    assert isinstance(pool, CandleConnectionPool)
    assert len(pool.owners) == count
    assert max(len(w.provider.subscriptions) for w in pool.workers) <= 100
    assert not pool.opened and not pool.tasks  # Factory inspection makes no source requests.
    assert all(not i.canonical_symbol.startswith("BENCH") for i, _ in pool.subscriptions)
    assert all(m.realtime_status == "unknown" for _, m in pool.subscriptions)


async def test_fixed_100_registry_preserves_approved_originals_and_dense_source_evidence(tmp_path):
    registry = Registry.load(Path("config/instruments.tradingview-100.json"))
    original = Registry.load(Path("config/instruments.tradingview-pilot.json"))
    excluded = "CRYPTO:OKX:XAUTUSDT"
    expected_originals = [
        entry for entry in original.entries if entry.instrument.canonical_symbol != excluded
    ]
    assert registry.entries[:33] == expected_originals
    assert len(registry.entries) == 100 and excluded not in registry.by_symbol
    evidence = json.loads(
        await asyncio.to_thread(Path("docs/ordinary-100-selection.json").read_text)
    )
    assert evidence["approved_inactive_source"]["history_and_identity_preserved"]
    assert len(evidence["selected_additions"]) == 67
    assert all(row["native"] == 60 for row in evidence["selected_additions"])
    spec = importlib.util.spec_from_file_location("prepare_100", "scripts/prepare_ordinary_100.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    output = tmp_path / "reproduced.json"
    await asyncio.to_thread(module.reproduce, Path("docs/ordinary-100-selection.json"), output)
    assert Registry.load(output).entries == registry.entries
    settings = Settings(
        _env_file=None,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=4,
        candle_pool_concurrency=8,
        candle_pool_max_instruments=100,
        candle_fetch_grace_seconds=5,
    )
    pool = (await build_providers(settings, registry, Metrics()))[0]
    assert [len(worker.provider.subscriptions) for worker in pool.workers] == [25] * 4
    assert pool.grace == 5 and not pool.opened


def test_observer_keeps_successful_source_omissions_in_expected_denominator():
    spec = importlib.util.spec_from_file_location(
        "observer", "scripts/observe_ordinary_collector.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    end = datetime(2026, 10, 12, 10, tzinfo=UTC)
    cycles = []
    first = {}
    for index in range(2):
        bucket = (end + timedelta(minutes=index)).isoformat()
        cycles.append(
            dict(
                trigger="minute",
                bucket_end=bucket,
                eligible_symbols=["A", "B"],
                requested_instruments=2,
                fetch_failures=0,
                completed_current_instruments=1,
                independent_current_commits=1,
                close_to_commit_seconds=5,
                cycle_duration_seconds=4,
                collection_budget_seconds=48,
                db_write_seconds=0.1,
                db_failures=0,
                retrieval={"workers": {"w00": {"state": "disconnected"}}},
            )
        )
        first["A", bucket] = {"close_to_commit_seconds": 5}
    report = module.summarize(cycles, first, [], True, 2)
    assert report["expected_targets"] == report["successful_requests"] == 4
    assert report["native_targets"] == report["immediate_commits"] == 2
    assert len(report["unresolved_targets"]) == 2
    assert report["eventual_completeness"] == 0.5 and not report["qualified"]
    assert not report["collector_qualified"] and not report["coverage_gate_passed"]
    first["B", end.isoformat()] = {"close_to_commit_seconds": 65}
    recovered = module.summarize(cycles, first, [], True, 3)
    assert recovered["recovered_targets"] == 1
    assert recovered["collector_qualified"] and not recovered["coverage_gate_passed"]
    assert recovered["immediate_commits"] == 2 and recovered["eventual_commits"] == 3
