"""Offline benchmark guards, separate transport/source counts, actual historical recovery."""

import importlib
from argparse import Namespace
from pathlib import Path

import pytest


@pytest.fixture
def harness(monkeypatch, caplog):
    caplog.set_level("ERROR")
    monkeypatch.syspath_prepend(str(Path("scripts").resolve()))
    return importlib.import_module("benchmark_candle_pool")


def args(**updates):
    options = dict(
        latency=0,
        handshake=0,
        service_capacity=0,
        slow_seconds=0,
        missing=[],
        deadline=5,
        bars=4,
        recovery=False,
    )
    options.update(updates)
    return Namespace(**options)


async def test_pool_benchmark_reports_beyond100_and_cleanup(harness):
    row = await harness.probe(args(), 128, 8, 2, 4)
    assert row["successful_requests"] == row["native_target_candles"] == 128
    assert row["retrieval"]["peak_inflight"] <= 4
    assert row["cleanup"]["tasks_after"] == 1
    assert harness.summarize([row])[0]["cleanup_passed"]


@pytest.mark.parametrize("connections", [1, 4])
async def test_permanent_source_omissions_are_not_falsely_recovered(harness, connections):
    row = await harness.probe(args(missing=[0, 2], recovery=True), 128, connections, 4, 8)
    assert row["successful_requests"] == 128
    assert row["native_target_candles"] == row["eventual_target_candles"] == 126
    assert row["source_completed_missing"] == 2 and not row["failures"]
    assert len(row["unresolved_primary"]) == 2


async def test_transient_deadline_failures_recover_with_bounded_history(harness):
    row = await harness.probe(args(slow_seconds=4, deadline=1, recovery=True), 8, 2, 2, 4)
    assert row["native_target_candles"] < 8
    assert row["eventual_target_candles"] == 8
    assert row["cleanup"]["sessions_remaining"] == 0


async def test_live_pool_fails_before_network(harness):
    with pytest.raises(ValueError, match="synthetic only"):
        await harness.run(Namespace(live=True))
