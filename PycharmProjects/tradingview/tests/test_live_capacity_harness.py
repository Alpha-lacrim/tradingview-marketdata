"""Offline verification of live benchmark gates and recovery accounting."""

import importlib
import json
import tracemalloc
from argparse import Namespace
from datetime import timedelta
from pathlib import Path

import pytest
from test_candle_pipeline import pipeline
from test_native_candles import NOW

from marketdata.domain import utc_now


@pytest.fixture
def harness(monkeypatch):
    monkeypatch.syspath_prepend(str(Path("scripts").resolve()))
    return importlib.import_module("benchmark_live_capacity")


async def test_cycle_uses_one_history_request_per_symbol_and_owns_cleanup(harness, monkeypatch):
    p, socket, pairs, end = pipeline(count=34, window=2)
    await p.subscribe(pairs)

    async def connect():
        p.socket = socket

    monkeypatch.setattr(p, "connect", connect)
    monkeypatch.setattr(harness, "utc_now", lambda: NOW)
    monkeypatch.setattr(harness, "next_collection", lambda *args: NOW)
    missing = {(pairs[0][0].canonical_symbol, end - timedelta(minutes=5))}
    tracemalloc.start()
    try:
        row, received, evidence = await harness.cycle(p, pairs, missing)
    finally:
        tracemalloc.stop()
    assert row["current_candles"] == 34 and row["missing_current"] == 0
    assert len(socket.requests) == 34  # overlap/backfill not one request per bar
    assert socket.requests[0][2] == 6
    assert all(job[2] == 4 for job in socket.requests[1:])
    assert missing <= received
    assert len(evidence) == 34
    assert row["receiver_tasks_after_cycle"] == 0
    assert row["retrieval"]["pending_futures"] == 0
    assert row["socket_closed"]


def test_escalation_gate_rejects_slow_incomplete_or_unresolved_stage(harness):
    row = dict(
        missing_current=0,
        current_candles=17,
        instrument_count=17,
        cycle_seconds=8,
        first_parsed_seconds=3,
        p50_parsed_seconds=5,
        p95_parsed_seconds=10,
        last_parsed_seconds=10,
        retrieval={},
        disconnects=0,
        skipped_minute_targets=0,
        possible_throttle=0,
        connection_error=None,
    )
    assert harness.stage_passes([row] * 10, set())
    assert not harness.stage_passes([row] * 10, {("symbol", NOW)})
    assert not harness.stage_passes([dict(row, cycle_seconds=40)] * 10, set())
    assert not harness.stage_passes([dict(row, missing_current=1)] * 10, set())
    assert not harness.stage_passes([dict(row, disconnects=1)] * 10, set())
    assert not harness.stage_passes([dict(row, skipped_minute_targets=1)] * 10, set())
    sparse = dict(row, missing_current=1, source_completed_without_target=["XAUT"])
    assert harness.stage_passes([sparse] * 10, {("XAUT", NOW)}, {"XAUT"})
    assert not harness.stage_passes([sparse] * 10, {("BTC", NOW)}, {"XAUT"})
    # An actual XAUT request timeout is not an explicit completed series lacking a bar.
    assert not harness.stage_passes(
        [dict(sparse, source_completed_without_target=[])] * 10, set(), {"XAUT"}
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"connection_error": "PermanentConnectionError"},
        {"timings": {"symbol": {}}},
        {"finished_at": (utc_now() - timedelta(hours=2)).isoformat()},
    ],
)
async def test_resume_rejects_access_errors_request_failures_and_stale_windows(
    harness, tmp_path, changes
):
    last = dict(connection_error="TimeoutError", timings={}, finished_at=utc_now().isoformat())
    last.update(changes)
    prior = tmp_path / "prior.json"
    prior.write_text(json.dumps(dict(cycles=[last])))
    args = Namespace(
        collector_stopped=True,
        cycles=3,
        baseline_cycles=3,
        counts=[17],
        candidates=Path("config/benchmarks/tradingview_candidates.json"),
        output=tmp_path / "output.json",
        resume=prior,
    )
    with pytest.raises(ValueError, match="resume only|one retry window"):
        await harness.run(args)


@pytest.mark.parametrize(
    "url,stopped",
    [
        ("postgresql://localhost/user_history", True),
        ("postgresql://localhost/disposable_test", False),
    ],
)
async def test_native_persistence_guard_precedes_any_source_or_database_access(
    harness, monkeypatch, tmp_path, url, stopped
):
    native = importlib.import_module("benchmark_native_persistence")
    monkeypatch.setenv("TEST_DATABASE_URL", url)
    args = Namespace(
        collector_stopped=stopped,
        capacity_results=tmp_path / "nonexistent.json",
        output=tmp_path / "output.json",
    )
    with pytest.raises(ValueError, match="disposable TEST_DATABASE_URL"):
        await native.run(args)
