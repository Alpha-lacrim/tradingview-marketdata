import importlib.util
from pathlib import Path


def test_native_probe_aborts_for_busy_cooldown_or_access_rejected_worker():
    spec = importlib.util.spec_from_file_location("probe", Path("scripts/probe_native_history.py"))
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    worker = dict(state="disconnected", http_status=None, backoff_remaining_seconds=0)
    retrieval = dict(
        workers={"w00": worker}, active_requests=0, sessions_current=0, pending_futures=0
    )
    assert probe.idle(retrieval)
    retrieval["active_requests"] = 1
    assert not probe.idle(retrieval)
    retrieval["active_requests"] = 0
    worker["backoff_remaining_seconds"] = 1
    assert not probe.idle(retrieval)
    worker["backoff_remaining_seconds"] = 0
    for status in (400, 401, 403, 429, 451):
        worker["http_status"] = status
        assert not probe.idle(retrieval)
    worker["http_status"] = None
    worker["state"] = "connecting"
    assert not probe.idle(retrieval)
    assert not probe.idle(dict(workers={}))


def test_requests_availability_commits_and_recovery_are_separate():
    spec = importlib.util.spec_from_file_location(
        "availability_audit", Path("scripts/analyze_native_availability.py")
    )
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    symbol, bucket = "BENCHLIVE:BINANCE:TESTUSDT", "2026-10-12T12:14:00+00:00"
    stage = dict(
        count=1,
        expected=[[symbol, bucket]],
        commits=[dict(symbol=symbol, bucket_start=bucket)],
        cycles=[
            dict(
                phase="baseline",
                bucket_end="2026-10-12T12:15:00+00:00",
                successful_requests=1,
                failures={},
                durable_current=0,
                ingestion=dict(
                    completed_current_instruments=0,
                    instruments={
                        symbol: dict(source_status="streaming", completed_candle_available_at=None)
                    },
                ),
            )
        ],
    )
    result = audit.analyze(dict(stages=[stage]))
    row = result["stages"][0]
    assert row["successful_requests"] == 1
    assert row["available_native_current"] == row["immediate_committed"] == 0
    assert row["eventual_committed"] == row["recovered_after_current_cycle"] == 1
    assert row["unresolved"] == 0
    assert row["cycles"][0]["successful_responses_without_target"] == ["BINANCE:TESTUSDT"]
    stage["cycles"][0]["failures"][symbol] = "TimeoutError"
    stage["cycles"][0]["successful_requests"] = 0
    failed = audit.analyze(dict(stages=[stage]))["stages"][0]
    assert failed["successful_requests"] == 0
    assert failed["failed_requests"] == 1
    assert failed["cycles"][0]["successful_responses_without_target"] == []
