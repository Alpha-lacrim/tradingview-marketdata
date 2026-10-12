"""Offline availability audit of live-pool checkpoints; never requests or persists candles."""

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path


def analyze(data):
    stages = []
    recurrent = defaultdict(dict)
    common = set.intersection(*[{s for s, _ in stage["expected"]} for stage in data["stages"]])
    for stage in data["stages"]:
        expected = {tuple(k) for k in stage["expected"]}
        committed = {(c["symbol"], c["bucket_start"]) for c in stage["commits"]} & expected
        missing = expected - committed
        by_source = Counter(s for s, _ in missing)
        for symbol, count in by_source.items():
            recurrent[symbol.removeprefix("BENCHLIVE:")][str(stage["count"])] = count
        cycles = []
        for cycle in stage["cycles"]:
            ingestion = cycle["ingestion"]
            bucket = (
                datetime.fromisoformat(cycle["bucket_end"]) - timedelta(minutes=1)
            ).isoformat()
            timings = ingestion.get("instruments", {})
            successful_without_target = [
                symbol.removeprefix("BENCHLIVE:")
                for symbol, timing in timings.items()
                if symbol not in cycle["failures"]
                and timing.get("source_status") in ("streaming", "realtime")
                and timing["completed_candle_available_at"] is None
            ]
            cycles.append(
                dict(
                    phase=cycle["phase"],
                    bucket_start=bucket,
                    successful_requests=cycle["successful_requests"],
                    failed_requests=len(cycle["failures"]),
                    available_native_current=ingestion.get("completed_current_instruments", 0),
                    committed_current=cycle["durable_current"],
                    eventual_current=sum(b == bucket for _, b in committed),
                    successful_responses_without_target=sorted(successful_without_target),
                    db_commit_acknowledged_at=ingestion.get("db_commit_completed_at"),
                    db_failures=ingestion.get("db_failures", 0),
                    close_to_commit_seconds=ingestion.get("close_to_commit_seconds"),
                )
            )
        baseline = [c for c in cycles if c["phase"] == "baseline"]
        immediate = sum(c["committed_current"] for c in cycles)
        stages.append(
            dict(
                count=stage["count"],
                expected=len(expected),
                successful_requests=sum(c["successful_requests"] for c in cycles),
                failed_requests=sum(c["failed_requests"] for c in cycles),
                available_native_current=sum(c["available_native_current"] for c in cycles),
                immediate_committed=immediate,
                eventual_committed=len(committed),
                recovered_after_current_cycle=len(committed) - immediate,
                unresolved=len(missing),
                baseline=dict(
                    expected=len(baseline) * stage["count"],
                    successful_requests=sum(c["successful_requests"] for c in baseline),
                    available_native_current=sum(c["available_native_current"] for c in baseline),
                    committed_current=sum(c["committed_current"] for c in baseline),
                ),
                common_cohort=dict(
                    sources=len(common),
                    expected=sum(s in common for s, _ in expected),
                    eventual_committed=sum(s in common for s, _ in committed),
                ),
                missing_by_venue=dict(
                    Counter(s.removeprefix("BENCHLIVE:").split(":", 1)[0] for s, _ in missing)
                ),
                sources_missing_all_planned_minutes=sorted(
                    s.removeprefix("BENCHLIVE:")
                    for s, n in by_source.items()
                    if n == sum(symbol == s for symbol, _ in expected)
                ),
                cycles=cycles,
            )
        )
    return dict(
        classification="Observed native response/commit shortfalls; upstream cause unverified",
        production_writes=False,
        stages=stages,
        recurring_unresolved_sources=[
            dict(source=source, missing_by_stage=counts)
            for source, counts in sorted(recurrent.items())
            if len(counts) > 1
        ],
        limitations=[
            "Non-simultaneous stages use nested universes; count and time are confounded.",
            "Historical raw frames/bounds were not retained; absent parsed targets do not prove "
            "permanent upstream omissions or no-trade minutes.",
            "Recovered targets may reflect late availability or earlier retrieval/transport "
            "shortfalls; commit timing alone cannot distinguish these causes.",
            "Venue/source-qualified identities remain separate; scanner discovery is "
            "not verification.",
        ],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.checkpoint.read_bytes()
    result = analyze(json.loads(raw))
    result["checkpoint_sha256"] = hashlib.sha256(raw).hexdigest()
    args.output.write_text(json.dumps(result, indent=2), "utf-8")
