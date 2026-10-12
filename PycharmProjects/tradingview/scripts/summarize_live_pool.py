"""Publish sanitized live checkpoints, retaining first-commit latency for every candle."""

import argparse
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

from benchmark_candle_retrieval import quantiles


def summarize_run(data):
    stages = []
    for stage in data["stages"]:
        cycles = stage["cycles"]
        records = {(c["symbol"], c["bucket_start"]): c for c in stage["commits"]}
        baseline = [r for r in cycles if r["phase"] == "baseline"]
        immediate_latencies = []
        for row in cycles:
            bucket = (datetime.fromisoformat(row["bucket_end"]) - timedelta(minutes=1)).isoformat()
            cutoff = row["ingestion"].get("db_commit_completed_at")
            immediate_latencies.extend(
                c["close_to_commit_seconds"]
                for (s, b), c in records.items()
                if b == bucket and cutoff and c["commit_acknowledged_at"] <= cutoff
            )
        faults = [r for r in cycles if r["phase"] == "fault"]
        fault_recovery = []
        for fault in faults:
            bucket = (
                datetime.fromisoformat(fault["bucket_end"]) - timedelta(minutes=1)
            ).isoformat()
            lost = set(fault["failures"])
            recovered = [(s, bucket) for s in lost if (s, bucket) in records]
            fault_recovery.append(
                dict(
                    bucket_start=bucket,
                    failed_requests=len(lost),
                    eventually_committed_failed_targets=len(recovered),
                    unresolved_failed_targets=sorted(s for s in lost if (s, bucket) not in records),
                    recovery_close_to_commit_seconds=quantiles(
                        [records[k]["close_to_commit_seconds"] for k in recovered]
                    ),
                    database_retry_count=fault["ingestion"].get("db_failures"),
                )
            )
        worker_rows = [w for r in cycles for w in r["retrieval"].get("workers", {}).values()]
        stages.append(
            dict(
                count=stage["count"],
                namespace=stage["namespace"],
                summary=stage["summary"],
                completed_planned_cycles=len(cycles) == len(baseline) + 3,
                baseline=dict(
                    cycles=len(baseline),
                    expected=len(baseline) * stage["count"],
                    immediate=sum(r["durable_current"] for r in baseline),
                    cycle_seconds=quantiles([r["wall_seconds"] for r in baseline]),
                ),
                immediate_close_to_commit_seconds=quantiles(immediate_latencies),
                requests=dict(
                    successful=sum(r["successful_requests"] for r in cycles),
                    failed=sum(len(r["failures"]) for r in cycles),
                    worker_error_categories=sorted({w["error"] for w in worker_rows if w["error"]}),
                    worker_ready_seconds_including_pacing=quantiles(
                        [
                            w["handshake_seconds"] + w.get("connect_wait_seconds", 0)
                            for w in worker_rows
                            if "handshake_seconds" in w
                        ]
                    ),
                    worker_elapsed_seconds=quantiles([w["duration_seconds"] for w in worker_rows]),
                ),
                resources=dict(
                    cpu_seconds=quantiles([r["cpu_seconds"] for r in cycles]),
                    cpu_one_core_percent=quantiles([r["cpu_one_core_percent"] for r in cycles]),
                    rss_peak_bytes=max(r["rss_peak_bytes"] for r in cycles),
                    python_additional_peak_bytes=max(
                        r["python_additional_peak_bytes"] for r in cycles
                    ),
                    event_loop_lag_max_seconds=max(
                        r["loop_lag_seconds"].get("max", 0) for r in cycles
                    ),
                    peak_tasks=max(r["peak_tasks"] for r in cycles),
                ),
                cleanup_passed=all(
                    r["sockets_closed"] and r["receivers_after"] == 0 for r in cycles
                ),
                controlled_recovery=fault_recovery,
                cycles=[
                    {
                        k: r[k]
                        for k in (
                            "phase",
                            "bucket_end",
                            "wall_seconds",
                            "durable_current",
                            "unresolved_so_far",
                            "successful_requests",
                            "failures",
                            "injected_faults",
                            "error",
                        )
                    }
                    for r in cycles
                ],
                first_commits=[
                    {k: v for k, v in c.items() if k != "native"} for c in stage["commits"]
                ],
                native_samples=[c["native"] for c in stage["commits"][:2]],
            )
        )
    return dict(
        mode=data["mode"],
        started_at=data["started_at"],
        finished_at=data.get("finished_at"),
        stopped_reason=data.get("stopped_reason"),
        configuration=data["configuration"],
        database=data["database"],
        production_writes=data["production_writes"],
        universe=data["universe"],
        latency_definition=data["latency_definition"],
        stages=stages,
        not_tested=[n for n in (128, 256, 512) if n not in {s["count"] for s in stages}],
    )


def publish(inputs, output):
    runs = []
    for path in inputs:
        raw = path.read_bytes()
        run = summarize_run(json.loads(raw))
        run["raw_checkpoint_sha256"] = hashlib.sha256(raw).hexdigest()
        run["raw_checkpoint"] = path.as_posix()
        runs.append(run)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(dict(runs=runs), indent=2), "utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, default=Path("docs/candle-live-end-to-end.json"))
    args = parser.parse_args()
    publish(args.inputs, args.output)
