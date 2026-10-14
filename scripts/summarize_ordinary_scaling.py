"""Publish sanitized ordinary live checkpoints, preserving failures and denominators."""

import argparse
import hashlib
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

from observe_ordinary_collector import distribution


def publish(paths, output):
    stages = []
    for path in paths:
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        cycles = raw.get("cycles", [])
        scheduled = [c for c in cycles if c.get("trigger") == "minute"]
        resource = raw.get("resources", [])
        cpu = []
        for a, b in zip(resource, resource[1:], strict=False):
            seconds = (
                datetime.fromisoformat(b["observed_at"]) - datetime.fromisoformat(a["observed_at"])
            ).total_seconds()
            if seconds > 0:
                cpu.append(100 * (b["cpu_seconds"] - a["cpu_seconds"]) / seconds)
        errors = Counter()
        for cycle in scheduled:
            for worker in cycle.get("retrieval", {}).get("workers", {}).values():
                if worker.get("error"):
                    errors[
                        f"{worker.get('phase')}:{worker['error']}:HTTP{worker.get('http_status')}"
                    ] += 1
                for category, count in worker.get("retrieval", {}).get("errors", {}).items():
                    errors["request:" + category] += count
        metrics = raw.get("metrics", {})
        lag_count = metrics.get("marketdata_event_loop_lag_seconds_count[]", 0)
        buckets = sorted(
            (float(key.split("'le', '")[1].split("'")[0]), value)
            for key, value in metrics.items()
            if key.startswith("marketdata_event_loop_lag_seconds_bucket")
        )
        lag_p95 = next((bound for bound, value in buckets if value >= lag_count * 0.95), None)
        before, after = raw.get("before", {}), raw.get("after", {})
        identities = {row["canonical_symbol"]: row for row in after.get("coverage", [])}
        starts_preserved = all(
            identities.get(row["canonical_symbol"], {}).get("coverage_start")
            == row["coverage_start"]
            for row in before.get("coverage", [])
        )
        stage = dict(
            checkpoint=path.name,
            checkpoint_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
            mode=raw.get("mode"),
            started_at=raw.get("started_at"),
            finished_at=raw.get("finished_at"),
            stopped_reason=raw.get("stopped_reason"),
            summary=raw.get("summary"),
            registry_sha256=raw.get("registry_sha256"),
            app_container=raw.get("container_id"),
            image_id=raw.get("image_id"),
            configuration=scheduled[0].get("retrieval", {}) if scheduled else {},
            connection_errors=dict(errors),
            resources=dict(
                samples=len(resource),
                cpu_one_core_percent=distribution(cpu),
                rss_peak_bytes=max((r["rss_bytes"] for r in resource), default=None),
                loop_lag_histogram_samples=lag_count,
                loop_lag_p95_bucket_upper_seconds=lag_p95,
                loop_lag_max_seconds=metrics.get("marketdata_event_loop_lag_max_seconds[]"),
            ),
            integrity=dict(
                database_system_identifier=after.get("system_identifier"),
                schema=after.get("schema"),
                activation_starts_preserved=starts_preserved,
                original_history_preserved=before.get("history_sha256")
                == after.get("history_sha256"),
                history_cutoff=before.get("cutoff"),
                history_rows=before.get("history_rows"),
                history_sha256=before.get("history_sha256"),
                raw_ticks_before=before.get("raw_ticks"),
                raw_ticks_after=after.get("raw_ticks"),
            ),
            first_commits=raw.get("first_commits", []),
            metrics=metrics,
            cycles=[{k: v for k, v in c.items() if k != "instruments"} for c in cycles],
            observer_errors=raw.get("observer_errors", []),
            activation=[
                dict(symbol=r["canonical_symbol"], start=r["coverage_start"])
                for r in after.get("coverage", [])
            ],
        )
        stages.append(stage)
        print(
            json.dumps(
                dict(
                    checkpoint=path.name,
                    summary=stage["summary"],
                    connection_errors=stage["connection_errors"],
                    integrity=stage["integrity"],
                    resources=stage["resources"],
                )
            )
        )
    output.write_text(json.dumps(dict(stages=stages), indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoints", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, default=Path("docs/ordinary-collector-scaling.json"))
    args = parser.parse_args()
    publish(args.checkpoints, args.output)
