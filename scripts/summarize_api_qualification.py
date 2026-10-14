"""Publish measured Batch 4 evidence; never substitutes invented or default timings."""

import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path


def table_row(*values):
    return "| " + " | ".join(str(value) for value in values) + " |"


def main():
    reports = {}
    hashes = {}
    for engine in ("pg", "timescale"):
        path = Path(f"artifacts/batch4-{engine}-performance.json")
        reports[engine] = json.loads(path.read_text())
        hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        assert reports[engine]["worker_exit_code"] == 0
        assert all(row["failed_rate"] == 0 for row in reports[engine]["rest"])
    suites = {}
    for engine in ("pg", "timescale"):
        path = Path(f"artifacts/batch4-{engine}-final.xml")
        suite = ET.parse(path).getroot().find("testsuite")
        assert suite is not None and suite.attrib["failures"] == suite.attrib["errors"] == "0"
        suites[engine] = {
            k: suite.attrib[k] for k in ("tests", "failures", "errors", "skipped", "time")
        }
        hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    profiles = {}
    for engine in ("pg", "timescale"):
        path = Path(f"artifacts/batch4-{engine}-resources.json")
        if path.exists():
            profiles[engine] = json.loads(path.read_text())
            hashes[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    Path("docs/api-batch4-evidence.json").write_text(
        json.dumps(
            dict(
                reports=reports,
                resource_profiles=profiles,
                suites=suites,
                raw_artifact_sha256=hashes,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Batch 4 measured API performance",
        "",
        "Generated from retained reports. Scope: [qualification](api-batch4-verification.md).",
        "",
        "Latencies are milliseconds. Each REST row has 120 requests; normal failures are zero.",
        "",
        "| Engine | Request | Concurrent clients | p50 | p95 | p99 | RPS |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for engine, report in reports.items():
        for row in report["rest"]:
            lines.append(
                table_row(
                    engine,
                    row["name"],
                    row["concurrency"],
                    row["p50_ms"],
                    row["p95_ms"],
                    row["p99_ms"],
                    row["requests_per_second"],
                )
            )
    lines += [
        "",
        "| Engine | WS clients × 100 symbols | Delivered/expected | ACK→client p50 | p95 | p99 |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for engine, report in reports.items():
        for row in report["websocket"]:
            latency = row["ack_to_delivery"]
            lines.append(
                table_row(
                    engine,
                    row["clients"],
                    f"{row['received']}/{row['expected']}",
                    latency["p50_ms"],
                    latency["p95_ms"],
                    latency["p99_ms"],
                )
            )
    lines += [
        "",
        "| Engine | Elapsed s | CPU s / % one core | RSS MiB | Pool max | Queue max/bound |",
        "| --- | ---: | --- | ---: | ---: | --- |",
    ]
    for engine, report in reports.items():
        r = report["api_resources"]
        lines.append(
            table_row(
                engine,
                f"{r['elapsed_seconds']:.3f}",
                f"{r['cpu_seconds']:.3f} / {r['cpu_average_one_core_percent']:.2f}%",
                f"{r['peak_rss_bytes'] / 1048576:.2f}",
                r["peak_pool_used"],
                f"{r['observed_queue_high_water']}/256",
            )
        )
    lines += [
        "",
        "| Engine | Metric | p50 ms | p95 ms | p99 ms | max ms |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for engine, report in reports.items():
        for name in ("event_loop_lag", "database_queries"):
            metric = report["api_resources"][name]
            lines.append(
                table_row(
                    engine,
                    name,
                    metric["p50_ms"],
                    metric["p95_ms"],
                    metric["p99_ms"],
                    metric["max_ms"],
                )
            )
    lines += [
        "",
        "DB query samples include timeout injection and client decoding, not only execution.",
        "Windows asyncpg/event-loop clocks are coarse (including zero elapsed samples).",
        "",
        "| Engine | Fault requests | 503 p50/p95/p99 ms | Recovery ms | History SQL ms |",
        "| --- | ---: | --- | ---: | ---: |",
    ]
    for engine, report in reports.items():
        d = report["database_degradation"]
        lines.append(
            table_row(
                engine,
                d["samples"],
                f"{d['p50_ms']}/{d['p95_ms']}/{d['p99_ms']}",
                f"{report['database_recovery_seconds'] * 1000:.3f}",
                report["history_plan"][0]["Execution Time"],
            )
        )
    lines += ["", "[Full evidence, query plans and hashes](api-batch4-evidence.json).", ""]
    Path("docs/api-batch4-performance.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
