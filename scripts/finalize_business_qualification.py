"""Freeze the primary-source and gap ledgers from retained evidence; no network or DB I/O."""

import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path


def publish() -> None:
    report_path = Path("docs/business-source-qualification.json")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    rows = [dict(r) for r in report["candidates"] if r["proposed_active"]]
    for row in rows:
        row["durable_native_candle_count"] = row["eventual_candle_commits"]
        row["immediate_misses"] = row["expected_test_minutes"] - row["immediate_candle_commits"]
    followup_path = Path("artifacts/business-gap-followup.json")
    if followup_path.exists():
        followup_bytes = followup_path.read_bytes()
        followup = json.loads(followup_bytes)
        followup_hash = hashlib.sha256(followup_bytes).hexdigest()
    else:
        evidence = json.loads(Path("docs/business-live-evidence.json").read_text(encoding="utf-8"))
        capture = evidence["captures"][followup_path.name]
        followup = capture["data"]
        followup_hash = capture["sha256"]
    assert len(rows) == 99
    expected = sum(r["expected_test_minutes"] for r in rows)
    immediate = sum(r["immediate_candle_commits"] for r in rows)
    recovered = sum(r["recovered_candle_count"] for r in rows)
    unresolved = sum(r["unresolved_expected_gaps"] for r in rows)
    assert (expected, immediate, recovered, unresolved) == (5940, 5882, 0, 58)
    gaps = []
    for row in rows:
        later = followup["sources"].get(row["tradingview_symbol"])
        if later:
            assert set(later["unresolved_expected_buckets"]) == set(
                row["unresolved_expected_buckets"]
            )
            assert later["recovered_since_first_capture"] == 0
        for bucket in row["unresolved_expected_buckets"]:
            assert bucket in row["session_classification_evidence"]["EXPECTED"]
            gaps.append(
                dict(
                    tradingview_symbol=row["tradingview_symbol"],
                    canonical_symbol=row["canonical_symbol"],
                    category=row["category"],
                    bucket_start=bucket,
                    bucket_end=(datetime.fromisoformat(bucket) + timedelta(minutes=1)).isoformat(),
                    session_classification="EXPECTED",
                    immediate_committed=False,
                    recovered=False,
                    unresolved=True,
                    observed_native_source_absence=True,
                    successful_capture_count=3 if later else 2,
                    latest_targeted_recheck_at=followup["at"] if later else None,
                    transport_failure=False,
                    database_failure=False,
                    underlying_cause="UNKNOWN: no source trades versus publication omission",
                    permanent_loss_established=False,
                )
            )
    assert len(gaps) == 58
    categories = {}
    for category in dict.fromkeys(r["category"] for r in rows):
        group = [r for r in rows if r["category"] == category]
        healthy = sum(r["healthy_source_snapshot"] for r in group)
        categories[category] = dict(
            candidate_count=len(group),
            healthy_source_snapshot_count=healthy,
            unhealthy_or_unverified_count=len(group) - healthy,
            **{
                key: sum(r[key] for r in group)
                for key in (
                    "expected_test_minutes",
                    "immediate_candle_commits",
                    "recovered_candle_count",
                    "unresolved_expected_gaps",
                    "source_absent_observations",
                    "transport_failures",
                    "db_persistence_failures",
                    "unknown_session_minutes",
                    "not_expected_market_closed_minutes",
                )
            },
        )
    active = dict(
        status="PROVISIONAL_NOT_DEPLOYED",
        development_milestone="99.0236% accepted provisionally; not production qualification",
        historical_window_start=report["window_start"],
        historical_window_end=report["window_end"],
        source_report=report_path.name,
        expected_targets=expected,
        immediate_commits=immediate,
        recovered=recovered,
        eventual_commits=immediate + recovered,
        unresolved=unresolved,
        eventual_completeness_percentage=100 * (immediate + recovered) / expected,
        ordinary_scheduled_cycles=0,
        denominator_adjustment="none; EXPECTED 5940, NOT_EXPECTED 0, UNKNOWN 0",
        categories=categories,
        candidates=rows,
    )
    Path("docs/business-active-qualification.json").write_text(
        json.dumps(active, indent=2) + "\n", encoding="utf-8"
    )
    fields = list(rows[0])
    with Path("docs/business-active-qualification.csv").open(
        "w", newline="", encoding="utf-8-sig"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()}
            )
    ledger = dict(
        expected_targets=expected,
        immediate_commits=immediate,
        recovered=recovered,
        unresolved_targets=unresolved,
        source_report_sha256=hashlib.sha256(report_path.read_bytes()).hexdigest(),
        targeted_followup_sha256=followup_hash,
        unresolved_by_source=dict(Counter(r["tradingview_symbol"] for r in gaps)),
        targets=gaps,
    )
    Path("docs/business-missing-targets.json").write_text(
        json.dumps(ledger, indent=2) + "\n", encoding="utf-8"
    )
    with Path("docs/business-missing-targets.csv").open(
        "w", newline="", encoding="utf-8-sig"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=list(gaps[0]))
        writer.writeheader()
        writer.writerows(gaps)
    print(
        json.dumps(
            {
                "expected": expected,
                "immediate": immediate,
                "recovered": recovered,
                "unresolved": unresolved,
                "by_source": ledger["unresolved_by_source"],
            }
        )
    )


if __name__ == "__main__":
    publish()
