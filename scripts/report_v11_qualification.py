"""Read-only measurements and financial comparisons for a completed task run."""

# Bounded local evidence files; this offline report is not an application request path.
# ruff: noqa: ASYNC240

import argparse
import asyncio
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import asyncpg
from qualify_v11_pipeline import database_url, percentiles, task_database, write_json

from marketdata.domain import Tick
from marketdata.reconciliation import candle_fingerprint, revision_summary


def records(path):
    if not path.exists():
        return
    with path.open(encoding="utf-8") as incoming:
        for line in incoming:
            if line.endswith("\n"):
                yield json.loads(line)


def seconds(start, end):
    return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()


async def database_evidence(directory, config, report):
    names = report["retained_task_databases"]
    connections = {}
    evidence = {}
    try:
        for role in ("source", "destination"):
            name = next(n for n in names if n.endswith("_" + role + "_test"))
            dsn = database_url(config[role], name)
            task_database(dsn)
            connections[role] = await asyncpg.connect(
                dsn, server_settings={"default_transaction_read_only": "on"}
            )
        left, right = connections["source"], connections["destination"]
        candles = []
        for c in (left, right):
            rows = await c.fetch(
                "SELECT payload FROM market_candles ORDER BY instrument_id,provider,bucket_start"
            )
            candles.append([candle_fingerprint(json.loads(r["payload"])) for r in rows])
        evidence["candles"] = dict(
            equal=candles[0] == candles[1],
            counts=list(map(len, candles)),
            sha256=[hashlib.sha256(json.dumps(v).encode()).hexdigest() for v in candles],
        )
        keys = await left.fetch(
            "SELECT DISTINCT instrument_id,provider,bucket_start FROM candle_revisions"
        )
        revisions = []
        for key in keys:
            local = await revision_summary(
                left, key["instrument_id"], key["provider"], key["bucket_start"]
            )
            remote = await revision_summary(
                right, key["instrument_id"], key["provider"], key["bucket_start"]
            )
            revisions.append(
                dict(**dict(key), source=local, destination=remote, equal=local == remote)
            )
        evidence["revision_manifests"] = revisions
        evidence["revision_counts"] = [
            await c.fetchval("SELECT count(*) FROM candle_revisions") for c in (left, right)
        ]
        quotes = []
        for row in await left.fetch(
            "SELECT * FROM source_quote_state ORDER BY instrument_id,provider"
        ):
            record = json.loads(row["record"])
            source = Tick.model_validate(record["observation"])
            head = await right.fetchrow(
                "SELECT * FROM ingest_quote_heads WHERE instrument_id=$1 AND provider=$2",
                row["instrument_id"],
                row["provider"],
            )
            value = await right.fetchval(
                "SELECT payload FROM latest_prices WHERE instrument_id=$1 AND provider=$2",
                row["instrument_id"],
                row["provider"],
            )
            destination = Tick.model_validate_json(value) if value else None
            quotes.append(
                dict(
                    instrument_id=row["instrument_id"],
                    provider=row["provider"],
                    canonical_symbol=source.canonical_symbol,
                    provider_symbol=source.provider_symbol,
                    source_version=row["version"],
                    acknowledged_version=row["acked_version"],
                    observation_id=record["observation_id"],
                    source_epoch=report["source_epoch"],
                    destination_position=head["source_position"] if head else None,
                    same_full_snapshot=source == destination,
                    same_ordering_identity=head is not None
                    and head["source_position"] == row["version"]
                    and str(head["observation_id"]) == record["observation_id"]
                    and str(head["source_epoch"]) == report["source_epoch"],
                    source=source.model_dump(mode="json"),
                    destination=destination.model_dump(mode="json") if destination else None,
                )
            )
        evidence["quotes"] = dict(
            count=len(quotes),
            all_fields_and_ordering_equal=all(
                q["same_full_snapshot"] and q["same_ordering_identity"] for q in quotes
            ),
        )
        write_json(directory / "verified-quote-snapshots.json", quotes)
        coverage = []
        for row in await left.fetch(
            "SELECT * FROM candle_coverage ORDER BY instrument_id,provider"
        ):
            other = await right.fetchrow(
                "SELECT * FROM candle_coverage WHERE instrument_id=$1 AND provider=$2 AND "
                "price_basis=$3",
                row["instrument_id"],
                row["provider"],
                row["price_basis"],
            )
            fields = (
                "coverage_start",
                "coverage_end",
                "missing",
                "unknown",
                "not_expected",
                "session_info",
            )
            same = other is not None and all(row[f] == other[f] for f in fields)
            coverage.append(
                dict(
                    instrument_id=row["instrument_id"],
                    provider=row["provider"],
                    equal=same,
                    source={f: row[f] for f in fields},
                    destination={f: other[f] for f in fields} if other else None,
                )
            )
        evidence["coverage"] = dict(
            count=len(coverage), equal_count=sum(v["equal"] for v in coverage), rows=coverage
        )
        evidence["source_clock"] = dict(await left.fetchrow("SELECT * FROM source_capture_clock"))
        evidence["destination_outcomes"] = [
            dict(r)
            for r in await right.fetch(
                "SELECT stream,outcome,count(*) FROM ingest_observations GROUP BY "
                "stream,outcome ORDER BY stream,outcome"
            )
        ]
        evidence["destination_batch_count"] = await right.fetchval(
            "SELECT count(*) FROM ingest_batches"
        )
        evidence["source_batch_phases"] = [
            dict(r)
            for r in await left.fetch(
                "SELECT phase,count(*) FROM source_sync_batches GROUP BY phase"
            )
        ]
        evidence["source_archive_count"] = await left.fetchval(
            "SELECT count(*) FROM source_sync_archive"
        )
        mismatches = 0
        for row in await left.fetch("SELECT batch_id,payload_sha256 FROM source_sync_archive"):
            actual = await right.fetchval(
                "SELECT encode(payload_sha256,'hex') FROM ingest_batches WHERE "
                "producer_id=$1::uuid AND batch_id=$2",
                report["producer_id"],
                row["batch_id"],
            )
            mismatches += actual != row["payload_sha256"]
        evidence["archived_receipt_hash_mismatches"] = mismatches
        evidence["remaining_source_candles"] = await left.fetchval(
            "SELECT count(*) FROM source_candle_outbox"
        )
        evidence["remaining_source_quotes"] = await left.fetchval(
            "SELECT count(*) FROM source_quote_state WHERE acked_version<version"
        )
        evidence["database_sizes"] = {
            role: await c.fetchval("SELECT pg_database_size(current_database())")
            for role, c in connections.items()
        }
        for kind in ("candle", "quote"):
            sql = (
                "SELECT * FROM ingest_observations WHERE stream=$1 ORDER BY source_position LIMIT 1"
            )
            observation = await right.fetchrow(sql, "candles" if kind == "candle" else "quotes")
            if observation:
                trace = dict(observation)
                trace["payload"] = json.loads(trace["payload"])
                trace["observation_sha256"] = bytes(trace["observation_sha256"]).hex()
                receipt = await right.fetchval(
                    "SELECT receipt FROM ingest_batches WHERE producer_id=$1 AND batch_id=$2",
                    observation["producer_id"],
                    observation["batch_id"],
                )
                trace["receipt"] = json.loads(receipt)
                # Event IDs are journal identities, not observation delivery IDs.
                event = await right.fetchrow(
                    "SELECT * FROM marketdata_events WHERE instrument_id=$1 AND provider=$2"
                    " AND kind=$3 ORDER BY id LIMIT 1",
                    observation["instrument_id"],
                    observation["provider"],
                    "candle_completed" if kind == "candle" else "price_update",
                )
                trace["journal_event"] = dict(event) if event else None
                if event:
                    trace["websocket_event"] = next(
                        (
                            v
                            for v in records(directory / "websocket.ndjson")
                            if v["event"].get("event_id") == str(event["id"])
                        ),
                        None,
                    )
                write_json(directory / f"trace-{kind}.json", trace)
        write_json(directory / "database-comparison.json", evidence)
    finally:
        for c in connections.values():
            await c.close()
    return evidence


def measurements(directory):
    samples = list(records(directory / "samples.ndjson"))
    result = dict(
        phases={}, clocks="UTC wall-clock proxies; observer timestamps bound commit completion"
    )
    if samples:
        result["database_sampling"] = {
            role: dict(
                first_bytes=samples[0][role + "_bytes"],
                final_bytes=samples[-1][role + "_bytes"],
                peak_bytes=max(s[role + "_bytes"] for s in samples),
                peak_connections=max(s[role + "_connections"] for s in samples),
            )
            for role in ("source", "destination")
        }
    per_instrument = defaultdict(list)
    for phase in ("backfill", "acceptance", "steady", "burst", "final"):
        chosen = [s for s in samples if s["phase"] == phase]
        if not chosen:
            continue
        ages = [p["age_ms"] for s in chosen for p in s["prices"] if p["age_ms"] is not None]
        result["phases"][phase] = dict(
            sample_count=len(chosen),
            latest_price_age_ms=percentiles(ages),
            quote_stale_samples=sum(
                p["quote_freshness"] != "fresh" for s in chosen for p in s["prices"]
            ),
            all_status_streams_unexpired_samples=sum(
                s["live_status_streams"] == len(s["prices"]) for s in chosen
            ),
            max_pending_candles=max(s["candle_pending"] for s in chosen),
            max_pending_quote_streams=max(s["quote_pending"] for s in chosen),
        )
        for s in chosen:
            for p in s["prices"]:
                per_instrument[(p["symbol"], phase)].append(p)
    with (directory / "per-instrument.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(
            output, fieldnames=["symbol", "phase", "samples", "stale", "p50_ms", "p95_ms", "max_ms"]
        )
        writer.writeheader()
        for (symbol, phase), values in per_instrument.items():
            ages = percentiles([v["age_ms"] for v in values if v["age_ms"] is not None])
            writer.writerow(
                dict(
                    symbol=symbol,
                    phase=phase,
                    samples=len(values),
                    stale=sum(v["quote_freshness"] != "fresh" for v in values),
                    p50_ms=ages["p50"],
                    p95_ms=ages["p95"],
                    max_ms=ages["p100"],
                )
            )
    result["resources"] = {}
    for role in ("collector", "worker", "ingest", "read"):
        values = [
            dict(at=s["at"], **s["processes"][role])
            for s in samples
            if role in s.get("processes", {}) and not s["processes"][role].get("unavailable")
        ]
        if values:
            result["resources"][role] = dict(
                peak_rss_bytes=max(v["rss_bytes"] for v in values),
                cpu_seconds_observed=values[-1]["cpu_seconds"] - values[0]["cpu_seconds"]
                if values[0]["pid"] == values[-1]["pid"]
                else None,
                elapsed_seconds=seconds(values[0]["at"], values[-1]["at"]),
            )
    operations = defaultdict(list)
    errors = Counter()
    staged = {}
    for row in records(directory / "worker.ndjson"):
        if row["kind"] == "worker_http_result":
            operations[row["method"] + " " + row["operation"]].append(
                seconds(row["started_at"], row["observed_at"])
            )
        elif row["kind"] == "worker_http_error":
            errors[row["category"]] += 1
        elif row["kind"] == "worker_boundary" and row["boundary"] == "after_staging_commit":
            staged[row["batch_id"]] = row["observed_at"]
    result["http_seconds_including_client_quota_wait"] = {
        k: percentiles(v) for k, v in operations.items()
    }
    result["worker_http_errors"] = dict(errors)
    commits = defaultdict(list)
    staged_to_commit = defaultdict(list)
    for row in records(directory / "ingest.ndjson"):
        if row["kind"] == "destination_commit_observed":
            commits[row["stream"]].append(seconds(row["started_at"], row["observed_at"]))
            if row["batch_id"] in staged:
                staged_to_commit[row["stream"]].append(
                    seconds(staged[row["batch_id"]], row["observed_at"])
                )
    result["destination_transaction_seconds_observed"] = {
        k: percentiles(v) for k, v in commits.items()
    }
    result["staging_to_destination_commit_seconds_observed"] = {
        k: percentiles(v) for k, v in staged_to_commit.items()
    }
    local = defaultdict(list)
    for row in records(directory / "collector.ndjson"):
        if row["kind"] in {"local_quote_commit_observed", "local_candle_commit_observed"}:
            local[row["kind"]].append(seconds(row["started_at"], row["observed_at"]))
    result["local_store_transaction_seconds_observed"] = {
        k: percentiles(v) for k, v in local.items()
    }
    ws = []
    for row in records(directory / "websocket.ndjson"):
        event = row["event"]
        if "recorded_at" in event:
            ws.append(seconds(event["recorded_at"], row["observed_at"]))
    result["journal_append_to_websocket_observer_seconds"] = percentiles(ws)
    result["unmeasured"] = [
        "true upstream publication time",
        "individual server WAL flush instant",
        "all intermediate quote histories",
        "host-wide CPU attribution",
        "real company WAN/gateway latency",
    ]
    write_json(directory / "measurements.json", result)
    return result


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--database-file", type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    report = json.loads((directory / "report.json").read_text("utf-8"))
    result = measurements(directory)
    if args.database_file:
        result["database_verified"] = await database_evidence(
            directory, json.loads(args.database_file.read_text("utf-8")), report
        )
    print(json.dumps(dict(report_written=str(directory), phases=list(result["phases"]))))


if __name__ == "__main__":
    asyncio.run(main())
