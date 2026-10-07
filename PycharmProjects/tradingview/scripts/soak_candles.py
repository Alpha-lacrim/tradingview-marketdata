"""Read-only candle soak observer. Polls local API/DB, NEVER polls the upstream source.

Duration in seconds; observations survive interruption in --output. Does not inject faults.
"""

import argparse
import asyncio
import json
import math
import time
from pathlib import Path
from typing import Any

import asyncpg
import httpx
from prometheus_client.parser import text_string_to_metric_families

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.persistence import Store

STAGES = (
    "scheduler_lateness",
    "grace_to_fetch_start",
    "fetch_cycle_duration",
    "close_to_first_receive",
    "close_to_last_receive",
    "first_to_last_receive",
    "db_write",
    "close_to_commit",
)


def distribution(values: list[float]) -> dict[str, float | int]:
    ordered = sorted(values)
    if not ordered:
        return {"samples": 0}
    return dict(
        samples=len(ordered),
        p50=ordered[math.ceil(len(ordered) * 0.5) - 1],
        p95=ordered[math.ceil(len(ordered) * 0.95) - 1],
        max=ordered[-1],
    )


async def run(args: argparse.Namespace) -> None:
    if not 60 <= args.seconds <= 72 * 3600:
        raise ValueError("soak duration must be 60s..72h")
    settings = Settings()
    store = Store(settings)
    await store.connect()
    started = utc_now()
    timer = time.perf_counter()
    cycles: dict[str, dict[str, Any]] = {}
    failures = 0
    headers = (
        {"Authorization": "Bearer " + settings.api_key.get_secret_value()}
        if settings.api_key
        else {}
    )
    async with store.connection() as c:
        raw_before = await c.fetchval(
            "SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest'"
        )
        candle_before = await c.fetchval(
            "SELECT count(*) FROM market_candles WHERE provider='tradingview_guest'"
        )
    summary: dict[str, Any] = {}
    previous_counters: dict[str, float] = {}
    counter_deltas: dict[str, float] = {}
    symbol_latencies: dict[str, list[float]] = {}
    counter_resets = 0
    metric_snapshots = 0
    last_cycle: str | None = None
    database_evidence: dict[str, Any] = {}
    try:
        async with httpx.AsyncClient(base_url=args.base_url, headers=headers, timeout=10) as client:
            while time.perf_counter() - timer < args.seconds:
                try:
                    response = await client.get("/providers")
                    response.raise_for_status()
                    provider = next(
                        p for p in response.json() if p["provider"] == "tradingview_guest"
                    )
                    metric_response = await client.get("/metrics")
                    metric_response.raise_for_status()
                    for family in text_string_to_metric_families(metric_response.text):
                        for sample in family.samples:
                            if sample.name not in (
                                "marketdata_candle_fetch_cycle_total",
                                "marketdata_candles_total",
                                "marketdata_database_errors_total",
                                "marketdata_reconnects_total",
                            ):
                                continue
                            key = sample.name + str(sorted(sample.labels.items()))
                            if key in previous_counters or metric_snapshots:
                                delta = sample.value - previous_counters.get(key, 0)
                                if delta < 0:
                                    counter_resets += 1
                                    delta = sample.value
                                counter_deltas[key] = counter_deltas.get(key, 0) + delta
                            previous_counters[key] = sample.value
                    metric_snapshots += 1
                    cycle = provider.get("last_candle_cycle", {})
                    if cycle and cycle["fetch_cycle_started_at"] >= started.isoformat():
                        key = cycle["fetch_cycle_started_at"]
                        if key not in cycles:
                            for symbol, timing in cycle.get("instruments", {}).items():
                                symbol_latencies.setdefault(symbol, []).append(
                                    timing["duration_seconds"]
                                )
                        cycles[key] = {k: v for k, v in cycle.items() if k != "instruments"}
                    summary = dict(
                        started_at=started.isoformat(),
                        observed_at=utc_now().isoformat(),
                        actual_duration_seconds=time.perf_counter() - timer,
                        collection_cycles=len(cycles),
                        startup_cycles=sum(r.get("trigger") == "startup" for r in cycles.values()),
                        normal_cycles=sum(
                            r.get("trigger", "minute") == "minute" for r in cycles.values()
                        ),
                        counter_deltas=counter_deltas,
                        counter_resets=counter_resets,
                        per_symbol_fetch_latency={
                            s: distribution(v) for s, v in symbol_latencies.items()
                        },
                        normal_cycle_duplicate_buckets=len(
                            [r for r in cycles.values() if r.get("trigger") == "minute"]
                        )
                        - len(
                            {
                                r["bucket_end"]
                                for r in cycles.values()
                                if r.get("trigger") == "minute"
                            }
                        ),
                        cycles=list(cycles.values()),
                        observer_failures=failures,
                        provider_state=provider["state"],
                        symbols=provider["symbols"],
                        latency={
                            s: distribution(
                                [
                                    r[s + "_seconds"]
                                    for r in cycles.values()
                                    if s + "_seconds" in r
                                    and r.get("trigger", "minute") == "minute"
                                ]
                            )
                            for s in STAGES
                        },
                        corrections=sum(r.get("corrections", 0) for r in cycles.values()),
                        db_failures=sum(r.get("db_failures", 0) for r in cycles.values()),
                    )
                    summary.update(database_evidence)
                    if cycle.get("fetch_cycle_started_at") == last_cycle and database_evidence:
                        await asyncio.sleep(
                            min(5, max(0, args.seconds - (time.perf_counter() - timer)))
                        )
                        args.output.write_text(json.dumps(summary, indent=2, default=str))
                        continue
                    last_cycle = cycle.get("fetch_cycle_started_at")
                    async with store.connection() as c:
                        raw_after = await c.fetchval(
                            "SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest'"
                        )
                        candles = await c.fetchval(
                            "SELECT count(*) FROM market_candles WHERE provider='tradingview_guest'"
                        )
                        buckets = await c.fetch(
                            """SELECT bucket_start,count(*) AS candles FROM market_candles
                            WHERE provider='tradingview_guest' AND bucket_end >= $1
                            GROUP BY bucket_start ORDER BY bucket_start""",
                            started,
                        )
                        malformed = await c.fetchval("""SELECT count(*) FROM market_candles
                            WHERE provider='tradingview_guest'
                            AND (bucket_end > received_timestamp OR bucket_end > clock_timestamp()
                                OR high<low OR high<open OR high<close OR low>open OR low>close)""")
                        duplicates = await c.fetchval("""SELECT count(*) FROM (SELECT 1
                            FROM market_candles
                            GROUP BY instrument_id,provider,interval,bucket_start,price_basis
                            HAVING count(*)>1) d""")
                    database_evidence = dict(
                        raw_ticks_before=raw_before,
                        raw_ticks_after=raw_after,
                        raw_ticks_unchanged=raw_after == raw_before,
                        candle_rows_added=candles - candle_before,
                        buckets=[dict(r) for r in buckets],
                        malformed_candles=malformed,
                        duplicate_logical_candles=duplicates,
                        unresolved_expected_gaps=sum(
                            s["missing_candle_count"] for s in provider["symbols"].values()
                        ),
                        unknown_minutes=sum(
                            s["unknown_minutes"] for s in provider["symbols"].values()
                        ),
                        not_expected_minutes=sum(
                            s["not_expected_minutes"] for s in provider["symbols"].values()
                        ),
                    )
                    summary.update(database_evidence)
                    args.output.write_text(json.dumps(summary, indent=2, default=str))
                except (
                    httpx.HTTPError,
                    OSError,
                    TimeoutError,
                    asyncpg.PostgresError,
                    asyncpg.InterfaceError,
                ):
                    failures += 1
                await asyncio.sleep(min(5, max(0, args.seconds - (time.perf_counter() - timer))))
    finally:
        summary["actual_duration_seconds"] = time.perf_counter() - timer
        summary["observer_failures"] = failures
        args.output.write_text(json.dumps(summary, indent=2, default=str))
        await store.close()
    print(
        json.dumps(
            {k: v for k, v in summary.items() if k not in ("cycles", "symbols", "buckets")},
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=3600)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--output", type=Path, default=Path("artifacts/candle-soak.json"))
    asyncio.run(run(parser.parse_args()))
