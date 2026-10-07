"""Candle retrieval/capacity benchmarks: live opt-in, synthetic transport, or test-DB writes.

Live: existing 17 sources, ONE diagnostic cycle/minute, conservative escalation gate.
Synthetic: no Internet, isolated synthetic identities; never writes production market data.
Persistence: TEST_DATABASE_URL must end _test; writes clearly simulated benchmark records.
"""

import argparse
import asyncio
import json
import math
import os
import time
import tracemalloc
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from marketdata.candle_benchmark import SyntheticChartSocket, synthetic_pairs
from marketdata.candles import MarketCandle, expected_minute, minute_start, next_collection
from marketdata.config import Settings
from marketdata.domain import DataStatus, RegistryEntry, utc_now
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import Store
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


def quantiles(values):
    ordered = sorted(v for v in values if v is not None)
    return (
        dict(
            samples=len(ordered),
            p50=ordered[math.ceil(len(ordered) * 0.5) - 1],
            p95=ordered[math.ceil(len(ordered) * 0.95) - 1],
            max=ordered[-1],
        )
        if ordered
        else dict(samples=0)
    )


def summaries(rows):
    groups = sorted({(r["instrument_count"], r["window"]) for r in rows})
    return [
        dict(
            instrument_count=n,
            window=w,
            strategy="per_request",
            sockets=1,
            cycle=quantiles(
                [
                    r["cycle_seconds"]
                    for r in rows
                    if r["instrument_count"] == n and r["window"] == w
                ]
            ),
            first=quantiles(
                [
                    r["first_receive_seconds"]
                    for r in rows
                    if r["instrument_count"] == n and r["window"] == w
                ]
            ),
            last=quantiles(
                [
                    r["last_receive_seconds"]
                    for r in rows
                    if r["instrument_count"] == n and r["window"] == w
                ]
            ),
            instrument_p50=quantiles(
                [
                    r["median_receive_seconds"]
                    for r in rows
                    if r["instrument_count"] == n and r["window"] == w
                ]
            ),
            instrument_p95=quantiles(
                [
                    r["p95_receive_seconds"]
                    for r in rows
                    if r["instrument_count"] == n and r["window"] == w
                ]
            ),
            cpu_seconds=quantiles(
                [r["cpu_seconds"] for r in rows if r["instrument_count"] == n and r["window"] == w]
            ),
            additional_python_peak_bytes=quantiles(
                [
                    r["python_peak_bytes"]
                    for r in rows
                    if r["instrument_count"] == n and r["window"] == w
                ]
            ),
            timeouts=sum(
                r["timeouts"] for r in rows if r["instrument_count"] == n and r["window"] == w
            ),
            missing=sum(
                r["missing_current"]
                for r in rows
                if r["instrument_count"] == n and r["window"] == w
            ),
            disconnects=sum(
                r["disconnects"] for r in rows if r["instrument_count"] == n and r["window"] == w
            ),
            errors=sum(
                len(r["failures"]) + bool(r["connection_error"])
                for r in rows
                if r["instrument_count"] == n and r["window"] == w
            ),
        )
        for n, w in groups
    ]


async def probe(settings, pairs, count, synthetic_latency=None):
    p = TradingViewCandleProvider(settings, Metrics())
    await p.subscribe(pairs)
    if synthetic_latency is None:
        target = next_collection(utc_now(), settings.candle_fetch_grace_seconds)
        while True:
            remaining = (target - utc_now()).total_seconds()
            if remaining <= 0:
                break
            await asyncio.sleep(min(30, remaining))
        end = expected_minute(target, p.grace) + timedelta(minutes=1)
    else:
        end = minute_start(utc_now()) - timedelta(minutes=1)
        p.socket = SyntheticChartSocket(end, latency=synthetic_latency)
    start = end - timedelta(minutes=count - 1)
    started, timer, cpu = utc_now(), time.perf_counter(), time.process_time()
    tracemalloc.reset_peak()
    memory_before = tracemalloc.get_traced_memory()[0]
    error, results = None, {}
    try:
        if synthetic_latency is None:
            await p.connect()
        results = await p.fetch_many([(pair, start, end, count) for pair in pairs])
    except Exception as exc:
        error = type(exc).__name__
    finally:
        await p.disconnect()
    duration = time.perf_counter() - timer
    current = [
        c
        for bars in results.values()
        if isinstance(bars, list)
        for c in bars
        if c.bucket_end == end
    ]
    reference = end if synthetic_latency is None else started
    availability = sorted(
        max(
            0,
            (
                datetime.fromisoformat(v["completed_candle_available_at"]) - reference
            ).total_seconds(),
        )
        for v in p.timings.values()
        if v.get("completed_candle_available_at")
    )
    failures = {s: type(v).__name__ for s, v in results.items() if isinstance(v, Exception)}
    stats = p.health.candle_retrieval
    return dict(
        window=settings.tradingview_candle_concurrency,
        effective_window=stats.get("window"),
        strategy="per_request",
        sockets=1,
        instrument_count=len(pairs),
        requested_bars=count,
        started_at=started.isoformat(),
        bucket_end=end.isoformat(),
        cycle_seconds=duration,
        cpu_seconds=time.process_time() - cpu,
        python_peak_bytes=max(0, tracemalloc.get_traced_memory()[1] - memory_before),
        current_candles=len(current),
        missing_current=len(pairs) - len(current),
        receipt_reference="bucket_end" if synthetic_latency is None else "cycle_start",
        first_receive_seconds=availability[0] if availability else None,
        median_receive_seconds=availability[math.ceil(len(availability) * 0.5) - 1]
        if availability
        else None,
        p95_receive_seconds=availability[math.ceil(len(availability) * 0.95) - 1]
        if availability
        else None,
        last_receive_seconds=availability[-1] if availability else None,
        timeouts=stats.get("timeouts", 0),
        disconnects=p.metrics.tradingview_disconnects._value.get(),
        failures=failures,
        connection_error=error,
        retrieval=stats,
        timings=p.timings,
        candle_rows_received=sum(len(v) for v in results.values() if isinstance(v, list)),
    )


async def persistence(args, output):
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not urlsplit(url).path.endswith("_test"):
        raise ValueError("persistence benchmark requires disposable TEST_DATABASE_URL ending _test")
    store = Store(Settings(_env_file=None, database_url=url))
    namespace = uuid4().hex[:12]
    output["simulated_namespace"] = namespace
    await store.connect()
    try:
        for size in (17, 100, 500):
            pairs = [
                (
                    i.model_copy(update={"canonical_symbol": f"BENCH:{namespace.upper()}:{index}"}),
                    m.model_copy(update={"provider": f"benchmark_{namespace}"}),
                )
                for index, (i, m) in enumerate(synthetic_pairs(size))
            ]
            await store.register(
                Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in pairs])
            )
            for round_index in range(args.rounds):
                end = minute_start(utc_now()) - timedelta(days=1, minutes=round_index + size)
                bars = [
                    MarketCandle(
                        instrument_id=i.id,
                        canonical_symbol=i.canonical_symbol,
                        provider=m.provider,
                        provider_symbol=m.provider_symbol,
                        bucket_start=end - timedelta(minutes=1),
                        bucket_end=end,
                        open="100.1234567890123456789",
                        high="102",
                        low="99",
                        close="101",
                        volume="10",
                        realtime_status=DataStatus.SIMULATED,
                        source_status="streaming",
                        received_timestamp=utc_now(),
                        metadata={"provenance": "simulated_benchmark"},
                    )
                    for i, m in pairs
                ]
                coverage = [
                    (
                        c.instrument_id,
                        c.provider,
                        c.bucket_start,
                        c.bucket_end,
                        {"session": "24x7", "timezone": "Etc/UTC"},
                    )
                    for c in bars
                ]
                timer, cpu = time.perf_counter(), time.process_time()
                changed = await store.write_candles(bars, coverage)
                elapsed = time.perf_counter() - timer
                row = dict(
                    instrument_count=size,
                    changed_rows=changed,
                    cycle_seconds=elapsed,
                    cpu_seconds=time.process_time() - cpu,
                )
                output["cycles"].append(row)
                args.output.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
                print(json.dumps(row), flush=True)
    finally:
        await store.close()


async def run(args):
    if not 2 <= args.rounds <= 15 or any(w not in (1, 2, 4, 6, 8, 12) for w in args.windows):
        raise ValueError("use 2..15 rounds and candidate windows 1/2/4/6/8/12")
    if not 0 <= args.latency <= 5 or not 2 <= args.bars <= 301:
        raise ValueError("synthetic latency 0..5 and requested bars 2..301")
    if any(n not in (17, 34, 64, 100) for n in args.counts):
        raise ValueError("synthetic universe must be 17/34/64/100")
    windows = sorted(set(args.windows))
    if args.live and any(
        w > 2 and {4: 2, 6: 4, 8: 6, 12: 8}[w] not in windows for w in windows if w > 2
    ):
        raise ValueError("live escalation requires each preceding candidate, starting at 1 or 2")
    configure_logging("ERROR")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output = dict(
        mode="LIVE" if args.live else "PERSISTENCE" if args.persistence else "SYNTHETIC",
        synthetic_latency_seconds=args.latency if args.synthetic else None,
        safety_budget_seconds=60
        - Settings().candle_fetch_grace_seconds
        - Settings().candle_cycle_safety_margin_seconds,
        cycles=[],
    )
    if args.persistence:
        await persistence(args, output)
        return
    tracemalloc.start()
    live_pairs = (
        Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
            "tradingview_guest"
        )
        if args.live
        else []
    )
    for window in windows:
        for size in [17] if args.live else args.counts:
            for _ in range(args.rounds):
                settings = Settings(tradingview_candle_concurrency=window)
                row = await probe(
                    settings,
                    live_pairs if args.live else synthetic_pairs(size),
                    args.bars,
                    None if args.live else args.latency,
                )
                output["cycles"].append(row)
                output["summary"] = summaries(output["cycles"])
                args.output.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
                print(json.dumps({k: v for k, v in row.items() if k != "timings"}), flush=True)
                if row["failures"] or row["connection_error"] or row["missing_current"]:
                    output["stopped_reason"] = "incomplete cycle/overload symptoms; no escalation"
                    args.output.write_text(
                        json.dumps(output, indent=2, default=str), encoding="utf-8"
                    )
                    print("STOP: incomplete cycle; no higher-concurrency probe", flush=True)
                    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true")
    mode.add_argument("--synthetic", action="store_true")
    mode.add_argument("--persistence", action="store_true")
    parser.add_argument("--windows", nargs="+", type=int, default=[1, 2, 4])
    parser.add_argument("--counts", nargs="+", type=int, default=[17, 34, 64, 100])
    parser.add_argument("--bars", type=int, default=4)
    parser.add_argument("--latency", type=float, default=0.35)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("artifacts/retrieval-probes.json"))
    asyncio.run(run(parser.parse_args()))
