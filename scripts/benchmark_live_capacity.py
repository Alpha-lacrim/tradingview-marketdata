"""Staged, retrieval-only live capacity experiment; never registers/writes market data.

Stop the normal collector first. Each verification or capacity cycle uses one UTC minute
target, one socket, ceiling two, and the existing native parser/transport. A failed stage
prevents escalation. Results and the verified universe are checkpointed after every cycle.
"""

import argparse
import asyncio
import json
import time
import tracemalloc
from datetime import datetime, timedelta
from pathlib import Path

from benchmark_candle_retrieval import quantiles

from marketdata.candles import expected_minute, next_collection
from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics, configure_logging
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


def summarize(rows):
    return dict(
        cycles=len(rows),
        immediate_complete_cycles=sum(r["missing_current"] == 0 for r in rows),
        immediate_candles=sum(r["current_candles"] for r in rows),
        expected_candles=sum(r["instrument_count"] for r in rows),
        cycle_seconds=quantiles([r["cycle_seconds"] for r in rows]),
        under_budget_percent=100 * sum(r["cycle_seconds"] < 48 for r in rows) / len(rows),
        safety_margin_seconds=48 - quantiles([r["cycle_seconds"] for r in rows])["p95"],
        first_parsed=quantiles([r["first_parsed_seconds"] for r in rows]),
        median_parsed=quantiles([r["p50_parsed_seconds"] for r in rows]),
        p95_parsed=quantiles([r["p95_parsed_seconds"] for r in rows]),
        last_parsed=quantiles([r["last_parsed_seconds"] for r in rows]),
        timeouts=sum(r["retrieval"].get("timeouts", 0) for r in rows),
        disconnects=sum(r["disconnects"] for r in rows),
        budget_exceeded=sum(r["cycle_seconds"] >= 48 for r in rows),
        skipped_minute_targets=sum(r["skipped_minute_targets"] for r in rows),
        possible_throttle=sum(r["possible_throttle"] for r in rows),
    )


def stage_passes(rows, unresolved, allowed_sparse=frozenset()):
    """Conservative empirical escalation gate, not proof of long-term reliability."""
    s = summarize(rows)
    return (
        sum(
            row["missing_current"]
            <= len(set(row.get("source_completed_without_target", [])) & allowed_sparse)
            for row in rows
        )
        / len(rows)
        >= 0.9
        and not any(symbol not in allowed_sparse for symbol, _ in unresolved)
        and s["cycle_seconds"]["p95"] < 38
        and not s["disconnects"]
        and not s["skipped_minute_targets"]
        and not any(
            r["connection_error"] or r["retrieval"].get("errors", {}).get("protocol") for r in rows
        )
    )


async def cycle(provider, pairs, pending, previous_end=None):
    target = next_collection(utc_now(), provider.grace)
    while True:
        remaining = (target - utc_now()).total_seconds()
        if remaining <= 0:
            break
        await asyncio.sleep(min(30, remaining))
    end = expected_minute(target, provider.grace) + timedelta(minutes=1)
    timer, cpu = time.perf_counter(), time.process_time()
    memory_before = tracemalloc.get_traced_memory()[0]
    tracemalloc.reset_peak()
    jobs = []
    for pair in pairs:
        symbol = pair[0].canonical_symbol
        missing = [bucket for s, bucket in pending if s == symbol]
        start = min([end - timedelta(minutes=3), *missing])
        if previous_end and end - previous_end > timedelta(minutes=1):
            start = min(start, previous_end)
        start = max(start, end - timedelta(minutes=provider.settings.candle_backfill_minutes))
        jobs.append((pair, start, end, int((end - start).total_seconds() // 60) + 1))
    before_disconnects = provider.metrics.tradingview_disconnects._value.get()
    throttle = lambda: sum(  # noqa: E731
        sample.value
        for metric in provider.metrics.tradingview_throttle.collect()
        for sample in metric.samples
        if sample.name.endswith("_total")
    )
    before_throttle = throttle()
    results, error = {}, None
    provider.timings.clear()
    provider.health.candle_retrieval = {}
    try:
        await provider.connect()
        results = await provider.fetch_many(jobs)
    except Exception as exc:
        error = type(exc).__name__
    finally:
        await provider.disconnect()
    current, received, evidence, failures, native_missing = set(), set(), {}, {}, []
    for pair in pairs:
        instrument, mapping = pair
        result = results.get(instrument.canonical_symbol)
        if isinstance(result, list):
            for candle in result:
                assert candle.instrument_id == instrument.id
                assert candle.provider_symbol == mapping.provider_symbol
                assert candle.price_basis == "native" and candle.interval == "1m"
                assert candle.realtime_status == "realtime"
                assert candle.bucket_end <= end
                received.add((instrument.canonical_symbol, candle.bucket_start))
                if candle.bucket_end == end:
                    current.add(instrument.canonical_symbol)
                    evidence[instrument.canonical_symbol] = candle.model_dump(mode="json")
            if instrument.canonical_symbol not in current:
                native_missing.append(instrument.canonical_symbol)
        else:
            failures[instrument.canonical_symbol] = type(result).__name__
    parsed = sorted(
        max(
            0,
            (
                datetime.fromisoformat(provider.timings[s]["parser_completed_at"]) - end
            ).total_seconds(),
        )
        for s in current
        if s in provider.timings
    )
    q = quantiles(parsed)
    stats = dict(provider.health.candle_retrieval) if provider.timings else {}
    return (
        dict(
            started_at=target.isoformat(),
            finished_at=utc_now().isoformat(),
            bucket_end=end.isoformat(),
            instrument_count=len(pairs),
            sockets=1,
            configured_window=2,
            effective_window=stats.get("window"),
            next_window=provider.effective_concurrency,
            cycle_seconds=time.perf_counter() - timer,
            cpu_seconds=time.process_time() - cpu,
            python_current_bytes=tracemalloc.get_traced_memory()[0],
            python_additional_peak_bytes=max(0, tracemalloc.get_traced_memory()[1] - memory_before),
            tasks_after_cycle=len(asyncio.all_tasks()),
            receiver_tasks_after_cycle=sum(
                t.get_name() == "tradingview-candle-receiver" for t in asyncio.all_tasks()
            ),
            current_candles=len(current),
            missing_current=len(pairs) - len(current),
            first_parsed_seconds=parsed[0] if parsed else None,
            p50_parsed_seconds=q.get("p50"),
            p95_parsed_seconds=q.get("p95"),
            last_parsed_seconds=q.get("max"),
            retrieval=stats,
            timings=dict(provider.timings),
            failures=failures,
            source_completed_without_target=native_missing,
            connection_error=error,
            disconnects=provider.metrics.tradingview_disconnects._value.get() - before_disconnects,
            close_code=provider.health.last_close_code,
            possible_throttle=throttle() - before_throttle,
            skipped_minute_targets=max(0, int((end - previous_end).total_seconds() // 60) - 1)
            if previous_end
            else 0,
            # No deletion acknowledgements exist in the captured protocol. These are client sends.
            socket_closed=provider.socket is None,
        ),
        received,
        evidence,
    )


async def run(args):
    if not args.collector_stopped:
        raise ValueError("stop the normal collector; pass --collector-stopped")
    if not 3 <= args.cycles <= 15 or not 3 <= args.baseline_cycles <= 15:
        raise ValueError("use 3..15 cycles per stage")
    if args.counts != [17, 34, 64, 100][: len(args.counts)]:
        raise ValueError("capacity stages must be a prefix of 17 34 64 100")
    configure_logging("ERROR")
    output = dict(
        mode="LIVE",
        started_at=utc_now().isoformat(),
        cycles=[],
        verification=[],
        stages=[],
        production_writes=0,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def checkpoint():
        args.output.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")

    base = Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
        "tradingview_guest"
    )
    candidates = Registry.load(args.candidates).subscriptions("tradingview_guest")
    if len(base) != 17 or set(p[1].provider_symbol for p in base) & set(
        p[1].provider_symbol for p in candidates
    ):
        raise ValueError("expected production 17 plus disjoint benchmark-only candidate registry")
    verified = []
    verified_details = {}
    carry_missing = set()
    next_window = 2
    if args.resume:
        prior = json.loads(args.resume.read_text(encoding="utf-8"))
        last = prior["cycles"][-1]
        if last["connection_error"] != "TimeoutError" or last["timings"]:
            raise ValueError("resume only a pre-request transient handshake timeout")
        age = (utc_now() - datetime.fromisoformat(last["finished_at"])).total_seconds()
        if not 0 <= age <= 3600 or prior.get("resumed_from"):
            raise ValueError("one retry window, within one hour; retain all prior results")
        output["resumed_from"] = str(args.resume)
        output["prior_window"] = prior
        identities = {v["source_symbol"]: v for v in prior["verified_universe"]}
        verified = [p for p in candidates if p[1].provider_symbol in identities]
        for pair in verified:
            detail = identities[pair[1].provider_symbol]
            if (
                detail["realtime_status"] != "realtime"
                or detail["update_mode"] not in ("streaming", "realtime")
                or detail["source_info"]["full_name"] != pair[1].provider_symbol
            ):
                raise ValueError("prior independent native verification is incomplete")
            verified_details[pair[0].canonical_symbol] = {
                key: detail[key]
                for key in ("realtime_status", "update_mode", "source_info", "verified_at")
            }
        if len(verified) != len(identities):
            raise ValueError("resume candidates must contain the exact verified identities")
        carry_missing = {
            (r["symbol"], datetime.fromisoformat(r["bucket"]))
            for r in prior["stages"][-1]["unresolved"]
        }
        next_window = last["next_window"]
    liquid_base = [pair for pair in base if pair[1].provider_symbol != "OKX:XAUTUSDT"]
    output["expanded_universe_exclusions"] = {
        "OKX:XAUTUSDT": "known sparse native trade bars; production baseline retains it"
    }
    tracemalloc.start()
    provider = TradingViewCandleProvider(Settings(tradingview_candle_concurrency=2), Metrics())
    provider.effective_concurrency = next_window
    previous_end = None
    for size in args.counts:
        seed = base if size == 17 else liquid_base
        needed = size - len(seed) - len(verified)
        if needed > 0:
            # Verification is separately labelled; each batch is <=17, one cycle/minute.
            selection = candidates[len(verified) : len(verified) + needed]
            if len(selection) != needed:
                output["stopped_reason"] = "insufficient independently verified candidates"
                checkpoint()
                return
            for offset in range(0, needed, 17):
                chunk = selection[offset : offset + 17]
                await provider.subscribe(chunk)
                row, received, evidence = await cycle(provider, chunk, set(), previous_end)
                previous_end = datetime.fromisoformat(row["bucket_end"])
                output["verification"].append(dict(cycle=row, candles=evidence))
                checkpoint()
                # Three consecutive native completed bars reduce sparse-source confounding.
                if (
                    row["missing_current"]
                    or row["failures"]
                    or any(
                        sum(s == pair[0].canonical_symbol for s, _ in received) < 3
                        for pair in chunk
                    )
                ):
                    output["stopped_reason"] = (
                        "candidate native verification incomplete; no escalation"
                    )
                    checkpoint()
                    return
                verified.extend(chunk)
                for pair in chunk:
                    symbol = pair[0].canonical_symbol
                    timing = row["timings"][symbol]
                    verified_details[symbol] = dict(
                        realtime_status="realtime",
                        update_mode=timing["source_status"],
                        source_info=timing["source_info"],
                        verified_at=row["finished_at"],
                    )
                print(json.dumps(dict(verified_candidates=len(verified))), flush=True)
        pairs = [*seed, *verified][:size]
        # Retain decrease-only effective concurrency across every cycle/stage.
        await provider.subscribe(pairs)
        rows, expected, received_all, pending = [], set(), set(), set()
        stage_cycles = args.baseline_cycles if size == 17 else args.cycles
        last_stage_end = None
        for _ in range(stage_cycles):
            row, received, _ = await cycle(provider, pairs, pending | carry_missing, previous_end)
            previous_end = datetime.fromisoformat(row["bucket_end"])
            bucket = previous_end - timedelta(minutes=1)
            first = last_stage_end or bucket
            while first <= bucket:
                expected.update((p[0].canonical_symbol, first) for p in pairs)
                first += timedelta(minutes=1)
            last_stage_end = previous_end
            received_all.update(received)
            recovered = len((pending | carry_missing) & received)
            recovered_buckets = [
                dict(symbol=s, bucket=b.isoformat())
                for s, b in sorted((pending | carry_missing) & received)
            ]
            pending = expected - received_all
            carry_missing -= received
            output["prior_window_unresolved"] = [
                dict(symbol=s, bucket=b.isoformat()) for s, b in sorted(carry_missing)
            ]
            row.update(
                recovered_previous_minutes=recovered,
                recovered_buckets=recovered_buckets,
                unresolved_minutes=len(pending),
            )
            rows.append(row)
            output["cycles"].append(row)
            checkpoint()
            print(
                json.dumps({k: v for k, v in row.items() if k not in ("timings", "retrieval")}),
                flush=True,
            )
            if row["connection_error"] or row["disconnects"] or row["possible_throttle"] >= 2:
                output["stopped_reason"] = "unstable source window; no escalation"
                break
        allowed_sparse = {"CRYPTO:OKX:XAUTUSDT"} if size == 17 else set()
        passed = len(rows) == stage_cycles and stage_passes(rows, pending, allowed_sparse)
        if size > 17 and carry_missing:
            passed = False
        output["stages"].append(
            dict(
                instrument_count=size,
                summary=summarize(rows),
                escalation_gate_passed=passed,
                eventual_candles=len(expected & received_all),
                expected_candles=len(expected),
                unresolved=[dict(symbol=s, bucket=b.isoformat()) for s, b in sorted(pending)],
                verdict="VERIFIED LIVE"
                if passed and not pending and all(r["missing_current"] == 0 for r in rows)
                else "PARTIALLY VERIFIED"
                if passed
                else "UNSTABLE / INCONCLUSIVE",
            )
        )
        output["verified_universe"] = [
            dict(
                canonical_benchmark_identifier=p[0].canonical_symbol,
                source_symbol=p[1].provider_symbol,
                asset_class=p[0].asset_class,
                instrument_type=p[0].instrument_type,
                exchange=p[0].exchange,
                independent_native_verification=True,
                **verified_details[p[0].canonical_symbol],
            )
            for p in verified
        ]
        checkpoint()
        if not passed:
            output.setdefault("stopped_reason", "stage failed conservative escalation gate")
            checkpoint()
            return
    output["finished_at"] = utc_now().isoformat()
    checkpoint()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-stopped", action="store_true")
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--counts", type=int, nargs="+", default=[17, 34, 64, 100])
    parser.add_argument("--cycles", type=int, default=10)
    parser.add_argument("--baseline-cycles", type=int, default=3)
    parser.add_argument("--output", type=Path, default=Path("artifacts/live-capacity.json"))
    parser.add_argument("--resume", type=Path)
    asyncio.run(run(parser.parse_args()))
