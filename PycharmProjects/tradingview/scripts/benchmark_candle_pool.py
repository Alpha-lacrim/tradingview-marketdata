"""Reusable offline pool comparison. No upstream calls and no database writes.

Same synthetic universe and latency function across configurations. A shared service
capacity model deliberately separates provider-wide capacity from client parallelism.
Live guest pooling is blocked until a permitted provider integration supplies limits.
"""

import argparse
import asyncio
import ctypes
import json
import math
import os
import time
import tracemalloc
from datetime import timedelta
from pathlib import Path

from benchmark_candle_retrieval import quantiles

from marketdata.candle_benchmark import synthetic_pairs
from marketdata.candles import minute_start
from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics, configure_logging
from marketdata.providers.candle_pool import CandleConnectionPool, PoolLimits
from marketdata.providers.synthetic_candles import SyntheticCandleProvider


def rss_bytes():
    if os.name != "nt":
        return None

    class Memory(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong),
            ("faults", ctypes.c_ulong),
            *[
                (name, ctypes.c_size_t)
                for name in (
                    "peak",
                    "working",
                    "paged_peak",
                    "paged",
                    "nonpaged_peak",
                    "nonpaged",
                    "pagefile",
                    "pagefile_peak",
                )
            ],
        ]

    memory = Memory()
    memory.cb = ctypes.sizeof(memory)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetCurrentProcess.restype = ctypes.c_void_p
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(Memory), ctypes.c_ulong]
    if psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(memory), memory.cb):
        return memory.working
    return None


async def probe(args, size, connections, window, global_window):
    end = minute_start(utc_now()) - timedelta(minutes=1)
    settings = Settings(
        _env_file=None,
        candle_fetch_grace_seconds=0,
        candle_pool_connections=connections,
        candle_pool_worker_capacity=math.ceil(size / connections),
        candle_pool_max_instruments=size,
        candle_pool_concurrency=global_window,
        candle_pool_deadline_seconds=args.deadline,
        tradingview_candle_concurrency=window,
    )
    metrics = Metrics()
    service = asyncio.Semaphore(args.service_capacity) if args.service_capacity else None
    delays = {0: args.slow_seconds} if args.slow_seconds else {}
    pairs = synthetic_pairs(size)
    factory = lambda _: SyntheticCandleProvider(  # noqa: E731
        settings,
        metrics,
        end,
        latency=args.latency,
        handshake_seconds=args.handshake,
        service_slots=service,
        missing=set(args.missing),
        delays=delays,
    )
    # Compare the original worker path against the pool; only the offline worker
    # subclass lifts local job limits. Live TradingView retains both 100 caps.
    provider = (
        factory("")
        if connections == 1
        else CandleConnectionPool(
            "benchmark", settings, metrics, factory, PoolLimits(32, 4096, 4096, 128)
        )
    )
    if connections == 1:
        provider.effective_concurrency = min(window, global_window)
    await provider.subscribe(pairs)
    timer, cpu = time.perf_counter(), time.process_time()
    started = utc_now()
    rss_before = rss_bytes()
    tracemalloc.reset_peak()
    before = tracemalloc.get_traced_memory()[0]
    result, connection_error = {}, None
    try:
        if connections > 1:
            await provider.connect()
            result = await provider.fetch_many(
                [(pair, end - timedelta(minutes=args.bars - 1), end, args.bars) for pair in pairs]
            )
        else:
            # Single path uses the same deadline and incremental partial-result retention.
            async with asyncio.timeout(args.deadline):
                await provider.connect()
                result = await provider.fetch_many(
                    [
                        (pair, end - timedelta(minutes=args.bars - 1), end, args.bars)
                        for pair in pairs
                    ]
                )
    except Exception as error:
        connection_error = type(error).__name__
        result = dict(provider.completed_results)
        result.update(
            {i.canonical_symbol: error for i, _ in pairs if i.canonical_symbol not in result}
        )
    finally:
        await provider.disconnect()
    elapsed = time.perf_counter() - timer
    workers = [w.provider for w in provider.workers] if connections > 1 else [provider]
    current = {
        s
        for s, bars in result.items()
        if isinstance(bars, list) and any(c.bucket_end == end for c in bars)
    }
    native_missing = [
        s for s, bars in result.items() if isinstance(bars, list) and s not in current
    ]
    availability = sorted(
        2 + max(0, (c.received_timestamp - started).total_seconds())
        for s, bars in result.items()
        if isinstance(bars, list) and s in current
        for c in bars
        if c.bucket_end == end
    )
    # Synthetic historical end is deliberately old. Cycle-relative values remove
    # that arbitrary offset; never call them measured live close-to-receive latency.
    request_origin = min((t["request_started_at"] for t in provider.timings.values()), default=None)
    from datetime import datetime

    parsed = (
        sorted(
            (
                datetime.fromisoformat(t["parser_completed_at"])
                - datetime.fromisoformat(request_origin)
            ).total_seconds()
            for s, t in provider.timings.items()
            if s in current
        )
        if request_origin
        else []
    )
    counts = dict(
        successful_requests=sum(isinstance(v, list) for v in result.values()),
        native_target_candles=len(current),
        source_completed_missing=len(native_missing),
    )
    row = dict(
        count=size,
        connections=connections,
        window=window,
        global_window=global_window,
        duration_seconds=elapsed,
        safety_margin_seconds=args.deadline - elapsed,
        cpu_seconds=time.process_time() - cpu,
        python_peak_bytes=max(0, tracemalloc.get_traced_memory()[1] - before),
        rss_before_bytes=rss_before,
        rss_after_bytes=rss_bytes(),
        first_parsed_from_first_dispatch=parsed[0] if parsed else None,
        last_parsed_from_first_dispatch=parsed[-1] if parsed else None,
        modeled_close_to_first_seconds=availability[0] if availability else None,
        modeled_close_to_last_seconds=availability[-1] if availability else None,
        connection_error=connection_error,
        failures={s: type(v).__name__ for s, v in result.items() if isinstance(v, Exception)},
        **counts,
        immediate_complete=len(current) == size,
        retrieval=provider.health.candle_retrieval,
        cleanup=dict(
            sockets_closed=all(s.closed for w in workers for s in w.sockets),
            sessions_remaining=sum(len(s.sessions) for w in workers for s in w.sockets),
            emitters_remaining=sum(len(s.tasks) for w in workers for s in w.sockets),
            receivers_remaining=sum(
                t.get_name() == "tradingview-candle-receiver" for t in asyncio.all_tasks()
            ),
            tasks_after=len(asyncio.all_tasks()),
        ),
    )
    if args.recovery and (len(current) != size):
        # One later bounded history cycle, missing-bar profile retained: true absent
        # source minutes remain unresolved; transient slow request can be recovered.
        pending = {(s, end - timedelta(minutes=1)) for s in result if s not in current}
        for w in workers:
            w.end += timedelta(minutes=1)
            w.socket_options["delays"] = {}
        if connections > 1:
            for w in provider.workers:
                w.retry_at = 0  # simulated next scheduled cycle after backoff
        await provider.connect()
        try:
            recovered = await provider.fetch_many(
                [
                    (pair, end - timedelta(minutes=3), end + timedelta(minutes=1), 5)
                    for pair in pairs
                ]
            )
        finally:
            await provider.disconnect()
        received = {
            (s, c.bucket_start)
            for s, bars in recovered.items()
            if isinstance(bars, list)
            for c in bars
        }
        row["eventual_target_candles"] = size - len(pending - received)
        row["unresolved_primary"] = sorted(s for s, _ in pending - received)
    else:
        row["eventual_target_candles"] = len(current)
        row["recovery_tested"] = False
    return row


def summarize(rows):
    groups = sorted({(r["count"], r["connections"], r["window"], r["global_window"]) for r in rows})
    output = []
    for count, connections, window, global_window in groups:
        group = [
            r
            for r in rows
            if (r["count"], r["connections"], r["window"], r["global_window"])
            == (count, connections, window, global_window)
        ]
        output.append(
            dict(
                count=count,
                connections=connections,
                window=window,
                global_window=global_window,
                cycles=len(group),
                duration=quantiles([r["duration_seconds"] for r in group]),
                first_parsed=quantiles([r["first_parsed_from_first_dispatch"] for r in group]),
                last_parsed=quantiles([r["last_parsed_from_first_dispatch"] for r in group]),
                modeled_close_to_first=quantiles(
                    [r["modeled_close_to_first_seconds"] for r in group]
                ),
                modeled_close_to_last=quantiles(
                    [r["modeled_close_to_last_seconds"] for r in group]
                ),
                successful_requests=sum(r["successful_requests"] for r in group),
                native_targets=sum(r["native_target_candles"] for r in group),
                eventual_targets=sum(r["eventual_target_candles"] for r in group),
                expected_targets=count * len(group),
                failures=sum(len(r["failures"]) for r in group),
                cpu_seconds=quantiles([r["cpu_seconds"] for r in group]),
                python_peak_bytes=quantiles([r["python_peak_bytes"] for r in group]),
                cleanup_passed=all(
                    r["cleanup"]["sockets_closed"]
                    and r["cleanup"]["sessions_remaining"] == 0
                    and r["cleanup"]["emitters_remaining"] == 0
                    and r["cleanup"]["receivers_remaining"] == 0
                    for r in group
                ),
            )
        )
    return output


async def run(args):
    if args.live:
        raise ValueError("BLOCKED: guest pool permissions unverified; no live requests issued")
    if (
        not 1 <= args.rounds <= 20
        or any(not 1 <= n <= 4096 for n in args.counts)
        or not 0 <= args.latency <= 5
        or not 0 <= args.handshake <= 10
        or not 2 <= args.bars <= 301
        or not 0 < args.deadline <= 60
        or not 0 <= args.service_capacity <= 128
        or not 0 <= args.slow_seconds <= 60
    ):
        raise ValueError("benchmark parameters outside bounded limits")
    if args.partition_size is not None and (
        args.partition_size < 1 or max(args.counts) > 32 * args.partition_size
    ):
        raise ValueError("partition-size must fit the bounded 32-worker pool")
    configurations = [tuple(map(int, c.split(":"))) for c in args.configs]
    for c in configurations:
        if len(c) != 3 or not 1 <= c[0] <= 32 or not 1 <= c[1] <= 12 or not 1 <= c[2] <= 128:
            raise ValueError(
                "configs require connections:worker_window:global_window within bounds"
            )
    configure_logging("ERROR")
    tracemalloc.start()
    output = dict(
        status="INCONCLUSIVE",
        model=dict(
            latency=args.latency,
            handshake=args.handshake,
            service_capacity=args.service_capacity,
            deadline=args.deadline,
            bars=args.bars,
        ),
        cycles=[],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for count in sorted(set(args.counts)):
        configs = list(configurations)
        if args.partition_size:
            configs.append(
                (
                    math.ceil(count / args.partition_size),
                    2,
                    min(32, 2 * math.ceil(count / args.partition_size)),
                )
            )
        configs = list(dict.fromkeys(configs))
        # Alternate configuration order by round to reduce systematic host/order bias.
        for round_index in range(args.rounds):
            for connections, window, global_window in configs[:: 1 if round_index % 2 == 0 else -1]:
                row = await probe(args, count, connections, window, global_window)
                output["cycles"].append(row)
                output["summary"] = summarize(output["cycles"])
                output["status"] = (
                    "SYNTHETIC VERIFIED"
                    if all(
                        r["successful_requests"] == r["native_target_candles"] == r["count"]
                        and r["duration_seconds"] < args.deadline
                        and r["cleanup"]["sockets_closed"]
                        and r["cleanup"]["sessions_remaining"] == 0
                        and r["cleanup"]["receivers_remaining"] == 0
                        for r in output["cycles"]
                    )
                    else "PARTIALLY VERIFIED"
                )
                args.output.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
                print(
                    json.dumps(
                        {
                            k: row[k]
                            for k in (
                                "count",
                                "connections",
                                "window",
                                "global_window",
                                "duration_seconds",
                                "successful_requests",
                                "native_target_candles",
                            )
                        }
                    ),
                    flush=True,
                )
    tracemalloc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--synthetic", action="store_true")
    mode.add_argument("--live", action="store_true")
    parser.add_argument("--counts", nargs="+", type=int, default=[64, 128, 256])
    parser.add_argument("--configs", nargs="+", default=["1:2:2", "1:8:8", "4:2:8", "8:2:16"])
    parser.add_argument("--partition-size", type=int)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--latency", type=float, default=0.35)
    parser.add_argument("--handshake", type=float, default=0.03)
    parser.add_argument("--service-capacity", type=int, default=0)
    parser.add_argument("--slow-seconds", type=float, default=0)
    parser.add_argument("--missing", nargs="*", type=int, default=[])
    parser.add_argument("--bars", type=int, default=4)
    parser.add_argument("--deadline", type=float, default=48)
    parser.add_argument("--recovery", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("artifacts/candle-pool.json"))
    asyncio.run(run(parser.parse_args()))
