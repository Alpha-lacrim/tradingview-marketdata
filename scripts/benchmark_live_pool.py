"""Opt-in guest multi-WebSocket -> CandleIngestion -> isolated PostgreSQL experiment.

Local ceilings are experimental safeguards, not documented provider allowances. Access
rejections/throttling stop escalation. Never alters the application registry or deployment.
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

import httpx
from benchmark_candle_pool import rss_bytes
from benchmark_candle_retrieval import quantiles

from marketdata.candle_ingestion import CandleIngestion
from marketdata.candles import next_collection
from marketdata.config import Settings
from marketdata.domain import Instrument, ProviderInstrument, RegistryEntry, utc_now
from marketdata.fanout import Fanout
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import Store
from marketdata.providers.candle_pool import CandleConnectionPool, PoolLimits
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry

CATALOG_URL = "https://scanner.tradingview.com/crypto/scan"


async def discover(path):
    """Preserve reported identities; catalog inclusion is not native-bar verification."""
    request = dict(
        filter=[
            dict(left="exchange", operation="in_range", right=["BINANCE", "OKX"]),
            dict(left="type", operation="equal", right="spot"),
            dict(left="currency", operation="equal", right="USDT"),
        ],
        columns=["name", "exchange", "type", "currency", "base_currency", "volume", "close"],
        sort=dict(sortBy="volume", sortOrder="desc"),
        range=[0, 1500],
    )
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(CATALOG_URL, json=request)
        response.raise_for_status()
    data = response.json()
    entries = []
    # Float arithmetic here ranks candidates only, never financial persistence.
    rows = sorted(data["data"], key=lambda r: (r["d"][5] or 0) * (r["d"][6] or 0), reverse=True)
    for row in rows:
        name, exchange, kind, currency, base, _, _ = row["d"]
        if (
            exchange not in ("BINANCE", "OKX")
            or kind != "spot"
            or currency != "USDT"
            or row["s"] != f"{exchange}:{name}"
            or not base
        ):
            continue
        entries.append(
            RegistryEntry(
                instrument=Instrument(
                    canonical_symbol="BENCHLIVE:" + row["s"],
                    display_name="Live benchmark " + row["s"],
                    asset_class="crypto",
                    instrument_type="spot",
                    exchange=exchange,
                    currency=currency,
                    base_asset=base,
                    quote_asset=currency,
                    metadata={"benchmark_only": True},
                ),
                mappings=[
                    ProviderInstrument(
                        provider="tradingview_guest",
                        provider_symbol=row["s"],
                        realtime_status="unknown",
                        metadata={
                            "source_type": kind,
                            "catalog_evidence": CATALOG_URL,
                            "catalog_received_at": utc_now().isoformat(),
                            "independently_verified": False,
                        },
                    )
                ],
            )
        )
    Registry(entries)  # Fail duplicate identity before any chart connections.
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps([e.model_dump(mode="json") for e in entries], indent=2), "utf-8")
    path.with_suffix(".evidence.json").write_text(
        json.dumps(dict(url=CATALOG_URL, request=request, response=data), indent=2), "utf-8"
    )
    return entries


class FaultWorker(TradingViewCandleProvider):
    """One explicitly labelled client-side disconnect of a real established socket."""

    drop_once = False

    async def fetch_many(self, requests):
        if not self.drop_once:
            return await super().fetch_many(requests)
        self.drop_once = False

        async def drop():
            await asyncio.sleep(0.5)
            if self.socket:
                await self.socket.close(code=1012, reason="benchmark controlled disconnect")

        task = asyncio.create_task(drop(), name="benchmark-fault-disconnect")
        try:
            return await super().fetch_many(requests)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class RecordingStore:
    """Production Store transaction with namespace isolation and independent read-back."""

    def __init__(self, store, namespace):
        self.store, self.namespace = store, namespace
        self.first_commits = {}
        self.fail_once = False
        self.last_candle_write = {}
        self.writes = []

    async def candle_progress(self, instrument_id, provider):
        return await self.store.candle_progress(instrument_id, self.namespace)

    async def write_candles(self, candles, coverage):
        if self.fail_once:
            self.fail_once = False
            raise ConnectionError("benchmark controlled pre-transaction database failure")
        isolated = [
            c.model_copy(
                update={
                    "provider": self.namespace,
                    "metadata": {
                        **c.metadata,
                        "original_provider": c.provider,
                        "isolated_benchmark": True,
                    },
                }
            )
            for c in candles
        ]
        plans = [(p[0], self.namespace, *p[2:]) for p in coverage]
        timer = time.perf_counter()
        changed = await self.store.write_candles(isolated, plans)
        acknowledged = utc_now()
        write_seconds = time.perf_counter() - timer
        # A newly acquired connection after transaction completion verifies visibility.
        async with self.store.connection() as connection:
            rows = await connection.fetch(
                "SELECT * FROM market_candles WHERE provider=$1 AND interval='1m' "
                "AND price_basis='native'",
                self.namespace,
            )
        actual = {(r["instrument_id"], r["bucket_start"]): r for r in rows}
        for c in isolated:
            row = actual[(c.instrument_id, c.bucket_start)]
            for field in (
                "provider_symbol",
                "bucket_end",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "realtime_status",
                "source_status",
            ):
                if row[field] != getattr(c, field):
                    raise AssertionError(f"durable candle mismatch: {c.canonical_symbol}/{field}")
            # Identical replay preserves the original durable receipt by design.
            if row["received_timestamp"] > c.received_timestamp:
                raise AssertionError("durable receipt newer than supplied observation")
            key = c.canonical_symbol, c.bucket_start
            self.first_commits.setdefault(
                key,
                dict(
                    symbol=c.canonical_symbol,
                    source_symbol=c.provider_symbol,
                    bucket_start=c.bucket_start.isoformat(),
                    bucket_end=c.bucket_end.isoformat(),
                    received_at=c.received_timestamp.isoformat(),
                    commit_acknowledged_at=acknowledged.isoformat(),
                    close_to_receive_seconds=(c.received_timestamp - c.bucket_end).total_seconds(),
                    close_to_commit_seconds=(acknowledged - c.bucket_end).total_seconds(),
                    native=c.model_dump(mode="json"),
                ),
            )
        # Translate Store diagnostics back to the provider identity used by ingestion.
        details = self.store.last_candle_write
        self.last_candle_write = {
            **details,
            "changed_keys": [(i, "tradingview_guest", b) for i, _, b in details["changed_keys"]],
            "changed_candles": [
                c.model_copy(update={"provider": "tradingview_guest"})
                for c in details["changed_candles"]
            ],
        }
        self.writes.append(
            dict(
                committed_at=acknowledged.isoformat(),
                seconds=write_seconds,
                changed=changed,
                supplied=len(candles),
                durable_rows=len(rows),
                readback_verified=True,
            )
        )
        return changed


async def resource_sample(stop, samples):
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        target = loop.time() + 0.25
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.25)
        except TimeoutError:
            pass
        samples.append(
            dict(
                rss_bytes=rss_bytes(),
                tasks=len(asyncio.all_tasks()),
                loop_lag_seconds=max(0, loop.time() - target),
            )
        )


def stage_summary(stage, commits):
    expected = {(s, datetime.fromisoformat(b)) for s, b in stage["expected"]}
    present = expected & commits.keys()
    cycles = stage["cycles"]
    return dict(
        expected_candles=len(expected),
        immediate_candles=sum(r["durable_current"] for r in cycles),
        eventual_candles=len(present),
        missing=[dict(symbol=s, bucket_start=b.isoformat()) for s, b in sorted(expected - present)],
        close_to_commit_seconds=quantiles([commits[k]["close_to_commit_seconds"] for k in present]),
        cycle_seconds=quantiles([r["wall_seconds"] for r in cycles]),
        db_write_seconds=quantiles([r["seconds"] for r in stage["writes"]]),
        recovery_tested=any(r["phase"] == "fault" for r in cycles),
        verdict="LIVE COMPLETE" if expected and expected == present else "LIVE INCOMPLETE",
    )


async def run(args):
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not args.collector_stopped or not urlsplit(url).path.endswith("_test"):
        raise ValueError(
            "stop the collector; TEST_DATABASE_URL must name an isolated _test database"
        )
    if (
        args.counts != [128, 256, 512][: len(args.counts)]
        or not args.counts
        or not 3 <= args.cycles <= 15
        or not 1 <= args.connections <= 16
        or math.ceil(max(args.counts) / args.connections) > 100
    ):
        raise ValueError("use prefix 128 256 512, 3..15 cycles and <=100 instruments/worker")
    configure_logging("ERROR")
    entries = (
        await discover(args.candidates) if args.discover else Registry.load(args.candidates).entries
    )
    pairs = Registry(entries).subscriptions("tradingview_guest")
    if len(pairs) < max(args.counts) or any(not i.metadata.get("benchmark_only") for i, _ in pairs):
        raise ValueError("need enough unique benchmark-only candidate identities")
    if any(i.asset_class != "crypto" or i.instrument_type != "spot" for i, _ in pairs):
        raise ValueError("this capacity experiment requires spot crypto with continuous sessions")
    metrics = Metrics()
    settings = Settings(
        _env_file=None,
        database_url=url,
        candle_pool_connections=args.connections,
        candle_pool_worker_capacity=100,
        candle_pool_max_instruments=max(args.counts),
        candle_pool_concurrency=args.connections * 2,
        tradingview_candle_concurrency=2,
        candle_pool_connect_interval_seconds=args.connect_interval,
    )
    store = Store(settings)
    await store.connect()
    async with store.connection() as connection:
        database_settings = dict(
            await connection.fetchrow(
                "SELECT version() AS version, current_setting('synchronous_commit') AS "
                "synchronous_commit, current_setting('fsync') AS fsync, "
                "current_setting('full_page_writes') AS full_page_writes"
            )
        )
    output = dict(
        mode="LIVE EXPERIMENTAL",
        started_at=utc_now().isoformat(),
        configuration=dict(
            connections=args.connections,
            worker_window=2,
            global_window=args.connections * 2,
            deadline_seconds=settings.candle_pool_deadline_seconds,
            connect_interval_seconds=args.connect_interval,
            grace_seconds=settings.candle_fetch_grace_seconds,
            backfill_minutes=settings.candle_backfill_minutes,
            correction_lookback_minutes=settings.candle_correction_lookback_minutes,
            request_timeout_seconds=settings.candle_request_timeout_seconds,
            cleanup_seconds=settings.candle_pool_cleanup_seconds,
        ),
        database=urlsplit(url).path.lstrip("/"),
        database_settings=database_settings,
        production_writes=0,
        latency_definition="local UTC acknowledgement after commit; not server commit time",
        universe=[
            dict(symbol=i.canonical_symbol, source=m.provider_symbol)
            for i, m in pairs[: max(args.counts)]
        ],
        stages=[],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def checkpoint():
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(output, indent=2, default=str), "utf-8")
        temporary.replace(args.output)

    tracemalloc.start()
    try:
        for count in args.counts:
            namespace = "benchmark_live_pool_" + uuid4().hex[:12]
            selected = pairs[:count]
            await store.register(
                Registry(
                    [
                        RegistryEntry(
                            instrument=i, mappings=[m.model_copy(update={"provider": namespace})]
                        )
                        for i, m in selected
                    ]
                )
            )
            recording = RecordingStore(store, namespace)
            pool = CandleConnectionPool(
                "tradingview_guest",
                settings,
                metrics,
                lambda _: FaultWorker(settings, metrics, experimental_pool=True),
                PoolLimits(args.connections, 100, max(args.counts), args.connections * 2, 2),
            )
            await pool.subscribe(selected)
            ingestion = CandleIngestion(settings, recording, pool, Fanout(256, 1, metrics))
            stage = dict(
                count=count, namespace=namespace, cycles=[], expected=[], writes=[], commits=[]
            )
            output["stages"].append(stage)
            expected = set()
            previous_end = None
            stage_start = utc_now()
            for index in range(args.cycles + 3):
                target = next_collection(utc_now(), pool.grace)
                while (remaining := (target - utc_now()).total_seconds()) > 0:  # noqa: ASYNC110
                    await asyncio.sleep(min(20, remaining))
                fault = index == args.cycles
                if fault:
                    pool.workers[0].provider.drop_once = True
                    recording.fail_once = True
                phase = "baseline" if index < args.cycles else "fault" if fault else "recovery"
                timer, cpu = time.perf_counter(), time.process_time()
                unresolved_before = expected - recording.first_commits.keys()
                before = tracemalloc.get_traced_memory()[0]
                tracemalloc.reset_peak()
                stop, samples = asyncio.Event(), []
                sampler = asyncio.create_task(
                    resource_sample(stop, samples), name="benchmark-resource"
                )
                error = None
                try:
                    async with asyncio.timeout(110):
                        await ingestion.cycle(target)
                except Exception as exc:
                    error = type(exc).__name__
                finally:
                    stop.set()
                    await sampler
                end = ingestion.last_attempted_end
                bucket = end - timedelta(minutes=1)
                first = previous_end or bucket
                while first <= bucket:
                    expected.update((i.canonical_symbol, first) for i, _ in selected)
                    first += timedelta(minutes=1)
                previous_end = end
                current = {(i.canonical_symbol, bucket) for i, _ in selected}
                wall, cpu_seconds = time.perf_counter() - timer, time.process_time() - cpu
                summary = pool.health.last_candle_cycle
                row = dict(
                    index=index,
                    phase=phase,
                    bucket_end=end.isoformat(),
                    error=error,
                    wall_seconds=wall,
                    cpu_seconds=cpu_seconds,
                    cpu_one_core_percent=100 * cpu_seconds / wall,
                    rss_peak_bytes=max((s["rss_bytes"] or 0 for s in samples), default=0),
                    python_additional_peak_bytes=max(
                        0, tracemalloc.get_traced_memory()[1] - before
                    ),
                    loop_lag_seconds=quantiles([s["loop_lag_seconds"] for s in samples]),
                    peak_tasks=max((s["tasks"] for s in samples), default=0),
                    receivers_after=sum(
                        t.get_name() == "tradingview-candle-receiver" for t in asyncio.all_tasks()
                    ),
                    sockets_closed=all(w.provider.socket is None for w in pool.workers),
                    durable_current=len(current & recording.first_commits.keys()),
                    unresolved_so_far=len(expected - recording.first_commits.keys()),
                    recovered_previous_targets=len(
                        unresolved_before & recording.first_commits.keys()
                    ),
                    successful_requests=pool.health.candle_retrieval.get("successful_requests", 0),
                    failures={
                        s: type(v).__name__
                        for s, v in pool.completed_results.items()
                        if isinstance(v, Exception)
                    },
                    ingestion=summary,
                    retrieval=pool.health.candle_retrieval,
                    injected_faults=["client_socket_close_w00", "database_pretransaction_failure"]
                    if fault
                    else [],
                )
                stage["cycles"].append(row)
                stage["expected"] = [(s, b.isoformat()) for s, b in sorted(expected)]
                stage["commits"] = list(recording.first_commits.values())
                stage["writes"] = recording.writes
                stage["summary"] = stage_summary(stage, recording.first_commits)
                stage["elapsed_seconds"] = (utc_now() - stage_start).total_seconds()
                checkpoint()
                print(
                    json.dumps(
                        {
                            k: row[k]
                            for k in (
                                "phase",
                                "bucket_end",
                                "wall_seconds",
                                "durable_current",
                                "unresolved_so_far",
                                "error",
                            )
                        }
                        | {"count": count}
                    ),
                    flush=True,
                )
                throttled = any(
                    s.value
                    for m in metrics.tradingview_throttle.collect()
                    for s in m.samples
                    if s.name.endswith("_total") and s.labels.get("reason") == "http_429"
                )
                if (
                    error
                    or pool.stopped
                    or throttled
                    or pool.effective_concurrency < args.connections * 2
                ):
                    output["stopped_reason"] = (
                        "cycle/DB error, access rejection, cleanup failure or provider throttling"
                    )
                    checkpoint()
                    return
            await pool.disconnect()
        output["finished_at"] = utc_now().isoformat()
        checkpoint()
    finally:
        await store.close()
        tracemalloc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-stopped", action="store_true")
    parser.add_argument("--discover", action="store_true")
    parser.add_argument(
        "--candidates", type=Path, default=Path("artifacts/live-pool-candidates.json")
    )
    parser.add_argument("--counts", type=int, nargs="+", default=[128, 256, 512])
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--connections", type=int, default=8)
    parser.add_argument("--connect-interval", type=float, default=2)
    parser.add_argument("--output", type=Path, default=Path("artifacts/live-pool.json"))
    asyncio.run(run(parser.parse_args()))
