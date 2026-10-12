"""One minute-aligned native retrieval -> isolated test-DB batch, after a live capacity gate.

Requires TEST_DATABASE_URL ending _test and a fresh successful capacity checkpoint.
Never connects to the production database. Never modifies the production registry.
"""

import argparse
import asyncio
import json
import os
import time
import tracemalloc
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from benchmark_live_capacity import cycle

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import RegistryEntry, utc_now
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import Store
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


async def run(args):
    url = os.environ.get("TEST_DATABASE_URL", "")
    if not args.collector_stopped or not urlsplit(url).path.endswith("_test"):
        raise ValueError("stop the collector; use only a disposable TEST_DATABASE_URL ending _test")
    data = json.loads(args.capacity_results.read_text(encoding="utf-8"))
    last = data["cycles"][-1]
    age = (utc_now() - datetime.fromisoformat(last["finished_at"])).total_seconds()
    if not 0 <= age <= 3600 or last["connection_error"] or last["next_window"] != 2:
        raise ValueError("require fresh, stable source evidence without concurrency backoff")
    sizes = [s["instrument_count"] for s in data["stages"] if s["escalation_gate_passed"]]
    if not sizes:
        raise ValueError("no successful live stage")
    size = max(sizes)
    base = Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
        "tradingview_guest"
    )
    candidates = Registry.load(Path("config/benchmarks/tradingview_candidates.json")).subscriptions(
        "tradingview_guest"
    )
    names = {v["source_symbol"] for v in data["verified_universe"]}
    additions = [p for p in candidates if p[1].provider_symbol in names]
    pairs = base if size == 17 else [p for p in base if p[1].provider_symbol != "OKX:XAUTUSDT"]
    pairs = [*pairs, *additions][:size]
    if len(pairs) != size:
        raise ValueError("verified universe does not match requested live stage")
    configure_logging("ERROR")
    p = TradingViewCandleProvider(Settings(tradingview_candle_concurrency=2), Metrics())
    await p.subscribe(pairs)
    tracemalloc.start()
    row, _, evidence = await cycle(p, pairs, set())
    if row["missing_current"] or row["connection_error"]:
        raise RuntimeError("native source incomplete; no successful end-to-end claim")
    namespace = "benchmark_live_" + uuid4().hex[:12]
    store = Store(Settings(_env_file=None, database_url=url))
    await store.connect()
    try:
        isolated = [
            (
                i,
                m.model_copy(
                    update={
                        "provider": namespace,
                        "metadata": {**m.metadata, "original_provider": "tradingview_guest"},
                    }
                ),
            )
            for i, m in pairs
        ]
        await store.register(
            Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in isolated])
        )
        candles = [
            MarketCandle.model_validate(v).model_copy(
                update={
                    "provider": namespace,
                    "metadata": {
                        **v["metadata"],
                        "original_provider": "tradingview_guest",
                        "isolated_benchmark": True,
                    },
                }
            )
            for v in evidence.values()
        ]
        coverage = [
            (
                c.instrument_id,
                c.provider,
                c.bucket_start,
                c.bucket_end,
                p.session_info[c.canonical_symbol],
            )
            for c in candles
        ]
        timer = time.perf_counter()
        changed = await store.write_candles(candles, coverage)
        duration, acknowledged = time.perf_counter() - timer, utc_now()
        async with store.connection() as connection:
            rows = await connection.fetchval(
                "SELECT count(*) FROM market_candles WHERE provider=$1", namespace
            )
        assert changed == rows == size
        row.update(
            isolated_database_name=urlsplit(url).path.lstrip("/"),
            isolated_provider=namespace,
            persisted_rows=rows,
            db_write_seconds=duration,
            close_to_commit_seconds=(acknowledged - candles[0].bucket_end).total_seconds(),
            production_writes=0,
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(row, indent=2, default=str), encoding="utf-8")
        print(
            json.dumps({k: v for k, v in row.items() if k not in ("timings", "retrieval")}),
            flush=True,
        )
    finally:
        await store.close()
        tracemalloc.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector-stopped", action="store_true")
    parser.add_argument("--capacity-results", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("artifacts/native-persistence.json"))
    asyncio.run(run(parser.parse_args()))
