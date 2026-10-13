"""Freeze ordinary source identities from retained live evidence, then verify natively.

Preflight is read-only and requires the sole ordinary collector to be stopped.
No database registration, synthetic candles, retries or benchmark namespaces are used.
"""

import argparse
import asyncio
import hashlib
import json
import subprocess
from collections import Counter
from datetime import timedelta
from pathlib import Path

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry


def freeze(candidates, prior, output):
    original = json.loads(Path("config/instruments.tradingview-pilot.json").read_text())
    existing = {e["mappings"][0]["provider_symbol"] for e in original}
    document = json.loads(prior.read_text())
    expected, supplied = Counter(), Counter()
    for stage in document["stages"]:
        sources = {r["symbol"]: r["source_symbol"] for r in stage["commits"]}
        for symbol, _ in stage["expected"]:
            expected[sources.get(symbol, symbol.removeprefix("BENCHLIVE:"))] += 1
        for row in stage["commits"]:
            supplied[row["source_symbol"]] += 1
    entries = json.loads(candidates.read_text())
    ranked = sorted(
        enumerate(entries),
        key=lambda p: (
            -supplied[p[1]["mappings"][0]["provider_symbol"]]
            / max(1, expected[p[1]["mappings"][0]["provider_symbol"]]),
            p[0],
        ),
    )
    evidence = []
    for _, row in ranked:
        source = row["mappings"][0]["provider_symbol"]
        if source in existing or supplied[source] < 3:
            continue
        venue, name = source.split(":", 1)
        instrument = row["instrument"]
        original.append(
            dict(
                instrument=dict(
                    canonical_symbol="CRYPTO:" + source,
                    display_name=f"{instrument['base_asset']}/USDT {venue} spot via TradingView",
                    asset_class="crypto",
                    instrument_type="spot",
                    exchange=venue,
                    currency="USDT",
                    base_asset=instrument["base_asset"],
                    quote_asset="USDT",
                ),
                mappings=[
                    dict(
                        provider="tradingview_guest",
                        provider_symbol=source,
                        realtime_status="unknown",
                        metadata={"source_type": "spot"},
                    )
                ],
            )
        )
        existing.add(source)
        evidence.append(
            dict(
                source=source,
                prior_expected=expected[source],
                prior_native_commits=supplied[source],
            )
        )
        if len(original) == 256:
            break
    if len(original) != 256:
        raise ValueError("insufficient previously native-verified candidate identities")
    for count in (64, 128, 256):
        path = Path(f"config/instruments.tradingview-{count}.json")
        path.write_text(json.dumps(original[:count], indent=2) + "\n", encoding="utf-8")
        Registry.load(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            dict(
                selection="prior native completeness descending, then retained catalog rank",
                candidates_sha256=hashlib.sha256(candidates.read_bytes()).hexdigest(),
                prior_sha256=hashlib.sha256(prior.read_bytes()).hexdigest(),
                additions=evidence,
            ),
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


async def verify(args):
    running = (
        await asyncio.to_thread(
            subprocess.check_output,
            ["docker", "inspect", args.container, "--format", "{{.State.Running}}"],
            text=True,
        )
    ).strip()
    if running != "false":
        raise ValueError("stop the sole ordinary collector before native preflight")
    registry = Registry.load(Path("config/instruments.tradingview-256.json"))
    settings = Settings(
        _env_file=None,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=4,
        candle_pool_concurrency=8,
        candle_pool_max_instruments=300,
        candle_pool_connect_interval_seconds=2,
        candle_pool_deadline_seconds=48,
        candle_pool_cleanup_seconds=3,
        tradingview_candle_concurrency=2,
    )
    provider = (await build_providers(settings, registry, Metrics()))[0]
    end = utc_now().replace(second=0, microsecond=0) - timedelta(minutes=1)
    start = end - timedelta(minutes=30)
    try:
        await provider.connect()
        results = await provider.fetch_many([(p, start, end, 31) for p in provider.subscriptions])
    finally:
        await provider.disconnect()
    rows = []
    for i, m in provider.subscriptions:
        bars = results[i.canonical_symbol]
        rows.append(
            dict(
                canonical_symbol=i.canonical_symbol,
                source=m.provider_symbol,
                error=type(bars).__name__ if isinstance(bars, Exception) else None,
                native_completed_bars=0 if isinstance(bars, Exception) else len(bars),
                compatible=not isinstance(bars, Exception) and len(bars) >= 3,
                native_sample=None
                if isinstance(bars, Exception) or not bars
                else bars[-1].model_dump(mode="json"),
                timing=provider.timings.get(i.canonical_symbol, {}),
            )
        )
    result = dict(
        observed_at=utc_now().isoformat(),
        start=start.isoformat(),
        end=end.isoformat(),
        database_writes=0,
        sources=rows,
        retrieval=provider.health.candle_retrieval,
    )
    args.output.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    print(
        json.dumps(
            dict(
                compatible=sum(r["compatible"] for r in rows),
                total=len(rows),
                errors=[r["source"] for r in rows if not r["compatible"]],
            )
        )
    )
    if not all(r["compatible"] for r in rows):
        raise ValueError("preflight failed; preserve evidence and do not activate failing sources")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", type=Path)
    parser.add_argument("--prior", type=Path)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--container", default="tradingview-app-1")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.verify:
        asyncio.run(verify(args))
    else:
        freeze(args.candidates, args.prior, args.output)
