"""Recheck the frozen business window for required sparse sources, without substitutions."""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from qualify_business_sources import idle

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry


async def run():
    original = json.loads(
        await asyncio.to_thread(Path("artifacts/business-source-qualification.json").read_text)
    )
    start, end = (datetime.fromisoformat(original[k]) for k in ("window_start", "window_end"))
    registry = Registry.load(Path("config/instruments.tradingview-business-candidates.json"))
    sources = {"BINANCE:RLUSDUSDT", "OANDA:XPDUSD", "OANDA:NATGASUSD"}
    selected = Registry([e for e in registry.entries if e.mappings[0].provider_symbol in sources])
    settings = Settings(
        _env_file=None,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=1,
        candle_pool_concurrency=2,
        candle_pool_max_instruments=3,
        candle_pool_connect_interval_seconds=2,
        candle_pool_deadline_seconds=12,
        candle_pool_cleanup_seconds=3,
        candle_request_timeout_seconds=8,
        candle_fetch_grace_seconds=5,
    )
    async with httpx.AsyncClient(timeout=5) as client:
        await idle(client)
        pool = (await build_providers(settings, selected, Metrics()))[0]
        count = int((utc_now() - start).total_seconds() // 60) + 2
        try:
            await pool.connect()
            results = await pool.fetch_many(
                [(p, start, end, count) for p in selected.subscriptions("tradingview_guest")]
            )
        finally:
            await pool.disconnect()
    report = dict(
        at=utc_now().isoformat(),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        sources={},
    )
    url = os.environ["BUSINESS_TEST_DATABASE_URL"]
    parsed = urlsplit(url)
    if (
        parsed.hostname not in ("localhost", "127.0.0.1")
        or parsed.path != "/marketdata_business_qualification_test"
    ):
        raise ValueError("require isolated qualification database")
    store = Store(Settings(_env_file=None, database_url=url, db_pool_min=1, db_pool_max=2))
    await store.connect()
    try:
        native = [c for value in results.values() if isinstance(value, list) for c in value]
        await store.write_candles(native, [])
        for i, m in selected.subscriptions("tradingview_guest"):
            previous = original["sources"][i.canonical_symbol]["passes"][-1]
            expected = set(previous["classifications"]["EXPECTED"])
            initial = set(original["sources"][i.canonical_symbol]["passes"][0]["native_buckets"])
            value = results[i.canonical_symbol]
            available = (
                {c.bucket_start.isoformat() for c in value} if isinstance(value, list) else set()
            )
            diag = pool.timings.get(i.canonical_symbol, {})
            report["sources"][m.provider_symbol] = dict(
                canonical_symbol=i.canonical_symbol,
                request_success=not isinstance(value, Exception),
                source_error=str(value) if isinstance(value, Exception) else None,
                expected_minutes=len(expected),
                native_minutes=len(available & expected),
                recovered_since_first_capture=len((available - initial) & expected),
                unresolved_expected_buckets=sorted(expected - available),
                source_info=diag.get("source_info"),
                requested_bars=count,
                received_bar_count=diag.get("received_bar_count"),
                received_first_bucket=diag.get("received_first_bucket"),
                received_last_bucket=diag.get("received_last_bucket"),
                deeper_omission_cause=(
                    "UNKNOWN: no source trades versus publication omissions not established; "
                    "no permanent-loss claim"
                ),
            )
        report["retrieval"] = pool.health.candle_retrieval
    finally:
        await store.close()
    await asyncio.to_thread(
        Path("artifacts/business-gap-followup.json").write_text,
        json.dumps(report, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                s: {k: v for k, v in r.items() if k != "unresolved_expected_buckets"}
                for s, r in report["sources"].items()
            },
            default=str,
        ),
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(run())
