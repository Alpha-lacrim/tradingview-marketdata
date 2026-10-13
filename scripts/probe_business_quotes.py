"""Separate post-fix FXCM quote/volume capture; preserves the initial rejected probe."""

import argparse
import asyncio
import json
import os
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from observe_ordinary_collector import inspect_container
from qualify_business_sources import idle, quote_probe

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry


async def run(args):
    registry = Registry.load(Path("config/instruments.tradingview-business-candidates.json"))
    pairs = [
        p
        for p in registry.subscriptions("tradingview_guest")
        if p[1].provider_symbol.startswith("FX:")
    ]
    app = await asyncio.to_thread(inspect_container, "tradingview-app-1")
    settings = Settings(
        _env_file=None,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=1,
        candle_pool_concurrency=2,
        candle_pool_max_instruments=16,
        candle_pool_connect_interval_seconds=2,
        candle_pool_deadline_seconds=12,
        candle_pool_cleanup_seconds=3,
        candle_request_timeout_seconds=8,
        candle_fetch_grace_seconds=5,
    )
    record = dict(
        started_at=utc_now().isoformat(),
        mode="FXCM quote identity correction; separate evidence",
        production_container=app["Id"],
        sources={},
        volume_capture={},
    )
    output = args.output
    async with httpx.AsyncClient(timeout=5) as client:
        for offset in range(0, 0 if args.volume_only else len(pairs), 16):
            await idle(client)
            record["sources"].update(await quote_probe(settings, pairs[offset : offset + 16]))
            await asyncio.to_thread(
                output.write_text,
                json.dumps(record, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
            print(
                json.dumps(
                    dict(
                        batch=offset // 16 + 1,
                        quotes_observed=sum(
                            bool(s["observations"]) for s in record["sources"].values()
                        ),
                    )
                ),
                flush=True,
            )
        selected = Registry(
            [
                e
                for e in registry.entries
                if e.mappings[0].provider_symbol
                in ("FX:EURUSD", "FX:XAUUSD", "FX:NGAS", "TVC:USOIL", "TVC:UKOIL")
            ]
        )
        pool = (await build_providers(settings, selected, Metrics()))[0]
        worker = pool.workers[0].provider
        original = worker._parse_native_bar

        def capture(values, pair, info, received, status):
            source = pair[1].provider_symbol
            row = record["volume_capture"].setdefault(
                source,
                dict(
                    source_info={
                        k: info[k]
                        for k in (
                            "full_name",
                            "volume_type",
                            "has_no_volume",
                            "has_volume",
                            "volume_precision",
                            "format",
                        )
                        if k in info
                    },
                    raw_volume_samples=[],
                ),
            )
            row["raw_volume_samples"].append(
                dict(
                    value=str(values[5]) if values[5] is not None else None,
                    numeric=isinstance(values[5], (int, Decimal))
                    and Decimal(values[5]).is_finite(),
                )
            )
            return original(values, pair, info, received, status)

        worker._parse_native_bar = capture
        await idle(client)
        end = utc_now().replace(second=0, microsecond=0) - timedelta(minutes=3)
        start = end - timedelta(minutes=10)
        try:
            await pool.connect()
            results = await pool.fetch_many(
                [(p, start, end, 14) for p in selected.subscriptions("tradingview_guest")]
            )
        finally:
            await pool.disconnect()
        if args.persist_volume:
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
                candles = [
                    c for value in results.values() if isinstance(value, list) for c in value
                ]
                await store.write_candles(candles, [])
                checks = []
                async with store.connection() as connection:
                    for c in candles:
                        row = await connection.fetchrow(
                            "SELECT volume,metadata FROM market_candles WHERE instrument_id=$1 "
                            "AND provider='tradingview_guest' AND interval='1m' "
                            "AND price_basis='native' AND bucket_start=$2",
                            c.instrument_id,
                            c.bucket_start,
                        )
                        checks.append(
                            bool(
                                row is not None
                                and row["volume"] is None
                                and json.loads(row["metadata"]).get("source_volume_raw")
                                == c.metadata.get("source_volume_raw")
                            )
                        )
                record["isolated_volume_persistence"] = dict(
                    test_database=parsed.path[1:],
                    native_candles=len(candles),
                    exact_null_volume_and_raw_metadata_matches=sum(checks),
                )
            finally:
                await store.close()
    after = await asyncio.to_thread(inspect_container, "tradingview-app-1")
    record.update(
        finished_at=utc_now().isoformat(),
        production_unchanged=after["Id"] == app["Id"] and after["Image"] == app["Image"],
        production_running=after["State"]["Running"],
    )
    await asyncio.to_thread(
        output.write_text, json.dumps(record, indent=2, default=str) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            dict(
                finished_at=record["finished_at"],
                sources=len(record["sources"]),
                quotes_observed=sum(bool(s["observations"]) for s in record["sources"].values()),
                volume_capture=record["volume_capture"],
            )
        ),
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--volume-only", action="store_true")
    parser.add_argument("--persist-volume", action="store_true")
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/business-fxcm-quote-correction.json")
    )
    asyncio.run(run(parser.parse_args()))
