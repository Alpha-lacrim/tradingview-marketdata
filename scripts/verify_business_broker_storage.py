"""One isolated two-broker cycle, real quote persistence, and stored-only API readback.

No additional continuously running collector, public port, production mutation or UI
streaming claim. Every TradingView request remains bounded to a production idle phase.
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi.testclient import TestClient
from observe_ordinary_collector import inspect_container, matches
from qualify_business_sources import idle, quote_probe

from marketdata.api import create_app
from marketdata.candle_ingestion import CandleIngestion
from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.fanout import Fanout
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry


def api_readback(url, pairs, candle_start, candle_end, quote_start, quote_end):
    settings = Settings(
        _env_file=None, database_url=url, enabled_asset_classes=[], db_pool_min=1, db_pool_max=2
    )
    records = []
    with TestClient(create_app(settings)) as client:
        assert client.get("/providers").json() == []  # Stored-only app opens NO provider.
        for instrument, mapping in pairs:
            candles = client.get(
                "/candles/" + instrument.canonical_symbol,
                params=dict(
                    provider="tradingview_guest",
                    basis="native",
                    start=candle_start.isoformat(),
                    end=candle_end.isoformat(),
                    limit=1000,
                ),
            )
            candles.raise_for_status()
            ticks = client.get(
                "/ticks/" + instrument.canonical_symbol,
                params=dict(
                    provider="tradingview_guest",
                    start=quote_start.isoformat(),
                    end=quote_end.isoformat(),
                    limit=1000,
                ),
            )
            ticks.raise_for_status()
            latest = client.get(
                "/prices/latest/" + instrument.canonical_symbol,
                params=dict(provider="tradingview_guest"),
            )
            latest.raise_for_status()
            assert all(
                r["provider_symbol"] == mapping.provider_symbol
                for r in candles.json()["items"] + ticks.json()["items"] + latest.json()
            )
            records.append(
                dict(
                    symbol=instrument.canonical_symbol,
                    source=mapping.provider_symbol,
                    candles=candles.json()["items"],
                    ticks=ticks.json()["items"],
                    latest=latest.json(),
                )
            )
    return records


async def run():
    url = os.environ["BUSINESS_TEST_DATABASE_URL"]
    parsed = urlsplit(url)
    if (
        parsed.hostname not in ("localhost", "127.0.0.1")
        or parsed.path != "/marketdata_business_qualification_test"
    ):
        raise ValueError("require the dedicated isolated database")
    registry = Registry.load(Path("config/instruments.tradingview-business-proposal.json"))
    pairs = [registry.by_provider["tradingview_guest", s] for s in ("OANDA:EURUSD", "FX:EURUSD")]
    selected = Registry(
        [e for e in registry.entries if e.instrument.id in {i.id for i, _ in pairs}]
    )
    app = await asyncio.to_thread(inspect_container, "tradingview-app-1")
    settings = Settings(
        _env_file=None,
        database_url=url,
        db_pool_min=1,
        db_pool_max=2,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=2,
        candle_pool_concurrency=4,
        candle_pool_max_instruments=2,
        candle_pool_connect_interval_seconds=2,
        candle_pool_deadline_seconds=12,
        candle_pool_cleanup_seconds=3,
        candle_request_timeout_seconds=8,
        candle_fetch_grace_seconds=5,
    )
    metrics = Metrics()
    store = Store(settings)
    await store.connect()
    before = {
        i.canonical_symbol: await store.candle_progress(i.id, "tradingview_guest") for i, _ in pairs
    }
    record = dict(
        started_at=utc_now().isoformat(),
        mode="isolated real two-broker cycle and quote persistence; stored-only API",
        test_database=parsed.path[1:],
        production_container=app["Id"],
        before=before,
    )
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await idle(client)
            pool = (await build_providers(settings, selected, metrics))[0]
            fanout = Fanout(1024, 1, metrics)
            subscriber = fanout.register()
            subscriber.symbols.update(i.canonical_symbol for i, _ in pairs)
            ingestion = CandleIngestion(settings, store, pool, fanout)
            try:
                await ingestion.cycle(utc_now())
            finally:
                await pool.disconnect()
            record["cycle"] = pool.health.last_candle_cycle
            if record["cycle"]["fetch_failures"] or any(
                w.get("http_status") in (400, 401, 403, 429, 451) or w.get("cleanup_error")
                for w in record["cycle"].get("retrieval", {}).get("workers", {}).values()
            ):
                raise ConnectionError("native failure/rejection; no further quote probe")
            record["after"] = {
                i.canonical_symbol: await store.candle_progress(i.id, "tradingview_guest")
                for i, _ in pairs
            }
            record["activation_starts_preserved"] = all(
                record["after"][s]["coverage_start"] == v["coverage_start"]
                for s, v in before.items()
            )
            record["local_durable_candle_fanout_events"] = subscriber.queue.qsize()
            await idle(client)
            quotes = []
            record["quote_probe"] = await quote_probe(settings, pairs, quotes)
            if not quotes or {q.provider_symbol for q in quotes} != {"OANDA:EURUSD", "FX:EURUSD"}:
                raise ValueError("both live broker quote captures required")
            await store.write(quotes)
            record["quote_commit_acknowledged_at"] = utc_now().isoformat()
            async with store.connection() as connection:
                rows = await connection.fetch(
                    "SELECT tick_id,payload FROM market_ticks WHERE tick_id=ANY($1::uuid[])",
                    [q.tick_id for q in quotes],
                )
            payload = {r["tick_id"]: json.loads(r["payload"]) for r in rows}
            exact = sum(payload.get(q.tick_id) == q.model_dump(mode="json") for q in quotes)
            record["quote_ticks"] = [q.model_dump(mode="json") for q in quotes]
            record["quote_db_readback"] = dict(
                accepted=len(quotes), durable=len(rows), exact_payload_matches=exact
            )
            if exact != len(quotes):
                raise ValueError("quote persistence did not preserve exact source payload")
            candle_start = min(v["coverage_start"] for v in before.values())
            candle_end = max(v["coverage_end"] for v in record["after"].values())
            quote_start = min(q.storage_timestamp for q in quotes) - timedelta(seconds=1)
            quote_end = max(q.storage_timestamp for q in quotes) + timedelta(seconds=1)
        await store.close()
        record["api_readback"] = await asyncio.to_thread(
            api_readback, url, pairs, candle_start, candle_end, quote_start, quote_end
        )
        record["api_verified_quote_counts"] = {
            r["source"]: len(r["ticks"]) for r in record["api_readback"]
        }
        record["api_quotes_all_stale_without_active_provider"] = all(
            q["stale"] is True for r in record["api_readback"] for q in r["latest"]
        )
        record["realtime_ui_streaming_verified"] = False
        record["api_native_db_exact"] = {}
        await store.connect()
        for i, m in pairs:
            rows = await store.native_candles(
                i.id, m.provider, candle_start, candle_end, "1m", 1000
            )
            api = next(r for r in record["api_readback"] if r["source"] == m.provider_symbol)[
                "candles"
            ]
            by_bucket = {
                datetime.fromisoformat(r["bucket_start"] or r["bucket"]).isoformat(): r for r in api
            }
            record["api_native_db_exact"][m.provider_symbol] = len(rows) == len(api) and all(
                r["bucket"].isoformat() in by_bucket
                and matches(by_bucket[r["bucket"].isoformat()], r)
                for r in rows
            )
        if not all(record["api_native_db_exact"].values()):
            raise ValueError("native API readback mismatch")
    except Exception as error:
        record["stopped_reason"] = str(error)
        raise
    finally:
        await store.close()
        after = await asyncio.to_thread(inspect_container, "tradingview-app-1")
        record.update(
            finished_at=utc_now().isoformat(),
            production_unchanged=after["Id"] == app["Id"] and after["Image"] == app["Image"],
            production_running=after["State"]["Running"],
        )
        await asyncio.to_thread(
            Path("artifacts/business-broker-storage.json").write_text,
            json.dumps(record, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
    print(
        json.dumps(
            {
                k: v
                for k, v in record.items()
                if k
                not in ("before", "after", "api_readback", "quote_ticks", "quote_probe", "cycle")
            },
            default=str,
        ),
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(run())
