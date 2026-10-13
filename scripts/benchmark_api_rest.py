"""Bounded synthetic REST benchmark on an explicitly supplied disposable database.

No providers or collectors are constructed. Fixtures are unique, visibly simulated,
and removed by their exact IDs. Output contains timings/plans, never credentials.
"""

import argparse
import asyncio
import json
import secrets
import statistics
import time
import tracemalloc
from contextlib import asynccontextmanager
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from pydantic import SecretStr

from marketdata.api import create_app
from marketdata.candles import MarketCandle, minute_start
from marketdata.config import Settings
from marketdata.domain import Instrument, ProviderInstrument, RegistryEntry, normalize, utc_now
from marketdata.persistence import Store
from marketdata.registry import Registry


def timing(samples):
    ordered = sorted(samples)
    return {
        "samples": len(samples),
        "p50_ms": round(statistics.median(samples) * 1000, 3),
        "p95_ms": round(ordered[int(0.95 * (len(samples) - 1))] * 1000, 3),
        "max_ms": round(max(samples) * 1000, 3),
    }


def plan_summary(plan):
    nodes = []

    def visit(node):
        nodes.append(
            {
                k: node[k]
                for k in ("Node Type", "Index Name", "Actual Rows", "Actual Loops")
                if k in node
            }
        )
        for child in node.get("Plans", []):
            visit(child)

    visit(plan[0]["Plan"])
    return {
        "execution_ms": plan[0]["Execution Time"],
        "planning_ms": plan[0]["Planning Time"],
        "nodes": nodes,
    }


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import os

    url = os.environ.get("TEST_DATABASE_URL", "")
    parsed = urlsplit(url)
    if not parsed.path.endswith("_test") or not parsed.hostname or parsed.port in (None, 5432):
        raise SystemExit(
            "use TEST_DATABASE_URL with a disposable _test database on a nonproduction port"
        )
    settings = Settings(_env_file=None, database_url=url, api_environment="test")
    writer = Store(settings)
    await writer.connect()
    suffix = uuid4().hex[:10].upper()
    price = Decimal("123.123456789012345678901")
    registry = Registry(
        [
            RegistryEntry(
                instrument=Instrument(
                    canonical_symbol=f"CRYPTO:APIBENCH_{suffix}_{i:03}",
                    display_name="Simulated API benchmark fixture",
                    asset_class="crypto",
                    instrument_type="spot",
                    exchange="SIM",
                    currency="USD",
                ),
                mappings=[
                    ProviderInstrument(
                        provider="mock",
                        provider_symbol=f"APIBENCH_{suffix}_{i:03}",
                        realtime_status="simulated",
                    )
                ],
            )
            for i in range(100)
        ]
    )
    identities = [entry.instrument.id for entry in registry.entries]
    tables = (
        "market_ticks",
        "latest_prices",
        "market_candles",
        "latest_candles",
        "candle_revisions",
        "candle_coverage",
        "provider_instruments",
        "instruments",
    )
    try:
        await writer.register(registry)
        end = minute_start(utc_now())
        start = end.replace(hour=0, minute=0) - timedelta(days=1)
        for instrument, mapping in registry.subscriptions("mock"):
            bars = [
                MarketCandle(
                    instrument_id=instrument.id,
                    canonical_symbol=instrument.canonical_symbol,
                    provider="mock",
                    provider_symbol=mapping.provider_symbol,
                    bucket_start=start + timedelta(minutes=i),
                    bucket_end=start + timedelta(minutes=i + 1),
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=None,
                    realtime_status="simulated",
                    source_status="streaming",
                    received_timestamp=utc_now(),
                )
                for i in range(1440)
            ]
            await writer.write_candles(
                bars,
                [
                    (
                        instrument.id,
                        "mock",
                        start,
                        start + timedelta(days=1),
                        {"session": "24x7", "timezone": "Etc/UTC"},
                    )
                ],
            )
        await writer.write(
            [normalize(i, m, utc_now(), last=price) for i, m in registry.subscriptions("mock")]
        )
        token = secrets.token_urlsafe(32)
        app = create_app(
            Settings(
                _env_file=None, database_url=url, api_environment="test", api_key=SecretStr(token)
            )
        )
        report = {
            "fixture_sources": 100,
            "fixture_candles": 144000,
            "populated_days_per_source": 1,
            "transport": "ASGI in process",
            "simulated": True,
            "measurements": {},
        }
        async with app.router.lifespan_context(app):
            queries = []
            original = app.state.store.connection

            @asynccontextmanager
            async def counted():
                async with original() as connection:
                    connection.add_query_logger(queries.append)
                    try:
                        yield connection
                    finally:
                        await asyncio.sleep(0)
                        connection.remove_query_logger(queries.append)

            app.state.store.connection = counted
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app),
                base_url="http://test",
                headers={"Authorization": "Bearer " + token},
            ) as client:
                symbols = [("symbols", e.instrument.canonical_symbol) for e in registry.entries]
                query = {
                    "symbol": registry.entries[0].instrument.canonical_symbol,
                    "provider": "mock",
                    "start": (end - timedelta(days=31)).isoformat(),
                    "end": end.isoformat(),
                    "limit": 1000,
                }
                for name, path, params, repetitions in (
                    ("latest_100", "/api/v1/prices/latest", symbols, 50),
                    ("latest_candles_100", "/api/v1/candles/latest", symbols, 20),
                    ("history_1m_31day_window", "/api/v1/candles", query, 20),
                    (
                        "coverage_100_31day_window",
                        "/api/v1/coverage",
                        symbols + [(k, str(v)) for k, v in query.items() if k in ("start", "end")],
                        10,
                    ),
                ):
                    samples = []
                    queries.clear()
                    for _ in range(repetitions):
                        began = time.perf_counter()
                        response = await client.get(path, params=params)
                        assert response.status_code == 200, response.text
                        samples.append(time.perf_counter() - began)
                    report["measurements"][name] = timing(samples)
                    report["measurements"][name]["select_queries_per_request"] = (
                        sum(q.query.lstrip().startswith(("SELECT", "WITH")) for q in queries)
                        // repetitions
                    )
                    if name == "latest_100":
                        assert len(response.json()["items"]) == 100
                        assert all(Decimal(r["last"]) == price for r in response.json()["items"])
                    if name.startswith("history"):
                        history_query = next(q for q in queries if "WITH rolled AS" in q.query)
                        async with original() as c:
                            plan = json.loads(
                                await c.fetchval(
                                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                                    + history_query.query,
                                    *history_query.args,
                                )
                            )
                            stored = await c.fetch(
                                "SELECT open,high,low,close,volume FROM market_candles "
                                "WHERE instrument_id=$1 ORDER BY bucket_start LIMIT 1000",
                                identities[0],
                            )
                        for actual, expected in zip(response.json()["items"], stored, strict=True):
                            assert all(
                                Decimal(actual[k]) == expected[k]
                                for k in ("open", "high", "low", "close")
                            )
                            assert actual["volume"] is expected["volume"] is None
                        report["history_plan"] = plan_summary(plan)
                tracemalloc.start()
                await client.get("/api/v1/prices/latest", params=symbols)
                report["latest_python_allocation_peak_bytes"] = tracemalloc.get_traced_memory()[1]
                tracemalloc.stop()
                report["readback_consistent"] = True
        async with writer.connection() as c:
            report["database_version"] = await c.fetchval("SELECT version()")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        async with writer.connection() as c, c.transaction():
            for table in tables:
                key = "id" if table == "instruments" else "instrument_id"
                await c.execute(f"DELETE FROM {table} WHERE {key}=ANY($1::uuid[])", identities)
        await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
