"""Real loopback HTTP/WS qualification; only Batch 4 disposable databases are accepted.

Run with TEST_DATABASE_URL on 55891/55892. API runs in a separate native process.
No provider is constructed. Synthetic fixtures are deleted by their exact identities.
"""

import argparse
import asyncio
import ctypes
import json
import os
import platform
import secrets
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import uvicorn
from websockets.asyncio.client import connect

from marketdata.api import create_app
from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import Instrument, ProviderInstrument, RegistryEntry, normalize, utc_now
from marketdata.persistence import Store
from marketdata.registry import Registry


def summary(samples):
    ordered = sorted(samples)
    if not ordered:
        return {"samples": 0}
    return dict(
        samples=len(samples),
        **{
            name: round(ordered[int(fraction * (len(ordered) - 1))] * 1000, 3)
            for name, fraction in (
                ("p50_ms", 0.5),
                ("p95_ms", 0.95),
                ("p99_ms", 0.99),
                ("max_ms", 1),
            )
        },
    )


def rss():
    if sys.platform == "win32":

        class Counters(ctypes.Structure):
            _fields_ = [("cb", ctypes.c_ulong), ("faults", ctypes.c_ulong)] + [
                (name, ctypes.c_size_t)
                for name in (
                    "peak",
                    "working",
                    "quota_peak",
                    "quota",
                    "nonpaged_peak",
                    "nonpaged",
                    "pagefile",
                    "pagefile_peak",
                    "private",
                )
            ]

        counters = Counters()
        kernel = ctypes.windll.kernel32
        kernel.GetCurrentProcess.restype = ctypes.c_void_p
        ctypes.windll.psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_ulong,
        ]
        if not ctypes.windll.psapi.GetProcessMemoryInfo(
            kernel.GetCurrentProcess(), ctypes.byref(counters), ctypes.sizeof(counters)
        ):
            raise OSError("process memory measurement failed")
        return counters.working
    import resource

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
        1 if sys.platform == "darwin" else 1024
    )


async def worker(args):
    app = create_app(Settings(_env_file=None))
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=args.port,
            ws="websockets",
            ws_max_size=65536,
            ws_max_queue=16,
            access_log=False,
            log_level="error",
        )
    )
    queries, lag, resources = [], [], []
    original = app.state.metrics.api_query_latency.observe

    def query(seconds):
        queries.append(seconds)
        original(seconds)

    app.state.metrics.api_query_latency.observe = query
    began, cpu = time.perf_counter(), time.process_time()

    async def sample():
        loop = asyncio.get_running_loop()
        while not args.stop_file.exists():
            expected = loop.time() + 0.1
            await asyncio.sleep(0.1)
            lag.append(max(0, loop.time() - expected))
            pool = app.state.store.pool
            hub = getattr(app.state, "market_hub", None)
            resources.append(
                dict(
                    rss_bytes=rss(),
                    pool_used=pool.get_size() - pool.get_idle_size() if pool else 0,
                    pool_size=pool.get_size() if pool else 0,
                    clients=len(hub.clients) if hub else 0,
                    max_queue=max((c.queue.qsize() for c in hub.clients), default=0) if hub else 0,
                )
            )
        server.should_exit = True

    task = asyncio.create_task(sample())
    try:
        await server.serve()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        elapsed = time.perf_counter() - began
        args.output.write_text(
            json.dumps(
                dict(
                    database_queries=summary(queries),
                    event_loop_lag=summary(lag),
                    elapsed_seconds=elapsed,
                    cpu_seconds=time.process_time() - cpu,
                    cpu_average_one_core_percent=100 * (time.process_time() - cpu) / elapsed,
                    peak_rss_bytes=max((r["rss_bytes"] for r in resources), default=0),
                    peak_pool_used=max((r["pool_used"] for r in resources), default=0),
                    peak_pool_size=max((r["pool_size"] for r in resources), default=0),
                    peak_ws_clients=max((r["clients"] for r in resources), default=0),
                    sampled_peak_ws_queue=max((r["max_queue"] for r in resources), default=0),
                    observed_queue_high_water=getattr(app.state.market_hub, "queue_high_water", 0),
                ),
                indent=2,
            )
        )


async def load(client, path, params, concurrency, requests, expected):
    samples, statuses, bodies = [], {}, []
    began = time.perf_counter()

    async def run(count):
        for _ in range(count):
            start = time.perf_counter()
            try:
                response = await client.get(path, params=params)
                status = str(response.status_code)
                if response.status_code == 200:
                    body = response.json()
                    assert len(body["items"]) == expected, (path, len(body["items"]), expected)
                    if not bodies:
                        bodies.append(body)
            except httpx.HTTPError:
                status = "transport_error"
            samples.append(time.perf_counter() - start)
            statuses[status] = statuses.get(status, 0) + 1

    await asyncio.gather(
        *(run(requests // concurrency + (i < requests % concurrency)) for i in range(concurrency))
    )
    elapsed = time.perf_counter() - began
    return dict(
        summary(samples),
        concurrency=concurrency,
        elapsed_seconds=round(elapsed, 3),
        requests_per_second=round(len(samples) / elapsed, 3),
        statuses=statuses,
        failed_rate=(len(samples) - statuses.get("200", 0)) / len(samples),
    ), bodies[0] if bodies else None


async def qualify(args):
    url = os.environ.get("TEST_DATABASE_URL", "")
    parsed = urlsplit(url)
    if (
        parsed.hostname != "127.0.0.1"
        or parsed.port not in (55891, 55892)
        or parsed.path != "/marketdata_batch4_test"
    ):
        raise SystemExit(
            "only the isolated Batch 4 loopback databases on 55891/55892 are permitted"
        )
    settings = Settings(_env_file=None, database_url=url, api_environment="test")
    writer = Store(settings)
    await writer.connect()
    suffix = uuid4().hex[:10].upper()
    entries = []
    for i in range(100):
        asset = "crypto" if i < 50 else "forex" if i < 90 else "metals" if i < 95 else "energy"
        entries.append(
            RegistryEntry(
                instrument=Instrument(
                    canonical_symbol=f"SIM:QUAL4:{suffix}:{i:03}",
                    display_name="Simulated qualification fixture",
                    asset_class=asset,
                    instrument_type="spot"
                    if asset == "crypto"
                    else "otc"
                    if asset != "energy"
                    else "cfd",
                    exchange="SIM",
                    currency="USD",
                ),
                mappings=[
                    ProviderInstrument(
                        provider="qualification",
                        provider_symbol=f"SIM_{i:03}",
                        realtime_status="simulated",
                    )
                ],
            )
        )
    registry = Registry(entries)
    pairs = registry.subscriptions("qualification")
    ids = [i.id for i, _ in pairs]
    role = "qual4_read_" + uuid4().hex
    password, token = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    process = None
    database_samples = []
    database_monitor = None
    container = f"marketdata-batch4-tests-{'postgres' if parsed.port == 55891 else 'timescale'}-1"

    async def database_profile():
        while True:
            result = await asyncio.to_thread(
                subprocess.run,
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", container],
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                row = json.loads(result.stdout)
                database_samples.append({k: row[k] for k in ("CPUPerc", "MemUsage", "PIDs")})
            await asyncio.sleep(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    stop = args.output.with_suffix(".stop")
    telemetry = args.output.with_suffix(".worker.json")
    if stop.exists():
        stop.unlink()
    origin = "https://frontend.company.example"
    report = dict(
        simulated=True,
        fixture_sources=100,
        configured_asset_classes=["crypto", "forex", "metals", "energy"],
        candles_per_source=1440,
        deliberately_missing_minutes=3,
        transport="real loopback TCP; separate native API process",
        collector="deterministic Store writes; no provider connection",
        limits=dict(pool=10, ws_queue=256, poll_seconds=1),
        platform=sys.platform,
        python_version=platform.python_version(),
        host_cpu=platform.processor(),
        host_logical_cpus=os.cpu_count(),
        measured_at=utc_now().isoformat(),
        rest=[],
        websocket=[],
    )
    start = datetime(2026, 10, 10, tzinfo=UTC)
    end = start + timedelta(days=1)
    price = Decimal("123.123456789012345678901")
    tables = (
        "instruments,provider_instruments,market_ticks,latest_prices,market_candles,"
        "latest_candles,candle_revisions,candle_coverage,collector_status,ingest_read_status,alembic_version,"
        "marketdata_event_clock,marketdata_events"
    )
    try:
        await writer.register(registry)
        for index, (instrument, mapping) in enumerate(pairs):
            bars = [
                MarketCandle(
                    instrument_id=instrument.id,
                    canonical_symbol=instrument.canonical_symbol,
                    provider=mapping.provider,
                    provider_symbol=mapping.provider_symbol,
                    bucket_start=start + timedelta(minutes=j),
                    bucket_end=start + timedelta(minutes=j + 1),
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=None,
                    realtime_status="simulated",
                    source_status="streaming",
                    received_timestamp=end,
                    metadata={"volume_type": "unknown"},
                )
                for j in range(1440)
                if not (index < 3 and j == 120)
            ]
            await writer.write_candles(
                bars,
                [
                    (
                        instrument.id,
                        mapping.provider,
                        start,
                        end,
                        {"session": "24x7", "timezone": "Etc/UTC"},
                    )
                ],
            )
        await writer.write(
            [normalize(i, m, utc_now(), event_timestamp=utc_now(), last=price) for i, m in pairs]
        )
        async with writer.connection() as c:
            await c.execute("REVOKE TEMP ON DATABASE marketdata_batch4_test FROM PUBLIC")
            await c.execute(f"CREATE ROLE {role} LOGIN PASSWORD '{password}'")
            await c.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
            await c.execute(f"GRANT SELECT ON {tables} TO {role}")
            report["database_version"] = await c.fetchval("SELECT version()")
            report["fixture_candles"] = await c.fetchval(
                "SELECT count(*) FROM market_candles WHERE instrument_id=ANY($1::uuid[])", ids
            )
        captured = []
        original_connection = writer.connection

        @asynccontextmanager
        async def traced():
            async with original_connection() as c:
                c.add_query_logger(captured.append)
                try:
                    yield c
                finally:
                    await asyncio.sleep(0)
                    c.remove_query_logger(captured.append)

        writer.connection = traced
        try:
            await writer.native_candles(
                ids[0],
                "qualification",
                end - timedelta(minutes=1000),
                end,
                "1m",
                1001,
                as_of=utc_now(),
                provider_symbol=pairs[0][1].provider_symbol,
            )
        finally:
            writer.connection = original_connection
        history_query = next(q for q in captured if "WITH rolled AS" in q.query)
        async with writer.connection() as c:
            report["history_plan"] = json.loads(
                await c.fetchval(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + history_query.query,
                    *history_query.args,
                )
            )
        read_url = urlunsplit(
            parsed._replace(netloc=f"{role}:{password}@{parsed.hostname}:{parsed.port}")
        )
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        env = dict(
            os.environ,
            DATABASE_URL=read_url,
            APPLICATION_ROLE="api",
            API_ENVIRONMENT="production",
            API_KEY=token,
            WEBSOCKET_ALLOWED_ORIGINS=json.dumps([origin]),
            DB_TIMEOUT_SECONDS="5",
        )
        with args.output.with_suffix(".worker.log").open("w") as log:
            process = await asyncio.to_thread(
                subprocess.Popen,
                [
                    sys.executable,
                    __file__,
                    "--worker",
                    "--port",
                    str(port),
                    "--stop-file",
                    str(stop),
                    "--output",
                    str(telemetry),
                ],
                env=env,
                stdout=log,
                stderr=log,
                creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
            )
        if args.db_resources:
            database_monitor = asyncio.create_task(database_profile())
        base = f"http://127.0.0.1:{port}"
        async with httpx.AsyncClient(
            base_url=base,
            headers={"Authorization": "Bearer " + token},
            timeout=15,
            limits=httpx.Limits(max_connections=64, max_keepalive_connections=64),
        ) as client:
            async with asyncio.timeout(20):
                while True:  # noqa: ASYNC110
                    assert process.poll() is None, "qualification worker exited"
                    try:
                        if (await client.get("/health")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
            symbols = [("symbols", i.canonical_symbol) for i, _ in pairs]
            hist = dict(
                symbol=pairs[0][0].canonical_symbol,
                provider="qualification",
                start=(end - timedelta(minutes=1000)).isoformat(),
                end=end.isoformat(),
                limit=1000,
            )
            for concurrency in args.concurrency:
                for name, path, params, expected in (
                    (
                        "instruments100",
                        "/api/v1/instruments",
                        {"provider": "qualification", "limit": 100},
                        100,
                    ),
                    ("latest100", "/api/v1/prices/latest", symbols, 100),
                    (
                        "history500",
                        "/api/v1/candles",
                        dict(hist, start=(end - timedelta(minutes=500)).isoformat(), limit=500),
                        500,
                    ),
                    ("history1000", "/api/v1/candles", hist, 1000),
                    (
                        "coverage100",
                        "/api/v1/coverage",
                        symbols + [("start", start.isoformat()), ("end", end.isoformat())],
                        100,
                    ),
                ):
                    result, body = await load(
                        client, path, params, concurrency, args.requests, expected
                    )
                    report["rest"].append(dict(name=name, **result))
                    if body and name == "history1000":
                        assert all(
                            Decimal(row["close"]) == price and row["complete"]
                            for row in body["items"]
                        )
                    if body and name == "coverage100":
                        assert sum(row["unresolved_expected_gaps"] for row in body["items"]) == 3
            # Paged history traverses the gap instead of filling it.
            query = dict(hist, start=start.isoformat(), limit=500)
            buckets = []
            while True:
                body = (await client.get("/api/v1/candles", params=query)).json()
                buckets.extend(row["bucket_start"] for row in body["items"])
                if not body["next_cursor"]:
                    break
                query["cursor"] = body["next_cursor"]
            assert len(buckets) == len(set(buckets)) == 1439
            report["pagination_gap_decimal_readback"] = True
            for clients in args.ws_clients:
                sockets, receipts, acknowledgements = [], [], {}
                try:
                    for _ in range(clients):
                        ws = await connect(
                            base.replace("http", "ws") + "/api/v1/ws/marketdata",
                            origin=origin,
                            additional_headers={"Authorization": "Bearer " + token},
                            max_queue=1024,
                        )
                        sockets.append(ws)
                        await ws.send(
                            json.dumps(
                                dict(
                                    action="subscribe",
                                    symbols=[i.canonical_symbol for i, _ in pairs],
                                )
                            )
                        )
                        async with asyncio.timeout(15):
                            while True:
                                message = json.loads(await ws.recv())
                                if message["type"] == "price_snapshot" and message["final"]:
                                    break

                    async def receive(ws, receipts=receipts):
                        for _ in range(300):
                            async with asyncio.timeout(15):
                                while True:
                                    message = json.loads(await ws.recv())
                                    if message["type"] == "price_update":
                                        receipts.append(
                                            (
                                                message["data"]["source_timestamp"],
                                                time.perf_counter(),
                                            )
                                        )
                                        break

                    readers = [asyncio.create_task(receive(ws)) for ws in sockets]

                    async def publish(acknowledgements=acknowledgements):
                        for cycle in range(3):
                            now = utc_now()
                            ticks = [
                                normalize(
                                    i, m, now, event_timestamp=now, last=price + Decimal(cycle + 1)
                                )
                                for i, m in pairs
                            ]
                            await writer.write(ticks)
                            acknowledgements[now.isoformat().replace("+00:00", "Z")] = (
                                time.perf_counter()
                            )
                            await asyncio.sleep(1.2)

                    result, _ = await asyncio.gather(
                        load(client, "/api/v1/candles", hist, 8, args.requests, 1000), publish()
                    )
                    await asyncio.gather(*readers)
                    delivery = [
                        received - acknowledgements[timestamp] for timestamp, received in receipts
                    ]
                    assert len(delivery) == clients * 300
                    report["websocket"].append(
                        dict(
                            clients=clients,
                            subscriptions=clients * 100,
                            received=len(delivery),
                            expected=clients * 300,
                            ack_to_delivery=summary(delivery),
                            negative_ack_deltas=sum(t < 0 for t in delivery),
                            rest_during_writes=result[0],
                        )
                    )
                finally:
                    for ws in sockets:
                        await ws.close()
            report["health_without_collector"] = (await client.get("/api/v1/health")).json()
            assert report["health_without_collector"]["status"] == "ok"

            async def docker(*arguments):
                result = await asyncio.to_thread(
                    subprocess.run,
                    ["docker", *arguments],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                assert result.returncode == 0, "disposable database operation failed"
                return result.stdout

            project = await docker(
                "inspect",
                "--format",
                '{{index .Config.Labels "com.docker.compose.project"}}',
                container,
            )
            assert project.strip() == "marketdata-batch4-tests"
            paused = False
            try:
                await docker("pause", container)
                paused = True
                degraded, _ = await load(client, "/api/v1/candles", hist, 8, 16, 1000)
                assert degraded["statuses"] == {"503": 16}
                assert (await client.get("/livez")).status_code == 200
                report["database_degradation"] = degraded
            finally:
                if paused:
                    await docker("unpause", container)
            recovery_started = time.perf_counter()
            async with asyncio.timeout(20):
                while (await client.get("/health")).status_code != 200:  # noqa: ASYNC110
                    await asyncio.sleep(0.1)
            report["database_recovery_seconds"] = time.perf_counter() - recovery_started
            report["recovered_history"], _ = await load(
                client, "/api/v1/candles", hist, 8, 24, 1000
            )
            assert report["recovered_history"]["failed_rate"] == 0
            report["worker_metrics"] = (await client.get("/metrics")).text
            report["read_only_database_role"] = True
    finally:
        if database_monitor is not None:
            database_monitor.cancel()
            await asyncio.gather(database_monitor, return_exceptions=True)
            report["database_resources"] = dict(
                sampled_by="docker stats --no-stream, one-second spacing after each sample",
                samples=database_samples,
                peak_cpu_percent=max(
                    (float(r["CPUPerc"].rstrip("%")) for r in database_samples), default=0
                ),
            )
        if process is not None:
            stop.touch()
            try:
                await asyncio.to_thread(process.wait, 15)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait, 5)
            report["worker_exit_code"] = process.returncode
            if telemetry.exists():
                report["api_resources"] = json.loads(telemetry.read_text())
        async with writer.connection() as c, c.transaction():
            for table in (
                "marketdata_events",
                "market_ticks",
                "latest_prices",
                "market_candles",
                "latest_candles",
                "candle_revisions",
                "candle_coverage",
                "provider_instruments",
                "instruments",
            ):
                key = "id" if table == "instruments" else "instrument_id"
                await c.execute(f"DELETE FROM {table} WHERE {key}=ANY($1::uuid[])", ids)
            if await c.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role):
                await c.execute(f"DROP OWNED BY {role}")
                await c.execute(f"DROP ROLE {role}")
        await writer.close()
        args.output.write_text(json.dumps(report, indent=2))
        print(
            json.dumps(
                {
                    k: report[k]
                    for k in (
                        "fixture_sources",
                        "fixture_candles",
                        "rest",
                        "websocket",
                        "api_resources",
                    )
                    if k in report
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--port", type=int)
    parser.add_argument("--stop-file", type=Path)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 8, 24])
    parser.add_argument("--ws-clients", type=int, nargs="+", default=[5, 20, 50])
    parser.add_argument("--requests", type=int, default=120)
    parser.add_argument("--db-resources", action="store_true")
    options = parser.parse_args()
    if (
        options.requests < max(options.concurrency)
        or any(not 1 <= c <= 64 for c in options.concurrency)
        or any(not 1 <= c <= 100 for c in options.ws_clients)
    ):
        parser.error("requests must cover concurrency; REST 1..64, WS 1..100")
    asyncio.run(worker(options) if options.worker else qualify(options))
