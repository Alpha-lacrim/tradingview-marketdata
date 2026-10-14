"""Bounded TradingView qualification in an isolated database, leaving production running.

Two historical captures of a fixed window plus a separate current-quote probe. Runs only
in confirmed idle production phases, at most two paced sockets / global four; no second
ordinary scheduler or API server. This is source qualification, not a deployed minute soak.
"""

import argparse
import asyncio
import json
import os
import time
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from observe_ordinary_collector import inspect_container, matches

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.providers.factory import build_providers
from marketdata.providers.tradingview import TradingViewGuestProvider
from marketdata.registry import Registry
from marketdata.sessions import Expectation, expectation


async def idle(client):
    """Finish bounded probes before the next ordinary fetch; never stop that collector."""
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = await client.get("http://localhost:8000/providers")
        response.raise_for_status()
        p = next(p for p in response.json() if p["provider"] == "tradingview_guest")
        state = p.get("candle_retrieval", {})
        workers = list(state.get("workers", {}).values())
        if any(w.get("http_status") in (400, 401, 403, 429, 451) for w in workers):
            raise ConnectionError("production source rejection/throttling; no qualification I/O")
        now = utc_now()
        cycle = p.get("last_candle_cycle", {})
        if (
            20 <= now.second <= 40
            and cycle.get("cycle_duration_seconds") is not None
            and cycle.get("bucket_end", "") >= now.replace(second=0, microsecond=0).isoformat()
            and not state.get("active_requests")
            and workers
            and all(
                w["state"] == "disconnected" and not w.get("backoff_remaining_seconds")
                for w in workers
            )
        ):
            return
        await asyncio.sleep(1)
    raise TimeoutError("no safe idle source-probe window; production untouched")


async def quote_probe(settings, pairs, captured_ticks=None):
    provider = TradingViewGuestProvider(settings, Metrics())
    await provider.subscribe(pairs)
    rows = {
        i.canonical_symbol: dict(observations=[], source_metadata={}, rejection=None)
        for i, _ in pairs
    }
    try:
        async with asyncio.timeout(10):
            await provider.connect()
            async for tick in provider.stream_ticks():
                if captured_ticks is not None:
                    captured_ticks.append(tick)
                rows[tick.canonical_symbol]["observations"].append(
                    dict(
                        last=str(tick.last) if tick.last is not None else None,
                        bid=str(tick.bid) if tick.bid is not None else None,
                        ask=str(tick.ask) if tick.ask is not None else None,
                        event_timestamp=tick.event_timestamp.isoformat()
                        if tick.event_timestamp
                        else None,
                        received_timestamp=tick.received_timestamp.isoformat(),
                        last_source_age_seconds=(
                            tick.received_timestamp - tick.event_timestamp
                        ).total_seconds()
                        if tick.last is not None and tick.event_timestamp
                        else None,
                        realtime_status=tick.realtime_status.value,
                    )
                )
    except TimeoutError:
        pass  # Deliberately bounded observation, not an invented transport failure.
    finally:
        async with asyncio.timeout(3):
            await provider.disconnect()
        for i, m in pairs:
            rows[i.canonical_symbol]["source_metadata"] = provider.source_metadata.get(
                m.provider_symbol, {}
            )
            rows[i.canonical_symbol]["rejection"] = provider.rejected.get(m.provider_symbol)
    return rows


async def run(args):
    url = os.environ["BUSINESS_TEST_DATABASE_URL"]
    parsed = urlsplit(url)
    if (
        parsed.hostname not in ("localhost", "127.0.0.1")
        or parsed.path != "/marketdata_business_qualification_test"
    ):
        raise ValueError(
            "require the dedicated localhost qualification database; no production writes"
        )
    app = await asyncio.to_thread(inspect_container, "tradingview-app-1")
    database = await asyncio.to_thread(inspect_container, "tradingview-db-1")
    if app["Config"]["Labels"].get(
        "com.docker.compose.project"
    ) != "tradingview" or "tradingview_marketdata_pg" not in {
        m.get("Name") for m in database["Mounts"]
    }:
        raise ValueError("unexpected existing production project/volume")
    registry = Registry.load(args.registry)
    pairs = registry.subscriptions("tradingview_guest")
    settings = Settings(
        _env_file=None,
        database_url=url,
        db_pool_min=1,
        db_pool_max=2,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=2,
        candle_pool_concurrency=4,
        candle_pool_max_instruments=args.batch_size,
        candle_pool_connect_interval_seconds=2,
        candle_pool_deadline_seconds=12,
        candle_pool_cleanup_seconds=3,
        candle_request_timeout_seconds=8,
        candle_fetch_grace_seconds=5,
    )
    store = Store(settings)
    await store.connect()
    await store.register(registry, candle_providers=["tradingview_guest"])
    end = utc_now().replace(second=0, microsecond=0) - timedelta(minutes=3)
    start = end - timedelta(minutes=60)
    report = dict(
        mode="isolated-business-source-qualification",
        started_at=utc_now().isoformat(),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        window_minutes=60,
        ordinary_scheduled_cycles=0,
        production_container=app["Id"],
        production_image=app["Image"],
        production_database_container=database["Id"],
        test_database=parsed.path[1:],
        configuration=dict(
            connections=2,
            global_concurrency=4,
            worker_concurrency=2,
            pacing_seconds=2,
            deadline_seconds=12,
            cleanup_seconds=3,
        ),
        sources={
            i.canonical_symbol: dict(source=m.provider_symbol, passes=[], quote=None)
            for i, m in pairs
        },
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            for attempt in range(2):
                for offset in range(0, len(pairs), args.batch_size):
                    batch = pairs[offset : offset + args.batch_size]
                    await idle(client)
                    subset = Registry(
                        [e for e in registry.entries if e.instrument.id in {i.id for i, _ in batch}]
                    )
                    pool = (await build_providers(settings, subset, Metrics()))[0]
                    count = int((utc_now() - start).total_seconds() // 60) + 2
                    timer = time.perf_counter()
                    try:
                        await pool.connect()
                        results = await pool.fetch_many(
                            [(pair, start, end, count) for pair in batch]
                        )
                    finally:
                        await pool.disconnect()
                    fetch_seconds = time.perf_counter() - timer
                    retrieval = pool.health.candle_retrieval
                    all_candles = [
                        c for value in results.values() if isinstance(value, list) for c in value
                    ]
                    write_timer = time.perf_counter()
                    db_error = None
                    try:
                        await store.write_candles(all_candles, [])
                        acknowledged = utc_now()
                    except Exception as error:
                        db_error = type(error).__name__
                        acknowledged = None
                    write_seconds = time.perf_counter() - write_timer
                    for i, _m in batch:
                        row = report["sources"][i.canonical_symbol]
                        value = results[i.canonical_symbol]
                        worker = next(
                            w.provider
                            for w in pool.workers
                            if i.canonical_symbol in w.provider.health.symbols
                        )
                        session = worker.session_info.get(i.canonical_symbol, {})
                        diag = pool.timings.get(i.canonical_symbol, {})
                        candles = value if isinstance(value, list) else []
                        classifications = {e.value: [] for e in Expectation}
                        for n in range(60):
                            minute = start + timedelta(minutes=n)
                            classifications[expectation(session, minute).value].append(
                                minute.isoformat()
                            )
                        async with store.connection() as connection:
                            stored = await connection.fetch(
                                "SELECT * FROM market_candles WHERE instrument_id=$1 "
                                "AND provider='tradingview_guest' AND interval='1m' "
                                "AND price_basis='native' AND bucket_start >=$2 AND bucket_end<=$3",
                                i.id,
                                start,
                                end,
                            )
                        actual = {r["bucket_start"].isoformat(): dict(r) for r in stored}
                        exact = all(
                            c.bucket_start.isoformat() in actual
                            and matches(
                                c.model_dump(mode="json"), actual[c.bucket_start.isoformat()]
                            )
                            for c in candles
                        )
                        row["passes"].append(
                            dict(
                                at=utc_now().isoformat(),
                                attempt=attempt + 1,
                                request_success=not isinstance(value, Exception),
                                source_error=str(value) if isinstance(value, Exception) else None,
                                error_class=type(value).__name__
                                if isinstance(value, Exception)
                                else None,
                                source_info=diag.get("source_info", {}),
                                source_status=diag.get("source_status"),
                                session_info=session,
                                classifications=classifications,
                                native_buckets=[c.bucket_start.isoformat() for c in candles],
                                native_volumes=[
                                    str(c.volume) if c.volume is not None else None for c in candles
                                ],
                                stored_buckets=list(actual),
                                exact_native_db_readback=exact,
                                db_error=db_error,
                                commit_acknowledged_at=acknowledged.isoformat()
                                if acknowledged
                                else None,
                                fetch_batch_seconds=fetch_seconds,
                                request_seconds=diag.get("duration_seconds"),
                                db_write_seconds=write_seconds,
                                retrieval=retrieval,
                            )
                        )
                    save()
                    print(
                        json.dumps(
                            dict(
                                pass_number=attempt + 1,
                                batch=offset // args.batch_size + 1,
                                sources=len(batch),
                                native=len(all_candles),
                                errors=sum(isinstance(v, Exception) for v in results.values()),
                                db_error=db_error,
                            )
                        ),
                        flush=True,
                    )
                    if any(
                        w.get("http_status") in (400, 401, 403, 429, 451) or w.get("cleanup_error")
                        for w in retrieval.get("workers", {}).values()
                    ):
                        raise ConnectionError(
                            "source rejection/throttling/cleanup failure; qualification stopped"
                        )
                    if attempt == 0:
                        await idle(client)
                        quotes = await quote_probe(settings, batch)
                        for symbol, q in quotes.items():
                            report["sources"][symbol]["quote"] = q
                        save()
    except BaseException as error:
        report["stopped_reason"] = str(error) or type(error).__name__
        raise
    finally:
        after = await asyncio.to_thread(inspect_container, "tradingview-app-1")
        after_db = await asyncio.to_thread(inspect_container, "tradingview-db-1")
        report.update(
            finished_at=utc_now().isoformat(),
            production_unchanged=after["Id"] == app["Id"]
            and after["Image"] == app["Image"]
            and after_db["Id"] == database["Id"]
            and after_db["State"]["StartedAt"] == database["State"]["StartedAt"],
            production_running=after["State"]["Running"],
        )
        save()
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("config/instruments.tradingview-business-candidates.json"),
    )
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/business-source-qualification.json")
    )
    parser.add_argument("--batch-size", type=int, choices=range(4, 17), default=16)
    asyncio.run(run(parser.parse_args()))
