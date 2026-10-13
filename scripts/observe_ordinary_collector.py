"""Read-only live ordinary-application qualification; never calls TradingView.

Retains partial/native-omission cycles, independent DB readback, stored WebSocket events,
REST comparisons, first commit acknowledgements, process resources and loop metrics.
"""

import argparse
import asyncio
import hashlib
import json
import math
import subprocess
import time
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import asyncpg
import httpx
from prometheus_client.parser import text_string_to_metric_families
from websockets.asyncio.client import connect

from marketdata.domain import utc_now
from marketdata.registry import Registry

FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "provider_symbol",
    "source_status",
    "realtime_status",
)


def distribution(values):
    values = sorted(values)
    if not values:
        return {"samples": 0}
    return dict(
        samples=len(values),
        p50=values[math.ceil(len(values) * 0.5) - 1],
        p95=values[math.ceil(len(values) * 0.95) - 1],
        max=values[-1],
    )


def inspect_container(name):
    return json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]


async def resources(name):
    code = (
        "import json,os,pathlib; p=pathlib.Path('/proc/1/stat').read_text().split(); "
        "print(json.dumps(dict(cpu_seconds=(int(p[13])+int(p[14]))/os.sysconf('SC_CLK_TCK'),"
        "rss_bytes=int(p[23])*os.sysconf('SC_PAGE_SIZE'))))"
    )
    p = await asyncio.create_subprocess_exec(
        "docker",
        "exec",
        name,
        ".venv/bin/python",
        "-c",
        code,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await p.communicate()
    if p.returncode:
        return None
    return {"observed_at": utc_now().isoformat(), **json.loads(stdout)}


def matches(a, b):
    for key in FIELDS:
        x, y = a[key], b[key]
        if key in ("open", "high", "low", "close", "volume"):
            x, y = (Decimal(str(v)) if v is not None else None for v in (x, y))
        if x != y:
            return False
    return True


async def snapshot(db, pairs, cutoff=None):
    rows = await db.fetch(
        "SELECT i.canonical_symbol,c.* FROM candle_coverage c JOIN instruments i "
        "ON i.id=c.instrument_id WHERE c.provider='tradingview_guest'"
    )
    cutoff = cutoff or utc_now().replace(second=0, microsecond=0) - timedelta(minutes=300)
    old = await db.fetch(
        "SELECT instrument_id,provider_symbol,bucket_start,open,high,low,close,volume "
        "FROM market_candles WHERE provider='tradingview_guest' AND bucket_start < $1 "
        "ORDER BY instrument_id,bucket_start",
        cutoff,
    )
    return dict(
        at=utc_now().isoformat(),
        cutoff=cutoff.isoformat(),
        database=await db.fetchval("SELECT current_database()"),
        system_identifier=await db.fetchval(
            "SELECT system_identifier::text FROM pg_control_system()"
        ),
        schema=await db.fetchval("SELECT version_num FROM alembic_version"),
        raw_ticks=await db.fetchval(
            "SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest'"
        ),
        history_rows=len(old),
        history_sha256=hashlib.sha256(
            json.dumps([dict(r) for r in old], default=str, sort_keys=True).encode()
        ).hexdigest(),
        coverage=[dict(r) for r in rows],
        selected_sources=[
            dict(symbol=i.canonical_symbol, id=str(i.id), source=m.provider_symbol)
            for i, m in pairs
        ],
    )


def summarize(cycles, first, resources_, consistent, ws_count):
    scheduled = [c for c in cycles if c["trigger"] == "minute"]
    expected = {(s, c["bucket_end"]) for c in scheduled for s in c["eligible_symbols"]}
    available = expected & first.keys()
    immediate = sum(c["independent_current_commits"] for c in scheduled)
    cleanup = all(
        not c["retrieval"].get("active_requests")
        and all(
            not w.get("cleanup_error")
            and w["state"] == "disconnected"
            and not any(
                w.get("retrieval", {}).get(k)
                for k in ("sessions_current", "pending_futures", "active_requests")
            )
            for w in c["retrieval"].get("workers", {}).values()
        )
        for c in scheduled
    )
    result = dict(
        scheduled_minutes=len(scheduled),
        consecutive_scheduled_minutes=all(
            datetime.fromisoformat(b["bucket_end"]) - datetime.fromisoformat(a["bucket_end"])
            == timedelta(minutes=1)
            for a, b in zip(scheduled, scheduled[1:], strict=False)
        ),
        expected_targets=len(expected),
        successful_requests=sum(
            c["requested_instruments"] - c["fetch_failures"] for c in scheduled
        ),
        native_targets=sum(c["completed_current_instruments"] for c in scheduled),
        immediate_commits=immediate,
        eventual_commits=len(available),
        recovered_targets=len(available) - immediate,
        unresolved_targets=[dict(symbol=s, bucket_end=b) for s, b in sorted(expected - available)],
        immediate_completeness=immediate / len(expected) if expected else None,
        eventual_completeness=len(available) / len(expected) if expected else None,
        current_close_to_commit=distribution(
            [c["close_to_commit_seconds"] for c in scheduled if c["independent_current_commits"]]
        ),
        eventual_first_commit=distribution(
            [first[k]["close_to_commit_seconds"] for k in available]
        ),
        cycle_seconds=distribution([c["cycle_duration_seconds"] for c in scheduled]),
        minimum_collection_budget_margin_seconds=min(
            (c["collection_budget_seconds"] - c["cycle_duration_seconds"] for c in scheduled),
            default=None,
        ),
        minimum_next_deadline_margin_seconds=min(
            (
                60 - c["cycle_duration_seconds"] - c.get("grace_to_fetch_start_seconds", 0)
                for c in scheduled
            ),
            default=None,
        ),
        db_write_seconds=distribution([c["db_write_seconds"] for c in scheduled]),
        request_errors=sum(c["fetch_failures"] for c in scheduled),
        db_retries=sum(c["db_failures"] for c in scheduled),
        cleanup_passed=cleanup,
        rest_db_consistent=consistent,
        websocket_verified_targets=ws_count,
        rss_peak_bytes=max((r["rss_bytes"] for r in resources_), default=None),
        cpu_seconds=(resources_[-1]["cpu_seconds"] - resources_[0]["cpu_seconds"])
        if len(resources_) > 1
        else None,
    )
    result["qualified"] = bool(
        expected
        and result["consecutive_scheduled_minutes"]
        and result["immediate_completeness"] >= 0.99
        and result["eventual_completeness"] >= 0.995
        and not result["request_errors"]
        and not result["db_retries"]
        and cleanup
        and result["minimum_collection_budget_margin_seconds"] >= 5
        and consistent
        and ws_count == len(available)
    )
    result["collector_qualified"] = bool(
        expected
        and result["native_targets"] > 0
        and len({symbol for symbol, _ in available}) == len({symbol for symbol, _ in expected})
        and result["consecutive_scheduled_minutes"]
        and result["successful_requests"] == len(expected)
        and result["immediate_commits"] == result["native_targets"]
        and not result["request_errors"]
        and not result["db_retries"]
        and cleanup
        and result["minimum_collection_budget_margin_seconds"] >= 5
        and consistent
        and ws_count == len(available)
    )
    result["coverage_gate_passed"] = result["qualified"]
    return result


async def run(args):
    registry = Registry.load(args.registry)
    pairs = registry.subscriptions("tradingview_guest")
    container = await asyncio.to_thread(inspect_container, args.container)
    if container["Config"]["Labels"].get("com.docker.compose.project") != "tradingview":
        raise ValueError("unexpected production Compose project")
    environment = dict(e.split("=", 1) for e in container["Config"]["Env"])
    url = environment["DATABASE_URL"].replace("@db:5432/", "@localhost:5432/")
    headers = (
        {"Authorization": "Bearer " + environment["API_KEY"]} if environment.get("API_KEY") else {}
    )
    db = await asyncpg.connect(url, command_timeout=15)
    started = utc_now()
    events, cycles, first, samples, metrics = {}, {}, {}, [], {}
    errors = []
    before = await snapshot(db, pairs)
    activations = {row["canonical_symbol"]: row["coverage_start"] for row in before["coverage"]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = dict(
        mode=args.mode,
        started_at=started.isoformat(),
        registry=str(args.registry),
        registry_sha256=hashlib.sha256(args.registry.read_bytes()).hexdigest(),
        container_id=container["Id"],
        image_id=container["Image"],
        before=before,
    )
    async with httpx.AsyncClient(base_url=args.base_url, headers=headers, timeout=15) as client:

        async def websocket():
            async with connect(
                args.base_url.replace("http", "ws", 1) + "/ws/prices",
                additional_headers=headers,
                max_queue=4096,
            ) as ws:
                await ws.send(
                    json.dumps(dict(action="subscribe", symbols=list(registry.by_symbol)))
                )
                async for message in ws:
                    row = json.loads(message)
                    if row.get("type") == "candle":
                        if row.get("persisted") is not True:
                            raise ValueError("ordinary candle event lacks durable acknowledgement")
                        bucket_end = datetime.fromisoformat(row["bucket_end"]).isoformat()
                        events[row["canonical_symbol"], bucket_end] = row

        task = asyncio.create_task(websocket(), name="ordinary-stored-websocket-observer")
        timer = time.monotonic()
        last_resource = 0
        try:
            while sum(c["trigger"] in ("minute", "settling") for c in cycles.values()) < (
                args.minutes + args.settle_minutes
            ):
                if time.monotonic() - timer > (args.minutes + args.settle_minutes) * 90 + 180:
                    raise TimeoutError("scheduled observation limit exceeded")
                if task.done():
                    task.result()
                    raise ConnectionError("WebSocket observer closed")
                provider = next(
                    p
                    for p in (await client.get("/providers")).json()
                    if p["provider"] == "tradingview_guest"
                )
                if set(provider["symbols"]) != set(registry.by_symbol):
                    raise ValueError(
                        "live ordinary registry differs from frozen observation universe"
                    )
                cycle = provider.get("last_candle_cycle", {})
                if (
                    cycle.get("cycle_duration_seconds") is not None
                    and cycle["fetch_cycle_started_at"] >= started.isoformat()
                    and cycle["fetch_cycle_started_at"] not in cycles
                ):
                    eligible = [
                        i.canonical_symbol
                        for i, _ in pairs
                        if activations[i.canonical_symbol]
                        < datetime.fromisoformat(cycle["bucket_end"])
                    ]
                    # Activated count is authoritative; avoid counting pre-activation startup.
                    if cycle["activated_current_instruments"] == len(pairs):
                        eligible = list(registry.by_symbol)
                    elif cycle["trigger"] == "minute":
                        cycle["original_trigger"] = cycle["trigger"]
                        cycle["trigger"] = "transition"
                    cycle["eligible_symbols"] = eligible
                    if (
                        cycle["trigger"] == "minute"
                        and sum(c["trigger"] == "minute" for c in cycles.values()) >= args.minutes
                    ):
                        cycle["original_trigger"] = "minute"
                        cycle["trigger"] = "settling"
                    end = datetime.fromisoformat(cycle["bucket_end"])
                    rows = await db.fetch(
                        "SELECT i.canonical_symbol,m.* FROM market_candles m JOIN instruments i "
                        "ON i.id=m.instrument_id WHERE m.instrument_id=ANY($1::uuid[]) "
                        "AND provider='tradingview_guest' AND interval='1m' "
                        "AND price_basis='native' "
                        "AND bucket_start >= $2 AND bucket_end <= $3",
                        [i.id for i, _ in pairs],
                        started.replace(second=0, microsecond=0) - timedelta(minutes=3),
                        end,
                    )
                    current = [r for r in rows if r["bucket_end"] == end]
                    cycle["independent_current_commits"] = len(current)
                    for row in rows:
                        key = row["canonical_symbol"], row["bucket_end"].isoformat()
                        first.setdefault(
                            key,
                            dict(
                                symbol=key[0],
                                bucket_end=key[1],
                                commit_acknowledged_at=cycle["db_commit_completed_at"],
                                close_to_commit_seconds=(
                                    datetime.fromisoformat(cycle["db_commit_completed_at"])
                                    - row["bucket_end"]
                                ).total_seconds(),
                            ),
                        )
                        event = events.get(key)
                        if event and not matches(event, dict(row)):
                            errors.append(
                                dict(
                                    category="websocket_db_mismatch",
                                    symbol=key[0],
                                    bucket_end=key[1],
                                )
                            )
                    cycles[cycle["fetch_cycle_started_at"]] = cycle
                    print(
                        json.dumps(
                            dict(
                                mode=args.mode,
                                bucket_end=cycle["bucket_end"],
                                trigger=cycle["trigger"],
                                native=len(current),
                                expected=cycle["activated_current_instruments"],
                                latency=cycle["close_to_commit_seconds"],
                                errors=cycle["fetch_failures"],
                            )
                        ),
                        flush=True,
                    )
                    if any(
                        w.get("http_status") in (400, 401, 403, 429, 451) or w.get("cleanup_error")
                        for w in cycle["retrieval"].get("workers", {}).values()
                    ):
                        raise ConnectionError(
                            "source rejected/throttled or cleanup failed; stop escalation"
                        )
                response = await client.get("/metrics")
                for family in text_string_to_metric_families(response.text):
                    for sample in family.samples:
                        if any(
                            term in sample.name
                            for term in (
                                "event_loop_lag",
                                "error",
                                "throttle",
                                "timeout",
                                "disconnect",
                                "reconnect",
                                "overrun",
                                "dropped",
                            )
                        ):
                            metrics[sample.name + str(sorted(sample.labels.items()))] = sample.value
                if time.monotonic() - last_resource > 10:
                    sample = await resources(args.container)
                    if sample:
                        samples.append(sample)
                    last_resource = time.monotonic()
                checkpoint.update(
                    cycles=list(cycles.values()),
                    first_commits=list(first.values()),
                    resources=samples,
                    metrics=metrics,
                    observer_errors=errors,
                )
                args.output.write_text(
                    json.dumps(checkpoint, indent=2, default=str) + "\n", encoding="utf-8"
                )
                await asyncio.sleep(2)
            # Compare all supplied target rows with REST, and the persisted WebSocket event.
            scheduled = [c for c in cycles.values() if c["trigger"] == "minute"]
            lo = min(datetime.fromisoformat(c["bucket_end"]) for c in scheduled) - timedelta(
                minutes=1
            )
            hi = max(datetime.fromisoformat(c["bucket_end"]) for c in scheduled)
            ws_count = 0
            for i, _ in pairs:
                rows = await db.fetch(
                    "SELECT * FROM market_candles WHERE instrument_id=$1 "
                    "AND provider='tradingview_guest' AND interval='1m' AND price_basis='native' "
                    "AND bucket_start >= $2 AND bucket_end <= $3",
                    i.id,
                    lo,
                    hi,
                )
                response = await client.get(
                    "/candles/" + i.canonical_symbol,
                    params=dict(
                        provider="tradingview_guest",
                        start=lo.isoformat(),
                        end=hi.isoformat(),
                        limit=1000,
                    ),
                )
                response.raise_for_status()
                rest = {r["bucket_end"]: r for r in response.json()["items"]}
                for row in rows:
                    key = i.canonical_symbol, row["bucket_end"].isoformat()
                    rest_row = rest.get(key[1], rest.get(key[1].replace("+00:00", "Z")))
                    if rest_row is None or not matches(rest_row, dict(row)):
                        errors.append(
                            dict(category="rest_db_mismatch", symbol=key[0], bucket_end=key[1])
                        )
                    if key in events and matches(events[key], dict(row)):
                        ws_count += 1
                    first.setdefault(
                        key, dict(symbol=key[0], bucket_end=key[1], close_to_commit_seconds=None)
                    )
            checkpoint["summary"] = summarize(
                list(cycles.values()), first, samples, not errors, ws_count
            )
            checkpoint["summary"]["required_eventual_completeness"] = args.required_completeness
            checkpoint["summary"]["settling_cycles"] = args.settle_minutes
            checkpoint["summary"]["near_lossless_qualified"] = bool(
                checkpoint["summary"]["eventual_completeness"] >= args.required_completeness
                and checkpoint["summary"]["rest_db_consistent"]
                and checkpoint["summary"]["cleanup_passed"]
                and checkpoint["summary"]["immediate_commits"]
                == checkpoint["summary"]["native_targets"]
                and checkpoint["summary"]["websocket_verified_targets"]
                == checkpoint["summary"]["eventual_commits"]
                and checkpoint["summary"]["consecutive_scheduled_minutes"]
                and checkpoint["summary"]["minimum_collection_budget_margin_seconds"] >= 5
            )
        except Exception as error:
            checkpoint["stopped_reason"] = str(error)
            raise
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            checkpoint.update(
                finished_at=utc_now().isoformat(),
                after=await snapshot(db, pairs, datetime.fromisoformat(before["cutoff"])),
                observer_errors=errors,
                websocket_events=list(events.values()),
            )
            args.output.write_text(
                json.dumps(checkpoint, indent=2, default=str) + "\n", encoding="utf-8"
            )
            await db.close()
    print(json.dumps(checkpoint["summary"], indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minutes", type=int, choices=range(2, 61), default=10)
    parser.add_argument("--settle-minutes", type=int, choices=range(0, 11), default=0)
    parser.add_argument("--required-completeness", type=float, default=0.995)
    parser.add_argument("--mode", choices=("baseline", "recovery"), default="baseline")
    parser.add_argument("--container", default="tradingview-app-1")
    parser.add_argument("--base-url", default="http://localhost:8000")
    asyncio.run(run(parser.parse_args()))
