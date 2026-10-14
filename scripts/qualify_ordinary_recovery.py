"""Labelled bounded interruption of the existing collector; no extra source requests.

An independent hidden watchdog and a finally block restart the same container/image.
Only the app is stopped; the database and volume are never stopped or recreated.
"""

import argparse
import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import asyncpg
import httpx
from observe_ordinary_collector import distribution, inspect_container, matches, resources, snapshot
from websockets.asyncio.client import connect

from marketdata.domain import utc_now
from marketdata.registry import Registry


async def docker(*arguments):
    process = await asyncio.create_subprocess_exec(
        "docker", *arguments, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=45)
    except TimeoutError:
        process.kill()
        await process.wait()
        raise
    if process.returncode:
        raise RuntimeError(stderr.decode().strip())
    return stdout.decode().strip()


async def run(args):
    registry = Registry.load(args.registry)
    pairs = registry.subscriptions("tradingview_guest")
    app = await asyncio.to_thread(inspect_container, "tradingview-app-1")
    database = await asyncio.to_thread(inspect_container, "tradingview-db-1")
    if (
        not app["State"]["Running"]
        or app["Config"]["Labels"].get("com.docker.compose.project") != "tradingview"
        or database["Config"]["Labels"].get("com.docker.compose.project") != "tradingview"
        or "tradingview_marketdata_pg" not in {m.get("Name") for m in database["Mounts"]}
    ):
        raise ValueError("unexpected existing production identity; no interruption")
    names = await docker(
        "ps", "--filter", "label=com.docker.compose.service=app", "--format", "{{.Names}}"
    )
    if names.splitlines() != ["tradingview-app-1"]:
        raise ValueError("expected exactly one collector; no interruption")
    environment = dict(e.split("=", 1) for e in app["Config"]["Env"])
    headers = (
        {"Authorization": "Bearer " + environment["API_KEY"]} if environment.get("API_KEY") else {}
    )
    db = await asyncpg.connect(environment["DATABASE_URL"].replace("@db:5432/", "@localhost:5432/"))
    if (
        await db.fetchval("SELECT system_identifier::text FROM pg_control_system()")
        != "7693817192740921379"
    ):
        await db.close()
        raise ValueError("unexpected PostgreSQL system identifier; no interruption")
    events, cycles, acknowledgements, samples = {}, {}, {}, []
    record = dict(
        mode="fault-injection",
        app_container=app["Id"],
        image_id=app["Image"],
        database_container=database["Id"],
        requested_interruption_seconds=args.seconds,
    )
    record["before"] = await snapshot(db, pairs)
    task = watchdog = None
    args.output.parent.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(
        base_url="http://localhost:8000", headers=headers, timeout=5
    ) as client:

        async def provider():
            response = await client.get("/providers")
            response.raise_for_status()
            state = next(p for p in response.json() if p["provider"] == "tradingview_guest")
            if set(state["symbols"]) != set(registry.by_symbol):
                raise ValueError("live registry differs from frozen recovery universe")
            return state

        async def websocket():
            for _ in range(60):
                try:
                    async with connect(
                        "ws://localhost:8000/ws/prices", additional_headers=headers, max_queue=4096
                    ) as ws:
                        await ws.send(
                            json.dumps(dict(action="subscribe", symbols=list(registry.by_symbol)))
                        )
                        async for message in ws:
                            row = json.loads(message)
                            if row.get("type") == "candle":
                                if row.get("persisted") is not True:
                                    raise ValueError("event lacks durable acknowledgement")
                                events[
                                    row["canonical_symbol"],
                                    datetime.fromisoformat(row["bucket_end"]).isoformat(),
                                ] = row
                    raise ConnectionError("recovery WebSocket closed")
                except OSError:
                    await asyncio.sleep(0.5)
            raise TimeoutError("recovery WebSocket could not connect")

        try:
            deadline = time.monotonic() + 75
            while True:
                state = await provider()
                last = state.get("last_candle_cycle", {})
                retrieval = state.get("candle_retrieval", {})
                current = utc_now().replace(second=0, microsecond=0)
                if (
                    20 <= utc_now().second <= 40
                    and last.get("cycle_duration_seconds") is not None
                    and datetime.fromisoformat(last["bucket_end"]) >= current
                    and not retrieval.get("active_requests")
                    and all(
                        w["state"] == "disconnected" for w in retrieval.get("workers", {}).values()
                    )
                ):
                    break
                if time.monotonic() > deadline:
                    raise TimeoutError("no idle interruption window; collector left running")
                await asyncio.sleep(1)
            last_end = datetime.fromisoformat(last["bucket_end"])
            record["last_completed_before_stop"] = last
            # The watchdog survives failure of this harness. It restarts the SAME ID.
            code = (
                "import subprocess,sys,time; time.sleep(110); "
                "subprocess.run(['docker','start',sys.argv[1]],timeout=45,check=True)"
            )
            watchdog = await asyncio.to_thread(
                subprocess.Popen,
                [sys.executable, "-c", code, app["Id"]],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            try:
                record["stop_requested_at"] = utc_now().isoformat()
                await docker("stop", "--timeout", "35", app["Id"])
                record["stopped_at"] = utc_now().isoformat()
                print(
                    json.dumps(
                        dict(
                            event="collector_stopped",
                            at=record["stopped_at"],
                            restart_after_seconds=args.seconds,
                        )
                    ),
                    flush=True,
                )
                await asyncio.sleep(args.seconds)
            finally:
                record["restart_requested_at"] = utc_now().isoformat()
                task = asyncio.create_task(websocket(), name="recovery-stored-events")
                await docker("start", app["Id"])
                record["restart_returned_at"] = utc_now().isoformat()
            resumed = await asyncio.to_thread(inspect_container, app["Id"])
            db_after = await asyncio.to_thread(inspect_container, database["Id"])
            if (
                resumed["Image"] != app["Image"]
                or resumed["Config"]["Env"] != app["Config"]["Env"]
                or db_after["Mounts"] != database["Mounts"]
                or db_after["State"]["StartedAt"] != database["State"]["StartedAt"]
            ):
                raise ValueError("restart changed image/configuration or database identity")
            if not resumed["State"]["Running"]:
                raise RuntimeError("collector did not restart; watchdog remains armed")
            watchdog.terminate()
            await asyncio.to_thread(watchdog.wait, timeout=5)
            watchdog = None
            record["restart_identity_verified"] = True
            end = datetime.fromisoformat(record["restart_returned_at"]).replace(
                second=0, microsecond=0
            )
            expected = {
                (i.canonical_symbol, (last_end + timedelta(minutes=n)).isoformat())
                for i, _ in pairs
                for n in range(1, int((end - last_end).total_seconds() / 60) + 1)
            }
            record["missed_expected_targets"] = [
                dict(symbol=s, bucket_end=b) for s, b in sorted(expected)
            ]
            print(
                json.dumps(
                    dict(
                        event="same_collector_restarted",
                        at=record["restart_returned_at"],
                        missed_expected=len(expected),
                    )
                ),
                flush=True,
            )
            deadline = time.monotonic() + (args.cycles + 2) * 90
            last_resource = 0
            while sum(c["trigger"] == "minute" for c in cycles.values()) < args.cycles:
                if time.monotonic() > deadline:
                    raise TimeoutError("recovery observation deadline exceeded")
                if task.done():
                    task.result()
                    raise ConnectionError("recovery WebSocket ended")
                try:
                    state = await provider()
                except httpx.TransportError:
                    await asyncio.sleep(0.5)
                    continue
                cycle = state.get("last_candle_cycle", {})
                if (
                    cycle.get("cycle_duration_seconds") is not None
                    and cycle.get("fetch_cycle_started_at", "") >= record["restart_requested_at"]
                    and cycle["fetch_cycle_started_at"] not in cycles
                ):
                    rows = await db.fetch(
                        "SELECT i.canonical_symbol,m.* FROM market_candles m JOIN instruments i "
                        "ON i.id=m.instrument_id WHERE m.instrument_id=ANY($1::uuid[]) "
                        "AND provider='tradingview_guest' AND interval='1m' "
                        "AND price_basis='native' AND bucket_end>$2 AND bucket_end<=$3",
                        [i.id for i, _ in pairs],
                        last_end,
                        end,
                    )
                    for row in rows:
                        key = row["canonical_symbol"], row["bucket_end"].isoformat()
                        acknowledgements.setdefault(
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
                    cycles[cycle["fetch_cycle_started_at"]] = cycle
                    print(
                        json.dumps(
                            dict(
                                event="recovery_cycle",
                                trigger=cycle["trigger"],
                                end=cycle["bucket_end"],
                                recovered=len(acknowledgements),
                                expected=len(expected),
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
                            "source rejection/throttling or cleanup failure; no escalation"
                        )
                if time.monotonic() - last_resource > 10:
                    sample = await resources("tradingview-app-1")
                    if sample:
                        samples.append(sample)
                    last_resource = time.monotonic()
                record.update(
                    cycles=list(cycles.values()),
                    first_commits=list(acknowledgements.values()),
                    resources=samples,
                )
                args.output.write_text(
                    json.dumps(record, indent=2, default=str) + "\n", encoding="utf-8"
                )
                await asyncio.sleep(1)
            results = []
            for i, _ in pairs:
                rows = await db.fetch(
                    "SELECT * FROM market_candles WHERE instrument_id=$1 "
                    "AND provider='tradingview_guest' AND interval='1m' "
                    "AND price_basis='native' AND bucket_end>$2 AND bucket_end<=$3",
                    i.id,
                    last_end,
                    end,
                )
                response = await client.get(
                    "/candles/" + i.canonical_symbol,
                    params=dict(
                        provider="tradingview_guest",
                        start=last_end.isoformat(),
                        end=end.isoformat(),
                        limit=1000,
                    ),
                )
                response.raise_for_status()
                rest = {
                    datetime.fromisoformat(r["bucket_end"]).isoformat(): r
                    for r in response.json()["items"]
                }
                for row in rows:
                    key = i.canonical_symbol, row["bucket_end"].isoformat()
                    results.append(
                        dict(
                            symbol=key[0],
                            bucket_end=key[1],
                            rest_db_match=key[1] in rest and matches(rest[key[1]], dict(row)),
                            websocket_db_match=key in events and matches(events[key], dict(row)),
                        )
                    )
            available = {(r["symbol"], r["bucket_end"]) for r in results}
            record["summary"] = dict(
                expected_targets=len(expected),
                durable_recovered_targets=len(expected & available),
                unresolved_targets=[
                    dict(symbol=s, bucket_end=b) for s, b in sorted(expected - available)
                ],
                rest_db_matches=sum(r["rest_db_match"] for r in results),
                websocket_db_matches=sum(r["websocket_db_match"] for r in results),
                scheduled_recovery_cycles=sum(c["trigger"] == "minute" for c in cycles.values()),
                request_errors=sum(c["fetch_failures"] for c in cycles.values()),
                db_retries=sum(c["db_failures"] for c in cycles.values()),
                successful_requests=sum(
                    c["requested_instruments"] - c["fetch_failures"] for c in cycles.values()
                ),
                current_native_targets=sum(
                    c["completed_current_instruments"] for c in cycles.values()
                ),
                recovered_first_commit=distribution(
                    [r["close_to_commit_seconds"] for r in acknowledgements.values()]
                ),
                observed_cycle_seconds=distribution(
                    [c["cycle_duration_seconds"] for c in cycles.values()]
                ),
                worker_cleanup_passed=all(
                    not c["retrieval"].get("active_requests")
                    and all(
                        w["state"] == "disconnected"
                        and not w.get("cleanup_error")
                        and not any(
                            w.get("retrieval", {}).get(k)
                            for k in ("sessions_current", "pending_futures", "active_requests")
                        )
                        for w in c["retrieval"].get("workers", {}).values()
                    )
                    for c in cycles.values()
                ),
                rss_peak_bytes=max((r["rss_bytes"] for r in samples), default=None),
                cpu_seconds=samples[-1]["cpu_seconds"] - samples[0]["cpu_seconds"]
                if len(samples) > 1
                else None,
                interruption_seconds=(
                    datetime.fromisoformat(record["restart_returned_at"])
                    - datetime.fromisoformat(record["stopped_at"])
                ).total_seconds(),
            )
        except BaseException as error:
            record["stopped_reason"] = str(error) or type(error).__name__
            raise
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            record.update(
                finished_at=utc_now().isoformat(),
                cycles=list(cycles.values()),
                first_commits=list(acknowledgements.values()),
                websocket_events=list(events.values()),
                resources=samples,
                after=await snapshot(db, pairs, datetime.fromisoformat(record["before"]["cutoff"])),
            )
            args.output.write_text(
                json.dumps(record, indent=2, default=str) + "\n", encoding="utf-8"
            )
            await db.close()
    print(json.dumps(record["summary"], indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry", type=Path, default=Path("config/instruments.tradingview-100.json")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, choices=range(30, 91), default=70)
    parser.add_argument("--cycles", type=int, choices=range(2, 6), default=3)
    asyncio.run(run(parser.parse_args()))
