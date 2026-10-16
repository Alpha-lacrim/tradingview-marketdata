"""Bounded full-pipeline rate, freshness, revocation and handover qualification."""

# Bounded private task files are diagnostic inputs, never production persistence.
# ruff: noqa: ASYNC240

import asyncio
import json
from pathlib import Path
from uuid import uuid4

from qualify_v11_pipeline import Pipeline, arguments, write_json

from marketdata.domain import utc_now


async def run(args):
    if args.genuine or args.feeds != 1:
        raise ValueError("edge qualification requires one simulated feed")
    p = Pipeline(args)
    try:
        await p.initialize()
        await p.acceptance()
        s, d = p.connections["source"], p.connections["destination"]
        initial_cursor = (await p.reader.get("/api/v1/marketdata/changes")).json()["cursor"]

        # With no new market observations, an otherwise healthy API becomes stale.
        last = p.quotes(9000, fields=("last",))[0].model_copy(update={"event_timestamp": None})
        # Add a genuine independent simulated observation with no upstream clock.
        with p.quotes_path.open("a", encoding="utf-8") as output:
            output.write(
                last.model_copy(
                    update={"tick_id": uuid4(), "received_timestamp": utc_now()}
                ).model_dump_json()
                + "\n"
            )
        await p.wait("last-only snapshot delivery", p.destination_caught_up)
        await asyncio.sleep(32)
        updated = await p.observe_quotes(9001, fields=("bid", "ask"))
        result = await p.reader.get("/api/v1/prices/latest", params={"limit": 1})
        item = result.json()["items"][0]
        p.check(
            "new bid/ask cannot freshen old last", item["quote_freshness"] == "stale", item=item
        )
        p.check("stale quote while API healthy", (await p.reader.get("/livez")).status_code == 200)

        async def delivered_stale():
            return any(
                e["type"] == "price_update"
                and e["data"].get("bid") == str(updated[0].bid)
                and e["data"]["quote_freshness"] == "stale"
                for e in p.ws_events
            )

        await p.wait("committed stale quote WebSocket event", delivered_stale)
        price_events = [e for e in p.ws_events if e["type"] == "price_update"]
        p.check(
            "committed WebSocket preserves stale primary clock",
            price_events[-1]["data"]["quote_freshness"] == "stale",
        )
        # Same numeric price, newly independently observed, is a fresh observation.
        await p.observe_quotes(9100, fields=("last",))
        await p.wait("same-value independently observed refresh", p.destination_caught_up)
        item = (await p.reader.get("/api/v1/prices/latest", params={"limit": 1})).json()["items"][0]
        p.check(
            "same price with new observation refreshes its clock",
            item["quote_freshness"] == "fresh",
        )

        # Exercise actual per-process ingestion quota and durable retry/backpressure.
        rate_limited = False
        for _ in range(130):
            response = await p.ingest.get("/api/ingest/v1/receipts/" + str(uuid4()))
            if response.status_code == 429:
                rate_limited = True
                break
        p.check("actual authenticated HTTP rate limit", rate_limited)
        p.quotes(9200)

        async def backed_off():
            return await s.fetchval(
                "SELECT EXISTS(SELECT FROM source_sync_batches WHERE phase='staged' AND attempts>0)"
            )

        await p.wait("durable rate-limit backoff", backed_off, 30)
        pending = await s.fetchrow(
            "SELECT batch_id,payload FROM source_sync_batches WHERE phase='staged' ORDER BY"
            " created_at LIMIT 1"
        )
        p.check(
            "rate limiting retains pending source",
            await s.fetchval("SELECT count(*) FROM source_quote_state WHERE acked_version<version")
            == 1,
        )
        await p.stop("ingest")
        p.launch("ingest")
        await p.wait("rate-limit restart recovery", p.destination_caught_up, 120)
        saved = await s.fetchrow(
            "SELECT payload FROM source_sync_batches WHERE batch_id=$1", pending["batch_id"]
        )
        p.check(
            "rate-limit retry preserves batch bytes",
            saved is None or bytes(saved["payload"]) == bytes(pending["payload"]),
        )

        # Use the existing writer's bounded retention, not artificial cursor edits.
        await p.stop("ingest")
        p.configs["ingest"]["settings"]["marketdata_event_retention_count"] = 3
        p.launch("ingest")
        for number in range(5):
            await p.observe_quotes(9300 + number)
            await p.wait("bounded journal update", p.destination_caught_up)
        expired = await p.reader.get("/api/v1/marketdata/changes", params={"after": initial_cursor})
        p.check("expired replay cursor requires REST resynchronization", expired.status_code == 409)
        before = len(p.ws_events)
        p.ws_task.cancel()
        await asyncio.gather(p.ws_task, return_exceptions=True)
        p.ws_task = asyncio.create_task(p.ws_consumer())
        await p.observe_quotes(9400)
        await p.wait("reconnected WebSocket observation delivered", p.destination_caught_up)

        async def reconnected():
            return len(p.ws_events) > before

        await p.wait("new committed WebSocket event after reconnect", reconnected)
        p.check("WebSocket reconnect resumes committed updates", True)

        # Disabled producer cannot renew/write/read receipts; pending source is retained.
        await p.stop("worker")
        await d.execute(
            "UPDATE ingest_producers SET enabled=false WHERE producer_id=$1", p.producer
        )
        p.quotes(9500)
        p.launch("worker")
        code = await asyncio.wait_for(asyncio.to_thread(p.children["worker"].wait), 30)
        p.running.discard("worker")
        p.check("revocation blocks retained producer credentials", code != 0)
        p.check(
            "revocation retains pending latest state",
            await s.fetchval("SELECT count(*) FROM source_quote_state WHERE acked_version<version")
            == 1,
        )
        # Explicit test-administrator re-enable, never a worker/API capability.
        await d.execute("UPDATE ingest_producers SET enabled=true WHERE producer_id=$1", p.producer)
        await p.launch_worker()
        await p.wait("administrator re-enabled test producer resume", p.destination_caught_up)
        await p.parity()

        # A future source clock must fail closed while preserving pending records.
        p.quotes(9700, age=-600)

        async def quarantined():
            return await s.fetchval(
                "SELECT EXISTS(SELECT FROM source_sync_batches WHERE phase='quarantined' "
                "AND error_code IN ('source_clock_ahead','collector_clock_ahead'))"
            )

        # Nonretryable worker exit is expected; inspect durable state without readiness shortcut.
        async with asyncio.timeout(40):
            while not await quarantined():  # noqa: ASYNC110 - external durable state
                await asyncio.sleep(0.2)
        p.check("future timestamp rejected and quarantined", True)
        p.check(
            "future observation not incorrectly acknowledged",
            await s.fetchval("SELECT count(*) FROM source_quote_state WHERE acked_version<version")
            == 1,
        )

        # Administrator handover to a distinct producer/credential installation.
        await p.stop("worker")
        next_producer = uuid4()
        await d.execute(
            "INSERT INTO "
            "ingest_producers(producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)"
            " VALUES($1,'M4 handover target',$2,$3,1,clock_timestamp()+interval '120 "
            "seconds')",
            next_producer,
            str(uuid4()),
            uuid4(),
        )
        await d.execute(
            "UPDATE ingest_source_owners SET "
            "producer_id=$1,ownership_generation=ownership_generation+1 WHERE "
            "producer_id=$2",
            next_producer,
            p.producer,
        )
        count = await d.fetchval("SELECT count(*) FROM ingest_observations")
        p.quotes(9600)
        p.launch("worker")
        code = await asyncio.wait_for(asyncio.to_thread(p.children["worker"].wait), 30)
        p.running.discard("worker")
        p.check(
            "old producer cannot regain transferred streams",
            code != 0 and await d.fetchval("SELECT count(*) FROM ingest_observations") == count,
        )
        p.check(
            "handover retains old pending source records",
            await s.fetchval("SELECT count(*) FROM source_quote_state WHERE acked_version<version")
            == 1,
        )

        p.report["completed_at"] = utc_now()
    except BaseException as error:
        import traceback

        p.report.update(
            failure_category=type(error).__name__,
            failure_frames=[
                dict(file=Path(f.filename).name, line=f.lineno, function=f.name)
                for f in traceback.extract_tb(error.__traceback__)
            ],
        )
        write_json(p.directory / "report.json", p.report)
        raise
    finally:
        await p.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(run(arguments()))
    except Exception as error:
        print(json.dumps(dict(edge_qualification_failed=type(error).__name__)), flush=True)
        raise SystemExit(1) from None
