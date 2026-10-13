"""Independent final readback for recorded ordinary targets; no source calls or writes.

Does not rewrite the frozen initial observation or assign unknown commit timestamps.
"""

import argparse
import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

import asyncpg
import httpx
from observe_ordinary_collector import inspect_container, matches

from marketdata.domain import utc_now


async def audit(args):
    raw = json.loads(args.checkpoint.read_text(encoding="utf-8-sig"))
    environment = dict(
        e.split("=", 1)
        for e in (await asyncio.to_thread(inspect_container, "tradingview-app-1"))["Config"]["Env"]
    )
    db = await asyncpg.connect(environment["DATABASE_URL"].replace("@db:5432/", "@localhost:5432/"))
    headers = (
        {"Authorization": "Bearer " + environment["API_KEY"]} if environment.get("API_KEY") else {}
    )
    events = {
        (r["canonical_symbol"], datetime.fromisoformat(r["bucket_end"]).isoformat()): r
        for r in raw.get("websocket_events", [])
    }
    results = []
    cycles = [c for c in raw["cycles"] if c["trigger"] == "minute"]
    try:
        async with httpx.AsyncClient(base_url="http://localhost:8000", headers=headers) as client:
            for source in raw["before"]["selected_sources"]:
                for cycle in cycles:
                    if source["symbol"] not in cycle["eligible_symbols"]:
                        continue
                    end = datetime.fromisoformat(cycle["bucket_end"])
                    start = end - timedelta(minutes=1)
                    row = await db.fetchrow(
                        "SELECT * FROM market_candles WHERE instrument_id=$1::text::uuid "
                        "AND provider='tradingview_guest' AND interval='1m' "
                        "AND price_basis='native' AND bucket_start=$2",
                        source["id"],
                        start,
                    )
                    rest = (
                        await client.get(
                            "/candles/" + source["symbol"],
                            params=dict(
                                provider="tradingview_guest",
                                start=start.isoformat(),
                                end=end.isoformat(),
                            ),
                        )
                    ).json()["items"]
                    event = events.get((source["symbol"], end.isoformat()))
                    audited_event = False
                    if event and row and not matches(event, dict(row)):
                        revisions = await db.fetch(
                            "SELECT old_values FROM candle_revisions WHERE instrument_id="
                            "$1::text::uuid AND provider='tradingview_guest' AND bucket_start=$2",
                            source["id"],
                            start,
                        )
                        audited_event = any(
                            matches(
                                event,
                                dict(
                                    json.loads(revision["old_values"]),
                                    provider_symbol=row["provider_symbol"],
                                ),
                            )
                            for revision in revisions
                        )
                    results.append(
                        dict(
                            symbol=source["symbol"],
                            bucket_end=end.isoformat(),
                            stored=row is not None,
                            rest_db_match=(bool(rest) and matches(rest[0], dict(row)))
                            if row
                            else not rest,
                            websocket_db_match=matches(event, dict(row)) if event and row else None,
                            websocket_correction_audit_match=audited_event,
                            backfilled=json.loads(row["metadata"]).get("backfilled")
                            if row
                            else None,
                        )
                    )
        result = dict(
            checkpoint=args.checkpoint.name,
            checkpoint_sha256=hashlib.sha256(args.checkpoint.read_bytes()).hexdigest(),
            audited_at=utc_now().isoformat(),
            expected=len(results),
            stored=sum(r["stored"] for r in results),
            rest_db_matches=sum(r["rest_db_match"] for r in results),
            websocket_db_matches=sum(r["websocket_db_match"] is True for r in results),
            websocket_audited_corrections=sum(
                r["websocket_correction_audit_match"] for r in results
            ),
            targets=results,
        )
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({k: v for k, v in result.items() if k != "targets"}))
    finally:
        await db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkpoint", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(audit(parser.parse_args()))
