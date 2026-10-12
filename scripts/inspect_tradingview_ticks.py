"""Tick volume / derived-candle export; deletion requires --delete-raw and verification.

Derived exports are NOT native candles and never enter market_candles. Inspect/verify
and retain the export. --delete-raw refuses changed or incompletely covered observations.
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

from marketdata.config import Settings
from marketdata.domain import utc
from marketdata.persistence import Store


async def run(args: argparse.Namespace) -> None:
    store = Store(Settings())
    await store.connect()
    try:
        async with store.connection() as c, c.transaction(isolation="serializable"):
            if args.delete_raw:
                if args.output is None:
                    raise ValueError("--delete-raw requires an existing --output export")
                await c.execute("LOCK TABLE market_ticks IN SHARE ROW EXCLUSIVE MODE")
            rows = await c.fetch("""
                SELECT i.canonical_symbol,count(*) AS ticks,
                    min(t.received_timestamp) AS first_receipt,
                    max(t.received_timestamp) AS last_receipt,
                    count(*) FILTER(WHERE last IS NOT NULL) AS last_observations,
                    count(*) FILTER(WHERE bid IS NOT NULL AND ask IS NOT NULL) AS mid_observations
                FROM market_ticks t JOIN instruments i ON i.id=t.instrument_id
                WHERE provider='tradingview_guest' GROUP BY i.canonical_symbol
                ORDER BY i.canonical_symbol
            """)
            print(json.dumps([dict(r) for r in rows], default=str, indent=2))
            if args.output is None:
                return
            if not args.start or not args.end:
                raise ValueError("export requires explicit aware --start and --end")
            start, end = (
                utc(datetime.fromisoformat(args.start)),
                utc(datetime.fromisoformat(args.end)),
            )
            if not start < end or end - start > timedelta(days=1):
                raise ValueError("export range must be positive and at most one day")
            if any(t.second or t.microsecond for t in (start, end)):
                raise ValueError("export boundaries must align to UTC minutes")
            count = await c.fetchval(
                """SELECT count(*) FROM market_ticks
                WHERE provider='tradingview_guest' AND storage_timestamp >= $1
                    AND storage_timestamp < $2""",
                start,
                end,
            )
            if count > 1_000_000:
                raise ValueError("maintenance window exceeds 1,000,000 rows; use smaller windows")
            fingerprint = await c.fetchval(
                """SELECT md5(string_agg(
                md5(payload::text),'' ORDER BY storage_timestamp,tick_id)) FROM market_ticks
                WHERE provider='tradingview_guest' AND storage_timestamp >= $1
                    AND storage_timestamp < $2""",
                start,
                end,
            )
            # Query the observations directly: no stale continuous aggregate dependency.
            price = "last" if args.basis == "last" else "(bid+ask)/2"
            condition = (
                "last IS NOT NULL"
                if args.basis == "last"
                else "bid IS NOT NULL AND ask IS NOT NULL"
            )
            derived = await c.fetch(
                f"""
                SELECT instrument_id,provider,provider_symbol,
                    date_bin('1 minute',storage_timestamp,'1970-01-01') AS bucket_start,
                    date_bin('1 minute',storage_timestamp,'1970-01-01')
                        + INTERVAL '1 minute' AS bucket_end,
                    (array_agg({price} ORDER BY order_ns,received_timestamp,tick_id))[1] AS open,
                    max({price}) AS high,min({price}) AS low,
                    (array_agg({price} ORDER BY order_ns DESC,
                        received_timestamp DESC,tick_id DESC))[1] AS close,
                    count(*) AS observations,min(storage_timestamp) AS first_observation,
                    max(storage_timestamp) AS last_observation
                FROM market_ticks WHERE provider='tradingview_guest' AND {condition}
                    AND storage_timestamp >= $1 AND storage_timestamp < $2
                GROUP BY instrument_id,provider,provider_symbol,bucket_start
                ORDER BY instrument_id,bucket_start
            """,
                start,
                end,
            )
            payload = dict(
                provenance="derived_from_recorded_observations",
                interval="1m",
                complete_minute_coverage=False,
                price_basis=args.basis,
                native_equivalence=False,
                coverage="incomplete sampled observations; no full-minute guarantee",
                volume=None,
                start=start,
                end=end,
                items=[dict(r) for r in derived],
                source_rows=count,
                source_fingerprint=fingerprint,
            )
            if args.delete_raw:
                saved = json.loads(await asyncio.to_thread(args.output.read_text, encoding="utf-8"))
                if saved != json.loads(json.dumps(payload, default=str)):
                    raise ValueError("export verification failed; source/export changed")
                if sum(r["observations"] for r in derived) != count:
                    raise ValueError("export does not cover every raw observation; retain raw data")
                result = await c.execute(
                    """DELETE FROM market_ticks
                    WHERE provider='tradingview_guest' AND storage_timestamp >= $1
                        AND storage_timestamp < $2""",
                    start,
                    end,
                )
                print(result + "; verified export retained; refresh old continuous aggregates")
                return
            # Exclusive creation prevents accidentally overwriting a previous verified export.
            with args.output.open("x", encoding="utf-8") as target:  # noqa: ASYNC240
                json.dump(payload, target, default=str, indent=2)
            print(f"Exported {len(derived)} derived candles; raw ticks unchanged: {args.output}")
    finally:
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--basis", choices=["last", "mid"], default="last")
    parser.add_argument("--delete-raw", action="store_true")
    asyncio.run(run(parser.parse_args()))
