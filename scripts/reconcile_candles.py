"""Read-only historical coverage reconciliation, independent of the collector process."""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

from marketdata.candles import minute_start
from marketdata.config import Settings
from marketdata.domain import utc, utc_now
from marketdata.persistence import Store
from marketdata.sessions import Expectation, expectation


async def reconcile(store: Store, start: datetime, end: datetime) -> list[dict[str, object]]:
    start, end = utc(start), utc(end)
    if (
        start != minute_start(start)
        or end != minute_start(end)
        or not start < end
        or end - start > timedelta(days=31)
        or end > minute_start(utc_now())
    ):
        raise ValueError("require aligned completed [start,end) range <=31 days")
    async with store.connection() as c:
        mappings = await c.fetch("""SELECT i.id,i.canonical_symbol,c.provider,c.session_info
            FROM candle_coverage c JOIN instruments i ON i.id=c.instrument_id
            WHERE c.price_basis='native' ORDER BY i.canonical_symbol,c.provider""")
        summaries = []
        for row in mappings:
            bars = await c.fetch(
                """SELECT bucket_start,
                    coalesce((metadata->>'backfilled')::boolean,false) AS backfilled
                FROM market_candles WHERE instrument_id=$1 AND provider=$2 AND interval='1m'
                AND price_basis='native' AND bucket_start >= $3 AND bucket_start < $4""",
                row["id"],
                row["provider"],
                start,
                end,
            )
            corrected = await c.fetchval(
                """SELECT count(DISTINCT bucket_start)
                    FROM candle_revisions
                WHERE instrument_id=$1 AND provider=$2
                    AND bucket_start >= $3 AND bucket_start < $4""",
                row["id"],
                row["provider"],
                start,
                end,
            )
            supplied = {r["bucket_start"] for r in bars}
            info = json.loads(row["session_info"])
            counts = {s.value: 0 for s in Expectation}
            missing = []
            bucket = start
            while bucket < end:
                state = Expectation.EXPECTED if bucket in supplied else expectation(info, bucket)
                counts[state.value] += 1
                if state == Expectation.EXPECTED and bucket not in supplied:
                    missing.append(bucket.isoformat())
                bucket += timedelta(minutes=1)
            summaries.append(
                dict(
                    symbol=row["canonical_symbol"],
                    provider=row["provider"],
                    completed_wall_clock_minutes=int((end - start).total_seconds() // 60),
                    **counts,
                    persisted_native_candles=len(bars),
                    missing_expected_minutes=len(missing),
                    oldest_missing_expected_bucket=missing[0] if missing else None,
                    backfilled_minutes=sum(r["backfilled"] for r in bars),
                    corrected_minutes=corrected,
                    invariant_holds=counts["EXPECTED"] == len(bars) + len(missing),
                    classification_source=(
                        "Latest persisted source session metadata plus observed native bars; "
                        "historical schedule revisions limited to supported syntax"
                    ),
                )
            )
        return summaries


async def run(args: argparse.Namespace) -> None:
    store = Store(Settings())
    await store.connect()
    try:
        result = await reconcile(
            store, datetime.fromisoformat(args.start), datetime.fromisoformat(args.end)
        )
        text = json.dumps(result, indent=2, default=str)
        if args.output:
            args.output.write_text(text)
        else:
            print(text)
    finally:
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path)
    asyncio.run(run(parser.parse_args()))
