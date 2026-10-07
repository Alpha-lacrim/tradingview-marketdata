"""Opt-in multi-minute LIVE verification: native source -> DB -> REST/WebSocket.

Makes additional guest chart requests for comparison; never alters stored prices.
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
from websockets.asyncio.client import connect

from marketdata.candles import expected_minute, next_collection
from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


async def smoke(base_url: str, minutes: int) -> dict[str, object]:
    settings = Settings()
    registry = Registry.load(Path("config/instruments.tradingview.json"))
    pairs = registry.subscriptions("tradingview_guest")
    source = TradingViewCandleProvider(settings, Metrics())
    await source.subscribe(pairs)
    store = Store(settings)
    await store.connect()
    headers = (
        {"Authorization": "Bearer " + settings.api_key.get_secret_value()}
        if settings.api_key
        else {}
    )
    observations = []
    try:
        async with store.connection() as c:
            before = await c.fetchval(
                "SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest'"
            )
        socket_url = (
            base_url.replace("https://", "wss://").replace("http://", "ws://").rstrip("/")
            + "/ws/prices"
        )
        async with (
            httpx.AsyncClient(base_url=base_url, headers=headers, timeout=15) as client,
            connect(socket_url, additional_headers=headers) as ws,
        ):
            await ws.send(json.dumps(dict(action="subscribe", symbols=list(registry.by_symbol))))
            assert json.loads(await ws.recv())["type"] == "subscribed"
            for _ in range(minutes):
                target = next_collection(utc_now(), settings.candle_fetch_grace_seconds)
                bucket = expected_minute(target, settings.candle_fetch_grace_seconds)
                end = bucket + timedelta(minutes=1)
                seen: set[str] = set()

                def observe(
                    raw: str | bytes, expected: datetime = bucket, observed: set[str] = seen
                ) -> None:
                    message = json.loads(raw)
                    if (
                        message.get("type") == "candle"
                        and datetime.fromisoformat(message["bucket_start"].replace("Z", "+00:00"))
                        == expected
                    ):
                        assert message["persisted"] is True
                        observed.add(message["symbol"])

                # Windows timers can wake slightly early. Recheck the wall-clock deadline
                # and use its intended bucket rather than a possibly premature sample.
                # Drain overlap/startup messages while waiting: a full client receive queue
                # otherwise also delays control frames and can cause a false ping timeout.
                while True:
                    remaining = (target - utc_now()).total_seconds()
                    if remaining <= 0:
                        break
                    try:
                        observe(await asyncio.wait_for(ws.recv(), min(30, max(0.001, remaining))))
                    except TimeoutError:
                        pass
                print(json.dumps(dict(awaiting_bucket=bucket.isoformat())), flush=True)
                try:
                    async with asyncio.timeout(45):
                        while len(seen) < len(pairs):
                            observe(await ws.recv())
                except TimeoutError:
                    raise AssertionError(
                        f"Missing WebSocket candles for {bucket}: "
                        f"{sorted(set(registry.by_symbol) - seen)}"
                    ) from None
                verified = []
                await source.connect()
                try:
                    for pair in pairs:
                        bars = await source.fetch_completed(pair, bucket, end, 3)
                        if not bars:
                            continue
                        candle = bars[-1]
                        response = await client.get(
                            "/candles/" + pair[0].canonical_symbol,
                            params=dict(
                                provider="tradingview_guest",
                                start=bucket.isoformat(),
                                end=end.isoformat(),
                            ),
                        )
                        response.raise_for_status()
                        row = response.json()["items"][0]
                        assert row["complete"] and row["price_basis"] == "native"
                        assert [Decimal(row[k]) for k in ("open", "high", "low", "close")] == [
                            candle.open,
                            candle.high,
                            candle.low,
                            candle.close,
                        ], pair[1].provider_symbol
                        async with store.connection() as c:
                            dbrow = await c.fetchrow(
                                "SELECT open,high,low,close FROM market_candles "
                                "WHERE instrument_id=$1 AND provider=$2 AND bucket_start=$3",
                                pair[0].id,
                                "tradingview_guest",
                                bucket,
                            )
                        assert list(dbrow) == [candle.open, candle.high, candle.low, candle.close]
                        verified.append(pair[1].provider_symbol)
                finally:
                    await source.disconnect()
                assert len(verified) == 17
                latest = (
                    await client.get("/prices/latest", params=dict(provider="tradingview_guest"))
                ).json()["items"]
                assert len(latest) == 17 and all(t["price_basis"] == "candle_close" for t in latest)
                result = dict(
                    bucket=bucket.isoformat(), verified=verified, websocket_instruments=len(seen)
                )
                observations.append(result)
                print(json.dumps(result), flush=True)
        async with store.connection() as c:
            after = await c.fetchval(
                "SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest'"
            )
            counts = await c.fetch(
                "SELECT bucket_start,count(*) AS rows FROM market_candles "
                "WHERE provider='tradingview_guest' GROUP BY bucket_start ORDER BY bucket_start"
            )
        assert before == after
        return dict(
            minutes=observations,
            raw_ticks_before=before,
            raw_ticks_after=after,
            candle_rows_by_minute=[dict(r) for r in counts],
        )
    finally:
        await source.disconnect()
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--minutes", type=int, choices=range(2, 11), default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = asyncio.run(smoke(args.base_url, args.minutes))
    serialized = json.dumps(result, default=str, indent=2)
    print(serialized)
    if args.output:
        args.output.write_text(serialized + "\n", encoding="utf-8")
