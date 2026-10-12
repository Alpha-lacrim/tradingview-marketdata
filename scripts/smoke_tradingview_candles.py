"""Opt-in multi-minute LIVE verification: native source -> DB -> REST/WebSocket.

Default makes additional guest chart requests for comparison; never alters stored prices.
--stored-only observes the normal collector's committed WebSocket -> DB -> REST path instead.
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


async def smoke(
    base_url: str,
    minutes: int,
    compare_source: bool = True,
    registry_file: Path = Path("config/instruments.tradingview.json"),
    checkpoint: Path | None = None,
) -> dict[str, object]:
    settings = Settings()
    registry = Registry.load(registry_file)
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
                payloads: dict[str, dict] = {}

                def observe(
                    raw: str | bytes,
                    expected: datetime = bucket,
                    observed: set[str] = seen,
                    received_payloads: dict[str, dict] = payloads,
                ) -> None:
                    message = json.loads(raw)
                    if (
                        message.get("type") == "candle"
                        and datetime.fromisoformat(message["bucket_start"].replace("Z", "+00:00"))
                        == expected
                    ):
                        assert message["persisted"] is True
                        observed.add(message["symbol"])
                        received_payloads[message["symbol"]] = message

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
                    if checkpoint:
                        await asyncio.to_thread(
                            checkpoint.write_text,
                            json.dumps(
                                dict(
                                    passed=False,
                                    minutes=observations,
                                    incomplete_bucket=bucket.isoformat(),
                                    missing=sorted(set(registry.by_symbol) - seen),
                                    websocket_instruments=len(seen),
                                    provider_health=(await client.get("/providers")).json(),
                                ),
                                indent=2,
                            )
                            + "\n",
                            encoding="utf-8",
                        )
                    raise AssertionError(
                        f"Missing WebSocket candles for {bucket}: "
                        f"{sorted(set(registry.by_symbol) - seen)}"
                    ) from None
                verified = []
                if compare_source:
                    await source.connect()
                try:
                    for pair in pairs:
                        if compare_source:
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
                        actual = [Decimal(row[k]) for k in ("open", "high", "low", "close")]
                        if not compare_source:
                            delivered = payloads[pair[0].canonical_symbol]
                            assert delivered["provider_symbol"] == pair[1].provider_symbol
                            assert [
                                Decimal(delivered[k]) for k in ("open", "high", "low", "close")
                            ] == actual
                        if compare_source:
                            assert actual == [candle.open, candle.high, candle.low, candle.close], (
                                pair[1].provider_symbol
                            )
                        async with store.connection() as c:
                            dbrow = await c.fetchrow(
                                "SELECT open,high,low,close,provider_symbol FROM market_candles "
                                "WHERE instrument_id=$1 AND provider=$2 AND bucket_start=$3",
                                pair[0].id,
                                "tradingview_guest",
                                bucket,
                            )
                        assert list(dbrow)[:4] == actual
                        assert dbrow["provider_symbol"] == pair[1].provider_symbol
                        verified.append(pair[1].provider_symbol)
                finally:
                    await source.disconnect()
                assert len(verified) == len(pairs)
                latest = (
                    await client.get("/prices/latest", params=dict(provider="tradingview_guest"))
                ).json()["items"]
                latest_by_symbol = {t["canonical_symbol"]: t for t in latest}
                assert all(
                    latest_by_symbol[s]["price_basis"] == "candle_close" for s in registry.by_symbol
                )
                result = dict(
                    bucket=bucket.isoformat(),
                    verified=verified,
                    websocket_instruments=len(seen),
                    provider_health=(await client.get("/providers")).json(),
                )
                observations.append(result)
                if checkpoint:
                    await asyncio.to_thread(
                        checkpoint.write_text,
                        json.dumps(dict(minutes=observations), indent=2) + "\n",
                        encoding="utf-8",
                    )
                print(
                    json.dumps({k: v for k, v in result.items() if k != "provider_health"}),
                    flush=True,
                )
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
            independent_source_comparison=compare_source,
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
    parser.add_argument(
        "--registry", type=Path, default=Path("config/instruments.tradingview.json")
    )
    parser.add_argument(
        "--stored-only",
        action="store_true",
        help="no extra upstream requests; compare normal collector WebSocket, database and REST",
    )
    args = parser.parse_args()
    try:
        result = asyncio.run(
            smoke(args.base_url, args.minutes, not args.stored_only, args.registry, args.output)
        )
    except AssertionError as error:
        if args.output:
            previous = json.loads(args.output.read_text()) if args.output.exists() else {}
            args.output.write_text(
                json.dumps(
                    {
                        **previous,
                        "passed": False,
                        "failure": str(error),
                        "independent_source_comparison": not args.stored_only,
                    },
                    indent=2,
                )
                + "\n",
                encoding="utf-8",
            )
        raise
    serialized = json.dumps(result, default=str, indent=2)
    print(serialized)
    if args.output:
        args.output.write_text(serialized + "\n", encoding="utf-8")
