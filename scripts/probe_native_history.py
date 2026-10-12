"""Opt-in, single-request native history capture while the ordinary pool is idle.

No registration, database writes, retry, extra collector or access workaround. Raw response
evidence belongs in ignored artifacts. Request completion does not prove history completeness.
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx

from marketdata.candles import minute_start
from marketdata.config import Settings
from marketdata.domain import utc, utc_now
from marketdata.observability import Metrics
from marketdata.providers.tradingview import FrameDecoder
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


class Capture:
    def __init__(self, socket):
        self.socket = socket
        self.decoder = FrameDecoder()
        self.messages = []
        self.received_units = 0

    async def send(self, raw):
        await self.socket.send(raw)

    async def recv(self):
        raw = await self.socket.recv()
        self.received_units += len(raw)
        if self.received_units > 2_000_000:
            raise ValueError("diagnostic_capture_bound")
        for body in self.decoder.feed(raw):
            if body.startswith("~h~"):
                continue
            message = json.loads(body, parse_float=Decimal)
            if message.get("m") in (
                "symbol_resolved",
                "timescale_update",
                "du",
                "series_completed",
                "series_error",
                "symbol_error",
            ):
                self.messages.append(dict(received_at=utc_now().isoformat(), message=message))
        if len(self.messages) > 100:
            raise ValueError("diagnostic_capture_bound")
        return raw

    async def close(self):
        await self.socket.close()


def idle(retrieval):
    return (
        not any(
            retrieval.get(k) for k in ("sessions_current", "pending_futures", "active_requests")
        )
        and bool(retrieval.get("workers"))
        and all(w["state"] == "disconnected" for w in retrieval["workers"].values())
        and not any(
            w.get("http_status") in (400, 401, 403, 429, 451)
            or w.get("backoff_remaining_seconds", 0) > 0
            for w in retrieval["workers"].values()
        )
    )


async def run(args):
    if not args.live:
        raise ValueError("explicit --live required")
    settings = Settings(
        candle_pool_connections=1,
        tradingview_candle_pool_enabled=False,
        tradingview_candle_concurrency=1,
        candle_request_timeout_seconds=10,
        candle_backfill_minutes=min(Settings().candle_backfill_minutes, 300),
    )
    target = utc(datetime.fromisoformat(args.target))
    if target != minute_start(target):
        raise ValueError("require aligned UTC target bucket")
    pair = next(
        p
        for p in Registry.load(args.registry).subscriptions("tradingview_guest")
        if p[1].provider_symbol == args.source
    )
    headers = (
        {"Authorization": "Bearer " + settings.api_key.get_secret_value()}
        if settings.api_key
        else {}
    )
    now = utc_now()
    start_at = minute_start(now) + timedelta(seconds=25)
    if now > start_at:
        start_at += timedelta(minutes=1)
    print("Awaiting idle diagnostic window " + start_at.isoformat(), flush=True)
    await asyncio.sleep(max(0, (start_at - utc_now()).total_seconds()))
    async with httpx.AsyncClient(headers=headers, timeout=3) as client:
        response = await client.get(args.base_url.rstrip("/") + "/providers")
        response.raise_for_status()
        health = next(p for p in response.json() if p["provider"] == "tradingview_guest")
    retrieval = health["candle_retrieval"]
    if not idle(retrieval) or not 23 <= utc_now().second <= 30:
        raise RuntimeError(
            "ordinary collector busy, cooling down or idle window missed; abort probe"
        )
    end = minute_start(utc_now())
    start = end - timedelta(minutes=settings.candle_backfill_minutes)
    if not start <= target < end:
        raise ValueError("target outside completed bounded history window")
    provider = TradingViewCandleProvider(settings, Metrics())
    await provider.subscribe([pair])
    capture = None
    result = dict(
        started_at=utc_now().isoformat(),
        production_writes=False,
        source=args.source,
        target_bucket=target.isoformat(),
        ordinary_retrieval_before=retrieval,
        requested_start=start.isoformat(),
        requested_end=end.isoformat(),
        requested_bars=settings.candle_backfill_minutes + 1,
    )
    try:
        async with asyncio.timeout(15):
            await provider.connect()
            capture = Capture(provider.socket)
            provider.socket = capture
            candles = await provider.fetch_completed(
                pair, start, end, settings.candle_backfill_minutes + 1
            )
            result["candles"] = [c.model_dump(mode="json") for c in candles]
            result["successful_requests"] = 1
    except Exception as error:
        result["error"] = type(error).__name__
        result["successful_requests"] = 0
    finally:
        await provider.disconnect()
        result.update(
            finished_at=utc_now().isoformat(),
            messages=capture.messages if capture else [],
            timings=provider.timings,
            retrieval=provider.health.candle_retrieval,
        )
        args.output.write_text(json.dumps(result, default=str, indent=2), "utf-8")
    print(
        json.dumps(
            dict(
                successful_requests=result["successful_requests"],
                error=result.get("error"),
                native_candles=len(result.get("candles", [])),
                production_writes=False,
            )
        ),
        flush=True,
    )
    if result["successful_requests"] != 1:
        raise RuntimeError("native history probe unsuccessful; evidence saved, no retry")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--source", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument(
        "--registry", type=Path, default=Path("config/instruments.tradingview-pilot.json")
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(run(parser.parse_args()))
