"""Observe the ordinary collector through authenticated versioned REST/WS, without writes.

No database credentials or additional TradingView connections are used. Missing native
minutes fail the strict smoke and remain visible in the checkpoint, including closed markets.
"""

import argparse
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
from pydantic import TypeAdapter
from websockets.asyncio.client import connect
from websockets.typing import Origin

from marketdata.api_models import CompletedCandle, HistoricalCandlePage, ServiceHealth
from marketdata.domain import utc_now
from marketdata.registry import Registry
from marketdata.ws_models import CandleEvent, MarketEvent


async def reconcile(client: httpx.AsyncClient, event: CandleEvent) -> None:
    candle = event.data
    response = await client.get(
        "/api/v1/candles",
        params={
            "symbol": candle.canonical_symbol,
            "provider": candle.provider,
            "provider_symbol": candle.provider_symbol,
            "interval": "1m",
            "start": candle.bucket_start.isoformat(),
            "end": candle.bucket_end.isoformat(),
        },
    )
    response.raise_for_status()
    page = HistoricalCandlePage.model_validate(response.json())
    # Corrections can race the read; reconcile against the latest observed revision after
    # collection, and report a mismatch rather than silently accepting different prices.
    assert candle in page.items, "WebSocket candle differs from persisted REST history"


def validate_candle(candle: CompletedCandle, sources: dict[str, str]) -> None:
    assert sources.get(candle.canonical_symbol) == candle.provider_symbol, "source mismatch"
    assert candle.provider == "tradingview_guest", "unexpected provider"
    assert candle.realtime_status == "realtime", "source is not upstream-reported realtime"
    assert candle.complete and candle.origin == "native", "incomplete or derived candle"
    assert candle.bucket_end <= utc_now(), "forming candle"
    assert candle.bucket_end - candle.bucket_start == timedelta(minutes=1), "invalid interval"
    assert candle.bucket_start.second == candle.bucket_start.microsecond == 0


async def smoke(
    base_url: str,
    token: str,
    origin: str,
    registry: Registry,
    minutes: int,
    output: Path,
) -> dict[str, Any]:
    sources = {
        i.canonical_symbol: m.provider_symbol
        for i, m in registry.subscriptions("tradingview_guest")
    }
    assert 0 < len(sources) <= 100, "smoke requires 1..100 explicitly selected sources"
    headers = {"Authorization": "Bearer " + token}
    socket_url = base_url.rstrip("/").replace("https://", "wss://").replace("http://", "ws://")
    report: dict[str, Any] = {"passed": False, "sources": sources, "minutes": minutes, "checks": []}
    events: dict[tuple[str, datetime], CandleEvent] = {}
    try:
        async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=15) as client:
            assert (await client.get("/livez")).status_code == 200, "API not alive"
            assert (await client.get("/health")).status_code == 200, "database not ready"
            assert (
                await client.get("/api/v1/instruments", headers={"Authorization": ""})
            ).status_code == 401, "unauthenticated API admitted"
            report["checks"].append("liveness/readiness/authentication")
            async with connect(
                socket_url + "/api/v1/ws/marketdata",
                origin=Origin(origin),
                additional_headers=headers,
                max_queue=256,
                max_size=1048576,
            ) as ws:
                await ws.send(json.dumps({"action": "subscribe", "symbols": list(sources)}))
                async with asyncio.timeout(15):
                    while True:
                        event: MarketEvent = TypeAdapter(MarketEvent).validate_json(await ws.recv())
                        if event.type == "subscription_ack":
                            assert set(event.symbols) == set(sources)
                            report["snapshot_cursor"] = event.snapshot_cursor
                            break
                        assert event.type not in {"error", "resync_required"}, "WS rejected"
                # Observe future complete minutes only, excluding startup/backfill history.
                start = utc_now().replace(second=0, microsecond=0)
                end = start + timedelta(minutes=minutes)
                deadline = end + timedelta(seconds=55)
                report.update(start=start.isoformat(), end=end.isoformat())
                while (remaining := (deadline - utc_now()).total_seconds()) > 0:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), remaining)
                    except TimeoutError:
                        break
                    event = TypeAdapter(MarketEvent).validate_json(raw)
                    assert event.type not in {"error", "resync_required"}, "WS requires resync"
                    if isinstance(event, CandleEvent) and start <= event.data.bucket_start < end:
                        validate_candle(event.data, sources)
                        events[event.data.canonical_symbol, event.data.bucket_start] = event
                report["checks"].append("authenticated versioned WebSocket")
            missing = [
                {"symbol": symbol, "bucket_start": bucket.isoformat()}
                for symbol in sources
                for index in range(minutes)
                if (symbol, bucket := start + timedelta(minutes=index)) not in events
            ]
            report["missing"] = missing
            report["events"] = [event.model_dump(mode="json") for event in events.values()]
            for event in events.values():
                await reconcile(client, event)
            report["checks"].append("exact Decimal/native identity REST reconciliation")
            params: dict[str, str | int] = {"provider": "tradingview_guest", "limit": 1000}
            for route in ("/prices/latest", "/candles/latest", "/coverage"):
                query = dict(params)
                if route == "/coverage":
                    query.update(start=start.isoformat(), end=end.isoformat())
                response = await client.get("/api/v1" + route, params=query)
                response.raise_for_status()
                payload = response.json()
                assert set(sources) <= {r["canonical_symbol"] for r in payload["items"]}
                report[route] = payload
            response = await client.get("/api/v1/health")
            response.raise_for_status()
            health = ServiceHealth.model_validate(response.json())
            report["health"] = health.model_dump(mode="json")
            assert health.collector == "running", "collector not reporting ownership"
            assert health.database == "available"
            report["checks"].append("latest prices/candles/coverage/collector health")
            assert not missing, "native minutes missing; see checkpoint"
            report["passed"] = True
    except Exception as error:
        report["failure_category"] = type(error).__name__
        raise
    finally:
        # No credentials, URLs or arbitrary upstream exception text in checkpoints.
        report["observed_candles"] = len(events)
        report["events"] = [event.model_dump(mode="json") for event in events.values()]
        output.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.to_thread(
            output.write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--api-key-file", required=True, type=Path)
    parser.add_argument("--origin", required=True)
    parser.add_argument(
        "--registry", type=Path, default=Path("config/instruments.tradingview-100.json")
    )
    parser.add_argument("--minutes", type=int, choices=range(2, 11), default=3)
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/production-tradingview-smoke.json")
    )
    args = parser.parse_args()
    parsed = urlsplit(args.base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or (parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"})
    ):
        parser.error("use an HTTPS API origin or a loopback HTTP origin without credentials")
    try:
        token = args.api_key_file.read_text(encoding="utf-8").strip()
        if len(token) < 32:
            raise ValueError("invalid API token")
        result = asyncio.run(
            smoke(
                args.base_url.rstrip("/"),
                token,
                args.origin,
                Registry.load(args.registry),
                args.minutes,
                args.output,
            )
        )
    except Exception as error:
        print(f"Production smoke failed ({type(error).__name__}); inspect checkpoint if present")
        raise SystemExit(1) from None
    print(json.dumps({"passed": result["passed"], "observed_candles": result["observed_candles"]}))


if __name__ == "__main__":
    main()
