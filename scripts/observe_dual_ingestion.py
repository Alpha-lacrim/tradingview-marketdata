"""Read-only dual-ingestion observer; opens no provider connection and performs no SQL writes.

Quote event rates measure persisted delivery, which can coalesce upstream observations.
Journal recorded_at is an append-time proxy, not the timestamp of transaction commit.
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

from marketdata.api_models import HistoricalCandlePage
from marketdata.domain import utc_now
from marketdata.registry import Registry
from marketdata.ws_models import CandleEvent, MarketEvent, PriceEvent


def summarize(
    sources: dict[str, str], events: list[dict[str, Any]], seconds: float
) -> dict[str, Any]:
    result = {}
    for symbol, source in sources.items():
        quotes = [
            e
            for e in events
            if e["type"] == "price_update" and e["data"]["canonical_symbol"] == symbol
        ]
        receipts = [datetime.fromisoformat(e["data"]["received_timestamp"]) for e in quotes]
        result[symbol] = {
            "provider_symbol": source,
            "persisted_quote_updates": len(quotes),
            "persisted_delivery_updates_per_second": len(quotes) / seconds if seconds else 0,
            "receipt_intervals_seconds": [
                (later - earlier).total_seconds()
                for earlier, later in zip(receipts, receipts[1:], strict=False)
            ],
            "intra_minute_receipts": sum(bool(t.second or t.microsecond) for t in receipts),
            "source_to_journal_append_seconds": [
                (
                    datetime.fromisoformat(e["recorded_at"])
                    - datetime.fromisoformat(e["data"]["source_timestamp"])
                ).total_seconds()
                if e["data"]["source_timestamp"]
                else None
                for e in quotes
            ],
            "receipt_to_journal_append_seconds": [
                (
                    datetime.fromisoformat(e["recorded_at"])
                    - datetime.fromisoformat(e["data"]["received_timestamp"])
                ).total_seconds()
                for e in quotes
            ],
            "journal_append_to_client_seconds": [
                (
                    datetime.fromisoformat(e["client_received_at"])
                    - datetime.fromisoformat(e["recorded_at"])
                ).total_seconds()
                for e in quotes
            ],
            "receipt_to_client_seconds": [
                (
                    datetime.fromisoformat(e["client_received_at"])
                    - datetime.fromisoformat(e["data"]["received_timestamp"])
                ).total_seconds()
                for e in quotes
            ],
        }
    return result


async def observe(
    base_url: str, token: str, origin: str, registry: Registry, seconds: int, output: Path
) -> dict[str, Any]:
    sources = {
        i.canonical_symbol: m.provider_symbol
        for i, m in registry.subscriptions("tradingview_guest")
    }
    if not 1 <= len(sources) <= 100:
        raise ValueError("observer requires 1..100 explicitly selected sources")
    report: dict[str, Any] = {
        "passed": False,
        "sources": sources,
        "events": [],
        "measurement_scope": (
            "bounded persisted quote delivery; journal append times are commit-time proxies; "
            "no source access or sustained-capacity claim"
        ),
    }
    headers = {"Authorization": "Bearer " + token}
    url = base_url.rstrip("/").replace("https://", "wss://").replace("http://", "ws://")
    adapter: TypeAdapter[MarketEvent] = TypeAdapter(MarketEvent)
    start = utc_now()
    try:
        async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=10) as client:
            assert (await client.get("/health")).status_code == 200
            assert (
                await client.get("/api/v1/instruments", headers={"Authorization": ""})
            ).status_code == 401
            response = await client.get("/providers")
            response.raise_for_status()
            report["providers_before"] = response.json()
            async with connect(
                url + "/api/v1/ws/marketdata",
                origin=Origin(origin),
                additional_headers=headers,
                max_queue=256,
            ) as ws:
                await ws.send(json.dumps({"action": "subscribe", "symbols": list(sources)}))
                async with asyncio.timeout(15):
                    while True:
                        event = adapter.validate_json(await ws.recv())
                        assert event.type not in {"error", "resync_required"}
                        if event.type == "subscription_ack":
                            assert set(event.symbols) == set(sources)
                            report["snapshot_cursor"] = event.snapshot_cursor
                            break
                start = utc_now()
                deadline = asyncio.get_running_loop().time() + seconds
                seen = set()
                while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), remaining)
                    except TimeoutError:
                        break
                    received = utc_now()
                    event = adapter.validate_json(raw)
                    assert event.type not in {"error", "resync_required"}
                    if not isinstance(event, (PriceEvent, CandleEvent)):
                        continue
                    data = event.data
                    if data.provider != "tradingview_guest":
                        continue
                    assert sources.get(data.canonical_symbol) == data.provider_symbol
                    assert data.realtime_status == "realtime"
                    assert event.persisted and event.event_id not in seen
                    seen.add(event.event_id)
                    if isinstance(event, PriceEvent):
                        assert event.data.price_type == "quote"
                    else:
                        assert (
                            event.data.origin == "native"
                            and event.data.complete
                            and event.data.bucket_end <= received
                        )
                    record = event.model_dump(mode="json")
                    record["client_received_at"] = received.isoformat()
                    if len(report["events"]) >= 20000:
                        raise OverflowError("bounded observation checkpoint exhausted")
                    report["events"].append(record)
            end = utc_now()
            report.update(start=start.isoformat(), end=end.isoformat())
            report["per_instrument"] = summarize(
                sources, report["events"], (end - start).total_seconds()
            )
            response = await client.get("/providers")
            response.raise_for_status()
            report["providers_after"] = response.json()
            for symbol, row in report["per_instrument"].items():
                counts = [
                    next(
                        (
                            p["symbols"][symbol]["observation_count"]
                            for p in report[key]
                            if p.get("storage_mode") == "quotes" and symbol in p["symbols"]
                        ),
                        None,
                    )
                    for key in ("providers_before", "providers_after")
                ]
                before, after = counts
                row["accepted_observation_counter_delta"] = (
                    after - before
                    if isinstance(before, int) and isinstance(after, int) and after >= before
                    else None
                )
                row["counter_scope"] = (
                    "collector health snapshots; independently exclude process resets"
                )
            # Immutable event identity and numeric fields reconcile through cursor replay.
            replay = []
            cursor = report["snapshot_cursor"]
            fence = None
            while True:
                params = {"after": cursor, "limit": 1000}
                if fence:
                    params["until"] = fence
                response = await client.get("/api/v1/marketdata/changes", params=params)
                response.raise_for_status()
                page = response.json()
                replay.extend(page["items"])
                cursor, fence = page["cursor"], page["until"]
                if not page["has_more"]:
                    break
            by_id = {e["event_id"]: e for e in replay}
            for event in report["events"]:
                stored = by_id[event["event_id"]]
                for field in ("provider", "provider_symbol", "instrument_id", "received_timestamp"):
                    assert event["data"][field] == stored["data"][field]
                fields = (
                    ("last", "bid", "ask", "source_timestamp")
                    if event["type"] == "price_update"
                    else ("open", "high", "low", "close", "volume", "bucket_start")
                )
                assert all(event["data"][f] == stored["data"][f] for f in fields)
            # Reconcile latest observed native revisions with bounded REST history.
            latest_bars = {
                (
                    e["data"]["canonical_symbol"],
                    datetime.fromisoformat(e["data"]["bucket_start"]),
                ): e
                for e in report["events"]
                if e["type"] in {"candle_completed", "candle_corrected"}
            }
            for event in latest_bars.values():
                data = event["data"]
                response = await client.get(
                    "/api/v1/candles",
                    params={
                        "symbol": data["canonical_symbol"],
                        "provider": "tradingview_guest",
                        "provider_symbol": data["provider_symbol"],
                        "start": data["bucket_start"],
                        "end": data["bucket_end"],
                    },
                )
                response.raise_for_status()
                assert (
                    CandleEvent.model_validate(
                        {k: v for k, v in event.items() if k != "client_received_at"}
                    ).data
                    in HistoricalCandlePage.model_validate(response.json()).items
                )
            first_full_minute = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
            closed_end = end.replace(second=0, microsecond=0)
            buckets = [
                first_full_minute + timedelta(minutes=n)
                for n in range(max(0, int((closed_end - first_full_minute).total_seconds() // 60)))
            ]
            report["missing_native_minutes"] = [
                {"symbol": symbol, "bucket_start": bucket.isoformat()}
                for symbol in sources
                for bucket in buckets
                if (symbol, bucket) not in latest_bars
            ]
            report["evaluated_native_minutes_per_source"] = len(buckets)
            prices = await client.get(
                "/api/v1/prices/latest",
                params={
                    "symbols": ",".join(sources),
                    "provider": "tradingview_guest",
                    "limit": 1000,
                },
            )
            prices.raise_for_status()
            report["latest_prices"] = prices.json()
            assert buckets, "no full native-minute window evaluated"
            assert not report["missing_native_minutes"], "missing completed native minutes"
            assert all(
                row["persisted_quote_updates"] >= 2 for row in report["per_instrument"].values()
            ), "insufficient quote observations; subscription is not continuous-delivery proof"
            items = report["latest_prices"]["items"]
            assert len(items) == len(sources) and all(p["realtime_quote_available"] for p in items)
            report["passed"] = True
    except Exception as error:
        report["failure_category"] = type(error).__name__
    finally:
        await asyncio.to_thread(output.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(
            output.write_text, json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
    return report


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--api-key-file", required=True, type=Path)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--output", type=Path, default=Path("artifacts/dual-observation.json"))
    args = parser.parse_args()
    parsed = urlsplit(args.base_url)
    if (
        parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or not (
            parsed.scheme == "https"
            or parsed.scheme == "http"
            and parsed.hostname in {"localhost", "127.0.0.1"}
        )
    ):
        parser.error("require HTTPS or loopback HTTP without credentials/query/path")
    if not 120 <= args.seconds <= 1800:
        parser.error("seconds must be 120..1800")
    token = (await asyncio.to_thread(args.api_key_file.read_text, encoding="utf-8")).strip()
    report = await observe(
        args.base_url,
        token,
        args.origin,
        await asyncio.to_thread(Registry.load, args.registry),
        args.seconds,
        args.output,
    )
    print(
        json.dumps(
            {
                "passed": report["passed"],
                "failure_category": report.get("failure_category"),
                "output": str(args.output),
            }
        )
    )
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
