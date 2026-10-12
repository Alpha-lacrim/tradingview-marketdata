"""Opt-in LIVE smoke: requires the running TradingView Compose override and Internet."""

import argparse
import asyncio
import json
from pathlib import Path

import httpx
from websockets.asyncio.client import connect

from marketdata.config import Settings
from marketdata.registry import Registry


async def smoke(base_url: str) -> None:
    settings = Settings()
    headers = (
        {"Authorization": "Bearer " + settings.api_key.get_secret_value()}
        if settings.api_key
        else {}
    )
    registry = Registry.load(Path("config/instruments.tradingview.json"))
    expected = set(registry.by_symbol)
    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=10) as client:
        modes = await client.get("/providers")
        modes.raise_for_status()
        if any(p.get("storage_mode") == "candles" for p in modes.json()):
            raise ValueError("Use scripts/smoke_tradingview_candles.py for candle mode")

        async def get(path: str, **params: str) -> object:
            response = await client.get(path, params=params)
            response.raise_for_status()
            return response.json()

        async with asyncio.timeout(20):
            while True:
                latest = await get("/prices/latest", provider="tradingview_guest")
                items = latest["items"]
                if expected <= {tick["canonical_symbol"] for tick in items} and all(
                    not tick["stale"] for tick in items
                ):
                    break
                await asyncio.sleep(1)
        assert all(tick["realtime_status"] == "realtime" for tick in items)
        assert all(
            tick["metadata"]["upstream"]["update_mode"] in ("streaming", "realtime")
            for tick in items
        )
        socket_url = base_url.replace("https://", "wss://").replace("http://", "ws://")
        async with connect(socket_url.rstrip("/") + "/ws/prices", additional_headers=headers) as ws:
            await ws.send(json.dumps({"action": "subscribe", "symbols": sorted(expected)}))
            observed = set()
            async with asyncio.timeout(25):
                while len(observed) < 3:
                    message = json.loads(await ws.recv())
                    if message.get("type") == "tick":
                        assert message["provider"] == "tradingview_guest"
                        assert message["realtime_status"] == "realtime"
                        assert message["persisted"] is False
                        observed.add(message["symbol"])
        history = await get("/ticks/CRYPTO:BTCUSDT", provider="tradingview_guest", limit="5")
        candles = await get("/candles/CRYPTO:BTCUSDT", provider="tradingview_guest", interval="1s")
        assert history["items"] and candles["items"]
        providers = await get("/providers")
        assert len(providers) == 1 and providers[0]["provider"] == "tradingview_guest"
        assert providers[0]["last_success_at"] is not None
        assert providers[0]["transport"] == "connected"
        assert providers[0]["state"] == "healthy"
        assert providers[0]["configured_symbol_count"] == 17
        assert providers[0]["fresh_symbol_count"] == 17
        assert providers[0]["subscription_restored"] is True
        metrics = await client.get("/metrics")
        metrics.raise_for_status()
        assert "marketdata_persisted_total" in metrics.text
        assert 'marketdata_connection_up{provider="tradingview_guest"} 1.0' in metrics.text
        assert 'marketdata_feed_healthy{provider="tradingview_guest"} 1.0' in metrics.text
        assert 'marketdata_symbols{provider="tradingview_guest",state="fresh"} 17.0' in metrics.text
        print(
            json.dumps(
                {
                    "latest_instruments": len(items),
                    "websocket_symbols_observed": sorted(observed),
                    "historical_ticks": len(history["items"]),
                    "candles": len(candles["items"]),
                    "provider_state": providers[0]["state"],
                    "stale_symbols": providers[0]["stale_symbols"],
                }
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    asyncio.run(smoke(parser.parse_args().base_url))
