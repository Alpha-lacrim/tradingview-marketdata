"""Exercise a running mock deployment, including the actual network WebSocket."""

import asyncio
import json

import httpx
from websockets.asyncio.client import connect


async def main():
    async with httpx.AsyncClient(base_url="http://localhost:8000", timeout=10) as client:
        health = await client.get("/health")
        health.raise_for_status()
        instruments = (await client.get("/instruments")).json()
        assert len(instruments) == 30
        latest = (await client.get("/prices/latest/CRYPTO:BTCUSDT")).json()
        assert latest and latest[0]["realtime_status"] == "simulated"
        history = (await client.get("/ticks/CRYPTO:BTCUSDT?limit=2")).json()
        assert history["items"]
        candles = await client.get("/candles/CRYPTO:BTCUSDT?provider=mock&interval=1m")
        candles.raise_for_status()
        assert candles.json()["items"]
        async with connect("ws://localhost:8000/ws/prices") as socket:
            await socket.send(json.dumps({"action": "subscribe", "symbols": ["CRYPTO:BTCUSDT"]}))
            assert json.loads(await asyncio.wait_for(socket.recv(), 5))["type"] == "subscribed"
            tick = json.loads(await asyncio.wait_for(socket.recv(), 5))
            assert tick["type"] == "tick" and tick["persisted"] is False
        metrics = await client.get("/metrics")
        assert "marketdata_persisted_total" in metrics.text
        print(
            json.dumps(
                {
                    "health": health.json(),
                    "instruments": len(instruments),
                    "history_rows": len(history["items"]),
                    "candles": len(candles.json()["items"]),
                    "websocket": "passed",
                },
                indent=2,
            )
        )


asyncio.run(main())
