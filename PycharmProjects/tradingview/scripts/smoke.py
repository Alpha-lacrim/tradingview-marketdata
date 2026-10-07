"""Exercise a running mock deployment, including the actual network WebSocket."""

import argparse
import asyncio
import json

import httpx
from websockets.asyncio.client import connect


async def main(base_url):
    async with httpx.AsyncClient(base_url=base_url, timeout=10) as client:
        health = await client.get("/health")
        health.raise_for_status()
        instruments = (await client.get("/instruments")).json()
        assert len(instruments) == 30
        latest = (await client.get("/prices/latest/CRYPTO:BTCUSDT")).json()
        assert latest and latest[0]["realtime_status"] == "simulated"
        assert latest[0]["stale"] is False and latest[0]["age_ms"] >= 0
        history = (await client.get("/ticks/CRYPTO:BTCUSDT?limit=2")).json()
        assert history["items"]
        candles = await client.get("/candles/CRYPTO:BTCUSDT?provider=mock&interval=1m")
        candles.raise_for_status()
        assert candles.json()["items"]
        socket_url = base_url.replace("https://", "wss://").replace("http://", "ws://")
        async with connect(socket_url.rstrip("/") + "/ws/prices") as socket:
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


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base-url", default="http://localhost:8000")
asyncio.run(main(parser.parse_args().base_url))
