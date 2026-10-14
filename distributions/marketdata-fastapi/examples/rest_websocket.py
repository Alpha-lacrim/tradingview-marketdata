"""REST + snapshot/replay example; live updates come from the separate sample writer."""

import argparse
import asyncio
import json
import os

import httpx
from websockets.asyncio.client import connect


async def run(base_url: str, updates: int) -> None:
    base_url = base_url.rstrip("/")
    token = os.environ.get("API_KEY", "sample-development-token-32-characters")
    headers = {"Authorization": "Bearer " + token}
    async with httpx.AsyncClient(base_url=base_url, headers=headers) as client:
        for path in ("/api/v1/instruments", "/api/v1/prices/latest", "/api/v1/coverage"):
            response = (await client.get(path)).raise_for_status()
            print(path, response.json(), flush=True)
        candles = (
            await client.get(
                "/api/v1/candles",
                params={"symbol": "DEMO:BTCUSD", "provider": "sample", "interval": "1m"},
            )
        ).raise_for_status()
        print("Historical 1m candles:", len(candles.json()["items"]), flush=True)
        checkpoint = (
            (await client.get("/api/v1/marketdata/changes")).raise_for_status().json()["cursor"]
        )
        ws_url = base_url.replace("https://", "wss://").replace("http://", "ws://")
        async with connect(
            ws_url + "/api/v1/ws/marketdata", origin=base_url, additional_headers=headers
        ) as ws:
            await ws.send(json.dumps({"action": "subscribe", "symbols": ["DEMO:BTCUSD"]}))
            observed = 0
            async with asyncio.timeout(90):
                while True:
                    event = json.loads(await ws.recv())
                    print("WebSocket:", event, flush=True)
                    if event["type"] == "price_update":
                        observed += 1
                    if (updates == 0 and event["type"] == "price_snapshot") or (
                        updates > 0 and observed >= updates
                    ):
                        break
        replay = (
            (await client.get("/api/v1/marketdata/changes", params={"after": checkpoint}))
            .raise_for_status()
            .json()
        )
        print("Replay:", replay, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8015")
    parser.add_argument("--updates", type=int, default=0, choices=range(31))
    args = parser.parse_args()
    asyncio.run(run(args.base_url, args.updates))
