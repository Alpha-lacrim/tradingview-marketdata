"""Read-only HTTPS/WSS contract smoke. Does not claim source acquisition or data freshness."""

import argparse
import asyncio
import json
import ssl
from pathlib import Path

import httpx
from websockets.asyncio.client import connect


async def smoke(args):
    if not args.endpoint.startswith("https://"):
        raise ValueError("verified HTTPS endpoint required")
    context = ssl.create_default_context(cafile=str(args.ca_file) if args.ca_file else None)
    headers = {"Authorization": "Bearer " + args.token_file.read_text().strip()}
    async with httpx.AsyncClient(
        base_url=args.endpoint, verify=context, trust_env=False, headers=headers, timeout=15
    ) as client:
        for route in ["/health", "/api/v1/instruments", "/api/v1/prices/latest"]:
            (await client.get(route)).raise_for_status()
        result = await client.get(
            "/api/v1/dashboard/candles",
            params=dict(symbol=args.symbol, provider=args.provider, interval="1m"),
        )
        result.raise_for_status()
        arrays = result.json()
        assert set(arrays) == set("abcdefgh") and len({len(v) for v in arrays.values()}) == 1
    url = args.endpoint.replace("https://", "wss://", 1) + "/api/v1/ws/marketdata"
    async with connect(url, ssl=context, origin=args.origin, additional_headers=headers) as ws:
        await ws.send(json.dumps(dict(action="subscribe", symbols=[args.symbol])))
        async with asyncio.timeout(15):
            while json.loads(await ws.recv())["type"] != "subscription_ack":
                pass
    print("Verified REST/ChartData/WSS; inspect field ages and coverage separately.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--endpoint", required=True)
    p.add_argument("--token-file", type=Path, required=True)
    p.add_argument("--ca-file", type=Path)
    p.add_argument("--symbol", required=True)
    p.add_argument("--provider", required=True)
    p.add_argument("--origin", required=True)
    try:
        asyncio.run(smoke(p.parse_args()))
    except Exception as error:
        p.exit(
            1, f"contract smoke failed ({type(error).__name__}); inspect private configuration\n"
        )
