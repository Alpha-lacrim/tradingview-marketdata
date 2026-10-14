"""Subprocess test driver: local mock observations only, never live providers."""

import argparse
import asyncio
from pathlib import Path

import uvicorn

from marketdata.api import create_app
from marketdata.collector import run_collector
from marketdata.config import Settings
from marketdata.persistence import Store
from marketdata.providers.base import MarketDataProvider


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["api", "collector"])
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--stop-file", type=Path, required=True)
    args = parser.parse_args()
    settings = Settings(
        _env_file=None,
        application_role=args.role,
        registry_file=Path("config/instruments.mock.json"),
        market_data_provider_crypto="mock",
        market_data_provider_forex="mock",
        market_data_provider_metals="mock",
        market_data_provider_energy="mock",
        market_data_provider_futures="mock",
        collector_metrics_port=0,
        collector_status_interval_seconds=0.1,
        collector_status_ttl_seconds=2,
        batch_flush_seconds=0.01,
        log_level="ERROR",
    )
    stop = asyncio.Event()

    async def watch():
        while not args.stop_file.exists():  # noqa: ASYNC110 - polls external state
            await asyncio.sleep(0.05)
        stop.set()

    watcher = asyncio.create_task(watch())
    try:
        if args.role == "collector":
            await run_collector(settings, stop)
        else:

            def forbidden(*args, **kwargs):
                raise AssertionError("API worker attempted upstream construction or registration")

            MarketDataProvider.__init__ = forbidden
            Store.register = forbidden
            server = uvicorn.Server(
                uvicorn.Config(
                    create_app(settings), host="127.0.0.1", port=args.port, log_level="error"
                )
            )

            async def shutdown():
                await stop.wait()
                server.should_exit = True

            stopper = asyncio.create_task(shutdown())
            try:
                await server.serve()
            finally:
                stopper.cancel()
                await asyncio.gather(stopper, return_exceptions=True)
    finally:
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
