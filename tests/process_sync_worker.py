"""Test-only process termination at durable delivery boundaries; no provider IO."""

import asyncio
import importlib
import os
import ssl
import sys
from pathlib import Path
from urllib.parse import urlsplit

from marketdata.config import Settings
from marketdata.persistence import Store
from marketdata.sync_config import SyncConfig
from marketdata.sync_worker import SyncWorker


async def main():
    config = SyncConfig.load(Path(sys.argv[1]))
    dsn = config.source_dsn_file.read_text().strip()
    assert urlsplit(dsn).path.endswith("_test")
    if ssl.SSLContext.__module__.endswith("truststore._api"):
        ssl.SSLContext = importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext
    store = Store(
        Settings(
            _env_file=None,
            database_url=dsn,
            application_role="collector",
            source_capture_enabled=True,
        )
    )
    worker = SyncWorker(store, config)

    async def boundary(point):
        if point == sys.argv[2]:
            await asyncio.to_thread(Path(sys.argv[3]).write_text, point)
            if len(sys.argv) > 4 and sys.argv[4] == "hold":
                await asyncio.Event().wait()
            os._exit(81)

    worker.checkpoint = boundary
    try:
        await store.connect()
        await worker.supervise(once=True)
    finally:
        await worker.http.close()
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
