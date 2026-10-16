"""Test-only TLS process, killed at controlled recovery commit boundaries. No provider I/O."""

import asyncio
import importlib
import json
import socket
import ssl
import sys
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn

import marketdata.ingest_api as api
from marketdata.config import Settings


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    assert urlsplit(config["database_url"]).path.endswith("_test")
    # The injected workstation truststore wrapper is client-only. Preserve verifying SSL.
    if ssl.SSLContext.__module__.endswith("truststore._api"):
        ssl.SSLContext = importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext
    settings = Settings(
        _env_file=None,
        application_role="ingest",
        api_environment="production",
        database_url=config["database_url"],
        ingest_credentials_file=config["credentials"],
        api_key=None,
    )
    original = api.recover_session

    async def paused(store, principal, request):
        if config["mode"] == "before":
            async with store.connection() as c, c.transaction():
                await c.execute("SET LOCAL synchronous_commit=on")
                result = json.loads(
                    await c.fetchval(
                        "SELECT ingest_recover_session($1,$2,$3)",
                        principal.producer_id,
                        principal.key_id,
                        request.model_dump_json(),
                    )
                )
                assert result["state"] == "committed"
                await asyncio.to_thread(Path(config["marker"]).write_text, "pending")
                await asyncio.Event().wait()
        else:
            await original(store, principal, request)
            await asyncio.to_thread(Path(config["marker"]).write_text, "pending")
            await asyncio.Event().wait()

    if config["mode"] != "normal":
        api.recover_session = paused
    app = api.create_app(settings)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        Path(config["port_file"]).write_text(str(listener.getsockname()[1]))
        runner = uvicorn.Server(
            uvicorn.Config(
                app,
                log_level="critical",
                access_log=False,
                ssl_certfile=config["cert"],
                ssl_keyfile=config["key"],
                proxy_headers=False,
            )
        )
        runner.run(sockets=[listener])


if __name__ == "__main__":
    main()
