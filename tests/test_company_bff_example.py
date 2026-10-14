"""PROVISIONAL adapter tests with explicitly simulated sessions, never company login."""

import asyncio
import importlib.util
import socket
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from test_market_stream import ORIGIN, TOKEN, bar, receive, server, subscribe
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

spec = importlib.util.spec_from_file_location(
    "company_bff_example", Path("examples/company_bff.py")
)
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)
install_marketdata_proxy = example.install_marketdata_proxy


@asynccontextmanager
async def bff_server(app):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        runner = uvicorn.Server(uvicorn.Config(app, log_level="error"))
        task = asyncio.create_task(runner.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(5):
                while not runner.started:  # noqa: ASYNC110 - test-owned server readiness
                    await asyncio.sleep(0.01)
            yield f"http://127.0.0.1:{listener.getsockname()[1]}"
        finally:
            runner.should_exit = True
            await asyncio.wait_for(task, 5)


@pytest.mark.integration
async def test_provisional_cookie_adapter_relays_rest_ws_and_revoked_session_closes(store, pair):
    active = True

    async def simulated_session(connection):
        if active and connection.cookies.get("test_session") == "simulated":
            return datetime.now(UTC) + timedelta(seconds=0.1)
        return None

    async with server(store) as (_, base):
        bff = FastAPI()
        install_marketdata_proxy(
            bff, authorize=simulated_session, api_base=base, service_token=TOKEN, origins={ORIGIN}
        )
        async with bff_server(bff) as url:
            headers = {"Cookie": "test_session=simulated"}
            async with httpx.AsyncClient(base_url=url, headers=headers) as client:
                assert (await client.get("/marketdata/api/v1/instruments")).status_code == 200
                assert (await client.get("/marketdata/api/v1/not-allowed")).status_code == 404
            async with connect(
                url.replace("http://", "ws://") + "/marketdata/ws",
                origin=ORIGIN,
                additional_headers=headers,
            ) as ws:
                await subscribe(ws, [pair[0].canonical_symbol])
                await store.write_candles([bar(pair)], [])
                assert (await receive(ws, "candle_completed"))["persisted"]
                active = False
                with pytest.raises(ConnectionClosed) as revoked:
                    while True:
                        await asyncio.wait_for(ws.recv(), 3)
                assert revoked.value.rcvd.code == 1008


@pytest.mark.integration
async def test_provisional_adapter_denies_invalid_origin_or_missing_cookie_before_upstream(store):
    async def simulated_session(connection):
        return (
            datetime.now(UTC) + timedelta(minutes=1)
            if connection.cookies.get("test_session") == "simulated"
            else None
        )

    async with server(store) as (api, base):
        bff = FastAPI()
        install_marketdata_proxy(
            bff, authorize=simulated_session, api_base=base, service_token=TOKEN, origins={ORIGIN}
        )
        async with bff_server(bff) as url:
            async with httpx.AsyncClient(base_url=url) as client:
                assert (
                    await client.get(
                        "/marketdata/api/v1/instruments",
                        headers={"Authorization": "Bearer " + TOKEN},
                    )
                ).status_code == 401
            for origin, headers in [
                (ORIGIN, {}),
                ("https://evil.example.com", {"Cookie": "test_session=simulated"}),
            ]:
                with pytest.raises(InvalidStatus) as denied:
                    async with connect(
                        url.replace("http://", "ws://") + "/marketdata/ws",
                        origin=origin,
                        additional_headers=headers,
                    ):
                        pass
                assert denied.value.response.status_code == 403
            assert not api.state.market_hub.clients
