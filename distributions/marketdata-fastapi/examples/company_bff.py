"""PROVISIONAL adapter for an eventual company session system, not an implemented login.

Authorize must validate a real session and market-data permission, returning its aware expiry
or None. No fake credentials, user records, cookies, login or session schema are supplied.
The market-data service itself depends only on bearer headers and allowed Origins.
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from datetime import datetime
from urllib.parse import urlsplit

import anyio
import httpx
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import Response
from starlette.requests import HTTPConnection
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.typing import Origin

from marketdata.domain import utc, utc_now

Authorize = Callable[[HTTPConnection], Awaitable[datetime | None]]
PATHS = {
    "instruments",
    "prices/latest",
    "candles",
    "candles/latest",
    "coverage",
    "health",
    "marketdata/changes",
}


def install_marketdata_proxy(
    app: FastAPI,
    *,
    authorize: Authorize,
    api_base: str,
    service_token: str,
    origins: set[str],
) -> None:
    """Install into the actual company backend after its auth architecture is decided."""
    parsed = urlsplit(api_base)
    if (
        parsed.scheme not in ("http", "https")
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or (parsed.scheme == "http" and parsed.hostname not in ("localhost", "127.0.0.1"))
    ):
        raise ValueError("HTTPS API base required except on local loopback")
    if not service_token or not origins:
        raise ValueError("server credential and exact browser Origins required")
    base = api_base.rstrip("/")

    async def allowed(connection: HTTPConnection) -> datetime | None:
        expiry = await authorize(connection)
        return utc(expiry) if expiry is not None and utc(expiry) > utc_now() else None

    @app.get("/marketdata/api/v1/{path:path}")
    async def rest(path: str, request: Request) -> Response:
        if path not in PATHS:
            raise HTTPException(404)
        if not await allowed(request):
            raise HTTPException(401, "company session and market-data permission required")
        origin = request.headers.get("origin")
        if origin is not None and origin not in origins:
            raise HTTPException(403, "origin denied")
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                base + "/api/v1/" + path,
                params=list(request.query_params.multi_items()),
                headers={"Authorization": "Bearer " + service_token},
            )
        return Response(
            response.content, status_code=response.status_code, media_type="application/json"
        )

    @app.websocket("/marketdata/ws")
    async def websocket_proxy(browser: WebSocket) -> None:
        origin = browser.headers.get("origin")
        if origin not in origins or not await allowed(browser):
            await browser.close(code=1008, reason="company session or origin denied")
            return
        url = base.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
        code, reason = 1000, ""
        try:
            async with connect(
                url + "/api/v1/ws/marketdata",
                origin=Origin(origin),
                additional_headers={"Authorization": "Bearer " + service_token},
                max_size=65536,
                max_queue=16,
                open_timeout=5,
                close_timeout=2,
            ) as upstream:
                await browser.accept()

                async def inbound() -> None:
                    while True:
                        raw = await browser.receive_text()
                        if len(raw.encode()) > 65536:
                            await browser.close(code=1009)
                            return
                        async with asyncio.timeout(5):
                            await upstream.send(raw)

                async def outbound() -> None:
                    async for raw in upstream:
                        if not isinstance(raw, str):
                            return
                        async with asyncio.timeout(5):
                            await browser.send_text(raw)

                async def session() -> None:
                    nonlocal code, reason
                    while True:
                        expiry = await allowed(browser)
                        if expiry is None:
                            code, reason = 1008, "company session expired or revoked"
                            return
                        await asyncio.sleep(min(30, (expiry - utc_now()).total_seconds()))

                async with anyio.create_task_group() as group:

                    async def run(operation: Callable[[], Awaitable[None]]) -> None:
                        nonlocal code, reason
                        try:
                            await operation()
                        except ConnectionClosed as error:
                            code = error.rcvd.code if error.rcvd else 1013
                            reason = "upstream disconnected; resynchronize via REST"
                        except (WebSocketDisconnect, TimeoutError):
                            code, reason = 1013, "connection interrupted; resynchronize via REST"
                        finally:
                            group.cancel_scope.cancel()

                    for operation in (inbound, outbound, session):
                        group.start_soon(run, operation)
        except InvalidStatus as error:
            code = 1008 if error.response.status_code in (401, 403) else 1013
            reason = (
                "upstream authentication/origin denied" if code == 1008 else "upstream unavailable"
            )
        except (OSError, TimeoutError):
            code, reason = 1013, "market-data service unavailable"
        finally:
            with anyio.move_on_after(2, shield=True):
                if browser.application_state.name == "CONNECTED":
                    with contextlib.suppress(RuntimeError, WebSocketDisconnect, OSError):
                        await browser.close(code=code, reason=reason)
