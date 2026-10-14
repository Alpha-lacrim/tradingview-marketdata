"""Single listener with two real API workers; publisher never opens an upstream socket."""

import asyncio
import json
import os
import socket
import subprocess
import sys
from contextlib import AsyncExitStack

import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, TOKEN, bar, receive, subscribe, ws_url
from websockets.asyncio.client import connect


@pytest.mark.integration
async def test_real_uvicorn_two_workers_deliver_same_commits_and_reconcile(
    store, pair, tmp_path, readonly_database_url
):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    stop = tmp_path / "workers.stop"
    log_path = tmp_path / "workers.log"
    env = dict(
        os.environ,
        DATABASE_URL=readonly_database_url,
        API_KEY=TOKEN,
        API_ENVIRONMENT="production",
        APPLICATION_ROLE="api",
        WEBSOCKET_ALLOWED_ORIGINS=json.dumps([ORIGIN]),
        MARKETDATA_POLL_SECONDS="0.02",
        WEBSOCKET_HEARTBEAT_SECONDS="0.1",
    )
    with log_path.open("w") as log:
        process = await asyncio.to_thread(
            subprocess.Popen,
            [
                sys.executable,
                "tests/process_market_workers.py",
                "--port",
                str(port),
                "--stop-file",
                str(stop),
            ],
            env=env,
            stdout=log,
            stderr=log,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        )
    url = f"http://127.0.0.1:{port}"
    try:
        async with httpx.AsyncClient(base_url=url, headers=HEADERS, timeout=5) as client:
            async with asyncio.timeout(20):
                while True:  # noqa: ASYNC110 - test-owned process readiness
                    assert process.poll() is None, log_path.read_text()
                    try:
                        if (await client.get("/health")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.05)
            async with AsyncExitStack() as stack:
                async with asyncio.timeout(10):
                    while len(list(tmp_path.glob("worker-*.ready"))) < 2:  # noqa: ASYNC110 - worker readiness
                        await asyncio.sleep(0.05)
                workers = {}
                for _ in range(2):
                    ws = await stack.enter_async_context(
                        connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS)
                    )
                    pid = ws.response.headers["x-test-worker"]
                    await subscribe(ws, [pair[0].canonical_symbol])
                    workers.setdefault(pid, ws)
                    if len(workers) == 1:
                        # OS accept scheduling does not promise round robin. Keep the
                        # first connection alive while deterministically admitting the
                        # next connection through the other worker's listening handle.
                        (tmp_path / f"worker-{pid}.pause-accept").touch()
                        async with asyncio.timeout(5):
                            while not (tmp_path / f"worker-{pid}.accept-paused").exists():  # noqa: ASYNC110
                                await asyncio.sleep(0.05)
                assert len(workers) == 2, "both Uvicorn worker PIDs must own a subscription"
                assert str(process.pid) not in workers
                baseline = (await client.get("/api/v1/marketdata/changes")).json()["cursor"]
                await store.write_candles([bar(pair)], [])
                events = await asyncio.gather(
                    *(receive(ws, "candle_completed") for ws in workers.values())
                )
                assert events[0]["event_id"] == events[1]["event_id"]
                replay = (
                    await client.get("/api/v1/marketdata/changes", params={"after": baseline})
                ).json()
                assert replay["items"][0]["data"] == events[0]["data"]
                assert (await client.get("/api/v1/health")).json()["collector"] == "unknown"
            async with store.connection() as c:
                assert await c.fetchval("SELECT count(*) FROM collector_status") == 0
    finally:
        stop.touch()
        try:
            await asyncio.to_thread(process.wait, 45)
        except subprocess.TimeoutExpired:
            process.kill()
            await asyncio.to_thread(process.wait, 5)
            pytest.fail("test supervisor shutdown timed out: " + log_path.read_text())
        assert process.returncode == 0, log_path.read_text()
