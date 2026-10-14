"""Opt-in actual outage of a verified disposable Batch 3 container; no production target."""

import asyncio
import os
import subprocess
from urllib.parse import urlsplit

import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, bar, receive, server, subscribe, ws_url
from websockets.asyncio.client import connect


@pytest.mark.integration
async def test_actual_postgresql_outage_then_journal_and_rest_recover(store, pair):
    container = os.environ.get("BATCH4_TEST_CONTAINER") or os.environ.get("BATCH3_TEST_CONTAINER")
    if container is None:
        pytest.skip("set BATCH3_TEST_CONTAINER to a task-owned disposable Batch 3 container")
    batch = "batch4" if os.environ.get("BATCH4_TEST_CONTAINER") else "batch3"
    project_name = f"marketdata-{batch}-tests"
    assert container in (f"{project_name}-postgres-1", f"{project_name}-timescale-1")
    url = urlsplit(store.settings.database_url.get_secret_value())
    assert url.hostname == "127.0.0.1"
    assert url.port in ((55891, 55892) if batch == "batch4" else (55791, 55792))
    assert url.path == f"/marketdata_{batch}_test"

    async def docker(*arguments):
        result = await asyncio.to_thread(
            subprocess.run, ["docker", *arguments], capture_output=True, text=True, timeout=10
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    project = await docker(
        "inspect", "--format", '{{index .Config.Labels "com.docker.compose.project"}}', container
    )
    assert project.strip() == project_name
    async with (
        server(store, db_timeout_seconds=0.2) as (_, base),
        connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        await subscribe(ws, [pair[0].canonical_symbol])
        paused = False
        try:
            await docker("pause", container)
            paused = True
            async with asyncio.timeout(5):
                while (await receive(ws, "heartbeat"))["database"] != "unavailable":
                    pass
            async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
                response = await client.get("/api/v1/candles/latest")
                assert response.status_code == 503
                assert response.json()["error"]["code"] == "database_unavailable"
        finally:
            if paused:
                await docker("unpause", container)
        await store.write_candles([bar(pair)], [])
        assert (await receive(ws, "candle_completed"))["persisted"]
        async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
            assert (await client.get("/api/v1/candles/latest")).status_code == 200
