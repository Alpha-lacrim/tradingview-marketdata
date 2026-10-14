"""Actual Linux image smoke with mounted secrets, two API workers and no collector."""

import asyncio
import json
import os
import socket
import subprocess
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import httpx
import pytest
from test_market_stream import ORIGIN, TOKEN, bar, receive, subscribe, ws_url
from websockets.asyncio.client import connect


@pytest.mark.integration
async def test_linux_production_entrypoint_with_two_workers_and_readonly_files(
    store, pair, readonly_database_url, tmp_path
):
    image = os.environ.get("BATCH4_TEST_IMAGE")
    container = os.environ.get("BATCH4_TEST_CONTAINER")
    if not image or not container:
        pytest.skip("requires built Batch 4 image and disposable database container")
    assert image == "marketdata-batch4:qualification"
    assert container in (
        "marketdata-batch4-tests-postgres-1",
        "marketdata-batch4-tests-timescale-1",
    )
    parsed = urlsplit(readonly_database_url)
    assert parsed.hostname == "127.0.0.1" and parsed.port in (55891, 55892)
    assert parsed.path == "/marketdata_batch4_test"
    host = "postgres" if parsed.port == 55891 else "timescale"
    dsn = tmp_path / "database_url"
    token = tmp_path / "api_key"
    dsn.write_text(
        urlunsplit(parsed._replace(netloc=f"{parsed.username}:{parsed.password}@{host}:5432"))
    )
    token.write_text(TOKEN)
    name = "marketdata-batch4-api-" + uuid4().hex
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]

    async def docker(*args):
        result = await asyncio.to_thread(
            subprocess.run, ["docker", *args], capture_output=True, text=True, timeout=30
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    assert (
        await docker(
            "inspect",
            "--format",
            '{{index .Config.Labels "com.docker.compose.project"}}',
            container,
        )
    ).strip() == "marketdata-batch4-tests"
    created = False
    try:
        await docker(
            "run",
            "-d",
            "--name",
            name,
            "--label",
            "qualification=batch4",
            "--network",
            "marketdata-batch4-tests_default",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--publish",
            f"127.0.0.1:{port}:8000",
            "--mount",
            f"type=bind,source={dsn},target=/run/secrets/database_url,readonly",
            "--mount",
            f"type=bind,source={token},target=/run/secrets/api_key,readonly",
            "--env",
            "WEBSOCKET_ALLOWED_ORIGINS=" + json.dumps([ORIGIN]),
            image,
            ".venv/bin/python",
            "-m",
            "marketdata.production",
            "api",
        )
        created = True
        base = f"http://127.0.0.1:{port}"
        headers = {"Authorization": "Bearer " + TOKEN}
        async with httpx.AsyncClient(base_url=base, headers=headers, timeout=3) as client:

            async def ready():
                async with asyncio.timeout(25):
                    while True:  # noqa: ASYNC110
                        try:
                            if (await client.get("/health")).status_code == 200:
                                return
                        except httpx.HTTPError:
                            pass
                        await asyncio.sleep(0.1)

            await ready()
            assert (await client.get("/livez")).json() == {"api": "alive"}
            assert (
                await client.get("/api/v1/instruments", headers={"Authorization": ""})
            ).status_code == 401
            assert (await client.get("/api/v1/health")).json()["collector"] == "unknown"
            async with connect(ws_url(base), origin=ORIGIN, additional_headers=headers) as ws:
                await subscribe(ws, [pair[0].canonical_symbol])
                await store.write_candles([bar(pair)], [])
                assert (await receive(ws, "candle_completed"))["persisted"]
            before = (await client.get("/api/v1/candles/latest")).json()["items"]
            await docker("restart", name)
            await ready()
            assert (await client.get("/api/v1/candles/latest")).json()["items"] == before
            inspect = json.loads(await docker("inspect", name))[0]
            assert inspect["HostConfig"]["ReadonlyRootfs"]
            assert inspect["Config"]["User"] == "appuser"
            assert TOKEN not in json.dumps(inspect)
            assert parsed.password not in json.dumps(inspect)
            assert TOKEN not in await docker("logs", name)
    finally:
        if created:
            assert name.startswith("marketdata-batch4-api-")
            assert (
                await docker(
                    "inspect", "--format", '{{index .Config.Labels "qualification"}}', name
                )
            ).strip() == "batch4"
            await docker("rm", "-f", name)
