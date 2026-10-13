import asyncio
import os
import socket
import subprocess
import sys

import httpx
import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedError

pytestmark = pytest.mark.integration


async def test_real_processes_restart_independently_with_two_api_workers(store, tmp_path):
    owned = []

    def start(role):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        stop_file = tmp_path / f"{role}-{len(owned)}.stop"
        log_path = tmp_path / f"{role}-{len(owned)}.log"
        env = dict(os.environ, DATABASE_URL=store.settings.database_url.get_secret_value())
        with log_path.open("w") as log:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "tests/process_isolation_worker.py",
                    role,
                    "--port",
                    str(port),
                    "--stop-file",
                    str(stop_file),
                ],
                env=env,
                stdout=log,
                stderr=log,
            )
        owned.append((process, stop_file, log_path))
        return process, stop_file, log_path, f"http://127.0.0.1:{port}"

    async def stop(worker):
        process, stop_file, log_path, *_ = worker
        stop_file.touch()
        await asyncio.to_thread(process.wait, 10)
        assert process.returncode == 0, log_path.read_text()

    async def ready(worker):
        process, _, log_path, url = worker
        async with asyncio.timeout(15), httpx.AsyncClient(timeout=1) as client:
            while True:  # noqa: ASYNC110 - polls external state
                assert process.poll() is None, log_path.read_text()
                try:
                    if (await client.get(url + "/health")).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.05)

    async def history_count():
        async with store.connection() as c:
            return await c.fetchval("SELECT count(*) FROM market_ticks")

    try:
        async with store.connection() as c:
            before = await c.fetch("SELECT id,updated_at FROM instruments ORDER BY id")
        apis = [start("api"), start("api")]
        await asyncio.gather(*(ready(api) for api in apis))
        async with connect(apis[0][3].replace("http://", "ws://") + "/ws/prices") as websocket:
            with pytest.raises(ConnectionClosedError) as closure:
                await websocket.recv()
            assert closure.value.rcvd.code == 1013
            assert closure.value.rcvd.reason == (
                "cross-process streaming unavailable; query REST history"
            )
        async with store.connection() as c:
            assert await c.fetch("SELECT id,updated_at FROM instruments ORDER BY id") == before
            assert await c.fetchval("SELECT count(*) FROM collector_status") == 0
        collector = start("collector")
        async with asyncio.timeout(10):
            while await history_count() < 10:  # noqa: ASYNC110 - polls external state
                assert collector[0].poll() is None, collector[2].read_text()
                await asyncio.sleep(0.05)
        async with httpx.AsyncClient(timeout=2) as client:
            for api in apis:
                assert (await client.get(api[3] + "/prices/latest")).json()["items"]
                assert (await client.get(api[3] + "/providers")).json()
            await stop(collector)
            durable_count = await history_count()
            for api in apis:
                health = await client.get(api[3] + "/health")
                assert health.status_code == 200
                assert health.json()["ingestion_state"] == "stopped"
                assert (await client.get(api[3] + "/ticks/CRYPTO:BTCUSDT")).json()["items"]
            collector = start("collector")
            async with asyncio.timeout(10):
                while await history_count() <= durable_count:  # noqa: ASYNC110 - polls external state
                    await asyncio.sleep(0.05)
            collector_pid = collector[0].pid
            await stop(apis[0])
            before_restart = await history_count()
            apis[0] = start("api")
            await ready(apis[0])
            assert collector[0].pid == collector_pid and collector[0].poll() is None
            async with asyncio.timeout(5):
                while await history_count() <= before_restart:  # noqa: ASYNC110 - polls external state
                    await asyncio.sleep(0.05)
            assert (await client.get(apis[1][3] + "/health")).status_code == 200
            assert (await client.get(apis[0][3] + "/prices/latest")).json()["items"]
    finally:
        for process, stop_file, log_path in owned:
            if process.poll() is None:
                stop_file.touch()
                try:
                    await asyncio.to_thread(process.wait, 10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    await asyncio.to_thread(process.wait, 5)
                    pytest.fail(
                        f"owned test process failed graceful shutdown: {log_path.read_text()}"
                    )
