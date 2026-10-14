"""Actual pg_dump/pg_restore on an allow-listed disposable Batch 4 database."""

import asyncio
import os
import subprocess
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
from test_market_stream import bar


@pytest.mark.integration
async def test_backup_restore_preserves_history_and_invalidates_old_replay_cursor(store, pair):
    container = os.environ.get("BATCH4_TEST_CONTAINER")
    if not container:
        pytest.skip("requires the task-owned Batch 4 container")
    assert container in (
        "marketdata-batch4-tests-postgres-1",
        "marketdata-batch4-tests-timescale-1",
    )
    url = urlsplit(store.settings.database_url.get_secret_value())
    assert url.hostname == "127.0.0.1" and url.port in (55891, 55892)
    assert url.path == "/marketdata_batch4_test"
    await store.write_candles([bar(pair)], [])
    async with store.connection() as c:
        expected = await c.fetchval("SELECT payload FROM market_candles LIMIT 1")
        epoch = await c.fetchval("SELECT epoch FROM marketdata_event_clock")
    name = "restore_" + uuid4().hex + "_test"

    async def docker(*args):
        result = await asyncio.to_thread(
            subprocess.run, ["docker", *args], capture_output=True, text=True, timeout=45
        )
        assert result.returncode == 0, result.stderr
        return result.stdout

    project = await docker(
        "inspect", "--format", '{{index .Config.Labels "com.docker.compose.project"}}', container
    )
    assert project.strip() == "marketdata-batch4-tests"
    created = False
    try:
        await docker(
            "exec",
            container,
            "pg_dump",
            "-U",
            "batch4_test",
            "-d",
            "marketdata_batch4_test",
            "-Fc",
            "-f",
            "/tmp/batch4-restore.dump",
        )
        await docker("exec", container, "createdb", "-U", "batch4_test", name)
        created = True
        if store.has_aggregates:
            await docker(
                "exec",
                container,
                "psql",
                "-U",
                "batch4_test",
                "-d",
                name,
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();",
            )
        await docker(
            "exec",
            container,
            "pg_restore",
            "-U",
            "batch4_test",
            "-d",
            name,
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
            "/tmp/batch4-restore.dump",
        )
        if store.has_aggregates:
            await docker(
                "exec",
                container,
                "psql",
                "-U",
                "batch4_test",
                "-d",
                name,
                "-v",
                "ON_ERROR_STOP=1",
                "-c",
                "SELECT timescaledb_post_restore();",
            )
        # Restore into another database; the source tables/volume are never replaced.
        connection = await asyncpg.connect(
            store.settings.database_url.get_secret_value(), database=name
        )
        try:
            assert (
                await connection.fetchval("SELECT payload FROM market_candles LIMIT 1") == expected
            )
            assert await connection.fetchval("SELECT epoch FROM marketdata_event_clock") == epoch
            replacement = uuid4()
            await connection.execute("UPDATE marketdata_event_clock SET epoch=$1", replacement)
            assert await connection.fetchval("SELECT epoch FROM marketdata_event_clock") != epoch
        finally:
            await connection.close()
    finally:
        if created:
            # This unique test database is the only removal target, never marketdata.
            assert name.startswith("restore_") and name.endswith("_test")
            await docker("exec", container, "dropdb", "-U", "batch4_test", name)
