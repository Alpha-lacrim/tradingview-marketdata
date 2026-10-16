"""Populated 0007 -> 0008 upgrade and roundtrip without market/receipt loss."""

import pytest
from test_destination_ingestion import destination, record  # noqa: F401
from test_destination_migrations import migrate
from test_market_stream import bar


@pytest.mark.integration
async def test_populated_0007_runtime_upgrade_and_roundtrip(destination, pair):  # noqa: F811
    d = destination
    url = d.store.settings.database_url.get_secret_value()
    try:
        await migrate(url, "downgrade", "0007")
        payload = d.batch([record(bar(pair))])
        receipt = await d.writer.commit(d.producer, payload)
        tables = [
            "market_candles",
            "candle_revisions",
            "candle_coverage",
            "latest_candles",
            "marketdata_events",
            "marketdata_event_clock",
            "ingest_batches",
            "ingest_observations",
            "ingest_producers",
            "ingest_source_owners",
            "ingest_candle_heads",
            "candle_observation_heads",
        ]
        async with d.store.connection() as c:
            before = {table: await c.fetch(f"SELECT * FROM {table}") for table in tables}
        await migrate(url, "upgrade", "head")
        async with d.store.connection() as c:
            assert await c.fetchval("SELECT version_num FROM alembic_version") == "0010"
            assert await c.fetchval("SELECT count(*) FROM ingest_request_budgets") == 0
            assert {table: await c.fetch(f"SELECT * FROM {table}") for table in tables} == before
            assert await c.fetchval(
                "SELECT ingest_admit_request($1,$2,120)", d.producer, str(d.producer)
            )
            columns = await c.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='ingest_read_status'"
            )
            assert not {"credential_key_id", "session_id", "session_generation"} & {
                row["column_name"] for row in columns
            }
        assert await d.writer.commit(d.producer, payload) == receipt
        await migrate(url, "downgrade", "0007")
        await migrate(url, "upgrade", "head")
        assert await d.writer.commit(d.producer, payload) == receipt
        async with d.store.connection() as c:
            assert {table: await c.fetch(f"SELECT * FROM {table}") for table in tables} == before
    finally:
        await migrate(url, "upgrade", "head")
