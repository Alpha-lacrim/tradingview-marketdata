import os
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import pytest

from marketdata.config import Settings
from marketdata.persistence import Store
from marketdata.registry import Registry


@pytest.fixture
def registry():
    return Registry.load(Path("config/instruments.mock.json"))


@pytest.fixture
def pair(registry):
    return registry.subscriptions("mock")[0]


@pytest.fixture
def database_url():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("set TEST_DATABASE_URL to a migrated disposable database ending in _test")
    if not urlsplit(url).path.endswith("_test"):
        pytest.fail("integration database name MUST end in _test; its tables will be cleared")
    return url


@pytest.fixture
async def store(database_url, registry):
    settings = Settings(_env_file=None, database_url=database_url)
    store = Store(settings)
    await store.connect()
    async with store.connection() as c:
        await c.execute(
            "TRUNCATE latest_prices, market_ticks, provider_instruments, instruments CASCADE"
        )
        await c.execute("TRUNCATE collector_status")
        await c.execute("TRUNCATE ingest_producers CASCADE")
    await store.register(registry)
    try:
        yield store
    finally:
        await store.close()


@pytest.fixture
async def readonly_database_url(store):
    role = "batch4_read_" + uuid4().hex
    async with store.connection() as c:
        database = await c.fetchval("SELECT current_database()")
        assert database.endswith("_test") and database.replace("_", "").isalnum()
        await c.execute(f"REVOKE TEMP ON DATABASE {database} FROM PUBLIC")
        await c.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'disposable_test'")
        await c.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        await c.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}")
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    try:
        yield urlunsplit(
            parsed._replace(netloc=f"{role}:disposable_test@{parsed.hostname}:{parsed.port}")
        )
    finally:
        async with store.connection() as c:
            await c.execute(f"DROP OWNED BY {role}")
            await c.execute(f"DROP ROLE {role}")
