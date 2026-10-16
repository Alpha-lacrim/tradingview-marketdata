import asyncio
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect
from test_native_candles import REGISTRY, Replay, make_candle

from marketdata.api import create_app, create_combined_app
from marketdata.collector import collector_lifespan, run_collector
from marketdata.config import Settings
from marketdata.domain import normalize
from marketdata.ingestion import IngestionService
from marketdata.persistence import Store
from marketdata.providers.base import ProviderHealth
from marketdata.registry import Registry


def forbidden(*args, **kwargs):
    raise AssertionError("API invoked collector responsibilities")


async def test_two_api_lifespans_never_construct_providers_or_register(monkeypatch):
    stores = []

    class ReadStore:
        def __init__(self, settings, *, read_only):
            assert read_only
            self.connected = self.closed = False
            stores.append(self)

        async def connect(self):
            self.connected = True

        async def close(self):
            self.closed = True

    monkeypatch.setattr("marketdata.api.Store", ReadStore)
    monkeypatch.setattr("marketdata.collector.build_providers", forbidden)
    monkeypatch.setattr(IngestionService, "start", forbidden)
    monkeypatch.setattr(Store, "register", forbidden)
    monkeypatch.setattr(Registry, "load", forbidden)
    apps = [create_app(Settings(_env_file=None)) for _ in range(2)]
    async with apps[0].router.lifespan_context(apps[0]), apps[1].router.lifespan_context(apps[1]):
        assert all(s.connected for s in stores)
        assert all(app.state.service is None for app in apps)
    assert all(s.closed for s in stores)


def test_factories_require_explicit_roles():
    with pytest.raises(ValueError, match="APPLICATION_ROLE=api"):
        create_app(Settings(_env_file=None, application_role="combined"))
    with pytest.raises(ValueError, match="APPLICATION_ROLE=combined"):
        create_combined_app(Settings(_env_file=None))


def test_api_websocket_accepts_then_closes_with_1013_without_database_startup():
    app = create_app(Settings(_env_file=None))
    # No TestClient lifespan: this endpoint needs no database or collector resources.
    with TestClient(app).websocket_connect("/ws/prices") as socket:
        with pytest.raises(WebSocketDisconnect) as error:
            socket.receive_json()
    assert error.value.code == 1013
    assert error.value.reason == "cross-process streaming unavailable; query REST history"


@pytest.mark.integration
async def test_api_reads_persisted_identity_history_without_source_access(store, pair, monkeypatch):
    now = datetime(2026, 10, 7, tzinfo=UTC)
    tick = normalize(*pair, now, event_timestamp=now, last="123.1234567890123456789")
    await store.write([tick])
    monkeypatch.setattr("marketdata.collector.build_providers", forbidden)
    monkeypatch.setattr(Store, "register", forbidden)
    async with store.connection() as c:
        before = await c.fetch("SELECT id,updated_at FROM instruments ORDER BY id")
        mappings = await c.fetch(
            "SELECT * FROM provider_instruments ORDER BY instrument_id,provider"
        )
    settings = Settings(
        _env_file=None,
        database_url=store.settings.database_url,
        registry_file=Path("does-not-exist.json"),
        market_data_provider_crypto="tradingview_guest",
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with app.state.store.connection() as c:
            assert await c.fetchval("SHOW transaction_read_only") == "on"
            with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
                await c.execute("UPDATE instruments SET active=active")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            response = await client.get(
                f"/ticks/{pair[0].canonical_symbol}",
                params={"start": now.isoformat(), "end": (now + timedelta(hours=1)).isoformat()},
            )
            assert response.status_code == 200
            assert response.json()["items"][0] == tick.model_dump(mode="json")
            assert (await client.get("/health")).status_code == 200
            assert (await client.get("/health")).json()["ingestion_state"] == "unknown"
            assert (await client.get("/instruments")).json()
            assert (
                await client.get("/futures/SIM/CL/front?at=2026-10-07T00:00:00Z")
            ).status_code == 200
    assert app.state.store.pool is None
    async with store.connection() as c:
        assert await c.fetch("SELECT id,updated_at FROM instruments ORDER BY id") == before
        assert (
            await c.fetch("SELECT * FROM provider_instruments ORDER BY instrument_id,provider")
            == mappings
        )


@pytest.mark.integration
async def test_native_metadata_coverage_and_restart_preserved(store, monkeypatch):
    registry = Registry.load(REGISTRY)
    await store.register(registry, candle_providers=["tradingview_guest"])
    candle = make_candle(registry.subscriptions("tradingview_guest")[0])
    await store.write_candles([candle], [])
    async with store.connection() as c:
        coverage = await c.fetch("SELECT * FROM candle_coverage ORDER BY instrument_id,provider")
    monkeypatch.setattr("marketdata.collector.build_providers", forbidden)
    settings = Settings(
        _env_file=None,
        database_url=store.settings.database_url,
        registry_file=Path("absent.json"),
        tradingview_storage_mode="ticks",
    )
    for _ in range(2):
        app = create_app(settings)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://test"
            ) as client:
                response = await client.get(
                    f"/candles/{candle.canonical_symbol}",
                    params={
                        "provider": candle.provider,
                        "start": candle.bucket_start.isoformat(),
                        "end": candle.bucket_end.isoformat(),
                    },
                )
                assert response.status_code == 200, response.text
                assert response.json()["basis"] == "native"
                assert response.json()["items"][0]["close"] == str(candle.close)
                latest = (await client.get(f"/prices/latest/{candle.canonical_symbol}")).json()[0]
                assert latest["provider_symbol"] == candle.provider_symbol
                assert latest["price_basis"] == "candle_close"
    async with store.connection() as c:
        assert (
            await c.fetch("SELECT * FROM candle_coverage ORDER BY instrument_id,provider")
            == coverage
        )


@pytest.mark.integration
async def test_collector_alone_restarts_without_resetting_coverage(store, monkeypatch):
    async def build(settings, registry, metrics):
        provider = Replay()
        await provider.subscribe(registry.subscriptions("tradingview_guest")[:1])
        return [provider]

    monkeypatch.setattr("marketdata.collector.build_providers", build)
    monkeypatch.setattr(
        "marketdata.candle_ingestion.utc_now",
        lambda: (
            make_candle(Registry.load(REGISTRY).subscriptions("tradingview_guest")[0]).bucket_end
            + timedelta(seconds=2)
        ),
    )
    settings = Settings(
        _env_file=None,
        application_role="collector",
        database_url=store.settings.database_url,
        registry_file=REGISTRY,
        collector_metrics_port=0,
        market_data_provider_crypto="tradingview_guest",
        market_data_provider_forex="tradingview_guest",
        market_data_provider_metals="tradingview_guest",
        market_data_provider_energy="tradingview_guest",
        market_data_provider_futures="disabled",
    )
    monkeypatch.setattr(
        "marketdata.persistence.utc_now", lambda: datetime(2026, 10, 7, 10, 27, tzinfo=UTC)
    )
    previous_coverage = None
    for _ in range(2):
        async with collector_lifespan(settings) as runtime:
            async with asyncio.timeout(5):
                while not await store.latest(None, "tradingview_guest", 100):  # noqa: ASYNC110 - polls external state
                    await asyncio.sleep(0.01)
            # Wait for the initial transaction before comparing durable coverage.
            await asyncio.sleep(0.05)
            async with store.connection() as c:
                coverage = await c.fetch(
                    "SELECT instrument_id,provider,coverage_start,coverage_end,missing,unknown,"
                    "not_expected FROM candle_coverage ORDER BY instrument_id,provider"
                )
                assert await c.fetchval("SELECT count(*) FROM market_candles") > 0
                assert (
                    await c.fetchval(
                        "SELECT count(*) FROM provider_instruments "
                        "WHERE provider='tradingview_guest'"
                    )
                    == 17
                )
            if previous_coverage is not None:
                assert coverage == previous_coverage
            previous_coverage = coverage
        assert runtime.store.pool is None
        assert runtime.status_task.done()
        assert runtime.service.writer_task.done()
        assert runtime.service.monitor_task.done()
        assert all(t.done() for t in runtime.service.provider_tasks)
        assert (await store.collector_status())["running"] is False


@pytest.mark.integration
async def test_duplicate_collector_rejected_before_registration(store, monkeypatch):
    settings = Settings(
        _env_file=None,
        application_role="collector",
        database_url=store.settings.database_url,
        collector_metrics_port=0,
    )
    async with collector_lifespan(settings):
        monkeypatch.setattr(Store, "register", forbidden)
        monkeypatch.setattr("marketdata.collector.build_providers", forbidden)
        with pytest.raises(RuntimeError, match="already owns"):
            async with collector_lifespan(settings):
                pytest.fail("duplicate collector started")


@pytest.mark.integration
async def test_collector_runner_graceful_shutdown(store):
    settings = Settings(
        _env_file=None,
        application_role="collector",
        database_url=store.settings.database_url,
        collector_metrics_port=0,
        batch_flush_seconds=0.01,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(run_collector(settings, stop))
    async with asyncio.timeout(5):
        while not await store.latest(None, "mock", 1):  # noqa: ASYNC110 - polls external state
            await asyncio.sleep(0.01)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert (await store.collector_status())["running"] is False


@pytest.mark.integration
async def test_expired_health_never_claims_live_feed(store):
    await store.publish_collector_status(
        uuid4(),
        True,
        True,
        3,
        [ProviderHealth(provider="mock", state="healthy", transport="connected")],
    )
    async with store.connection() as c:
        await c.execute(
            "UPDATE collector_status SET expires_at=clock_timestamp()-interval '1 second'"
        )
    app = create_app(Settings(_env_file=None, database_url=store.settings.database_url))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            health = await client.get("/health")
            assert health.status_code == 200
            assert health.json()["ingestion_state"] == "unknown"
            assert health.json()["ingestion_running"] is False
            assert health.json()["provider_transports"] == {"mock": "disconnected"}
            assert (await client.get("/providers")).json()[0]["degradation_reasons"] == [
                "collector_status_expired"
            ]


@pytest.mark.integration
def test_api_websocket_explicitly_unavailable(database_url, store):
    with TestClient(create_app(Settings(_env_file=None, database_url=database_url))) as client:
        with client.websocket_connect("/ws/prices") as socket:
            with pytest.raises(WebSocketDisconnect) as error:
                socket.receive_json()
        assert error.value.code == 1013
        assert client.get("/health").json()["streaming_mode"] == "database"


@pytest.mark.integration
async def test_database_connection_loss_returns_503_then_recovers(store, pair):
    app = create_app(
        Settings(
            _env_file=None, database_url=store.settings.database_url, db_pool_min=1, db_pool_max=1
        )
    )
    async with app.router.lifespan_context(app):
        async with app.state.store.connection() as c:
            disconnected = asyncio.Event()
            c.add_termination_listener(lambda _: disconnected.set())
            pid = await c.fetchval("SELECT pg_backend_pid()")
            async with store.connection() as admin:
                # Terminate only this API's connection in the disposable test database.
                assert await admin.fetchval("SELECT pg_terminate_backend($1)", pid)
            await asyncio.wait_for(disconnected.wait(), 2)
            monkey = pytest.MonkeyPatch()

            async def lost(*args):
                await c.fetchval("SELECT 1")

            monkey.setattr(app.state.store, "instrument", lost)
            monkey.setattr(app.state.store, "ping", lost)
            try:
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://test"
                ) as client:
                    result = await client.get(f"/ticks/{pair[0].canonical_symbol}")
                    assert result.status_code == 503
                    assert result.json() == {"detail": "database temporarily unavailable"}
                    assert (await client.get("/health")).status_code == 503
            finally:
                monkey.undo()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            assert (await client.get("/instruments")).status_code == 200


@pytest.mark.integration
async def test_actual_select_only_role_serves_api(store, pair):
    role = "fastapi_read_" + uuid4().hex
    async with store.connection() as admin:
        await admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'disposable_test'")
        await admin.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        await admin.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}")
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    url = urlunsplit(
        parsed._replace(netloc=f"{role}:disposable_test@{parsed.hostname}:{parsed.port}")
    )
    try:
        app = create_app(
            Settings(_env_file=None, database_url=url, registry_file=Path("absent.json"))
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://test"
            ) as client:
                for path in (
                    "/health",
                    "/providers",
                    "/instruments",
                    "/prices/latest",
                    f"/instruments/{pair[0].canonical_symbol}",
                    f"/ticks/{pair[0].canonical_symbol}",
                    "/candles/CRYPTO:BTCUSDT?provider=mock",
                    "/futures/SIM/CL/front?at=2026-10-07T00:00:00Z",
                ):
                    response = await client.get(path)
                    assert response.status_code == 200, response.text
            async with app.state.store.connection() as c:
                assert not await c.fetchval(
                    "SELECT has_table_privilege(current_user,'instruments','INSERT')"
                )
    finally:
        async with store.connection() as admin:
            await admin.execute(f"REVOKE SELECT ON ALL TABLES IN SCHEMA public FROM {role}")
            await admin.execute(f"REVOKE USAGE ON SCHEMA public FROM {role}")
            await admin.execute(f"DROP ROLE {role}")


@pytest.mark.integration
async def test_collector_ownership_loss_stops_owned_tasks(store):
    settings = Settings(
        _env_file=None,
        application_role="collector",
        database_url=store.settings.database_url,
        collector_metrics_port=0,
        batch_flush_seconds=0.01,
        # Poll well inside the test deadline; the production default is five seconds.
        collector_status_interval_seconds=0.1,
    )
    async with collector_lifespan(settings) as runtime:
        async with store.connection() as c:
            pid = await c.fetchval("""
                SELECT a.pid FROM pg_stat_activity a JOIN pg_locks l ON a.pid=l.pid
                WHERE a.datname=current_database() AND a.application_name='marketdata-collector'
                    AND l.locktype='advisory' AND l.granted
            """)
            assert pid
            assert await c.fetchval("SELECT pg_terminate_backend($1)", pid)
        with pytest.raises(RuntimeError, match="ownership connection lost"):
            await asyncio.wait_for(runtime.status_task, 5)
        assert runtime.service.writer_task.done()
        assert runtime.service.monitor_task.done()
        assert all(t.done() for t in runtime.service.provider_tasks)
    assert runtime.store.pool is None


@pytest.mark.integration
async def test_collector_metrics_listener_closes_on_shutdown(store):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_collector(
            Settings(
                _env_file=None,
                application_role="collector",
                database_url=store.settings.database_url,
                collector_metrics_port=port,
                batch_flush_seconds=0.01,
            ),
            stop,
        )
    )
    try:
        async with asyncio.timeout(5), httpx.AsyncClient(timeout=1) as client:
            while True:
                try:
                    result = await client.get(f"http://127.0.0.1:{port}/metrics")
                    assert result.status_code == 200
                    assert "marketdata_received_total" in result.text
                    break
                except httpx.ConnectError:
                    await asyncio.sleep(0.01)
    finally:
        stop.set()
        await asyncio.wait_for(task, 5)
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", port)) != 0
