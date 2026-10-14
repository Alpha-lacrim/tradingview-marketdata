"""Qualification regressions: public liveness, sanitized failures and observability."""

import asyncio
from pathlib import Path

import httpx
import pytest
from pydantic import ValidationError
from test_api_v1 import api_client

from marketdata.api import create_app
from marketdata.config import Settings
from marketdata.registry import Registry


def test_production_guest_ingestion_is_blocked_before_source_io():
    registry = Registry.load(Path("config/instruments.tradingview.json"))
    settings = Settings(
        _env_file=None,
        api_environment="production",
        api_key="x" * 32,
        application_role="collector",
        tradingview_production_access_approved=False,
        market_data_provider_crypto="tradingview_guest",
        enabled_asset_classes=["crypto"],
    )
    with pytest.raises(ValueError, match="established access rights"):
        settings.selected_registry(registry)
    assert (
        settings.model_copy(update={"tradingview_production_access_approved": True})
        .selected_registry(registry)
        .entries
    )
    with pytest.raises(ValidationError):
        Settings(_env_file=None, api_environment="production")


async def test_approved_production_tradingview_builds_without_source_io():
    from marketdata.observability import Metrics
    from marketdata.providers.candle_pool import CandleConnectionPool
    from marketdata.providers.factory import build_providers

    settings = Settings(
        _env_file=None,
        api_environment="production",
        application_role="collector",
        registry_file=Path("config/instruments.tradingview-100.json"),
        market_data_provider_crypto="tradingview_guest",
        market_data_provider_forex="tradingview_guest",
        market_data_provider_metals="tradingview_guest",
        market_data_provider_energy="tradingview_guest",
        market_data_provider_futures="disabled",
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=4,
        candle_pool_concurrency=8,
        candle_pool_max_instruments=100,
        candle_pool_connect_interval_seconds=2,
        candle_fetch_grace_seconds=5,
    )
    assert settings.tradingview_production_access_approved
    registry = settings.selected_registry(Registry.load(settings.registry_file))
    providers = await build_providers(settings, registry, Metrics())
    assert len(providers) == 1
    pool = providers[0]
    assert isinstance(pool, CandleConnectionPool)
    assert [len(worker.provider.subscriptions) for worker in pool.workers] == [25] * 4
    assert not pool.opened and not pool.tasks


async def test_liveness_survives_database_failure_without_credentials(monkeypatch):
    app = create_app(Settings(_env_file=None, api_key="x" * 32, api_environment="production"))

    async def failed():
        raise OSError("postgresql://secret:credential@private/database")

    monkeypatch.setattr(app.state.store, "ping", failed)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as c:
        assert (await c.get("/livez")).json() == {"api": "alive"}
        assert (await c.get("/health")).status_code == 503
        assert (await c.get("/api/v1/health")).status_code == 401


async def test_unexpected_error_and_auth_metrics_never_log_sensitive_text(monkeypatch, caplog):
    app = create_app(Settings(_env_file=None, api_key="x" * 32))

    async def failed(*args, **kwargs):
        raise ValueError("postgresql://secret:credential@private/database")

    monkeypatch.setattr(app.state.store, "source_records", failed)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as c:
        assert (await c.get("/api/v1/instruments")).status_code == 401
        for method in ("UNRECOGNIZED_A", "UNRECOGNIZED_B"):
            assert (await c.request(method, "/api/v1/instruments")).status_code == 401
        response = await c.get(
            "/api/v1/instruments", headers={"Authorization": "Bearer " + "x" * 32}
        )
        assert response.status_code == 500
        assert response.json() == {
            "error": {"code": "internal_error", "message": "internal server error"}
        }
        assert response.headers["Cache-Control"] == "no-store"
    assert "credential" not in caplog.text
    assert app.state.metrics.api_requests.labels("GET", "401")._value.get() == 1
    assert app.state.metrics.api_requests.labels("OTHER", "401")._value.get() == 2


@pytest.mark.integration
async def test_runtime_metrics_and_health_observe_idle_and_failed_reader(store):
    async with api_client(store) as (app, client):
        assert (await client.get("/api/v1/health")).json()["websocket_delivery"] == "idle"
        await asyncio.sleep(1.1)
        metrics = (await client.get("/metrics")).text
        for metric in (
            "api_query_seconds_count",
            "api_pool_connections",
            "api_ws_queue_messages",
            "event_loop_lag_seconds_count",
        ):
            assert "marketdata_" + metric in metrics
        app.state.market_hub.task.cancel()
        await asyncio.gather(app.state.market_hub.task, return_exceptions=True)
        response = await client.get("/api/v1/health")
        assert response.status_code == 200  # Stored history remains ready.
        assert response.json()["websocket_delivery"] == "degraded"


@pytest.mark.integration
async def test_production_refuses_owner_dsn_and_accepts_select_only_role(
    store, readonly_database_url
):
    owner = create_app(
        Settings(
            _env_file=None,
            database_url=store.settings.database_url,
            api_environment="production",
            api_key="x" * 32,
        )
    )
    with pytest.raises(RuntimeError, match="SELECT-only"):
        async with owner.router.lifespan_context(owner):
            pytest.fail("owner credentials must never start a production API")
    assert owner.state.store.pool is None
    async with api_client(
        store, database_url=readonly_database_url, api_environment="production", api_key="x" * 32
    ) as (_, client):
        assert (await client.get("/health")).status_code == 200
