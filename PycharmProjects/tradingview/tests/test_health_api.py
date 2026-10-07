import asyncio
from types import SimpleNamespace

import httpx
from test_tradingview import Clock, make_provider, observe_all, quote_for

from marketdata.api import create_app
from marketdata.config import Settings
from marketdata.fanout import Fanout
from marketdata.ingestion import IngestionService


async def test_health_serialization_and_latest_receipt_freshness(monkeypatch):
    clock = Clock()
    monkeypatch.setattr("marketdata.api.utc_now", lambda: clock.now)
    monkeypatch.setattr("marketdata.providers.base.utc_now", lambda: clock.now)
    provider = await make_provider()
    rows = []

    class FakeStore:
        def __init__(self, settings):
            pass

        async def ping(self):
            return True

        async def latest(self, symbol, selected_provider, limit, after=""):
            return [tick for tick in rows if not symbol or tick.canonical_symbol == symbol][:limit]

        async def instrument(self, symbol):
            return {"id": "unused"}

    monkeypatch.setattr("marketdata.api.Store", FakeStore)
    app = create_app(Settings(_env_file=None))
    service = IngestionService(
        provider.settings, None, [provider], Fanout(1, 1, provider.metrics), provider.metrics
    )
    service.writer_task = SimpleNamespace(done=lambda: False)
    app.state.service = service
    provider.connection_started()
    provider.transport_connected(clock.now)
    provider.subscriptions_requested()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        response = await client.get("/health")
        assert response.status_code == 503
        assert response.json()["database_state"] == "healthy"
        assert response.json()["provider_transports"] == {provider.name: "connected"}
        assert response.json()["provider_states"] == {provider.name: "subscribing"}
        health = (await client.get("/providers")).json()[0]
        assert health["configured_symbol_count"] == 17
        assert health["stale_symbol_count"] == 17
        assert health["symbols"]["CRYPTO:BTCUSDT"]["last_observation_at"] is None
        assert health["symbols"]["CRYPTO:BTCUSDT"]["age_ms"] is None
        observe_all(provider, clock.now)
        rows[:] = list(provider.snapshots.values())
        assert (await client.get("/health")).status_code == 200
        original = rows[0]
        latest = (await client.get("/prices/latest/CRYPTO:BTCUSDT")).json()[0]
        assert latest["age_ms"] == 0 and latest["stale"] is False
        assert latest["provider_state"] == "healthy"
        clock.advance(31)
        observe_all(provider, clock.now, excluding="BINANCE:BTCUSDT")
        response = await client.get("/health")
        assert response.status_code == 503
        assert response.json()["provider_states"] == {provider.name: "degraded"}
        health = (await client.get("/providers")).json()[0]
        assert health["stale_symbols"] == ["CRYPTO:BTCUSDT"]
        assert health["fresh_symbol_count"] == 16
        assert health["symbols"]["CRYPTO:BTCUSDT"]["age_ms"] == 31000
        latest = (await client.get("/prices/latest")).json()["items"][0]
        assert latest["age_ms"] == 31000 and latest["stale"] is True
        assert (
            latest["received_timestamp"] == original.model_dump(mode="json")["received_timestamp"]
        )
        assert latest["event_timestamp"] == original.model_dump(mode="json")["event_timestamp"]
        # A recovered upstream feed cannot make an older DB row look fresh.
        provider.observed(provider.quote_tick(quote_for(), clock.now))
        latest = (await client.get("/prices/latest/CRYPTO:BTCUSDT")).json()[0]
        assert latest["provider_state"] == "healthy" and latest["stale"] is True
        service.database_healthy = False
        response = await client.get("/health")
        assert response.status_code == 503 and response.json()["database_state"] == "degraded"
        assert response.json()["provider_states"] == {provider.name: "healthy"}
        service.database_healthy = True
        service.providers = []
        latest = (await client.get("/prices/latest/CRYPTO:BTCUSDT")).json()[0]
        assert latest["stale"] is True and latest["provider_state"] is None


async def test_watchdog_does_not_swallow_shutdown_cancellation(pair):
    from marketdata.observability import Metrics
    from marketdata.providers.mock import MockMarketDataProvider

    provider = MockMarketDataProvider(Metrics())
    await provider.subscribe([pair])
    service = IngestionService(
        Settings(_env_file=None), None, [provider], Fanout(1, 1, provider.metrics), provider.metrics
    )
    task = asyncio.create_task(service.run_provider(provider))
    await asyncio.sleep(0)
    service.stopping = True
    provider.transport_timed_out = True
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()
    assert provider.health.transport == "disconnected"
    assert provider.health.state == "disconnected"
