import time
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from marketdata.api import create_combined_app as create_app
from marketdata.config import Settings

pytestmark = pytest.mark.integration


def test_complete_mock_rest_and_websocket_pipeline(database_url, store):
    app = create_app(
        Settings(
            _env_file=None,
            application_role="combined",
            database_url=database_url,
            mock_ticks_per_second=500,
            batch_flush_seconds=0.01,
        )
    )
    with TestClient(app) as client:
        with client.websocket_connect("/ws/prices") as websocket:
            websocket.send_json({"action": "subscribe", "symbols": ["CRYPTO:BTCUSDT"]})
            assert websocket.receive_json()["type"] == "subscribed"
            tick = websocket.receive_json()
            assert tick["symbol"] == "CRYPTO:BTCUSDT" and tick["realtime_status"] == "simulated"
            assert tick["persisted"] is False and isinstance(tick["last"], str)
            websocket.send_json({"action": "subscribe", "symbols": ["UNKNOWN"]})
            # An already queued tick may precede the error acknowledgement.
            while (message := websocket.receive_json())["type"] == "tick":
                pass
            assert message["type"] == "error"
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            latest = client.get("/prices/latest/CRYPTO:BTCUSDT").json()
            if latest and client.get("/health").status_code == 200:
                break
            time.sleep(0.01)
        assert latest and latest[0]["provider"] == "mock"
        assert latest[0]["stale"] is False and latest[0]["age_ms"] >= 0
        assert client.get("/health").status_code == 200
        assert client.get("/providers").json()[0]["state"] == "healthy"
        assert len(client.get("/instruments").json()) == 30
        assert client.get("/instruments/CRYPTO:BTCUSDT").json()["mappings"]
        assert client.get("/prices/latest?limit=1").json()["next_after"]
        assert client.get("/ticks/CRYPTO:BTCUSDT?limit=1").json()["items"]
        assert client.get("/ticks/CRYPTO:BTCUSDT?limit=1001").status_code == 422
        assert client.get("/ticks/CRYPTO:BTCUSDT?cursor=invalid").status_code == 422
        assert client.get("/ticks/UNKNOWN").status_code == 404
        assert client.get("/ticks/CRYPTO:BTCUSDT?start=2026-01-01T00:00:00").status_code == 422
        start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        candles = client.get(
            "/candles/CRYPTO:BTCUSDT",
            params={
                "provider": "mock",
                "interval": "1m",
                "start": start.isoformat(),
                "end": (start + timedelta(hours=1)).isoformat(),
            },
        )
        assert candles.status_code == 200, candles.text
        assert candles.json()["items"]
        assert client.get("/candles/CRYPTO:BTCUSDT?provider=mock").status_code == 200
        assert client.get("/candles/CRYPTO:BTCUSDT?provider=mock&interval=wrong").status_code == 422
        assert (
            client.get("/futures/SIM/CL/front?at=2026-10-07T00:00:00Z")
            .json()["instrument"]["contract_code"]
            .startswith("SIM-")
        )
        assert "marketdata_persisted_total" in client.get("/metrics").text


def test_authentication_covers_rest_and_websocket(database_url):
    settings = Settings(
        _env_file=None,
        application_role="combined",
        database_url=database_url,
        api_key=SecretStr("test-only-token"),
    )
    with TestClient(create_app(settings)) as client:
        assert client.get("/instruments").status_code == 401
        assert client.get("/metrics").status_code == 401
        assert (
            client.get(
                "/instruments", headers={"Authorization": "Bearer test-only-token"}
            ).status_code
            == 200
        )
        from starlette.websockets import WebSocketDisconnect

        with pytest.raises(WebSocketDisconnect), client.websocket_connect("/ws/prices"):
            pass
        with client.websocket_connect(
            "/ws/prices", headers={"Authorization": "Bearer test-only-token"}
        ) as ws:
            ws.send_json({"action": "subscribe", "symbols": ["CRYPTO:BTCUSDT"]})
            assert ws.receive_json()["type"] == "subscribed"
