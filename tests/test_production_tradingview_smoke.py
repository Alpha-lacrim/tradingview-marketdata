"""Smoke verification against real committed history and versioned TCP WebSocket delivery."""

import importlib.util
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, bar, receive, server, subscribe, ws_url
from websockets.asyncio.client import connect

from marketdata.registry import Registry
from marketdata.ws_models import CandleEvent

spec = importlib.util.spec_from_file_location(
    "production_smoke", "scripts/smoke_production_tradingview.py"
)
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


async def test_smoke_failure_checkpoint_excludes_credentials(tmp_path, monkeypatch):
    original_client = httpx.AsyncClient
    token = "private-test-token-never-in-checkpoint"

    async def respond(request):
        assert request.headers["Authorization"] == "Bearer " + token
        return httpx.Response(200 if request.url.path == "/livez" else 503)

    monkeypatch.setattr(
        smoke.httpx,
        "AsyncClient",
        lambda **kwargs: original_client(transport=httpx.MockTransport(respond), **kwargs),
    )
    output = tmp_path / "checkpoint.json"
    with pytest.raises(AssertionError, match="database not ready"):
        await smoke.smoke(
            "http://127.0.0.1:8002",
            token,
            ORIGIN,
            Registry.load(Path("config/instruments.tradingview-100.json")),
            3,
            output,
        )
    report = json.loads(output.read_text())
    assert report["passed"] is False and report["failure_category"] == "AssertionError"
    assert report["observed_candles"] == 0
    assert token not in output.read_text()


@pytest.mark.integration
async def test_smoke_reconciles_committed_ws_candle_and_rejects_changed_prices(store, pair):
    async with server(store) as (_, url):
        async with (
            httpx.AsyncClient(base_url=url, headers=HEADERS) as client,
            connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws,
        ):
            await subscribe(ws, [pair[0].canonical_symbol])
            await store.write_candles([bar(pair)], [])
            event = CandleEvent.model_validate(await receive(ws, "candle_completed"))
            await smoke.reconcile(client, event)
            event.data.close += Decimal("0.01")
            with pytest.raises(AssertionError, match="persisted REST history"):
                await smoke.reconcile(client, event)


@pytest.mark.integration
async def test_smoke_rejects_simulated_source_identity_and_forming_bars(store, pair):
    async with server(store) as (_, url):
        async with connect(ws_url(url), origin=ORIGIN, additional_headers=HEADERS) as ws:
            await subscribe(ws, [pair[0].canonical_symbol])
            await store.write_candles([bar(pair)], [])
            event = CandleEvent.model_validate(await receive(ws, "candle_completed"))
    candle = event.data.model_copy(update={"provider": "tradingview_guest"})
    sources = {candle.canonical_symbol: candle.provider_symbol}
    with pytest.raises(AssertionError, match="upstream-reported realtime"):
        smoke.validate_candle(candle, sources)
    candle = candle.model_copy(update={"realtime_status": "realtime"})
    smoke.validate_candle(candle, sources)
    with pytest.raises(AssertionError, match="source mismatch"):
        smoke.validate_candle(candle, {candle.canonical_symbol: "wrong"})
    candle = candle.model_copy(update={"bucket_end": candle.bucket_end + timedelta(days=1)})
    with pytest.raises(AssertionError, match="forming candle"):
        smoke.validate_candle(candle, sources)
