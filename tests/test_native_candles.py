import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from marketdata.candle_ingestion import CandleIngestion
from marketdata.candles import MarketCandle, expected_minute, minute_start, next_collection
from marketdata.config import Settings
from marketdata.fanout import Fanout
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentProviderError
from marketdata.providers.candles import CandleProvider
from marketdata.providers.factory import build_providers
from marketdata.providers.tradingview import FrameDecoder, encode_message
from marketdata.providers.tradingview_candles import TradingViewCandleProvider, parse_bar
from marketdata.registry import Registry

REGISTRY = Path("config/instruments.tradingview.json")
FIXTURE = Path("tests/fixtures/tradingview_guest_candles.json")
NOW = datetime(2026, 10, 7, 10, 30, 2, tzinfo=UTC)


def captured():
    return json.loads(FIXTURE.read_text(), parse_float=Decimal)


RAW_CAPTURED = json.loads(FIXTURE.read_text())


def source(row):
    return next(m["p"][2] for m in row["messages"] if m["m"] == "symbol_resolved")


def vectors(row):
    return next(m["p"][1]["s1"]["s"] for m in row["messages"] if m["m"] == "timescale_update")


def make_candle(pair, start=NOW - timedelta(minutes=1, seconds=2), **changes):
    data = dict(
        instrument_id=pair[0].id,
        canonical_symbol=pair[0].canonical_symbol,
        provider=pair[1].provider,
        provider_symbol=pair[1].provider_symbol,
        bucket_start=start,
        bucket_end=start + timedelta(minutes=1),
        open="100.123456789012345678901",
        high="102",
        low="99",
        close="101",
        volume="10",
        realtime_status="realtime",
        source_status="streaming",
        received_timestamp=NOW + timedelta(days=1),
        metadata={"volume_type": "base", "volume_additive": True},
    )
    data.update(changes)
    return MarketCandle(**data)


@pytest.mark.parametrize("row", captured(), ids=lambda r: r["symbol"])
def test_native_captured_all_17_ohlc_identity_time_and_volume(row):
    pair = next(
        p
        for p in Registry.load(REGISTRY).subscriptions("tradingview_guest")
        if p[1].provider_symbol == row["symbol"]
    )
    info = source(row)
    for bar in vectors(row):
        candle = parse_bar(
            bar["v"],
            pair,
            info,
            datetime.fromisoformat(row["observed_at"]) + timedelta(minutes=2),
            "streaming",
        )
        assert [candle.open, candle.high, candle.low, candle.close] == bar["v"][1:5]
        assert candle.instrument_id == pair[0].id
        assert candle.bucket_start.timestamp() == bar["v"][0]
        assert (
            candle.bucket_start.second == 0
            and candle.bucket_end - candle.bucket_start == timedelta(minutes=1)
        )
        assert candle.price_basis == "native"
        assert candle.metadata["provenance"] == "native_upstream_chart"
        assert candle.volume == (
            bar["v"][5] if info.get("volume_type") in ("base", "tick") else None
        )


@pytest.mark.parametrize(
    "change",
    [
        dict(open=1.25),
        dict(interval="5m"),
        dict(close="NaN"),
        dict(high="90"),
        dict(bucket_start=NOW),
        dict(received_timestamp=NOW - timedelta(minutes=2)),
        dict(bucket_start=datetime(2026, 1, 1)),
    ],
)
def test_candle_rejects_invalid_precision_bounds_time(change):
    pair = Registry.load(REGISTRY).subscriptions("tradingview_guest")[0]
    with pytest.raises(ValidationError):
        make_candle(pair, **change)


def test_parser_precision_and_status_rejection():
    row = captured()[0]
    pair = Registry.load(REGISTRY).subscriptions("tradingview_guest")[0]
    values = vectors(row)[0]["v"].copy()
    values[1:5] = [Decimal("1.123456789012345678901")] * 4
    candle = parse_bar(values, pair, source(row), NOW + timedelta(days=1), "streaming")
    assert candle.close == Decimal("1.123456789012345678901")
    with pytest.raises(PermanentProviderError):
        parse_bar(values, pair, source(row), NOW + timedelta(days=1), "delayed_streaming_900")
    with pytest.raises(PermanentProviderError):
        parse_bar(
            values, pair, dict(source(row), full_name="WRONG"), NOW + timedelta(days=1), "streaming"
        )
    with pytest.raises(ValueError):
        parse_bar(
            [values[0] + 1, *values[1:]], pair, source(row), NOW + timedelta(days=1), "streaming"
        )
    with pytest.raises(ValueError):
        parse_bar(values[:4], pair, source(row), NOW + timedelta(days=1), "streaming")


@pytest.mark.parametrize(
    "second,expected_second,minutes", [(0, 2, 0), (1, 2, 0), (2, 2, 1), (37, 2, 1), (59, 2, 1)]
)
def test_scheduler_startup_and_grace(second, expected_second, minutes):
    now = NOW.replace(second=second)
    assert next_collection(now, 2) == minute_start(now) + timedelta(
        minutes=minutes, seconds=expected_second
    )
    assert expected_minute(now, 2) == minute_start(now) - timedelta(minutes=2 if second < 2 else 1)
    assert next_collection(now + timedelta(hours=5, seconds=17), 2).second == 2
    with pytest.raises(ValueError):
        next_collection(now.replace(tzinfo=None), 2)


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(candle_interval="5m"),
        dict(candle_backfill_minutes=3, candle_correction_lookback_minutes=4),
        dict(candle_fetch_grace_seconds=-1),
        dict(candle_fetch_grace_seconds=60),
        dict(candle_backfill_minutes=2),
        dict(tradingview_storage_mode="snapshots"),
    ],
)
def test_candle_settings_validation(kwargs):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **kwargs)


class Socket:
    def __init__(self, messages):
        self.messages = messages
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def recv(self):
        if not self.messages:
            raise ConnectionError("disconnect")
        session = json.loads(FrameDecoder().feed(self.sent[0])[0])["p"][0]
        raw = self.messages.pop(0)
        if raw.startswith("~m~4~m~~h~"):
            return raw
        message = json.loads(FrameDecoder().feed(raw)[0].replace("cs_fixture", session))
        return encode_message(message["m"], message["p"])


async def test_fetch_native_rejects_forming_bar_and_echoes_heartbeat():
    row = captured()[0]
    pair = Registry.load(REGISTRY).subscriptions("tradingview_guest")[0]
    p = TradingViewCandleProvider(Settings(_env_file=None), Metrics())
    await p.subscribe([pair])
    socket = Socket(
        [
            "~m~4~m~~h~1",
            *[encode_message(m["m"], m["p"]) for m in RAW_CAPTURED[0]["messages"]],
        ]
    )
    p.socket = socket
    start = datetime.fromtimestamp(int(vectors(row)[0]["v"][0]), UTC)
    cutoff = start + timedelta(minutes=4)
    candles = await p.fetch_completed(pair, start, cutoff, 5)
    assert len(candles) == 4
    assert all(c.bucket_end <= cutoff for c in candles)
    assert "~m~4~m~~h~1" in socket.sent
    assert json.loads(FrameDecoder().feed(socket.sent[-1])[0])["m"] == "chart_delete_session"


class Replay(CandleProvider):
    def __init__(self):
        super().__init__("tradingview_guest", Metrics())
        self.fail = False
        self.calls = []
        self.omit = set()

    async def subscribe(self, pairs):
        self.session_info = {
            p[0].canonical_symbol: {"session": "24x7", "timezone": "Etc/UTC"} for p in pairs
        }
        await super().subscribe(pairs)

    async def connect(self):
        self.transport_connected()

    async def disconnect(self):
        self.transport_disconnected()

    async def fetch_completed(self, pair, start, end, count):
        self.calls.append((start, end, count))
        if self.fail:
            raise ConnectionError("test outage")
        return [
            make_candle(pair, start + timedelta(minutes=k))
            for k in range(int((end - start).total_seconds() // 60))
            if start + timedelta(minutes=k) not in self.omit
        ]


@pytest.mark.integration
async def test_persistence_gaps_idempotency_restart_backfill_and_no_ticks(store):
    registry = Registry.load(REGISTRY)
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    p = Replay()
    await p.subscribe([pair])
    service = CandleIngestion(Settings(_env_file=None), store, p, Fanout(10, 1, p.metrics))
    await service.cycle(NOW)
    first = await store.candle_progress(pair[0].id, p.name)
    assert first["missing_count"] == 0 and first["last_persisted_minute"] == NOW - timedelta(
        minutes=1, seconds=2
    )
    p.fail = True
    await service.cycle(NOW + timedelta(minutes=2))
    state = await store.candle_progress(pair[0].id, p.name)
    assert state["missing_count"] == 2
    p.refresh_health(NOW + timedelta(minutes=2))
    assert (
        p.health.state == "degraded"
        and p.health.symbols[pair[0].canonical_symbol].minutes_behind == 2
    )
    # New provider/service simulates restart. Resume from durable missing ranges.
    recovered = Replay()
    await recovered.subscribe([pair])
    resumed = CandleIngestion(
        Settings(_env_file=None), store, recovered, Fanout(10, 1, recovered.metrics)
    )
    await resumed.cycle(NOW + timedelta(minutes=3))
    assert (await store.candle_progress(pair[0].id, p.name))["missing_count"] == 0
    count = len(
        await store.native_candles(
            pair[0].id, p.name, NOW - timedelta(hours=1), NOW + timedelta(hours=1), "1m", 100
        )
    )
    assert count == 4
    await resumed.cycle(NOW + timedelta(minutes=3))
    assert len(recovered.calls) == 1  # already collected: no duplicate upstream request
    assert (
        await store.ticks(
            pair[0].id, p.name, NOW - timedelta(days=1), NOW + timedelta(days=1), 100, None
        )
        == []
    )
    assert recovered.metrics.received.labels(p.name)._value.get() == 0


@pytest.mark.integration
async def test_candle_upsert_latest_rollup_and_unsupported_backfill(store):
    registry = Registry.load(REGISTRY)
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    first = make_candle(pair)
    coverage = [
        (pair[0].id, pair[1].provider, first.bucket_start, first.bucket_end + timedelta(minutes=4))
    ]
    candles = [make_candle(pair, first.bucket_start + timedelta(minutes=k)) for k in range(5)]
    assert await store.write_candles(candles, coverage) == 5
    assert await store.write_candles(candles, coverage) == 0
    correction = first.model_copy(
        update={
            "close": Decimal("100"),
            "received_timestamp": first.received_timestamp + timedelta(seconds=1),
        }
    )
    assert await store.write_candles([correction], coverage) == 1
    assert await store.write_candles([first], coverage) == 0
    # Latest remains the most recent bucket, even after an older bucket correction.
    assert (await store.latest(pair[0].canonical_symbol, pair[1].provider, 10))[0] == candles[-1]
    from marketdata.domain import normalize

    raw = normalize(*pair, NOW, last="9")
    await store.write([raw])
    store.candle_providers = []
    assert (await store.latest(pair[0].canonical_symbol, pair[1].provider, 10)) == [raw]
    store.candle_providers = [pair[1].provider]
    assert (await store.latest(pair[0].canonical_symbol, pair[1].provider, 10)) == [candles[-1]]
    start = first.bucket_start.replace(minute=25)
    rolled = await store.native_candles(
        pair[0].id, pair[1].provider, start, start + timedelta(minutes=10), "5m", 10
    )
    assert sum(r["observations"] for r in rolled) == 5
    assert sum(r["volume"] for r in rolled) == 50
    assert rolled[0]["open"] == first.open and rolled[0]["high"] == 102
    for interval in ("15m", "1h", "1d"):
        rows = await store.native_candles(
            pair[0].id,
            pair[1].provider,
            first.bucket_start.replace(hour=0, minute=0),
            first.bucket_start.replace(hour=0, minute=0) + timedelta(days=1),
            interval,
            10,
        )
        assert sum(r["observations"] for r in rows) == 5
    p = Replay()
    await p.subscribe([pair])
    missing = first.bucket_end + timedelta(minutes=4)
    p.omit.add(missing)
    service = CandleIngestion(Settings(_env_file=None), store, p, Fanout(10, 1, p.metrics))
    await service.cycle(missing + timedelta(minutes=1, seconds=2))
    assert (await store.candle_progress(pair[0].id, p.name))["missing_count"] == 1
    # Source cannot supply this minute: keep explicit gap even on repeated fetches.
    await service.cycle(missing + timedelta(minutes=2, seconds=2))
    assert (await store.candle_progress(pair[0].id, p.name))["missing_count"] == 1


async def test_factory_retains_explicit_tick_mode():
    registry = Registry.load(REGISTRY)
    for mode in ("ticks", "candles"):
        providers = await build_providers(
            Settings(_env_file=None, tradingview_storage_mode=mode), registry, Metrics()
        )
        assert isinstance(providers[0], CandleProvider) == (mode == "candles")


async def test_scheduler_slow_cycle_recalculates_wall_clock(monkeypatch):
    clock = [NOW + timedelta(seconds=35)]
    p = Replay()
    service = CandleIngestion(Settings(_env_file=None), None, p, None)
    starts = []

    async def cycle():
        starts.append(clock[0])
        if len(starts) == 3:
            raise asyncio.CancelledError
        clock[0] += timedelta(seconds=75 if len(starts) == 1 else 7)

    async def sleep(seconds):
        clock[0] += timedelta(seconds=seconds)

    service.cycle = cycle
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: clock[0])
    monkeypatch.setattr("marketdata.candle_ingestion.asyncio.sleep", sleep)
    with pytest.raises(asyncio.CancelledError):
        await service.run()
    assert starts == [
        NOW + timedelta(seconds=35),
        NOW + timedelta(minutes=2),
        NOW + timedelta(minutes=3),
    ]


@pytest.mark.integration
async def test_candle_database_retry_preserves_bounded_batch(store, monkeypatch):
    registry = Registry.load(REGISTRY)
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    p = Replay()
    await p.subscribe([pair])
    real_write = store.write_candles
    calls = []

    async def write(candles, coverage):
        calls.append(list(candles))
        if len(calls) == 1:
            raise ConnectionError("temporary outage")
        return await real_write(candles, coverage)

    store.write_candles = write
    monkeypatch.setattr("marketdata.candle_ingestion.random.uniform", lambda *args: 0)
    service = CandleIngestion(Settings(_env_file=None), store, p, Fanout(10, 1, p.metrics))
    await service.cycle(NOW)
    assert len(calls) == 2 and calls[0] == calls[1]
    assert len(p.calls) == 1 and service.pending == [] and service.database_healthy
    # Regressed wall clock neither invents minutes nor creates an invalid coverage range.
    await service.cycle(NOW - timedelta(hours=1))
    assert len(p.calls) == 1


@pytest.mark.integration
async def test_old_gaps_remain_explicit_without_rewriting_existing_minutes(store):
    registry = Registry.load(REGISTRY)
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    p = Replay()
    await p.subscribe([pair])
    service = CandleIngestion(
        Settings(_env_file=None, candle_backfill_minutes=3), store, p, Fanout(10, 1, p.metrics)
    )
    await service.cycle(NOW)
    await service.cycle(NOW + timedelta(minutes=10))
    state = await store.candle_progress(pair[0].id, p.name)
    assert state["missing_count"] == 7
    assert len(p.calls) == 2 and p.calls[-1][2] == 4
    assert p.metrics.candles.labels(p.name, "persisted")._value.get() == 4
    await service.cycle(NOW + timedelta(minutes=11))
    assert p.metrics.candles.labels(p.name, "persisted")._value.get() == 5
    p.refresh_health(NOW + timedelta(minutes=11))
    assert p.health.state == "degraded"
    assert p.health.symbols[pair[0].canonical_symbol].missing_candle_count == 7


@pytest.mark.integration
async def test_maintenance_requires_verified_complete_export(store, tmp_path, monkeypatch):
    import argparse
    import importlib.util

    from marketdata.domain import normalize

    spec = importlib.util.spec_from_file_location(
        "tick_maintenance", "scripts/inspect_tradingview_ticks.py"
    )
    maintenance = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(maintenance)

    registry = Registry.load(REGISTRY)
    await store.register(registry)
    pair = registry.subscriptions("tradingview_guest")[0]
    await store.write(
        [
            normalize(*pair, NOW, event_timestamp=NOW, last="100"),
            normalize(
                *pair,
                NOW + timedelta(seconds=1),
                event_timestamp=NOW + timedelta(seconds=1),
                last="102",
            ),
        ]
    )
    monkeypatch.setattr(maintenance, "Settings", lambda: store.settings)
    output = tmp_path / "derived.json"
    args = argparse.Namespace(
        output=output,
        start=minute_start(NOW).isoformat(),
        end=(minute_start(NOW) + timedelta(minutes=1)).isoformat(),
        basis="last",
        delete_raw=False,
    )
    await maintenance.run(args)
    export = json.loads(output.read_text())
    assert export["native_equivalence"] is False and export["source_rows"] == 2
    assert (
        len(
            await store.ticks(
                pair[0].id, None, minute_start(NOW), NOW + timedelta(minutes=1), 10, None
            )
        )
        == 2
    )
    # Corrupted export cannot authorize deletion.
    corrupted = dict(export, source_fingerprint="wrong")
    output.write_text(json.dumps(corrupted))
    args.delete_raw = True
    with pytest.raises(ValueError, match="verification"):
        await maintenance.run(args)
    output.write_text(json.dumps(export))
    await maintenance.run(args)
    assert (
        await store.ticks(pair[0].id, None, minute_start(NOW), NOW + timedelta(minutes=1), 10, None)
        == []
    )


@pytest.mark.integration
def test_native_candle_rest_latest_and_websocket(database_url, store, monkeypatch):
    from fastapi.testclient import TestClient

    from marketdata.api import create_app

    async def build(settings, registry, metrics):
        p = Replay()
        await p.subscribe(registry.subscriptions("tradingview_guest")[:1])
        return [p]

    monkeypatch.setattr("marketdata.api.build_providers", build)
    # Test-only replay uses receipt after every generated completed bar.
    monkeypatch.setattr("marketdata.candle_ingestion.utc_now", lambda: NOW)
    monkeypatch.setattr("marketdata.api.utc_now", lambda: NOW + timedelta(days=1))
    monkeypatch.setattr("marketdata.persistence.utc_now", lambda: NOW - timedelta(minutes=2))
    settings = Settings(
        _env_file=None,
        database_url=database_url,
        registry_file=REGISTRY,
        market_data_provider_crypto="tradingview_guest",
        market_data_provider_forex="tradingview_guest",
        market_data_provider_metals="tradingview_guest",
        market_data_provider_energy="tradingview_guest",
        market_data_provider_futures="disabled",
    )
    with TestClient(create_app(settings)) as client:
        import time

        deadline = time.monotonic() + 5
        while not client.get("/prices/latest?provider=tradingview_guest").json()["items"]:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        item = client.get("/prices/latest/CRYPTO:BTCUSDT?provider=tradingview_guest").json()[0]
        assert (
            item["price_basis"] == "candle_close"
            and item["interval"] == "1m"
            and "bucket_end" in item
        )
        response = client.get(
            "/candles/CRYPTO:BTCUSDT",
            params=dict(
                provider="tradingview_guest",
                start="2026-10-07T10:00:00Z",
                end="2026-10-07T11:00:00Z",
            ),
        )
        assert response.status_code == 200, response.text
        assert response.json()["basis"] == "native" and response.json()["items"][0]["complete"]
        assert (
            client.get("/candles/CRYPTO:BTCUSDT?provider=tradingview_guest&interval=1s").status_code
            == 422
        )
        with client.websocket_connect("/ws/prices") as ws:
            ws.send_json(dict(action="subscribe", symbols=["CRYPTO:BTCUSDT"]))
            assert ws.receive_json()["type"] == "subscribed"
            candle = make_candle(Registry.load(REGISTRY).subscriptions("tradingview_guest")[0])
            app_service = client.app.state.service
            client.portal.call(app_service.fanout.publish, candle)
            msg = ws.receive_json()
            assert msg["type"] == "candle" and msg["persisted"] and msg["price_basis"] == "native"
