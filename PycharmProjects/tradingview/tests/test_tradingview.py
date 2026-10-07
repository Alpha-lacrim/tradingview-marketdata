import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from marketdata.config import Settings
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentProviderError
from marketdata.providers.factory import build_providers
from marketdata.providers.tradingview import (
    MAX_FRAME_UNITS,
    FrameDecoder,
    TradingViewGuestProvider,
    encode_message,
    parse_quote,
)
from marketdata.registry import Registry

NOW = datetime(2026, 10, 7, 9, tzinfo=UTC)
REGISTRY = Path("config/instruments.tradingview.json")
FIXTURE = Path("tests/fixtures/tradingview_guest_quotes.json")


def captured_messages():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["messages"]


def quote_for(symbol="BINANCE:BTCUSDT"):
    return next(
        json.loads(body, parse_float=Decimal)["p"][1]
        for body in captured_messages()
        if json.loads(body)["p"][1]["n"] == symbol
        and "update_mode" in json.loads(body)["p"][1]["v"]
    )


async def make_provider():
    provider = TradingViewGuestProvider(Settings(_env_file=None), Metrics())
    await provider.subscribe(Registry.load(REGISTRY).subscriptions("tradingview_guest"))
    provider.session = "qs_fixture"
    return provider


def wrap(body):
    return f"~m~{len(body.encode('utf-16-le')) // 2}~m~{body}"


def test_decoder_fragmentation_multiple_frames_and_utf16():
    frame = encode_message("quote_add_symbols", ["qs_test", "BINANCE:BTCUSDT"])
    decoder = FrameDecoder()
    assert decoder.feed(frame[:8]) == []
    assert decoder.feed(frame[8:] + wrap("~h~123") + wrap('"gold 🟡"')) == [
        json.dumps(
            {"m": "quote_add_symbols", "p": ["qs_test", "BINANCE:BTCUSDT"]}, separators=(",", ":")
        ),
        "~h~123",
        '"gold 🟡"',
    ]
    assert decoder.buffer == b""


@pytest.mark.parametrize(
    "raw", ["garbage", "~m~bad~m~x", "~m~0~m~", f"~m~{MAX_FRAME_UNITS + 1}~m~"]
)
def test_decoder_rejects_corrupt_or_excessive_lengths(raw):
    with pytest.raises(ValueError):
        FrameDecoder().feed(raw)


def test_decoder_bounds_unfinished_frames():
    with pytest.raises(ValueError, match="buffer"):
        FrameDecoder().feed("x" * (MAX_FRAME_UNITS + 33))


async def test_captured_quotes_all_seventeen_sources_verified():
    provider = await make_provider()
    ticks = []
    for body in captured_messages():
        tick = provider.quote_tick(json.loads(body, parse_float=Decimal)["p"][1], NOW)
        if tick is not None:
            ticks.append(tick)
    assert len({t.provider_symbol for t in ticks}) == 17
    assert all(t.realtime_status == "realtime" for t in ticks)
    assert all(t.metadata["upstream"]["update_mode"] == "streaming" for t in ticks)
    assert provider.rejected == {}
    assert (
        provider.metrics.dropped.labels(provider.name, "source_metadata_unverified")._value.get()
        > 0
    )
    assert not any(t.instrument_type == "future" for t in ticks)
    gold = next(t for t in ticks if t.provider_symbol == "OANDA:XAUUSD")
    token = next(t for t in ticks if t.provider_symbol == "OKX:XAUTUSDT")
    assert gold.instrument_id != token.instrument_id
    assert gold.instrument_type == "otc" and token.instrument_type == "spot"


@pytest.mark.parametrize(
    "mode", ["delayed", "delayed_streaming_600", "endofday", "snapshot", "forbidden", "unexpected"]
)
async def test_non_realtime_quotes_never_emitted_and_other_symbols_continue(mode):
    provider = await make_provider()
    quote = quote_for()
    quote["v"]["update_mode"] = mode
    assert provider.quote_tick(quote, NOW) is None
    assert provider.quote_tick(quote_for(), NOW) is None  # Rejected for this connection.
    assert provider.quote_tick(quote_for("TVC:USOIL"), NOW) is not None
    assert provider.rejected["BINANCE:BTCUSDT"] == "tradingview_not_realtime"
    assert provider.health.error_count == 1
    assert provider.metrics.dropped.labels(provider.name, "source_rejected")._value.get() == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("exchange", "OTHER"),
        ("currency_code", "USD"),
        ("type", "futures"),
        ("update_mode_seconds", 600),
    ],
)
async def test_conflicting_source_metadata_rejected(field, value):
    provider = await make_provider()
    quote = quote_for()
    quote["v"][field] = value
    assert provider.quote_tick(quote, NOW) is None
    assert "BINANCE:BTCUSDT" in provider.rejected


async def test_unknown_status_not_promoted_by_registry_or_cached_prices():
    provider = await make_provider()
    quote = quote_for()
    del quote["v"]["update_mode"]
    assert provider.quote_tick(quote, NOW) is None
    assert provider.quote_tick(quote_for(), NOW) is not None
    bid_only = {"n": quote["n"], "s": "ok", "v": {"bid": Decimal("84000.000000000000001")}}
    tick = provider.quote_tick(bid_only, NOW)
    assert tick.bid == Decimal("84000.000000000000001")
    assert tick.last is None and tick.ask is None
    assert tick.event_timestamp is None and tick.volume is None
    assert tick.sequence_number is None
    assert provider.quote_tick(bid_only, NOW).tick_id != tick.tick_id


async def test_last_time_and_decimal_preserved_without_inventing_trade_sizes():
    provider = await make_provider()
    quote = quote_for()
    quote["v"]["lp"] = Decimal("123.1234567890123456789012345678")
    tick = provider.quote_tick(quote, NOW)
    assert tick.last == quote["v"]["lp"]
    assert tick.event_timestamp_ns == quote["v"]["lp_time"] * 1000000000
    assert tick.event_timestamp != tick.received_timestamp
    assert tick.last_size is None
    pair = provider.subscriptions[0]
    with pytest.raises(ValueError, match="decimal"):
        parse_quote({"lp": 0.1}, pair, NOW)


class Socket:
    def __init__(self, records):
        self.records = iter(records)
        self.sent = []
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.records)
        except StopIteration:
            raise StopAsyncIteration from None

    async def send(self, data):
        self.sent.append(data)

    async def close(self):
        self.closed = True


async def test_stream_heartbeat_malformed_json_wrong_session_and_disconnect():
    provider = await make_provider()
    valid = next(body for body in captured_messages() if '"update_mode"' in body)
    socket = Socket(
        [wrap("~h~123"), wrap("{bad"), wrap(valid.replace("qs_fixture", "qs_other")), wrap(valid)]
    )
    provider.socket = socket
    ticks = [tick async for tick in provider.stream_ticks()]
    assert len(ticks) == 1
    assert socket.sent == [wrap("~h~123")]
    assert provider.health.error_count == 2
    await provider.disconnect()
    assert socket.closed and provider.socket is None


async def test_reconnect_restores_subscriptions_and_revalidates_status(monkeypatch):
    provider = await make_provider()
    socket = Socket([])

    async def connection(*args, **kwargs):
        return socket

    monkeypatch.setattr("marketdata.providers.tradingview.connect", connection)
    provider.quote_tick(quote_for(), NOW)
    provider.rejected["TEST"] = "old_session"
    await provider.connect()
    assert provider.source_metadata == {} and provider.rejected == {}
    commands = [json.loads(FrameDecoder().feed(body)[0]) for body in socket.sent]
    assert [cmd["m"] for cmd in commands] == [
        "set_auth_token",
        "quote_create_session",
        "quote_set_fields",
        "quote_add_symbols",
    ]
    assert len(commands[-1]["p"]) == 18
    assert commands[0]["p"] == ["unauthorized_user_token"]
    await provider.disconnect()


async def test_protocol_errors_require_operator_intervention():
    provider = await make_provider()
    provider.socket = Socket([encode_message("critical_error", ["not_authorized"])])
    with pytest.raises(PermanentProviderError, match="protocol_rejected"):
        await anext(provider.stream_ticks())


async def test_registry_factory_and_secure_configuration():
    settings = Settings(
        _env_file=None,
        **{
            f"market_data_provider_{name}": "tradingview_guest"
            for name in ["crypto", "forex", "metals", "energy"]
        },
        market_data_provider_futures="disabled",
    )
    providers = await build_providers(
        settings, settings.selected_registry(Registry.load(REGISTRY)), Metrics()
    )
    assert len(providers) == 1 and len(providers[0].subscriptions) == 17
    with pytest.raises(ValidationError):
        Settings(_env_file=None, tradingview_ws_url="ws://data.tradingview.com/socket.io/websocket")


@pytest.mark.integration
async def test_captured_guest_quotes_persist_and_latest_is_realtime(store):
    provider = await make_provider()
    await store.register(Registry.load(REGISTRY))
    ticks = []
    for body in captured_messages():
        tick = provider.quote_tick(json.loads(body, parse_float=Decimal)["p"][1], NOW)
        if tick is not None:
            ticks.append(tick)
    assert await store.write(ticks) == len(ticks)
    async with store.connection() as connection:
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM latest_prices WHERE provider='tradingview_guest'"
            )
            == 17
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM market_ticks WHERE provider='tradingview_guest' "
                "AND realtime_status <> 'realtime'"
            )
            == 0
        )


# Health checks use a controllable UTC clock and captured quotes; never live connectivity.
class Clock:
    def __init__(self):
        self.now = NOW

    def advance(self, seconds):
        from datetime import timedelta

        self.now += timedelta(seconds=seconds)
        return self.now


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr("marketdata.providers.base.utc_now", lambda: clock.now)
    monkeypatch.setattr("marketdata.providers.tradingview.utc_now", lambda: clock.now)
    monkeypatch.setattr("marketdata.ingestion.utc_now", lambda: clock.now)
    return clock


def observe_all(provider, now, excluding=None):
    for _, mapping in provider.subscriptions:
        if mapping.provider_symbol == excluding:
            continue
        tick = provider.quote_tick(quote_for(mapping.provider_symbol), now)
        assert tick is not None
        provider.observed(tick)
    provider.message_received(now)


async def test_health_requires_observations_not_handshake(clock, monkeypatch):
    provider = await make_provider()
    assert provider.health.state == "disconnected"
    socket = Socket([])
    during_handshake = []

    async def connection(*args, **kwargs):
        during_handshake.append(provider.health.state)
        return socket

    monkeypatch.setattr("marketdata.providers.tradingview.connect", connection)
    await provider.connect()
    assert during_handshake == ["connecting"]
    assert provider.health.transport == "connected"
    assert provider.health.state == "subscribing"
    assert provider.health.requested_symbol_count == 17
    assert provider.health.fresh_symbol_count == 0
    assert provider.health.stale_symbol_count == 17
    assert not provider.health.subscription_restored
    assert provider.metrics.connection_up.labels(provider.name)._value.get() == 1
    assert provider.metrics.feed_healthy.labels(provider.name)._value.get() == 0
    observe_all(provider, clock.now)
    assert provider.health.state == "healthy"
    assert provider.health.subscription_restored
    connection_id = provider.health.connection_id
    await provider.disconnect()
    assert provider.health.state == "disconnected"
    assert provider.health.last_disconnect_at == clock.now
    await provider.connect()
    assert provider.health.connection_id != connection_id
    assert provider.health.state == "subscribing"
    assert provider.health.fresh_symbol_count == 0
    assert all(item.last_observation_at == NOW for item in provider.health.symbols.values())
    # One recovered source cannot mask the sixteen pending sources.
    observe_all(provider, clock.now, excluding="BINANCE:BTCUSDT")
    assert provider.health.state != "healthy"
    await provider.disconnect()


async def test_connected_heartbeats_without_market_data_degrade(clock):
    provider = await make_provider()
    provider.connection_started()
    provider.transport_connected(clock.now)
    provider.subscriptions_requested()
    clock.advance(61)
    provider.message_received(clock.now)
    provider.refresh_health(clock.now)
    assert provider.health.transport == "connected"
    assert provider.health.state == "degraded"
    assert "no_valid_market_observations" in provider.health.degradation_reasons
    assert provider.health.stale_symbol_count == 17
    assert not provider.transport_expired(clock.now)
    assert provider.health.last_success_at is None
    observe_all(provider, clock.now)
    assert provider.health.state == "healthy"
    # Heartbeats also cannot hide a previously healthy feed that stops producing data.
    clock.advance(61)
    provider.message_received(clock.now)
    provider.refresh_health(clock.now)
    assert provider.health.state == "degraded"
    assert "no_valid_market_observations" in provider.health.degradation_reasons


async def test_one_stale_symbol_recovery_and_metrics(clock, caplog):
    import logging

    caplog.set_level(logging.INFO)
    provider = await make_provider()
    provider.connection_started()
    provider.transport_connected(clock.now)
    provider.subscriptions_requested()
    observe_all(provider, clock.now)
    clock.advance(20)
    observe_all(provider, clock.now, excluding="BINANCE:BTCUSDT")
    clock.advance(11)
    observe_all(provider, clock.now, excluding="BINANCE:BTCUSDT")
    h = provider.health
    assert h.transport == "connected" and h.state == "degraded"
    assert (h.configured_symbol_count, h.fresh_symbol_count, h.stale_symbol_count) == (17, 16, 1)
    assert h.stale_symbols == ["CRYPTO:BTCUSDT"]
    symbol = h.symbols["CRYPTO:BTCUSDT"]
    assert symbol.last_observation_at == NOW and symbol.age_ms == 31000
    assert symbol.observation_count == 1 and symbol.source_update_mode == "streaming"
    assert provider.metrics.symbol_age.labels(provider.name, "CRYPTO:BTCUSDT")._value.get() == 31
    assert provider.metrics.symbol_fresh.labels(provider.name, "CRYPTO:BTCUSDT")._value.get() == 0
    assert provider.metrics.symbol_count.labels(provider.name, "fresh")._value.get() == 16
    # Further ticks from other symbols must never promote the whole feed.
    observe_all(provider, clock.now, excluding="BINANCE:BTCUSDT")
    assert provider.health.state == "degraded"
    stale_logs = [r for r in caplog.records if r.message == "symbol became stale"]
    assert len(stale_logs) == 1
    provider.observed(provider.quote_tick(quote_for(), clock.now))
    assert h.state == "healthy" and h.fresh_symbol_count == 17
    assert symbol.observation_count == 2 and symbol.age_ms == 0
    assert (
        provider.metrics.symbol_observations.labels(provider.name, "CRYPTO:BTCUSDT")._value.get()
        == 2
    )
    assert provider.metrics.feed_healthy.labels(provider.name)._value.get() == 1


async def test_product_specific_freshness_and_configuration(clock):
    provider = TradingViewGuestProvider(
        Settings(_env_file=None, tradingview_symbol_stale_seconds={"TVC:UKOIL": 120}), Metrics()
    )
    await provider.subscribe(Registry.load(REGISTRY).subscriptions("tradingview_guest"))
    provider.connection_started()
    provider.transport_connected(clock.now)
    provider.subscriptions_requested()
    observe_all(provider, clock.now)
    clock.advance(31)
    observe_all(provider, clock.now, excluding="TVC:UKOIL")
    assert provider.health.state == "healthy"
    assert provider.health.symbols["REFERENCE:TVC:UKOIL"].stale_after_seconds == 120
    clock.advance(90)
    observe_all(provider, clock.now, excluding="TVC:UKOIL")
    assert provider.health.state == "degraded"
    assert provider.health.stale_symbols == ["REFERENCE:TVC:UKOIL"]
    for value in [0, -1, float("inf"), float("nan"), 86401]:
        with pytest.raises(ValidationError):
            Settings(_env_file=None, tradingview_symbol_stale_seconds={"TVC:UKOIL": value})
    bad = TradingViewGuestProvider(
        Settings(_env_file=None, tradingview_symbol_stale_seconds={"TYPO:SYMBOL": 120}), Metrics()
    )
    with pytest.raises(ValueError, match="unconfigured"):
        await bad.subscribe(Registry.load(REGISTRY).subscriptions("tradingview_guest"))


async def test_rejected_symbol_cannot_be_masked_by_other_ticks(clock):
    provider = await make_provider()
    provider.connection_started()
    provider.transport_connected(clock.now)
    provider.subscriptions_requested()
    observe_all(provider, clock.now)
    quote = quote_for()
    quote["v"]["update_mode"] = "delayed"
    assert provider.quote_tick(quote, clock.now) is None
    observe_all(provider, clock.now, excluding="BINANCE:BTCUSDT")
    assert provider.health.state == "degraded"
    item = provider.health.symbols["CRYPTO:BTCUSDT"]
    assert item.stale and item.rejected
    assert item.source_update_mode == "delayed"
    assert item.last_rejection_reason == "tradingview_not_realtime"
    assert (
        provider.metrics.rejections.labels(provider.name, "tradingview_not_realtime")._value.get()
        == 1
    )


async def test_unknown_messages_and_parse_logs_are_bounded(clock, caplog):
    provider = await make_provider()
    provider.socket = Socket(
        [encode_message("unknown_sensitive_value", [])] * 3 + [wrap("{bad")] * 3
    )
    assert [tick async for tick in provider.stream_ticks()] == []
    assert provider.metrics.unknown_messages.labels(provider.name)._value.get() == 3
    assert provider.metrics.parse_failures.labels(provider.name)._value.get() == 3
    assert provider.metrics.messages.labels(provider.name)._value.get() == 6
    assert (
        len([r for r in caplog.records if r.message == "unknown TradingView message observed"]) == 1
    )
    assert len([r for r in caplog.records if r.message == "provider malformed message"]) == 1
    assert "unknown_sensitive_value" not in caplog.text


async def test_watchdog_closes_once_and_preserves_safe_close_details(clock):
    from marketdata.fanout import Fanout
    from marketdata.ingestion import IngestionService

    provider = await make_provider()
    socket = Socket([])
    socket.close_code = 1001
    socket.close_reason = "private upstream secret"
    provider.socket = socket
    provider.connection_started()
    provider.transport_connected(clock.now)
    provider.subscriptions_requested()
    service = IngestionService(
        provider.settings, None, [provider], Fanout(1, 1, provider.metrics), provider.metrics
    )
    clock.advance(44)
    await service.monitor_once(clock.now)
    assert not socket.closed
    clock.advance(2)
    await service.monitor_once(clock.now)
    assert socket.closed
    assert provider.health.state == "reconnecting"
    assert provider.health.transport == "disconnected"
    assert provider.health.last_error == "transport_frame_timeout"
    assert provider.health.last_close_code == 1001
    assert provider.health.last_close_reason == "remote_close_reason_present"
    assert "private upstream secret" not in provider.health.model_dump_json()
    assert provider.metrics.connection_up.labels(provider.name)._value.get() == 0
    assert provider.metrics.transport_timeouts.labels(provider.name)._value.get() == 1
    await service.monitor_once(clock.now)
    assert provider.metrics.transport_timeouts.labels(provider.name)._value.get() == 1


@pytest.mark.parametrize("close_fails", [False, "blocked_error", "ended_error"])
async def test_watchdog_uses_owned_runner_to_reconnect_and_restore(clock, monkeypatch, close_fails):
    import asyncio

    from marketdata.fanout import Fanout
    from marketdata.ingestion import IngestionService

    class IdleSocket(Socket):
        def __init__(self, records=(), fail_close=False):
            super().__init__([])
            self.queue = asyncio.Queue()
            self.fail_close = fail_close
            for record in records:
                self.queue.put_nowait(record)

        async def __anext__(self):
            value = await self.queue.get()
            if value is None:
                raise StopAsyncIteration
            return value

        async def close(self):
            if self.fail_close == "ended_error":
                self.queue.put_nowait(None)
                # The runner can already be in backoff when close reports an error.
                await asyncio.sleep(0)
            if self.fail_close:
                raise ConnectionError("private detail")
            self.closed = True
            self.queue.put_nowait(None)

    provider = await make_provider()
    sockets = []
    connected = asyncio.Event()
    restored = asyncio.Event()
    real_observed = provider.observed

    def observed(tick):
        real_observed(tick)
        if provider.health.state == "healthy":
            restored.set()

    async def connection(*args, **kwargs):
        records = (
            []
            if not sockets
            else [
                wrap(body.replace("qs_fixture", provider.session)) for body in captured_messages()
            ]
        )
        socket = IdleSocket(records, fail_close=close_fails and not sockets)
        sockets.append(socket)
        connected.set()
        return socket

    provider.observed = observed
    monkeypatch.setattr("marketdata.providers.tradingview.connect", connection)
    monkeypatch.setattr("marketdata.ingestion.random.uniform", lambda *args: 0)
    service = IngestionService(
        provider.settings, None, [provider], Fanout(1, 1, provider.metrics), provider.metrics
    )
    task = asyncio.create_task(service.run_provider(provider))
    service.provider_tasks = [task]
    try:
        await asyncio.wait_for(connected.wait(), 1)
        connection_id = provider.health.connection_id
        clock.advance(46)
        await service.monitor_once(clock.now)
        await asyncio.wait_for(restored.wait(), 1)
        assert len(sockets) == 2
        assert provider.health.connection_id != connection_id
        assert provider.health.reconnect_count == 1
        assert provider.health.consecutive_reconnect_failures == 0
        assert provider.health.transport == "connected" and provider.health.state == "healthy"
        assert provider.metrics.reconnects.labels(provider.name)._value.get() == 1
        assert provider.metrics.transport_timeouts.labels(provider.name)._value.get() == 1
        assert provider.health.last_error == "transport_frame_timeout"
        assert len(json.loads(FrameDecoder().feed(sockets[1].sent[-1])[0])["p"]) == 18
    finally:
        service.stopping = True
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert provider.health.state == "disconnected"
