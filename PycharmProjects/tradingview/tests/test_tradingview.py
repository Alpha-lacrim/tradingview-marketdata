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
