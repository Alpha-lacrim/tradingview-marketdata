import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from marketdata.cli import oanda_entry
from marketdata.config import Settings
from marketdata.domain import AssetClass
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentProviderError
from marketdata.providers.binance import BinanceProvider, parse_trade
from marketdata.providers.databento import UNDEF_PRICE, DatabentoProvider, fixed_price, parse_mbp1
from marketdata.providers.mock import MockMarketDataProvider
from marketdata.providers.oanda import OandaProvider, parse_price
from marketdata.registry import Registry

NOW = datetime(2026, 10, 7, tzinfo=UTC)
TRADE = {
    "e": "trade",
    "E": 1672515782136,
    "s": "BTCUSDT",
    "t": 12345,
    "p": "16500.12345678",
    "q": "0.00000001",
    "T": 1672515782135,
    "m": True,
}


def test_binance_trade_fixture():
    pair = Registry.load(Path("config/instruments.binance.json")).subscriptions("binance")[0]
    tick = parse_trade(TRADE, *pair, NOW)
    assert tick.last == Decimal("16500.12345678")
    assert tick.last_size == Decimal("0.00000001")
    assert tick.event_timestamp_ns == 1672515782135000000
    assert tick.event_timestamp != tick.received_timestamp
    assert tick.bid is None
    assert tick.tick_id == parse_trade(TRADE, *pair, NOW).tick_id
    with pytest.raises(ValueError):
        parse_trade(dict(TRADE, s="ETHUSDT"), *pair, NOW)


class FakeSocket:
    def __init__(self, messages):
        self.messages = iter(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.messages)
        except StopIteration:
            raise StopAsyncIteration from None

    async def close(self):
        pass


async def test_binance_malformed_and_sequence_gap():
    provider = BinanceProvider(Settings(_env_file=None), Metrics())
    await provider.subscribe(
        Registry.load(Path("config/instruments.binance.json")).subscriptions("binance")
    )
    provider.socket = FakeSocket(
        ["{bad", json.dumps({"data": TRADE}), json.dumps({"data": dict(TRADE, t=12348)})]
    )
    ticks = [t async for t in provider.stream_ticks()]
    assert len(ticks) == 2
    assert provider.health.error_count == 1
    assert provider.metrics.sequence_gaps.labels("binance")._value.get() == 1
    await provider.disconnect()


def test_oanda_best_quote_no_invented_last():
    entry = oanda_entry(
        {"name": "EUR_USD", "type": "CURRENCY", "displayName": "EUR/USD"}, "live", AssetClass.ENERGY
    )
    payload = {
        "type": "PRICE",
        "instrument": "EUR_USD",
        "time": "2026-10-07T00:00:00.123456789Z",
        "tradeable": True,
        "bids": [{"price": "1.10", "liquidity": 100}, {"price": "1.11", "liquidity": 20}],
        "asks": [{"price": "1.13", "liquidity": 50}, {"price": "1.12", "liquidity": 10}],
    }
    tick = parse_price(payload, entry.instrument, entry.mappings[0], NOW)
    assert tick.bid == Decimal("1.11") and tick.ask == Decimal("1.12")
    assert tick.bid_size == 20 and tick.ask_size == 10
    assert tick.last is None and tick.sequence_number is None
    assert tick.metadata["upstream_time"].endswith("123456789Z")
    assert tick.event_timestamp_ns == 1791331200123456789
    assert tick.realtime_status == "indicative"


def test_oanda_practice_is_simulated_and_cfd_is_distinct():
    entry = oanda_entry(
        {"name": "TEST_USD", "type": "CFD", "displayName": "Test CFD"},
        "practice",
        AssetClass.ENERGY,
    )
    assert entry.instrument.instrument_type == "cfd"
    assert entry.instrument.canonical_symbol.startswith("CFD:")
    assert entry.mappings[0].realtime_status == "simulated"


def test_oanda_authentication_not_retried():
    response = httpx.Response(401, request=httpx.Request("GET", "https://example.com"))
    with pytest.raises(PermanentProviderError):
        OandaProvider.check_status(response)


def test_databento_fixed_precision_and_undefined(pair):
    i, m = pair
    m = m.model_copy(update={"provider": "databento", "metadata": {"dataset": "TEST"}})
    record = SimpleNamespace(
        levels=[SimpleNamespace(bid_px=-37630000000, ask_px=UNDEF_PRICE, bid_sz=2, ask_sz=0)],
        ts_event=1791331200123456789,
        ts_recv=1791331200123456790,
        action="T",
        price=-37620000000,
        size=1,
        sequence=123,
        publisher_id=1,
        instrument_id=42,
        flags=0,
    )
    tick = parse_mbp1(record, i, m, NOW)
    assert tick.bid == Decimal("-37.63") and tick.ask is None and tick.ask_size is None
    assert tick.last == Decimal("-37.62") and tick.last_size == 1
    assert tick.event_timestamp_ns == record.ts_event
    assert fixed_price(UNDEF_PRICE) is None
    record.action = "A"
    assert parse_mbp1(record, i, m, NOW).last is None


def test_sdk_bridge_is_bounded():
    settings = Settings(_env_file=None, queue_capacity=1, batch_size=1)
    provider = DatabentoProvider(settings, Metrics(), "TEST")
    provider.on_record(object())
    provider.on_record(object())
    assert provider.records.qsize() == 1
    assert provider.callback_error == "sdk_bridge_full"
    assert provider.metrics.dropped.labels(provider.name, "sdk_bridge_full")._value.get() == 1


async def test_mock_multi_instrument_stream_and_disconnect(registry):
    provider = MockMarketDataProvider(Metrics(), ticks_per_second=10000)
    await provider.subscribe(registry.subscriptions("mock"))
    await provider.connect()
    stream = provider.stream_ticks()
    ticks = [await anext(stream) for _ in range(len(provider.subscriptions))]
    assert len({t.instrument_id for t in ticks}) == 30
    assert {t.asset_class for t in ticks} == set(AssetClass)
    assert all(t.realtime_status == "simulated" and t.bid < t.ask for t in ticks)
    provider.observed(ticks[0])
    assert await provider.get_snapshot() == [ticks[0]]
    await provider.unsubscribe([ticks[0].canonical_symbol])
    assert await provider.get_snapshot() == []
    await provider.disconnect()
    with pytest.raises(StopAsyncIteration):
        await anext(stream)
