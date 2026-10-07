"""Real SDK record fixtures, offline; requires the optional SDK, never a key."""

from datetime import UTC, datetime

import pytest

from marketdata.config import Settings
from marketdata.domain import AssetClass
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentProviderError
from marketdata.providers.databento import DatabentoProvider, parse_mbp1
from marketdata.providers.definitions import definition_entry

db = pytest.importorskip("databento")


@pytest.fixture
def definition():
    # Deliberately fictional instrument; exercises official DBN field types without
    # pretending that any provider has listed this symbol or granted access to it.
    return db.InstrumentDefMsg(
        publisher_id=1,
        instrument_id=42,
        ts_event=1791331200000000000,
        ts_recv=1791331200000000001,
        min_price_increment=250000000,
        display_factor=1000000000,
        raw_symbol="TESTZ7",
        asset="TEST",
        security_type="FUT",
        instrument_class=db.InstrumentClass.FUTURE,
        security_update_action=db.SecurityUpdateAction.ADD,
        expiration=1829347200000000000,
        unit_of_measure_qty=50000000000,
        maturity_year=2027,
        maturity_month=12,
        currency="USD",
        exchange="TEST",
        unit_of_measure="USD",
    )


@pytest.fixture
def record():
    return db.MBP1Msg(
        publisher_id=1,
        instrument_id=42,
        ts_event=1791331200123456789,
        ts_recv=1791331200123456790,
        price=100250000000,
        size=2,
        action=db.Action.TRADE,
        side=db.Side.BID,
        depth=0,
        sequence=123,
        levels=db.BidAskPair(bid_px=100000000000, ask_px=100500000000, bid_sz=5, ask_sz=6),
    )


def test_authoritative_definition_import_and_sdk_price_record(definition, record):
    entry = definition_entry(definition, "TEST", AssetClass.FUTURES, "offline fixture")
    assert entry.instrument.canonical_symbol == "FUTURES:TEST:TEST:202712"
    assert entry.instrument.contract_code == "TESTZ7"
    assert entry.instrument.multiplier == 50
    assert entry.instrument.roll_at is None
    assert entry.mappings[0].realtime_status == "unknown"
    tick = parse_mbp1(record, entry.instrument, entry.mappings[0], datetime.now(UTC))
    assert tick.bid == 100 and tick.ask == 100.5 and tick.last_size == 2
    assert tick.event_timestamp_ns == record.ts_event


def test_definition_import_rejects_options_and_missing_maturity(definition):
    from types import SimpleNamespace

    fields = {
        name: getattr(definition, name)
        for name in (
            "instrument_class",
            "leg_count",
            "min_price_increment",
            "unit_of_measure_qty",
            "exchange",
            "currency",
            "asset",
            "maturity_year",
            "maturity_month",
        )
    }
    with pytest.raises(ValueError, match="outright"):
        definition_entry(
            SimpleNamespace(**dict(fields, instrument_class="C")),
            "TEST",
            AssetClass.FUTURES,
            "fixture",
        )
    with pytest.raises(ValueError):
        definition_entry(
            SimpleNamespace(**dict(fields, maturity_month=0)), "TEST", AssetClass.FUTURES, "fixture"
        )


class FakeClient:
    def __init__(self, connected=True):
        self.connected = connected

    def is_connected(self):
        return self.connected

    def terminate(self):
        self.connected = False

    async def wait_for_close(self, **kwargs):
        return


async def test_adapter_checks_real_definitions_before_ticks(definition, record):
    entry = definition_entry(definition, "TEST", AssetClass.FUTURES, "offline fixture")
    provider = DatabentoProvider(Settings(_env_file=None), Metrics(), "TEST")
    await provider.subscribe([(entry.instrument, entry.mappings[0])])
    provider.client = FakeClient()
    provider.on_record(definition)
    provider.on_record(record)
    stream = provider.stream_ticks()
    assert (await anext(stream)).instrument_id == entry.instrument.id
    assert provider.verified == {42}
    await stream.aclose()
    await provider.disconnect()


async def test_adapter_rejects_wrong_expiry(definition, record):
    from datetime import timedelta

    entry = definition_entry(definition, "TEST", AssetClass.FUTURES, "offline fixture")
    wrong = entry.instrument.model_copy(
        update={"expiry": entry.instrument.expiry + timedelta(days=1)}
    )
    provider = DatabentoProvider(Settings(_env_file=None), Metrics(), "TEST")
    await provider.subscribe([(wrong, entry.mappings[0])])
    provider.client = FakeClient()
    provider.on_record(definition)
    provider.on_record(record)
    with pytest.raises(PermanentProviderError, match="definition_mismatch"):
        await anext(provider.stream_ticks())
    await provider.disconnect()
    assert provider.metrics.dropped.labels(provider.name, "sdk_bridge_discarded")._value.get() == 1


async def test_bare_sdk_disconnect_is_retryable():
    provider = DatabentoProvider(Settings(_env_file=None), Metrics(), "TEST")
    provider.client = FakeClient(connected=False)
    with pytest.raises(ConnectionError):
        await anext(provider.stream_ticks())
    await provider.disconnect()
