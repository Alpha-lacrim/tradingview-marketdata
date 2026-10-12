from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from marketdata.api import decode_cursor, encode_cursor, history_window
from marketdata.config import Settings
from marketdata.domain import DataStatus, Instrument, ProviderInstrument, Tick, from_ns, normalize
from marketdata.registry import Registry, parse_contract_month

NOW = datetime(2026, 10, 7, tzinfo=UTC)


def test_decimal_and_serialization(pair):
    tick = normalize(*pair, NOW, last=Decimal("0.12345678901234567890123456789"))
    assert Tick.model_validate_json(tick.model_dump_json()).last == tick.last
    assert '"last":"0.12345678901234567890123456789"' in tick.model_dump_json()


def test_binary_float_input_rejected(pair):
    with pytest.raises(ValidationError, match="Decimal"):
        normalize(*pair, NOW, last=0.1)


def test_missing_upstream_timestamp_stays_null(pair):
    tick = normalize(*pair, NOW, bid=Decimal("1"))
    assert tick.event_timestamp is None
    assert tick.storage_timestamp == NOW


def test_timestamp_normalizes_timezone(pair):
    tick = normalize(*pair, datetime.fromisoformat("2026-10-07T03:30:00+03:30"), last="1")
    assert tick.received_timestamp == NOW


@pytest.mark.parametrize("field", ["event_timestamp", "received_timestamp"])
def test_naive_timestamps_rejected(pair, field):
    data = normalize(*pair, NOW, last="1").model_dump()
    data[field] = datetime(2026, 10, 7)
    with pytest.raises(ValidationError, match="timezone-aware"):
        Tick.model_validate(data)


def test_nanoseconds_preserved(pair):
    ns = 1791331200123456789
    tick = normalize(*pair, NOW, last="1", event_timestamp=from_ns(ns), event_timestamp_ns=ns)
    assert tick.event_timestamp.microsecond == 123456
    assert tick.event_timestamp_ns == ns
    with pytest.raises(ValidationError, match="match"):
        normalize(*pair, NOW, last="1", event_timestamp=NOW, event_timestamp_ns=ns)


@pytest.mark.parametrize("price", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_prices_rejected(pair, price):
    with pytest.raises(ValidationError):
        normalize(*pair, NOW, last=price)


def test_negative_futures_prices_allowed(pair):
    assert normalize(*pair, NOW, last="-37.63").last == Decimal("-37.63")


def test_mock_cannot_claim_realtime(pair):
    with pytest.raises(ValidationError, match="simulated"):
        ProviderInstrument(
            provider="mock", provider_symbol="TEST", realtime_status=DataStatus.REALTIME
        )
    with pytest.raises(ValidationError, match="simulated"):
        Tick.model_validate(
            dict(normalize(*pair, NOW, last="1").model_dump(), realtime_status="realtime")
        )


def test_future_requires_concrete_contract(registry):
    future = next(
        e.instrument for e in registry.entries if e.instrument.instrument_type == "future"
    )
    with pytest.raises(ValidationError, match="concrete"):
        Instrument.model_validate(dict(future.model_dump(), contract_code=None))
    with pytest.raises(ValidationError, match="aliases"):
        Instrument.model_validate(dict(future.model_dump(), canonical_symbol="FUTURES:CL:FRONT"))


def test_registry_identity_and_ambiguity(registry):
    first = registry.entries[0]
    assert (
        Instrument.model_validate_json(first.instrument.model_dump_json()).id == first.instrument.id
    )
    with pytest.raises(ValueError, match="duplicate"):
        Registry([first, first])


def test_roll_is_before_expiry_and_preserves_contracts(registry):
    at = datetime(2027, 12, 9, tzinfo=UTC)
    front = registry.resolve("SIM", "CL", at)
    following = registry.resolve("SIM", "CL", at, next_contract=True)
    assert front.contract_month.month == 12
    assert following.contract_month.month == 3
    assert registry.resolve("SIM", "CL", front.roll_at).id == following.id
    assert front.id != following.id
    assert len(registry.active_contracts("SIM", "CL", front.roll_at)) == 2


def test_roll_requires_explicit_policy(registry):
    future = next(e for e in registry.entries if e.instrument.root_symbol == "CL")
    entry = future.model_copy(
        update={"instrument": future.instrument.model_copy(update={"roll_at": None})}
    )
    with pytest.raises(ValueError, match="explicit"):
        Registry([entry]).resolve("SIM", "CL", NOW)
    with pytest.raises(LookupError):
        registry.resolve("SIM", "UNKNOWN", NOW)


def test_contract_parser_does_not_guess_century():
    assert parse_contract_month("GCZ2027") == ("GC", 2027, 12)
    with pytest.raises(ValueError):
        parse_contract_month("GCZ7")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"queue_capacity": 0},
        {"batch_size": 101, "queue_capacity": 100},
        {"db_pool_min": 11, "db_pool_max": 10},
        {"reconnect_min_seconds": 70},
        {"binance_ws_url": "ws://example.com"},
        {"database_url": "sqlite://:memory:"},
    ],
)
def test_configuration_validation(kwargs):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **kwargs)


def test_selection_never_falls_back_to_mock(registry):
    settings = Settings(_env_file=None, market_data_provider_crypto="binance")
    with pytest.raises(ValueError, match="missing binance"):
        settings.selected_registry(registry)


def test_history_cursor_and_window(pair):
    tick = normalize(*pair, NOW, last="1")
    assert decode_cursor(encode_cursor(tick)) == (NOW, tick.tick_id)
    assert history_window(NOW, NOW + timedelta(hours=1), 1)[0] == NOW
    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        decode_cursor("invalid")
    with pytest.raises(HTTPException):
        history_window(NOW.replace(tzinfo=None), NOW, 1)
