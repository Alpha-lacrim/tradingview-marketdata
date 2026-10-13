"""Business identity, conservative volume and source-specific reliability accounting."""

import importlib.util
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from test_native_candles import NOW, captured, make_candle, source, vectors

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.observability import Metrics
from marketdata.providers.tradingview import TradingViewGuestProvider
from marketdata.providers.tradingview_candles import parse_bar
from marketdata.registry import Registry

BUSINESS = Path("config/instruments.tradingview-business-proposal.json")


def test_business_sources_preserve_existing_ids_and_both_broker_provenances():
    registry = Registry.load(BUSINESS)
    original = Registry.load(Path("config/instruments.tradingview-256.json"))
    pairs = registry.subscriptions("tradingview_guest")
    binance = [p for p in pairs if p[1].provider_symbol.startswith("BINANCE:")]
    brokers = [p for p in pairs if p[0].asset_class == "forex"]
    assert len(pairs) == 99 and len(binance) == 50 and len(brokers) == 42
    assert all(i.instrument_type == "spot" and i.quote_asset == "USDT" for i, _ in binance)
    for i, m in pairs:
        previous = original.by_provider.get((m.provider, m.provider_symbol))
        if previous:
            assert i.id == previous[0].id and i == previous[0]
    assert registry.by_symbol["FX:OANDA:EURUSD"].id != registry.by_symbol["FX:FXCM:EURUSD"].id
    assert (
        registry.by_provider["tradingview_guest", "FX:EURUSD"][0].canonical_symbol
        == "FX:FXCM:EURUSD"
    )
    assert all("!" not in m.provider_symbol for _, m in pairs)


@pytest.mark.parametrize(
    "wrong", [None, "exchange", "currency_code", "update_mode", "type", "symbol"]
)
async def test_fxcm_quote_namespace_normalization_keeps_identity_and_delay_gates(wrong):
    registry = Registry.load(BUSINESS)
    pair = registry.by_provider["tradingview_guest", "FX:EURUSD"]
    provider = TradingViewGuestProvider(Settings(_env_file=None), Metrics())
    await provider.subscribe([pair])
    # Metadata matches captured FXCM identity; prices are deterministic test inputs.
    values = dict(
        type="forex",
        exchange="FXCM",
        currency_code="USD",
        update_mode="streaming",
        lp="1.2",
        lp_time=str(int(NOW.timestamp())),
    )
    if wrong and wrong != "symbol":
        values[wrong] = "delayed" if wrong == "update_mode" else "OTHER"
    tick = provider.quote_tick(
        dict(n="FX:GBPUSD" if wrong == "symbol" else "FX:EURUSD", s="ok", v=values), NOW
    )
    if wrong:
        assert tick is None
        if wrong != "symbol":
            assert provider.rejected
    else:
        assert tick.provider_symbol == "FX:EURUSD" and tick.canonical_symbol == "FX:FXCM:EURUSD"
        assert tick.metadata["upstream"]["exchange"] == "FXCM"


@pytest.mark.parametrize("raw", [Decimal("300.123456789123456789"), None, Decimal("NaN")])
def test_unclassified_source_volume_is_preserved_as_evidence_not_typed_or_additive(raw):
    row = captured()[0]
    pair = Registry.load(Path("config/instruments.tradingview.json")).by_provider[
        "tradingview_guest", row["symbol"]
    ]
    info = dict(source(row))
    info.pop("volume_type", None)
    values = list(vectors(row)[0]["v"])
    values[5] = raw
    candle = parse_bar(values, pair, info, NOW, "streaming")
    assert candle.volume is None and candle.metadata["volume_additive"] is False
    if raw is not None and raw.is_finite():
        assert candle.metadata["source_volume_raw"] == str(raw)
        assert candle.metadata["source_volume_classification"] == "unknown"
    else:
        assert "source_volume_raw" not in candle.metadata


@pytest.mark.integration
async def test_two_broker_candles_and_volume_remain_independent_in_postgresql(store):
    registry = Registry.load(BUSINESS)
    pairs = [registry.by_provider["tradingview_guest", s] for s in ("OANDA:EURUSD", "FX:EURUSD")]
    await store.register(
        Registry([e for e in registry.entries if e.instrument.id in {i.id for i, _ in pairs}]),
        candle_providers=["tradingview_guest"],
    )
    start = utc_now().replace(second=0, microsecond=0) + timedelta(minutes=1)
    end = start + timedelta(minutes=2)
    plans = [
        (i.id, m.provider, start, end, dict(session="24x7", timezone="Etc/UTC")) for i, m in pairs
    ]
    a = make_candle(
        pairs[0],
        start=start,
        received_timestamp=end + timedelta(seconds=5),
        open="1.1",
        high="1.2",
        low="1.0",
        close="1.15",
        volume="10",
        metadata=dict(volume_type="tick", volume_additive=True),
    )
    b = make_candle(
        pairs[1],
        start=start + timedelta(minutes=1),
        received_timestamp=end + timedelta(seconds=5),
        open="1.2",
        high="1.3",
        low="1.1",
        close="1.25",
        volume=None,
        metadata=dict(
            source_volume_raw="20", source_volume_classification="unknown", volume_additive=False
        ),
    )
    assert await store.write_candles([a, b], plans) == 2
    rows = [
        await store.native_candles(i.id, "tradingview_guest", start, end, "1m", 100)
        for i, _ in pairs
    ]
    assert rows[0][0]["close"] == Decimal("1.15") and rows[1][0]["close"] == Decimal("1.25")
    assert rows[0][0]["volume"] == Decimal("10") and rows[1][0]["volume"] is None
    async with store.connection() as connection:
        metadata = await connection.fetchval(
            "SELECT metadata FROM market_candles WHERE instrument_id=$1 AND bucket_start=$2",
            pairs[1][0].id,
            b.bucket_start,
        )
    assert json.loads(metadata)["source_volume_raw"] == "20"
    before = [await store.candle_progress(i.id, m.provider) for i, m in pairs]
    assert [p["missing_count"] for p in before] == [1, 1]
    assert before[0]["missing_ranges"] != before[1]["missing_ranges"]
    recovered = a.model_copy(
        update=dict(
            bucket_start=start + timedelta(minutes=1),
            bucket_end=end,
            received_timestamp=end + timedelta(seconds=6),
        )
    )
    await store.write_candles([recovered], plans)
    after = [await store.candle_progress(i.id, m.provider) for i, m in pairs]
    assert [p["missing_count"] for p in after] == [0, 1]
    assert [p["coverage_start"] for p in after] == [p["coverage_start"] for p in before]


def test_business_accounting_excludes_closures_and_keeps_unknown_and_source_absence_visible():
    spec = importlib.util.spec_from_file_location(
        "business_report", "scripts/report_business_qualification.py"
    )
    module = importlib.util.module_from_spec(spec)
    # Its companion module is already loaded by the scaling accounting test in a full suite;
    # expose scripts for this standalone test as well.
    import sys

    sys.path.insert(0, str(Path("scripts").resolve()))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    bucket = NOW.replace(second=0).isoformat()
    closed = (NOW.replace(second=0) + timedelta(minutes=1)).isoformat()
    candidate = dict(category="OANDA")
    attempt = dict(
        request_success=True,
        session_info=dict(session="test"),
        classifications=dict(EXPECTED=[bucket], NOT_EXPECTED=[closed], UNKNOWN=[]),
        native_buckets=[],
        stored_buckets=[],
        native_volumes=[],
        commit_acknowledged_at=NOW.isoformat(),
        exact_native_db_readback=True,
        db_error=None,
        attempt=1,
        source_info=dict(volume_type="tick"),
        source_status="streaming",
    )
    result = module.measure(candidate, dict(passes=[attempt], quote=None))
    assert (
        result["expected_test_minutes"] == 1 and result["not_expected_market_closed_minutes"] == 1
    )
    assert result["unresolved_expected_gaps"] == result["source_absent_observations"] == 1
    assert result["eventual_completeness_percentage"] == 0 and result["transport_failures"] == 0
    untouched = module.measure(candidate, {})
    assert untouched["expected_test_minutes"] is None and untouched["unknown_session_minutes"] == 60
    assert untouched["eventual_completeness_percentage"] is None
