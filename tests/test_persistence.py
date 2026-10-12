from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from marketdata.domain import ProviderInstrument, RegistryEntry, normalize
from marketdata.registry import Registry

pytestmark = pytest.mark.integration
NOW = datetime(2026, 10, 7, tzinfo=UTC)


async def test_copy_atomic_latest_and_out_of_order_history(store, pair):
    instrument, mapping = pair
    newest = normalize(
        instrument,
        mapping,
        NOW + timedelta(seconds=2),
        event_timestamp=NOW + timedelta(seconds=2),
        last="200.1234567890123456789",
    )
    oldest = normalize(instrument, mapping, NOW, event_timestamp=NOW, last="100")
    assert await store.write([newest, oldest]) == 2
    latest = await store.latest(instrument.canonical_symbol, "mock", 10)
    assert latest == [newest]
    assert latest[0].last == Decimal("200.1234567890123456789")
    historical = await store.ticks(instrument.id, "mock", NOW, NOW + timedelta(hours=1), 10, None)
    assert historical == [oldest, newest]
    page = await store.ticks(
        instrument.id,
        "mock",
        NOW,
        NOW + timedelta(hours=1),
        1,
        (oldest.storage_timestamp, oldest.tick_id),
    )
    assert page == [newest]


async def test_retry_is_idempotent_but_equal_prices_are_preserved(store, pair):
    first = normalize(*pair, NOW, event_timestamp=NOW, last="100")
    second = normalize(*pair, NOW, event_timestamp=NOW, last="100")
    assert await store.write([first, first, second]) == 2
    assert await store.write([first, second]) == 0
    assert len(await store.ticks(pair[0].id, None, NOW, NOW + timedelta(hours=1), 10, None)) == 2


async def test_latest_is_provider_isolated(store, pair):
    instrument, mock = pair
    live = ProviderInstrument(
        provider="test_vendor", provider_symbol="BTCUSDT", realtime_status="unknown"
    )
    await store.register(Registry([RegistryEntry(instrument=instrument, mappings=[mock, live])]))
    ticks = [normalize(instrument, m, NOW, last="100") for m in (mock, live)]
    await store.write(ticks)
    latest = await store.latest(instrument.canonical_symbol, None, 10)
    assert {t.provider for t in latest} == {"mock", "test_vendor"}
    assert (await store.instrument(instrument.canonical_symbol))["mappings"]


async def test_nanosecond_ordering_controls_latest(store, pair):
    ns = 1791331200000000000
    from marketdata.domain import from_ns

    later = normalize(
        *pair, NOW, event_timestamp=from_ns(ns + 2), event_timestamp_ns=ns + 2, last="2"
    )
    earlier = normalize(
        *pair,
        NOW + timedelta(seconds=1),
        event_timestamp=from_ns(ns + 1),
        event_timestamp_ns=ns + 1,
        last="1",
    )
    await store.write([later, earlier])
    assert await store.latest(pair[0].canonical_symbol, "mock", 10) == [later]


@pytest.mark.parametrize("aggregates", [True, False])
async def test_candles_trade_volume_mid_and_all_intervals(store, pair, aggregates):
    if aggregates and not store.has_aggregates:
        pytest.skip("Timescale aggregates not enabled")
    store.has_aggregates = aggregates
    ticks = [
        normalize(
            *pair,
            NOW + timedelta(milliseconds=offset),
            event_timestamp=NOW + timedelta(milliseconds=offset),
            last=price,
            last_size="2",
            bid=str(int(price) - 1),
            ask=str(int(price) + 1),
        )
        for offset, price in [(100, "100"), (200, "105"), (300, "99"), (400, "102")]
    ]
    await store.write(list(reversed(ticks)))
    if aggregates:
        # Materialized historical corrections are eventually consistent by policy.
        # Force refresh so reused disposable DBs cannot serve a previous test's bucket.
        async with store.connection() as c:
            for suffix in ("1s", "1m", "5m", "15m", "1h", "1d"):
                await c.execute(
                    f"CALL refresh_continuous_aggregate('candles_{suffix}_last', "
                    "$1::timestamptz, $2::timestamptz, force => true)",
                    NOW,
                    NOW + timedelta(days=1),
                )
            await c.execute(
                "CALL refresh_continuous_aggregate('candles_1m_mid', "
                "$1::timestamptz, $2::timestamptz, force => true)",
                NOW,
                NOW + timedelta(days=1),
            )
    for interval in ("1s", "1m", "5m", "15m", "1h", "1d"):
        candles = await store.candles(
            pair[0].id, "mock", NOW, NOW + timedelta(days=1), interval, "last", 100
        )
        candle = candles[0]
        assert [candle[k] for k in ("open", "high", "low", "close")] == [100, 105, 99, 102]
        assert candle["volume"] == 8
        assert candle["observations"] == 4
    quote = (
        await store.candles(pair[0].id, "mock", NOW, NOW + timedelta(days=1), "1m", "mid", 100)
    )[0]
    assert quote["close"] == 102
    assert quote["volume"] is None


async def test_batch_failure_rolls_back_history_and_latest(store, pair):
    good = normalize(*pair, NOW, event_timestamp=NOW, last="1")
    from uuid import uuid4

    bad = good.model_copy(update={"provider": "unregistered", "tick_id": uuid4()})
    import asyncpg

    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await store.write([good, bad])
    assert await store.latest(pair[0].canonical_symbol, None, 10) == []
    assert await store.ticks(pair[0].id, None, NOW, NOW + timedelta(hours=1), 10, None) == []


async def test_canonical_identity_cannot_be_reassigned(store, pair):
    i, m = pair
    with pytest.raises(ValueError, match="identity"):
        await store.register(
            Registry(
                [RegistryEntry(instrument=i.model_copy(update={"exchange": "OTHER"}), mappings=[m])]
            )
        )
    assert (await store.instrument(i.canonical_symbol))["exchange"] == i.exchange


@pytest.mark.parametrize("aggregates", [True, False])
async def test_candle_ties_are_deterministic_after_refresh(store, pair, aggregates):
    from uuid import UUID

    if aggregates and not store.has_aggregates:
        pytest.skip("Timescale aggregates not enabled")
    store.has_aggregates = aggregates
    first = normalize(*pair, NOW, tick_id=UUID(int=1), event_timestamp=NOW, last="1")
    last = normalize(*pair, NOW, tick_id=UUID(int=2), event_timestamp=NOW, last="2")
    await store.write([last, first])
    if aggregates:
        async with store.connection() as c:
            await c.execute(
                "CALL refresh_continuous_aggregate('candles_1m_last', "
                "$1::timestamptz, $2::timestamptz)",
                NOW,
                NOW + timedelta(minutes=1),
            )
    candle = (
        await store.candles(pair[0].id, "mock", NOW, NOW + timedelta(minutes=1), "1m", "last", 10)
    )[0]
    assert candle["open"] == 1 and candle["close"] == 2


@pytest.mark.parametrize("aggregates", [True, False])
async def test_candle_pagination_preserves_multiple_statuses_in_same_bucket(
    store, pair, aggregates
):
    from marketdata.domain import DataStatus

    if aggregates and not store.has_aggregates:
        pytest.skip("Timescale aggregates not enabled")
    store.has_aggregates = aggregates
    i, _ = pair
    mapping = ProviderInstrument(
        provider="test_vendor", provider_symbol="TEST", realtime_status="unknown"
    )
    await store.register(Registry([RegistryEntry(instrument=i, mappings=[mapping])]))
    ticks = [
        normalize(
            i,
            mapping.model_copy(update={"realtime_status": status}),
            NOW,
            event_timestamp=NOW,
            last="1",
        )
        for status in (DataStatus.DELAYED, DataStatus.REALTIME)
    ]
    await store.write(ticks)
    if aggregates:
        async with store.connection() as c:
            await c.execute(
                "CALL refresh_continuous_aggregate('candles_1m_last', "
                "$1::timestamptz, $2::timestamptz, force => true)",
                NOW,
                NOW + timedelta(minutes=1),
            )
    first = await store.candles(
        i.id, "test_vendor", NOW, NOW + timedelta(minutes=1), "1m", "last", 1
    )
    assert first[0]["realtime_status"] == "delayed"
    second = await store.candles(
        i.id,
        "test_vendor",
        NOW,
        NOW + timedelta(minutes=1),
        "1m",
        "last",
        1,
        after_status="delayed",
    )
    assert second[0]["realtime_status"] == "realtime"
