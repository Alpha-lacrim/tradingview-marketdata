"""Injected observations only; source schemas live in uniquely owned disposable DBs."""

import asyncio
import json
import os
import subprocess
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
from test_destination_migrations import migrate
from test_native_candles import make_candle

from marketdata.config import Settings
from marketdata.domain import Tick, normalize, utc_now
from marketdata.persistence import Store
from marketdata.price_selection import observation_age
from marketdata.registry import Registry
from marketdata.source_capture import (
    CaptureBackpressure,
    CaptureConflict,
    SourceCapture,
    field_observation,
    merge_fields,
)


async def extend(url):
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-m", "alembic", "-c", "alembic-source.ini", "upgrade", "head"],
        env=dict(os.environ, DATABASE_URL=url),
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.fixture
async def source(store, registry):
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    database = "m3a_source_" + uuid4().hex + "_test"
    url = urlunsplit(parsed._replace(path="/" + database))
    async with store.connection() as c:
        await c.execute(f'CREATE DATABASE "{database}"')
    local = Store(
        Settings(
            _env_file=None,
            database_url=url,
            application_role="collector",
            source_capture_enabled=True,
        )
    )
    try:
        args = ["-x", "timescale=true"] if store.has_aggregates else []
        await migrate(url, *args, "upgrade", "0006")
        await extend(url)
        await local.connect()
        await local.register(registry)
        yield local, SourceCapture(local)
    finally:
        await local.close()
        async with store.connection() as c:
            await c.execute(f'DROP DATABASE "{database}" WITH (FORCE)', timeout=60)


def quote(pair, when=None, **fields):
    now = when or utc_now()
    return normalize(*pair, now, event_timestamp=now, **fields)


@pytest.mark.integration
async def test_consumer_cleanup_terminates_busy_connection_and_releases_lock(source):
    local, capture = source
    manager = capture.consumer_lock()
    connection = await manager.__aenter__()
    pid = connection.get_server_pid()
    sleeper = asyncio.create_task(connection.execute("SELECT pg_sleep(20)"))
    try:
        async with local.connection() as observer, asyncio.timeout(3):
            # Poll the actual PostgreSQL wait state, not an in-process notification.
            while not await observer.fetchval(  # noqa: ASYNC110
                "SELECT wait_event='PgSleep' FROM pg_stat_activity WHERE pid=$1", pid
            ):
                await asyncio.sleep(0.01)
        async with asyncio.timeout(3):
            await manager.__aexit__(None, None, None)
        result = await asyncio.gather(sleeper, return_exceptions=True)
        assert isinstance(result[0], (asyncpg.PostgresError, asyncpg.InterfaceError))
        async with capture.consumer_lock() as replacement:
            assert replacement.get_server_pid() != pid
            assert await replacement.fetchval("SELECT head FROM source_capture_clock") == 0
    finally:
        sleeper.cancel()
        await asyncio.gather(sleeper, return_exceptions=True)


def test_capture_requires_opt_in_collector_and_partial_fields_keep_original_clocks(pair):
    with pytest.raises(ValueError, match="collector"):
        Settings(_env_file=None, source_capture_enabled=True)
    old = quote(pair, last="101.123456789012345678901", bid="100")
    fields = {}
    assert merge_fields(fields, old)
    update = quote(pair, old.received_timestamp + timedelta(seconds=1), ask="102")
    assert merge_fields(fields, update)
    assert fields["last"]["value"] == "101.123456789012345678901"
    assert fields["last"]["received_timestamp"] == old.received_timestamp.isoformat()
    assert not merge_fields(fields, old)
    assert (
        field_observation(
            old.model_copy(update={"metadata": {"timestamped_fields": ["last"]}}), "bid"
        )["event_timestamp"]
        is None
    )


@pytest.mark.integration
async def test_candle_atomic_idempotent_historical_correction_late_arrival(source, pair):
    local, capture = source
    first = make_candle(pair, realtime_status="simulated")
    assert await local.write_candles([first], []) == 1
    original = await capture.page()
    assert original["after"] == 0 and len(original["records"]) == 1
    record = original["records"][0]
    assert record["observation"]["open"] == str(first.open)
    assert record["observation"][
        "received_timestamp"
    ] == first.received_timestamp.isoformat().replace("+00:00", "Z")
    assert await local.write_candles([first], []) == 0
    later = first.model_copy(
        update={"received_timestamp": first.received_timestamp + timedelta(seconds=3)}
    )
    assert await local.write_candles([later], []) == 0
    stale = first.model_copy(
        update={
            "close": first.open,
            "received_timestamp": first.received_timestamp + timedelta(seconds=2),
        }
    )
    assert await local.write_candles([stale], []) == 0
    corrected = first.model_copy(
        update={
            "close": first.open,
            "received_timestamp": first.received_timestamp + timedelta(seconds=4),
        }
    )
    assert await local.write_candles([corrected], []) == 1
    late = make_candle(
        pair, start=first.bucket_start - timedelta(days=10), realtime_status="simulated"
    )
    assert await local.write_candles([late], []) == 1
    records = (await capture.page())["records"]
    assert len(records) == 3 and records[0] == record
    assert list((await capture.page())["event_kinds"].values()) == [
        "candle_completed",
        "candle_corrected",
        "candle_completed",
    ]
    assert len({r["observation_id"] for r in records}) == 3
    async with local.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 3
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") == 1
        assert await c.fetchval("SELECT pending_rows FROM source_capture_clock") == 3


@pytest.mark.integration
async def test_candle_and_quote_rollback_then_retry(source, pair):
    local, capture = source
    bar = make_candle(pair, realtime_status="simulated")
    tick = quote(pair, last="100")
    with pytest.raises(RuntimeError, match="injected"):
        async with local.connection() as c, c.transaction():
            await local.write_candles_in_transaction(
                c, [bar], [(bar.instrument_id, bar.provider, bar.bucket_start, bar.bucket_end)]
            )
            await local.write_observations_in_transaction(c, [tick], history=False)
            raise RuntimeError("injected before commit")
    async with local.connection() as c:
        for table in (
            "market_candles",
            "latest_candles",
            "latest_prices",
            "source_quote_state",
            "candle_revisions",
            "source_candle_exports",
            "source_candle_outbox",
            "marketdata_events",
            "candle_observation_heads",
            "candle_coverage",
        ):
            assert await c.fetchval(f"SELECT count(*) FROM {table}") == 0
        assert await c.fetchval("SELECT head FROM source_capture_clock") == 0
    await local.write_candles([bar], [])
    await local.write_quotes([tick])
    assert len((await capture.page())["records"]) == 1
    assert len((await capture.pending_quotes())["records"]) == 1


@pytest.mark.integration
@pytest.mark.parametrize("rollback", [False, True])
async def test_commit_order_clock_cannot_skip_invisible_earlier_transaction(
    source, registry, rollback
):
    local, capture = source
    a, b = registry.subscriptions("mock")[:2]
    entered = asyncio.Event()
    release = asyncio.Event()

    async def first():
        try:
            async with local.connection() as c, c.transaction():
                await local.write_candles_in_transaction(
                    c, [make_candle(a, realtime_status="simulated")], []
                )
                entered.set()
                await release.wait()
                if rollback:
                    raise RuntimeError("rollback")
        except RuntimeError:
            if not rollback:
                raise

    t1 = asyncio.create_task(first())
    await asyncio.wait_for(entered.wait(), 5)
    t2 = asyncio.create_task(local.write_candles([make_candle(b, realtime_status="simulated")], []))
    try:
        await asyncio.sleep(0.1)
        assert not t2.done()  # Different source lock; blocked on the capture clock.
        page = await capture.page()
        assert page["records"] == [] and page["through"] == 0
        release.set()
        await asyncio.wait_for(asyncio.gather(t1, t2), 10)
        page = await capture.page()
        positions = [int(r["source_position"]) for r in page["records"]]
        assert positions == ([1] if rollback else [1, 2])
        await capture.acknowledge_candles(page["source_epoch"], page["after"], page["through"])
        assert (await capture.page())["records"] == []
    finally:
        release.set()
        await asyncio.gather(t1, t2, return_exceptions=True)


@pytest.mark.integration
async def test_overlapping_concurrent_corrections_never_regress(source, pair):
    local, capture = source
    bar = make_candle(pair, realtime_status="simulated")
    await local.write_candles([bar], [])
    bars = [
        bar.model_copy(
            update={
                "close": Decimal("100") + Decimal(i) / 100,
                "received_timestamp": bar.received_timestamp + timedelta(seconds=i),
            }
        )
        for i in range(1, 21)
    ]
    await asyncio.gather(*(local.write_candles([b], []) for b in reversed(bars)))
    async with local.connection() as c:
        last = await c.fetchval("SELECT close FROM market_candles")
        assert last == bars[-1].close
    records = (await capture.page())["records"]
    assert records[-1]["observation"]["close"] == str(bars[-1].close)


@pytest.mark.integration
async def test_latest_partial_fields_ack_race_restart_and_raw_history(source, pair):
    local, capture = source
    tick = quote(pair, last="101.123456789012345678901", bid="100")
    await local.write_quotes([tick])
    old = await capture.pending_quotes()
    old_record = old["records"][0]
    newer = quote(pair, tick.received_timestamp + timedelta(seconds=1), ask="102")
    await local.write_quotes([newer])
    pending = await capture.pending_quotes()
    rec = pending["records"][0]
    assert rec["observation"]["last"] == str(tick.last)
    assert rec["observation"]["bid"] == "100" and rec["observation"]["ask"] == "102"
    assert rec["observation"]["metadata"]["field_observations"]["last"]["observation_id"] == str(
        tick.tick_id
    )
    assert not await capture.acknowledge_quote(
        old["source_epoch"], tick.instrument_id, tick.provider, int(old_record["source_position"])
    )
    assert (await capture.pending_quotes())["records"] == [rec]
    await local.write_quotes([tick])  # Old observation cannot undo the new field.
    assert (await capture.pending_quotes())["records"] == [rec]
    await local.close()
    await local.connect()
    assert (await capture.pending_quotes()) == pending
    assert await capture.acknowledge_quote(
        pending["source_epoch"], tick.instrument_id, tick.provider, int(rec["source_position"])
    )
    assert not (await capture.pending_quotes())["records"]
    # History remains original deltas; snapshot assembly never invents raw ticks.
    history = quote(pair, newer.received_timestamp + timedelta(seconds=1), last="101")
    await local.write([history])
    async with local.connection() as c:
        raw = Tick.model_validate_json(await c.fetchval("SELECT payload FROM market_ticks"))
        assert raw == history and raw.ask is None
        assert await c.fetchval("SELECT count(*) FROM source_quote_state") == 1


@pytest.mark.integration
async def test_old_quote_source_time_and_nanosecond_order(source, pair):
    local, capture = source
    now = utc_now()
    ns = int(now.timestamp()) * 1000000000
    tick = normalize(
        *pair,
        now,
        event_timestamp=now.replace(microsecond=0),
        event_timestamp_ns=ns + 1,
        last="101",
    )
    await local.write_quotes([tick])
    later_received_old = tick.model_copy(
        update={
            "tick_id": uuid4(),
            "last": Decimal("99"),
            "received_timestamp": now + timedelta(seconds=1),
            "event_timestamp_ns": ns,
        }
    )
    await local.write_quotes([later_received_old])
    no_ns_old = later_received_old.model_copy(
        update={"tick_id": uuid4(), "event_timestamp_ns": None}
    )
    await local.write_quotes([no_ns_old])
    record = (await capture.pending_quotes())["records"][0]
    assert record["observation"]["last"] == "101"
    assert record["observation"]["event_timestamp_ns"] == str(ns + 1)


@pytest.mark.integration
async def test_bounded_storage_backpressure_cleanup_and_safe_cursors(source, pair):
    local, capture = source
    local.settings.source_capture_max_rows = 1
    first = make_candle(pair, realtime_status="simulated")
    await local.write_candles([first], [])
    second = make_candle(
        pair, start=first.bucket_start - timedelta(minutes=1), realtime_status="simulated"
    )
    with pytest.raises(CaptureBackpressure):
        await local.write_candles([second], [])
    async with local.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1
    assert await capture.cleanup() == 0
    page = await capture.page()
    with pytest.raises(CaptureConflict):
        await capture.acknowledge_candles(uuid4(), 0, page["through"])
    await capture.acknowledge_candles(page["source_epoch"], 0, page["through"])
    await capture.acknowledge_candles(page["source_epoch"], 0, page["through"])
    assert await capture.cleanup() == 1
    await local.write_candles([second], [])
    assert len((await capture.page())["records"]) == 1
    with pytest.raises(CaptureConflict):
        await capture.acknowledge_candles(page["source_epoch"], 0, 2)
    local.settings.source_capture_max_bytes = 1024
    with pytest.raises(CaptureBackpressure):
        await local.write_candles(
            [
                make_candle(
                    pair, start=first.bucket_start - timedelta(days=1), realtime_status="simulated"
                )
            ],
            [],
        )


@pytest.mark.integration
async def test_quote_coalescing_is_bounded_not_tick_history(source, pair):
    local, capture = source
    now = utc_now()
    for i in range(100):
        await local.write_quotes([quote(pair, now + timedelta(microseconds=i), last=str(100 + i))])
    records = (await capture.pending_quotes())["records"]
    assert len(records) == 1 and records[0]["observation"]["last"] == "199"
    async with local.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0
        assert await c.fetchval("SELECT count(*) FROM source_quote_state") == 1


@pytest.mark.integration
async def test_seed_old_history_without_a_timestamp_cursor_and_quotes(source, pair):
    local, capture = source
    bar = make_candle(pair, realtime_status="simulated")
    await local.write_candles([bar], [])
    # Simulate representative pre-extension history by removing capture-only metadata.
    async with local.connection() as c, c.transaction():
        await c.execute("TRUNCATE source_candle_outbox,source_candle_exports")
        await c.execute("UPDATE source_capture_clock SET head=0,pending_rows=0,pending_bytes=0")
    assert await capture.seed_candles(bar.instrument_id, bar.provider) == 1
    assert list((await capture.page())["event_kinds"].values()) == ["history_seed"]
    assert await capture.seed_candles(bar.instrument_id, bar.provider) == 0
    # A correction immediately after seeding becomes a distinct later event.
    corrected = bar.model_copy(
        update={
            "close": bar.open,
            "received_timestamp": bar.received_timestamp + timedelta(seconds=1),
        }
    )
    await local.write_candles([corrected], [])
    assert len((await capture.page())["records"]) == 2
    await local.write_quotes([quote(pair, last="100")])
    async with local.connection() as c:
        await c.execute("TRUNCATE source_quote_state")
    assert await capture.seed_quotes() == 1
    assert await capture.seed_quotes() == 0


@pytest.mark.integration
@pytest.mark.parametrize("base", ["0004", "0006"])
async def test_representative_source_upgrade_preserves_rows_without_destination_tables(
    store, registry, pair, base
):
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    database = "m3a_upgrade_" + uuid4().hex + "_test"
    url = urlunsplit(parsed._replace(path="/" + database))
    async with store.connection() as c:
        await c.execute(f'CREATE DATABASE "{database}"')
    local = None
    try:
        args = ["-x", "timescale=true"] if store.has_aggregates else []
        await migrate(url, *args, "upgrade", base)
        c = await asyncpg.connect(url)
        # Use the common registration implementation without bypassing startup in production.
        seed = Store(Settings(_env_file=None, database_url=url))
        seed.pool = await asyncpg.create_pool(url, min_size=1, max_size=2)
        await seed.register(registry)
        bar = make_candle(pair, realtime_status="simulated")
        await c.execute(
            """INSERT INTO market_candles(instrument_id,provider,provider_symbol,
            interval,bucket_start,bucket_end,open,high,low,close,volume,price_basis,
            realtime_status,source_status,received_timestamp,metadata,payload)
            VALUES ($1,$2,$3,'1m',$4,$5,$6,$7,$8,$9,$10,'native','simulated','streaming',
                $11,$12::jsonb,$13::jsonb)""",
            bar.instrument_id,
            bar.provider,
            bar.provider_symbol,
            bar.bucket_start,
            bar.bucket_end,
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.received_timestamp,
            json.dumps(bar.metadata),
            bar.model_dump_json(),
        )
        before = await c.fetch("SELECT * FROM market_candles")
        newer_receipt = bar.received_timestamp + timedelta(seconds=3)
        await c.execute(
            """INSERT INTO latest_candles
            (instrument_id,provider,price_basis,bucket_start,received_timestamp,payload)
            VALUES ($1,$2,'native',$3,$4,$5::jsonb)""",
            bar.instrument_id,
            bar.provider,
            bar.bucket_start,
            newer_receipt,
            bar.model_copy(update={"received_timestamp": newer_receipt}).model_dump_json(),
        )
        before_latest = await c.fetch("SELECT * FROM latest_candles")
        await seed.close()
        await c.close()
        await migrate(url, "upgrade", "0006")
        await extend(url)
        local = Store(
            Settings(
                _env_file=None,
                database_url=url,
                application_role="collector",
                source_capture_enabled=True,
            )
        )
        await local.connect()
        async with local.connection() as c:
            assert await c.fetch("SELECT * FROM market_candles") == before
            assert await c.fetch("SELECT * FROM latest_candles") == before_latest
            assert (
                await c.fetchval("SELECT received_timestamp FROM candle_observation_heads")
                == newer_receipt
            )
            assert await c.fetchval("SELECT version_num FROM alembic_version") == "0006"
            assert await c.fetchval("SELECT version_num FROM source_capture_version") == "sc0002"
            assert await c.fetchval("SELECT to_regclass('ingest_producers')") is None
        capture = SourceCapture(local)
        assert await capture.seed_candles(bar.instrument_id, bar.provider) == 1
        assert await local.write_candles([bar], []) == 0
        stale_correction = bar.model_copy(
            update={
                "close": bar.open,
                "received_timestamp": bar.received_timestamp + timedelta(seconds=2),
            }
        )
        assert await local.write_candles([stale_correction], []) == 0
    finally:
        if local:
            await local.close()
        async with store.connection() as c:
            await c.execute(f'DROP DATABASE "{database}"')


@pytest.mark.integration
async def test_source_identity_and_broker_separation(source):
    local, capture = source
    registry = Registry.load(Path("config/instruments.tradingview-business-proposal.json"))
    await local.register(registry, candle_providers=["tradingview_guest"])
    pairs = [registry.by_provider["tradingview_guest", s] for s in ("OANDA:EURUSD", "FX:EURUSD")]
    bars = [make_candle(pair) for pair in pairs]
    await local.write_candles(bars, [])
    await local.write_quotes([quote(pair, last="1.1") for pair in pairs])
    records = (await capture.page())["records"]
    assert {r["observation"]["provider_symbol"] for r in records} == {"OANDA:EURUSD", "FX:EURUSD"}
    assert len({r["observation"]["instrument_id"] for r in records}) == 2
    assert len((await capture.pending_quotes())["records"]) == 2
    wrong = bars[0].model_copy(
        update={
            "provider_symbol": "FX:EURUSD",
            "received_timestamp": bars[0].received_timestamp + timedelta(seconds=1),
            "close": bars[0].open,
        }
    )
    with pytest.raises(ValueError, match="identity"):
        await local.write_candles([wrong], [])
    assert (await capture.page())["records"] == records
    wrong_tick = quote(pairs[0], last="1.1").model_copy(update={"exchange": "FX"})
    with pytest.raises(ValueError, match="identity"):
        await local.write_quotes([wrong_tick])


@pytest.mark.integration
async def test_quote_freshness_and_journal_keep_selected_field_clock(source, pair):
    local, capture = source
    now = utc_now()
    last = normalize(*pair, now - timedelta(minutes=10), last="100")
    bid = normalize(*pair, now, bid="99")
    await local.write_quotes([last])
    await local.write_quotes([bid])
    async with local.connection() as c:
        snapshot = Tick.model_validate_json(await c.fetchval("SELECT payload FROM latest_prices"))
        journal = Tick.model_validate_json(
            await c.fetchval("SELECT payload FROM marketdata_events ORDER BY id DESC LIMIT 1")
        )
    for observed in (snapshot, journal):
        assert observation_age(observed, now)[0] >= 600000
        assert observed.event_timestamp is None
        assert observed.last == last.last
    pending = await capture.pending_quotes()
    before = observation_age(snapshot, now)
    await capture.acknowledge_quote(
        pending["source_epoch"],
        last.instrument_id,
        last.provider,
        int(pending["records"][0]["source_position"]),
    )
    assert observation_age(snapshot, now) == before


@pytest.mark.integration
async def test_process_exits_after_commit_then_reopens_same_events(source, pair):
    local, capture = source
    bar = make_candle(pair, realtime_status="simulated")
    child = """
import asyncio, os
from marketdata.persistence import Store
from marketdata.config import Settings
from marketdata.candles import MarketCandle
from marketdata.domain import Tick
async def run():
    store=Store(Settings(_env_file=None, database_url=os.environ['M3A_CHILD_DSN'],
        application_role='collector',source_capture_enabled=True))
    await store.connect()
    await store.write_candles([MarketCandle.model_validate_json(os.environ['M3A_CHILD_BAR'])],[])
    await store.write_quotes([Tick.model_validate_json(os.environ['M3A_CHILD_TICK'])])
    os._exit(71)
asyncio.run(run())
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", child],
        env=dict(
            os.environ,
            M3A_CHILD_DSN=local.settings.database_url.get_secret_value(),
            M3A_CHILD_BAR=bar.model_dump_json(),
            M3A_CHILD_TICK=quote(pair, last="100.123").model_dump_json(),
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 71, result.stderr
    page = await capture.page()
    assert len(page["records"]) == 1
    pending = await capture.pending_quotes()
    assert len(pending["records"]) == 1
    await local.close()
    await local.connect()
    assert await capture.page() == page
    assert await capture.pending_quotes() == pending
    assert await local.write_candles([bar], []) == 0


@pytest.mark.integration
async def test_consumer_lock_release_and_concurrent_quote_versions(source, pair):
    local, capture = source
    async with capture.consumer_lock():
        with pytest.raises(CaptureConflict, match="another local"):
            async with SourceCapture(local).consumer_lock():
                pytest.fail("second consumer must not acquire")
    async with capture.consumer_lock():
        pass
    now = utc_now()
    ticks = [quote(pair, now + timedelta(microseconds=i), last=str(100 + i)) for i in range(20)]
    await asyncio.gather(*(local.write_quotes([t]) for t in reversed(ticks)))
    rec = (await capture.pending_quotes())["records"][0]
    assert rec["observation"]["last"] == "119"


@pytest.mark.integration
async def test_quote_batch_keeps_partial_fields_and_plain_decimals(source, pair):
    local, capture = source
    now = utc_now()
    first = quote(pair, now, last="0.000000001", bid="0.0000000009")
    second = quote(pair, now + timedelta(microseconds=1), ask="0.0000000011")
    await local.write_quotes([first, second])
    record = (await capture.pending_quotes())["records"][0]
    assert record["observation"]["last"] == "0.000000001"
    assert record["observation"]["bid"] == "0.0000000009"
    assert record["observation"]["ask"] == "0.0000000011"
    async with local.connection() as c:
        assert await c.fetchval("SELECT head FROM source_capture_clock") == 1
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1


@pytest.mark.integration
async def test_capture_rejects_oversize_without_partial_financial_commit(source, pair):
    local, capture = source
    bar = make_candle(
        pair, realtime_status="simulated", metadata={"oversize": "x" * 17000, "volume_type": "base"}
    )
    with pytest.raises(CaptureBackpressure, match="metadata"):
        await local.write_candles([bar], [])
    tick = quote(pair, last="100", metadata={"oversize": "x" * 17000})
    with pytest.raises(CaptureBackpressure, match="quote snapshot"):
        await local.write_quotes([tick])
    assert (await capture.page())["records"] == []
    assert (await capture.pending_quotes())["records"] == []
    async with local.connection() as c:
        for table in ("market_candles", "latest_prices", "marketdata_events"):
            assert await c.fetchval(f"SELECT count(*) FROM {table}") == 0


@pytest.mark.integration
async def test_explicit_source_opt_in_and_destination_migration_boundary(source, store):
    local, _ = source
    disabled = Store(
        Settings(_env_file=None, database_url=local.settings.database_url.get_secret_value())
    )
    try:
        with pytest.raises(RuntimeError, match="do not run destination upgrade head"):
            await disabled.connect()
    finally:
        await disabled.close()
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-m", "alembic", "-c", "alembic-source.ini", "upgrade", "head"],
        env=dict(os.environ, DATABASE_URL=store.settings.database_url.get_secret_value()),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode != 0 and "source capture requires base 0006" in result.stderr
    async with store.connection() as c:
        assert await c.fetchval("SELECT version_num FROM alembic_version") == "0010"
        assert await c.fetchval("SELECT to_regclass('source_capture_version')") is None


@pytest.mark.integration
async def test_hundred_stream_candle_and_quote_capture_without_tick_history(source):
    local, capture = source
    registry = Registry.load(Path("config/instruments.tradingview-100.json"))
    await local.register(registry)
    now = utc_now()
    start = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
    pairs = registry.subscriptions("tradingview_guest")
    bars = [
        make_candle(pair, start=start, received_timestamp=now, realtime_status="simulated")
        for pair in pairs
    ]
    assert len(bars) == 100 and await local.write_candles(bars, []) == 100
    ticks = [
        normalize(
            i,
            m.model_copy(update={"realtime_status": "simulated"}),
            now,
            event_timestamp=now,
            last="100.123456789012345678901",
        )
        for i, m in pairs
    ]
    assert await local.write_quotes(ticks) == 100
    page = await capture.page(100)
    assert len(page["records"]) == 100 and page["through"] == 100
    assert len((await capture.pending_quotes(100))["records"]) == 100
    async with local.connection() as c:
        assert await c.fetchval("SELECT head FROM source_capture_clock") == 200
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 0


@pytest.mark.integration
async def test_caller_catching_capture_error_cannot_commit_partial_helper_changes(
    source, pair, registry
):
    local, capture = source
    bar = make_candle(
        pair, realtime_status="simulated", metadata={"oversize": "x" * 17000, "volume_type": "base"}
    )
    first, other = sorted(registry.subscriptions("mock")[:2], key=lambda p: str(p[0].id))
    good = quote(first, last="100")
    bad = quote(other, last="101", metadata={"oversize": "x" * 17000})
    async with local.connection() as c, c.transaction():
        with pytest.raises(CaptureBackpressure):
            await local.write_candles_in_transaction(c, [bar], [])
        with pytest.raises(CaptureBackpressure):
            await local.write_observations_in_transaction(c, [good, bad], history=False)
        assert await c.fetchval("SELECT 1") == 1  # The surrounding transaction can still commit.
    assert (await capture.page())["records"] == []
    assert (await capture.pending_quotes())["records"] == []
    async with local.connection() as c:
        for table in (
            "market_candles",
            "latest_candles",
            "source_candle_exports",
            "candle_observation_heads",
            "latest_prices",
            "marketdata_events",
        ):
            assert await c.fetchval(f"SELECT count(*) FROM {table}") == 0
        assert await c.fetchval("SELECT head FROM source_capture_clock") == 0
