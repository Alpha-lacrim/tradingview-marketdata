"""Simulated observations, real PostgreSQL transactions; no HTTP/provider acquisition."""

import asyncio
import copy
import json
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from test_market_stream import HEADERS, ORIGIN, receive, server, subscribe, ws_url
from test_native_candles import make_candle
from websockets.asyncio.client import connect

from marketdata.config import Settings
from marketdata.domain import (
    DataStatus,
    ProviderInstrument,
    RegistryEntry,
    from_ns,
    normalize,
    utc_now,
)
from marketdata.persistence import Store
from marketdata.registry import Registry
from marketdata.replication import DestinationWriter, ReplicationError
from marketdata.replication_models import canonical_bytes, decode_batch


def record(observation, position=1, observation_id=None, kind="candle"):
    item = dict(
        observation_id=str(observation_id or getattr(observation, "tick_id", uuid4())),
        source_position=str(position),
        kind=kind,
        observation=observation.model_dump(mode="json")
        if hasattr(observation, "model_dump")
        else observation,
    )
    if kind == "quote" and item["observation"].get("event_timestamp_ns") is not None:
        item["observation"]["event_timestamp_ns"] = str(item["observation"]["event_timestamp_ns"])
    return item


def candle(pair, **changes):
    return make_candle(pair, realtime_status="simulated", **changes)


@pytest.fixture
async def destination(store, registry):
    producer, session, epoch = uuid4(), uuid4(), uuid4()
    async with store.connection() as c:
        await c.execute(
            """INSERT INTO ingest_producers
                (producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)
                VALUES ($1,'test-only',$2,$3,1,clock_timestamp()+interval '1 hour')""",
            producer,
            str(producer),
            session,
        )
        for entry in registry.entries:
            for mapping in entry.mappings:
                for stream in ("candles", "quotes", "coverage", "status"):
                    await c.execute(
                        """INSERT INTO ingest_source_owners
                            (instrument_id,provider,stream,producer_id,source_epoch,
                                ownership_generation,allowed_statuses)
                            VALUES ($1,$2,$3,$4,$5,1,ARRAY['simulated'])""",
                        entry.instrument.id,
                        mapping.provider,
                        stream,
                        producer,
                        epoch,
                    )

    def batch(records, stream="candles", batch_id=None, **changes):
        data = dict(
            protocol_version=1,
            producer_id=str(producer),
            session_id=str(session),
            session_generation=1,
            source_epoch=str(epoch),
            batch_id=str(batch_id or uuid4()),
            stream=stream,
            records=records,
        )
        data.update(changes)
        return canonical_bytes(data)

    return SimpleNamespace(
        writer=DestinationWriter(store),
        producer=producer,
        session=session,
        epoch=epoch,
        batch=batch,
        store=store,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"open": 100},
        {"close": True},
        {"volume": 1.2},
        {"high": "NaN"},
        {"open": "1e2"},
        {"received_timestamp": "2026-10-08T10:30:00"},
        {"bucket_end": "2099-01-01T00:00:00Z"},
    ],
)
def test_wire_observation_validation_before_database(pair, change):
    data = record(candle(pair))
    data["observation"].update(change)
    payload = canonical_bytes(
        dict(
            protocol_version=1,
            producer_id=str(uuid4()),
            session_id=str(uuid4()),
            session_generation=1,
            source_epoch=str(uuid4()),
            batch_id=str(uuid4()),
            stream="candles",
            records=[data],
        )
    )
    with pytest.raises(ValueError):
        decode_batch(payload)


@pytest.mark.parametrize(
    "payload",
    [b'{"x":1,"x":2}', b'{"x":NaN}', b" " * (2 * 1024 * 1024 + 1)],
    ids=["duplicate-key", "nonfinite", "body-limit"],
)
def test_wire_ambiguity_and_size_rejected(payload):
    with pytest.raises(ValueError):
        decode_batch(payload)


def test_read_openapi_contract_unchanged_from_frozen_v10():
    from marketdata.api import create_app

    expected = json.loads(Path("distributions/marketdata-fastapi/docs/openapi.json").read_text())
    from marketdata import __version__

    # Release metadata advances; every path/schema/auth contract stays identical.
    assert expected["info"]["version"] == "1.0.0"
    expected["info"]["version"] = __version__
    assert create_app(Settings(_env_file=None)).openapi() == expected


@pytest.mark.integration
async def test_batch_replay_conflict_and_cross_batch_observation_identity(destination, pair):
    d = destination
    item = record(candle(pair))
    payload = d.batch([item])
    first = await d.writer.commit(d.producer, payload)
    # Treat the first response as lost; a fresh writer/pool must recover its immutable receipt.
    restarted = Store(d.store.settings)
    await restarted.connect()
    try:
        assert await DestinationWriter(restarted).commit(d.producer, payload) == first
        assert (
            await DestinationWriter(restarted).receipt(d.producer, UUID(first["batch_id"])) == first
        )
    finally:
        await restarted.close()
    with pytest.raises(ReplicationError, match="batch_payload_conflict"):
        await d.writer.commit(d.producer, payload + b"\n")
    result = await d.writer.commit(d.producer, d.batch([item]))
    assert result["records"][0]["outcome"] == "duplicate"
    modified = copy.deepcopy(item)
    modified["observation"]["close"] = "100"
    with pytest.raises(ReplicationError, match="observation_payload_conflict"):
        await d.writer.commit(d.producer, d.batch([modified]))
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") == 0
        assert await c.fetchval("SELECT count(*) FROM ingest_observations") == 1
        assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 2


@pytest.mark.integration
async def test_correction_versions_noops_and_old_receipts_cannot_regress(destination, pair):
    d = destination
    original = candle(pair)
    newer = candle(
        pair, close="100.5", received_timestamp=original.received_timestamp + timedelta(seconds=3)
    )
    older = candle(
        pair, close="100", received_timestamp=original.received_timestamp + timedelta(seconds=2)
    )
    await d.writer.commit(d.producer, d.batch([record(original, 1)]))
    await d.writer.commit(d.producer, d.batch([record(newer, 3)]))
    stale = await d.writer.commit(d.producer, d.batch([record(older, 2)]))
    assert stale["records"][0]["outcome"] == "superseded"
    noop = newer.model_copy(
        update={"received_timestamp": newer.received_timestamp + timedelta(seconds=2)}
    )
    assert (await d.writer.commit(d.producer, d.batch([record(noop, 4)])))["records"][0][
        "outcome"
    ] == "recorded_no_change"
    # Even a higher transport position cannot defeat the shared collector-receipt guard.
    await d.writer.commit(d.producer, d.batch([record(older, 5)]))
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT close FROM market_candles") == newer.close
        assert json.loads(await c.fetchval("SELECT payload FROM latest_candles"))["close"] == str(
            newer.close
        )
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") == 1
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 2
        assert await c.fetchval("SELECT count(*) FROM ingest_observations") == 5
    with pytest.raises(ReplicationError, match="source_version_conflict"):
        await d.writer.commit(d.producer, d.batch([record(newer, 5)]))


@pytest.mark.integration
async def test_current_store_noop_receipt_and_same_key_insert_race(store, pair):
    first = candle(pair)
    noop = first.model_copy(
        update={"received_timestamp": first.received_timestamp + timedelta(seconds=3)}
    )
    stale = candle(
        pair, close="100", received_timestamp=first.received_timestamp + timedelta(seconds=2)
    )
    assert await store.write_candles([first], []) == 1
    assert await store.write_candles([noop], []) == 0
    assert await store.write_candles([stale], []) == 0
    other = candle(
        pair, bucket_start=first.bucket_start - timedelta(minutes=1), bucket_end=first.bucket_start
    )
    later = other.model_copy(
        update={
            "close": other.open,
            "received_timestamp": other.received_timestamp + timedelta(seconds=1),
        }
    )
    await asyncio.wait_for(
        asyncio.gather(store.write_candles([later], []), store.write_candles([other], [])), 5
    )
    async with store.connection() as c:
        rows = await c.fetch("SELECT close FROM market_candles ORDER BY bucket_start")
        assert [r["close"] for r in rows] == [later.close, first.close]
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") <= 1


@pytest.mark.integration
async def test_volume_units_are_semantic_corrections_but_transport_metadata_is_not(
    destination, pair
):
    d = destination
    bar = candle(pair)
    await d.writer.commit(d.producer, d.batch([record(bar, 1)]))
    diagnostics = bar.model_copy(
        update={
            "received_timestamp": bar.received_timestamp + timedelta(seconds=1),
            "metadata": dict(bar.metadata, request_latency="diagnostic only"),
        }
    )
    await d.writer.commit(d.producer, d.batch([record(diagnostics, 2)]))
    units = bar.model_copy(
        update={
            "received_timestamp": bar.received_timestamp + timedelta(seconds=2),
            "metadata": dict(bar.metadata, volume_type="tick"),
        }
    )
    await d.writer.commit(d.producer, d.batch([record(units, 3)]))
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT metadata->>'volume_type' FROM market_candles") == "tick"
        assert await c.fetchval("SELECT changed_fields FROM candle_revisions") == ["volume_type"]
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 2


@pytest.mark.integration
async def test_quote_history_latest_order_and_distinct_equal_prices(destination, pair):
    d = destination
    now = utc_now() - timedelta(seconds=10)
    newer = normalize(*pair, now, event_timestamp=now, last="123.1234567890123456789012345")
    older = normalize(
        *pair, now + timedelta(seconds=1), event_timestamp=now - timedelta(seconds=1), last="99"
    )
    repeated = normalize(
        *pair,
        now + timedelta(seconds=2),
        event_timestamp=now + timedelta(seconds=2),
        last=newer.last,
    )
    for position, quote in enumerate([newer, older, repeated], 1):
        await d.writer.commit(
            d.producer, d.batch([record(quote, position, kind="quote")], stream="quotes")
        )
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 3
        assert await c.fetchval("SELECT tick_id FROM latest_prices") == repeated.tick_id
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 2
        assert (
            await c.fetchval(
                "SELECT received_timestamp FROM market_ticks WHERE tick_id=$1", older.tick_id
            )
            == older.received_timestamp
        )
        assert (
            await c.fetchval("SELECT min(server_ingested_at) FROM ingest_observations")
            > repeated.received_timestamp
        )


@pytest.mark.integration
async def test_nanosecond_quote_and_receipt_only_order_survive_persistence(destination, pair):
    d = destination
    seconds = int((utc_now() - timedelta(seconds=10)).timestamp())
    ns = seconds * 1000000000 + 123456789
    one = normalize(
        *pair,
        from_ns(ns) + timedelta(seconds=1),
        event_timestamp=from_ns(ns),
        event_timestamp_ns=ns,
        last="100.000000000000000001",
    )
    two = normalize(
        *pair,
        one.received_timestamp,
        event_timestamp=from_ns(ns + 1),
        event_timestamp_ns=ns + 1,
        last="100.000000000000000002",
    )
    for pos, quote in enumerate([two, one], 1):
        payload = d.batch([record(quote, pos, kind="quote")], stream="quotes")
        assert isinstance(
            json.loads(payload)["records"][0]["observation"]["event_timestamp_ns"], str
        )
        await d.writer.commit(d.producer, payload)
    receipt_only = normalize(*pair, from_ns(ns) - timedelta(seconds=1), bid="99")
    await d.writer.commit(
        d.producer, d.batch([record(receipt_only, 3, kind="quote")], stream="quotes")
    )
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT tick_id FROM latest_prices") == two.tick_id
        assert (
            await c.fetchval(
                "SELECT event_timestamp_ns FROM market_ticks WHERE tick_id=$1", one.tick_id
            )
            == ns
        )
        assert (
            await c.fetchval(
                "SELECT event_timestamp FROM market_ticks WHERE tick_id=$1", receipt_only.tick_id
            )
            is None
        )


@pytest.mark.integration
async def test_delayed_fixture_quotes_cannot_replace_newer_current_price(destination, pair):
    d = destination
    # Fixture classification only, not a live provider capability assertion.
    mapping = ProviderInstrument(provider="fixture_quotes", provider_symbol="fixture:quote")
    await d.store.register(Registry([RegistryEntry(instrument=pair[0], mappings=[mapping])]))
    async with d.store.connection() as c:
        await c.execute(
            """INSERT INTO ingest_source_owners
            (instrument_id,provider,stream,producer_id,source_epoch,ownership_generation,allowed_statuses)
            VALUES ($1,$2,'quotes',$3,$4,1,ARRAY['realtime','delayed'])""",
            pair[0].id,
            mapping.provider,
            d.producer,
            d.epoch,
        )
    now = utc_now() - timedelta(seconds=10)
    live = normalize(
        pair[0],
        mapping.model_copy(update={"realtime_status": DataStatus.REALTIME}),
        now,
        event_timestamp=now,
        last="100",
    )
    delayed_mapping = mapping.model_copy(update={"realtime_status": DataStatus.DELAYED})
    late = normalize(
        pair[0],
        delayed_mapping,
        now + timedelta(seconds=1),
        event_timestamp=now - timedelta(minutes=15),
        last="99",
    )
    untimed = normalize(pair[0], delayed_mapping, now + timedelta(seconds=2), last="98")
    for position, quote in enumerate([live, late, untimed], 1):
        await d.writer.commit(
            d.producer, d.batch([record(quote, position, kind="quote")], stream="quotes")
        )
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT tick_id FROM latest_prices") == live.tick_id
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 3
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1


@pytest.mark.integration
async def test_concurrent_duplicate_and_overlapping_requests(destination, pair):
    d = destination
    original = candle(pair)
    payload = d.batch([record(original)])
    results = await asyncio.wait_for(
        asyncio.gather(*(d.writer.commit(d.producer, payload) for _ in range(8))), 10
    )
    assert all(r == results[0] for r in results)
    versions = [10, 3, 9, 4, 8, 5, 7, 6, 2]
    batches = [
        d.batch(
            [
                record(
                    candle(
                        pair,
                        close=f"100.{n}",
                        received_timestamp=original.received_timestamp + timedelta(seconds=n),
                    ),
                    n,
                )
            ]
        )
        for n in versions
    ]
    await asyncio.wait_for(asyncio.gather(*(d.writer.commit(d.producer, p) for p in batches)), 10)
    async with d.store.connection() as c:
        assert str(await c.fetchval("SELECT close FROM market_candles")) == "100.10"
        assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 10
        assert await c.fetchval("SELECT count(*) FROM ingest_observations") == 10
        assert await c.fetchval("SELECT source_position FROM ingest_candle_heads") == 10
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1 + await c.fetchval(
            "SELECT count(*) FROM candle_revisions"
        )


@pytest.mark.integration
async def test_cross_producer_quote_uuid_collision_is_an_explicit_conflict(destination, registry):
    d = destination
    first, second = registry.subscriptions("mock")[:2]
    producer, session, epoch = uuid4(), uuid4(), uuid4()
    async with d.store.connection() as c:
        await c.execute(
            """INSERT INTO ingest_producers
            (producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)
            VALUES ($1,'other source owner',$2,$3,1,clock_timestamp()+interval '1 hour')""",
            producer,
            str(producer),
            session,
        )
        await c.execute(
            """UPDATE ingest_source_owners SET producer_id=$1,source_epoch=$2,
            ownership_generation=2 WHERE instrument_id=$3 AND provider=$4 AND stream='quotes'""",
            producer,
            epoch,
            second[0].id,
            second[1].provider,
        )
    now = utc_now() - timedelta(seconds=1)
    one = normalize(*first, now, last="100")
    two = normalize(*second, now, tick_id=one.tick_id, last="200")
    a = d.batch([record(one, 1, kind="quote")], stream="quotes")
    b = d.batch(
        [record(two, 1, kind="quote")],
        stream="quotes",
        producer_id=str(producer),
        session_id=str(session),
        source_epoch=str(epoch),
    )
    results = await asyncio.wait_for(
        asyncio.gather(
            d.writer.commit(d.producer, a), d.writer.commit(producer, b), return_exceptions=True
        ),
        5,
    )
    assert sum(isinstance(r, dict) for r in results) == 1
    conflicts = [r for r in results if isinstance(r, ReplicationError)]
    assert len(conflicts) == 1 and conflicts[0].code == "observation_payload_conflict"
    async with d.store.connection() as c:
        for table in ["market_ticks", "latest_prices", "ingest_batches", "ingest_observations"]:
            assert await c.fetchval(f"SELECT count(*) FROM {table}") == 1


@pytest.mark.integration
@pytest.mark.parametrize("failure_stage", ["event", "receipt"])
async def test_rollback_after_journal_or_receipt_and_safe_retry(
    destination, pair, monkeypatch, failure_stage
):
    import marketdata.events as events

    d = destination
    payload = d.batch([record(candle(pair))])
    original = events.append_events if failure_stage == "event" else d.writer._commit

    async def fail(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("injected task-owned failure")

    target = events if failure_stage == "event" else d.writer
    attr = "append_events" if failure_stage == "event" else "_commit"
    monkeypatch.setattr(target, attr, fail)
    async with d.store.connection() as c:
        head = await c.fetchval("SELECT head FROM marketdata_event_clock")
    with pytest.raises(RuntimeError, match="injected"):
        await d.writer.commit(d.producer, payload)
    async with d.store.connection() as c:
        for table in [
            "market_candles",
            "latest_candles",
            "candle_revisions",
            "ingest_batches",
            "ingest_observations",
            "ingest_candle_heads",
            "candle_observation_heads",
        ]:
            assert await c.fetchval(f"SELECT count(*) FROM {table}") == 0
        assert await c.fetchval("SELECT head FROM marketdata_event_clock") == head
    monkeypatch.setattr(target, attr, original)
    assert (await d.writer.commit(d.producer, payload))["state"] == "committed"


@pytest.mark.integration
async def test_no_receipt_or_market_visibility_before_commit(destination, pair, monkeypatch):
    d = destination
    paused, release = asyncio.Event(), asyncio.Event()
    original = d.writer._commit

    async def controlled(*args):
        result = await original(*args)
        paused.set()
        await release.wait()
        return result

    monkeypatch.setattr(d.writer, "_commit", controlled)
    payload = d.batch([record(candle(pair))])
    task = asyncio.create_task(d.writer.commit(d.producer, payload))
    try:
        await asyncio.wait_for(paused.wait(), 5)
        async with d.store.connection() as c:
            for table in ["market_candles", "ingest_batches", "ingest_observations"]:
                assert await c.fetchval(f"SELECT count(*) FROM {table}") == 0
        assert not task.done()
    finally:
        release.set()
        await asyncio.wait_for(task, 5)


@pytest.mark.integration
async def test_fences_scopes_transfer_and_expired_sessions(destination, pair):
    d = destination
    payload = d.batch([record(candle(pair))])
    with pytest.raises(ReplicationError, match="producer_not_authorized"):
        await d.writer.commit(uuid4(), payload)
    with pytest.raises(ReplicationError, match="source_not_authorized"):
        await d.writer.commit(
            d.producer, d.batch([record(candle(pair))], source_epoch=str(uuid4()))
        )
    receipt = await d.writer.commit(d.producer, payload)
    async with d.store.connection() as c:
        with pytest.raises(asyncpg.CheckViolationError):
            await c.execute(
                "UPDATE ingest_producers SET session_id=$1 WHERE producer_id=$2",
                uuid4(),
                d.producer,
            )
        await c.execute(
            "UPDATE ingest_producers SET session_id=$1,session_generation=2 WHERE producer_id=$2",
            uuid4(),
            d.producer,
        )
    with pytest.raises(ReplicationError, match="producer_session_fenced"):
        await d.writer.commit(d.producer, payload)
    assert await d.writer.receipt(d.producer, UUID(receipt["batch_id"])) == receipt
    async with d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_id=$1,session_generation=3,"
            "session_expires_at=clock_timestamp()-interval '1 second' WHERE producer_id=$2",
            d.session,
            d.producer,
        )
    with pytest.raises(ReplicationError, match="producer_session_fenced"):
        await d.writer.commit(d.producer, d.batch([record(candle(pair))], session_generation=3))
    async with d.store.connection() as c:
        with pytest.raises(asyncpg.CheckViolationError):
            await c.execute(
                "UPDATE ingest_producers SET session_expires_at="
                "clock_timestamp()+interval '1 hour' "
                "WHERE producer_id=$1",
                d.producer,
            )


@pytest.mark.integration
async def test_source_identity_status_and_unauthorized_stream(destination, pair):
    d = destination
    for change in [{"provider_symbol": "other"}, {"canonical_symbol": "FX:OTHER:EURUSD"}]:
        item = record(candle(pair))
        item["observation"].update(change)
        with pytest.raises(ReplicationError, match="source_identity_conflict"):
            await d.writer.commit(d.producer, d.batch([item]))
    async with d.store.connection() as c:
        await c.execute("DELETE FROM ingest_source_owners WHERE stream='candles'")
    with pytest.raises(ReplicationError, match="source_not_authorized"):
        await d.writer.commit(d.producer, d.batch([record(candle(pair))]))


@pytest.mark.integration
async def test_authoritative_transfer_is_fenced_and_serialized_with_inflight_commit(
    destination, pair, monkeypatch
):
    d = destination
    replacement, session, epoch = uuid4(), uuid4(), uuid4()
    async with d.store.connection() as c:
        await c.execute(
            """INSERT INTO ingest_producers
                (producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)
                VALUES ($1,'replacement',$2,$3,1,clock_timestamp()+interval '1 hour')""",
            replacement,
            str(replacement),
            session,
        )
        with pytest.raises(asyncpg.CheckViolationError):
            await c.execute(
                "UPDATE ingest_source_owners SET producer_id=$1 WHERE stream='candles'",
                replacement,
            )
        with pytest.raises(asyncpg.UniqueViolationError):
            await c.execute(
                "INSERT INTO ingest_source_owners SELECT * FROM ingest_source_owners LIMIT 1"
            )
    paused, release = asyncio.Event(), asyncio.Event()
    original = d.writer._commit

    async def held(*args):
        result = await original(*args)
        paused.set()
        await release.wait()
        return result

    monkeypatch.setattr(d.writer, "_commit", held)
    bar = candle(pair)
    write = asyncio.create_task(d.writer.commit(d.producer, d.batch([record(bar, 10)])))

    async def transfer():
        async with d.store.connection() as c:
            await c.execute(
                """UPDATE ingest_source_owners SET producer_id=$1,source_epoch=$2,
                    ownership_generation=ownership_generation+1
                    WHERE instrument_id=$3 AND provider=$4 AND stream='candles'""",
                replacement,
                epoch,
                pair[0].id,
                pair[1].provider,
            )

    handover = None
    try:
        await asyncio.wait_for(paused.wait(), 5)
        handover = asyncio.create_task(transfer())
        async with asyncio.timeout(5):
            while True:  # noqa: ASYNC110 - observe the actual task-owned database lock
                async with d.store.connection() as c:
                    waiting = await c.fetchval("""SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                        WHERE datname=current_database() AND wait_event_type='Lock'
                        AND query LIKE 'UPDATE ingest_source_owners SET producer_id=%')""")
                if waiting:
                    break
                await asyncio.sleep(0.01)
        assert not handover.done()
    finally:
        release.set()
        await asyncio.wait_for(write, 5)
        if handover:
            await asyncio.wait_for(handover, 5)
    monkeypatch.setattr(d.writer, "_commit", original)
    with pytest.raises(ReplicationError, match="source_not_authorized"):
        await d.writer.commit(d.producer, d.batch([record(bar, 11)]))
    corrected = bar.model_copy(
        update={
            "close": bar.open,
            "received_timestamp": bar.received_timestamp + timedelta(seconds=1),
        }
    )
    replacement_payload = d.batch(
        [record(corrected, 1)],
        producer_id=str(replacement),
        session_id=str(session),
        source_epoch=str(epoch),
    )
    assert (await d.writer.commit(replacement, replacement_payload))["state"] == "committed"
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT close FROM market_candles") == corrected.close
        assert await c.fetchval("SELECT ownership_generation FROM ingest_candle_heads") == 2


@pytest.mark.integration
async def test_multiple_helpers_same_transaction_and_coverage_rollback(store, pair):
    bar = candle(pair)
    second = bar.model_copy(
        update={
            "bucket_start": bar.bucket_start + timedelta(minutes=1),
            "bucket_end": bar.bucket_end + timedelta(minutes=1),
        }
    )
    quote = normalize(*pair, utc_now(), last="100")
    async with store.connection() as c:
        with pytest.raises(ValueError, match="caller-owned"):
            await store.write_candles_in_transaction(c, [bar], [])
        with pytest.raises(RuntimeError, match="rollback all helpers"):
            async with c.transaction():
                await store.lock_sources(c, [(bar.instrument_id, bar.provider)])
                await store.write_candles_in_transaction(
                    c,
                    [bar],
                    [
                        (
                            bar.instrument_id,
                            bar.provider,
                            bar.bucket_start,
                            second.bucket_end,
                        )
                    ],
                )
                await store.write_candles_in_transaction(c, [second], [])
                await store.write_observations_in_transaction(c, [quote])
                raise RuntimeError("rollback all helpers")
        for table in ["market_candles", "latest_prices", "market_ticks", "candle_coverage"]:
            assert await c.fetchval(f"SELECT count(*) FROM {table}") == 0
    async with store.connection() as c, c.transaction():
        await store.write_candles_in_transaction(c, [bar], [])
        await store.write_candles_in_transaction(c, [second], [])
        await store.write_observations_in_transaction(c, [quote])
        await store.write_observations_in_transaction(c, [quote])
    async with store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 2
        assert await c.fetchval("SELECT count(*) FROM market_ticks") == 1
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 3


@pytest.mark.integration
async def test_coverage_snapshots_backfill_unknown_bridges_and_actual_bars(destination, pair):
    d = destination
    bar = candle(pair)
    start = bar.bucket_start
    source = dict(
        instrument_id=str(pair[0].id),
        canonical_symbol=pair[0].canonical_symbol,
        provider=pair[1].provider,
        provider_symbol=pair[1].provider_symbol,
    )

    def coverage(lo, hi, unknown=None, not_expected=None):
        return dict(
            **source,
            start=lo.isoformat(),
            end=hi.isoformat(),
            unknown=unknown or [],
            not_expected=not_expected or [],
            source_missing=[],
            session_info={},
        )

    end = start + timedelta(minutes=3)
    cov = coverage(
        start,
        end,
        unknown=[
            dict(
                start=(start + timedelta(minutes=1)).isoformat(),
                end=(start + timedelta(minutes=2)).isoformat(),
            )
        ],
    )
    await d.writer.commit(d.producer, d.batch([record(cov, 1, kind="coverage")], stream="coverage"))
    await d.writer.commit(d.producer, d.batch([record(bar, 2)]))
    progress = await d.store.candle_progress(pair[0].id, pair[1].provider)
    assert progress["missing_count"] == 1 and progress["unknown_count"] == 1
    earlier = coverage(start - timedelta(minutes=4), start - timedelta(minutes=2))
    with pytest.raises(ReplicationError, match="coverage_window_conflict"):
        await d.writer.commit(
            d.producer, d.batch([record(earlier, 3, kind="coverage")], stream="coverage")
        )
    earlier = coverage(
        start - timedelta(minutes=4),
        end,
        unknown=[
            dict(start=(start - timedelta(minutes=2)).isoformat(), end=start.isoformat()),
            dict(
                start=(start + timedelta(minutes=1)).isoformat(),
                end=(start + timedelta(minutes=2)).isoformat(),
            ),
        ],
    )
    await d.writer.commit(
        d.producer, d.batch([record(earlier, 3, kind="coverage")], stream="coverage")
    )
    progress = await d.store.candle_progress(pair[0].id, pair[1].provider)
    assert progress["coverage_start"] == start - timedelta(minutes=4)
    assert progress["unknown_count"] == 3 and progress["missing_count"] == 3
    stale = await d.writer.commit(
        d.producer, d.batch([record(cov, 2, kind="coverage")], stream="coverage")
    )
    assert stale["records"][0]["outcome"] == "superseded"
    assert (await d.store.candle_progress(pair[0].id, pair[1].provider))["unknown_count"] == 3


@pytest.mark.integration
async def test_status_capture_expiry_and_session_rotation(destination, pair):
    d = destination
    captured = utc_now() - timedelta(minutes=5)
    source = dict(
        instrument_id=str(pair[0].id),
        canonical_symbol=pair[0].canonical_symbol,
        provider=pair[1].provider,
        provider_symbol=pair[1].provider_symbol,
    )
    status = dict(
        **source,
        run_id=str(uuid4()),
        captured_at=captured.isoformat(),
        expires_at=(captured + timedelta(seconds=20)).isoformat(),
        running=True,
        database_healthy=True,
        queue_depth=0,
        health=dict(
            provider=pair[1].provider,
            updated_at=captured.isoformat(),
            symbols={
                pair[0].canonical_symbol: dict(
                    canonical_symbol=pair[0].canonical_symbol,
                    provider_symbol=pair[1].provider_symbol,
                )
            },
        ),
    )
    await d.writer.commit(d.producer, d.batch([record(status, 1, kind="status")], stream="status"))
    stored = await d.writer.source_status(pair[0].id, pair[1].provider)
    assert not stored["fresh"] and stored["captured_at"] == captured
    status.update(
        captured_at=utc_now().isoformat(),
        expires_at=(utc_now() + timedelta(seconds=60)).isoformat(),
    )
    await d.writer.commit(d.producer, d.batch([record(status, 2, kind="status")], stream="status"))
    assert (await d.writer.source_status(pair[0].id, pair[1].provider))["fresh"]
    async with d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_id=$1,session_generation=2 WHERE producer_id=$2",
            uuid4(),
            d.producer,
        )
    assert not (await d.writer.source_status(pair[0].id, pair[1].provider))["fresh"]


@pytest.mark.integration
async def test_reader_writer_grants_and_existing_rest_chart_ws(destination, pair):
    d = destination
    async with d.store.connection() as admin:
        sql = await asyncio.to_thread(Path("scripts/sql/v11_destination_roles.sql").read_text)
        await admin.execute(sql)
        for role in ["marketdata_v11_read", "marketdata_v11_ingest"]:
            await admin.execute(f"ALTER ROLE {role} PASSWORD 'disposable_test'")
    parsed = urlsplit(d.store.settings.database_url.get_secret_value())

    def role_store(role, readonly=False):
        url = urlunsplit(
            parsed._replace(netloc=f"{role}:disposable_test@{parsed.hostname}:{parsed.port}")
        )
        return Store(
            Settings(
                _env_file=None, database_url=url, api_environment="production", api_key="x" * 32
            ),
            read_only=readonly,
        )

    writer = role_store("marketdata_v11_ingest")
    reader = role_store("marketdata_v11_read", True)
    try:
        await writer.connect()
        await reader.connect()
        await DestinationWriter(writer).validate_permissions()
        with pytest.raises(RuntimeError, match="restricted destination writer"):
            await d.writer.validate_permissions()
        bar = candle(pair)
        async with (
            server(reader) as (_, base),
            connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws,
        ):
            await subscribe(ws, [pair[0].canonical_symbol])
            await DestinationWriter(writer).commit(d.producer, d.batch([record(bar)]))
            event = await receive(ws, "candle_completed")
            assert event["persisted"] and event["data"]["close"] == str(bar.close)
            params = dict(
                symbol=pair[0].canonical_symbol,
                provider=pair[1].provider,
                start=bar.bucket_start.isoformat(),
                end=bar.bucket_end.isoformat(),
            )
            async with httpx.AsyncClient(base_url=base, headers=HEADERS) as client:
                rest = await client.get("/api/v1/candles", params=params)
                chart = await client.get("/api/v1/dashboard/candles", params=params)
                assert rest.status_code == chart.status_code == 200, (rest.text, chart.text)
                assert rest.json()["items"][0]["close"] == str(bar.close)
                assert chart.json() == dict(
                    a=[int(bar.bucket_start.timestamp())],
                    b=[float(bar.open)],
                    c=[float(bar.close)],
                    d=[float(bar.high)],
                    e=[float(bar.low)],
                    f=[0],
                    g=[float(bar.volume)],
                    h=[0.0],
                )
                quote = normalize(
                    *pair, utc_now() - timedelta(seconds=1), last="123.1234567890123456789"
                )
                await DestinationWriter(writer).commit(
                    d.producer, d.batch([record(quote, 2, kind="quote")], stream="quotes")
                )
                assert (await receive(ws, "price_update"))["persisted"]
                latest = (await client.get("/api/v1/prices/latest")).json()["items"]
                item = next(
                    item for item in latest if item["canonical_symbol"] == pair[0].canonical_symbol
                )
                assert item["price_type"] == "quote" and item["last"] == str(quote.last)
                corrected = bar.model_copy(
                    update={
                        "close": bar.open,
                        "received_timestamp": bar.received_timestamp + timedelta(seconds=1),
                    }
                )
                await DestinationWriter(writer).commit(d.producer, d.batch([record(corrected, 3)]))
                assert (await receive(ws, "candle_corrected"))["data"]["close"] == str(
                    corrected.close
                )
        for target, statements in [
            (
                reader,
                [
                    "UPDATE ingest_batches SET receipt=receipt",
                    "DELETE FROM marketdata_events",
                    "SELECT * FROM ingest_lock_producer(NULL)",
                ],
            ),
            (
                writer,
                [
                    "UPDATE ingest_producers SET enabled=false",
                    "UPDATE ingest_source_owners SET ownership_generation=2",
                    "DELETE FROM market_ticks",
                    "TRUNCATE market_candles",
                    "CREATE TABLE forbidden_test(id int)",
                    "UPDATE instruments SET active=false",
                    "UPDATE ingest_batches SET receipt=receipt",
                    "UPDATE ingest_observations SET payload=payload",
                ],
            ),
        ]:
            async with target.connection() as c:
                await c.execute("SET default_transaction_read_only=off")
                for sql in statements:
                    with pytest.raises(asyncpg.InsufficientPrivilegeError):
                        await c.execute(sql)
        async with d.store.connection() as c:
            await c.execute("GRANT INSERT ON ingest_batches TO marketdata_v11_read")
        unsafe = role_store("marketdata_v11_read", True)
        try:
            with pytest.raises(RuntimeError, match="SELECT-only"):
                await unsafe.connect()
        finally:
            await unsafe.close()
    finally:
        await writer.close()
        await reader.close()
        async with d.store.connection() as c:
            for role in ["marketdata_v11_read", "marketdata_v11_ingest"]:
                await c.execute(f"DROP OWNED BY {role}")
                await c.execute(f"DROP ROLE {role}")
