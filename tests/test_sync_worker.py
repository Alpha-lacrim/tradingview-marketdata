"""Injected observations, task-owned source DBs and verified TLS destination IO."""

import asyncio
import copy
import importlib
import json
import secrets
import socket
import ssl
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import pytest
import uvicorn
from test_destination_ingestion import destination, record  # noqa: F401
from test_ingest_http import ingress, tls_files, tls_server  # noqa: F401
from test_native_candles import make_candle
from test_source_capture import quote, source  # noqa: F401

from marketdata.config import Settings
from marketdata.domain import Tick, utc_now
from marketdata.ingest_api import Rejection, create_app
from marketdata.ingest_auth import Credentials
from marketdata.persistence import Store
from marketdata.providers.base import ProviderHealth, SymbolHealth
from marketdata.source_capture import CaptureConflict, field_observation
from marketdata.sync_config import SyncConfig
from marketdata.sync_http import SyncHTTP, SyncProblem
from marketdata.sync_worker import SyncWorker


@asynccontextmanager
async def worker_tls_server(app, certificates):
    """Task-owned TLS with bounded shutdown of Windows SSL keep-alive transports."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        runner = uvicorn.Server(
            uvicorn.Config(
                app,
                log_level="critical",
                access_log=False,
                proxy_headers=False,
                ssl_certfile=str(certificates[0]),
                ssl_keyfile=str(certificates[1]),
                # Keep deterministic integration IO separate from idle-close races.
                # The client's pool expires at five seconds; explicit fault tests
                # independently exercise connection loss and durable retries.
                timeout_keep_alive=30,
                timeout_graceful_shutdown=2,
            )
        )
        task = asyncio.create_task(runner.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not runner.started:  # noqa: ASYNC110
                    assert not task.done()
                    await asyncio.sleep(0.01)
            yield f"https://127.0.0.1:{listener.getsockname()[1]}"
        finally:
            runner.should_exit = True
            await asyncio.wait_for(task, 10)


def snapshot(pair, *, last=None, bid=None, ask=None, fields=None, when=None):
    observed = quote(
        pair, when=when or utc_now() - timedelta(seconds=10), last=last, bid=bid, ask=ask
    )
    fields = copy.deepcopy(fields or {})
    for name in ("last", "bid", "ask"):
        if getattr(observed, name) is not None:
            fields[name] = field_observation(observed, name)
    primary = next(fields[n] for n in ("last", "bid", "ask") if n in fields)
    return Tick.model_validate(
        dict(
            **{
                **observed.model_dump(),
                "tick_id": uuid4(),
                "event_timestamp": primary["event_timestamp"],
                "event_timestamp_ns": primary["event_timestamp_ns"],
                "metadata": {
                    "quote_semantics": "coalesced_partial_fields",
                    "field_observations": fields,
                },
                **{n: fields[n]["value"] if n in fields else None for n in ("last", "bid", "ask")},
            }
        )
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "mode",
    [
        "forward",
        "reverse",
        "equal",
        "primary",
        "partial",
        "same_value",
        "concurrent",
        "independent_clock",
    ],
)
async def test_destination_snapshot_ordering(destination, pair, mode):  # noqa: F811
    d = destination
    old_time = utc_now() - timedelta(seconds=20)
    first = snapshot(pair, bid="99", when=old_time)
    fields = first.metadata["field_observations"]
    second = snapshot(
        pair,
        last="100" if mode == "primary" else None,
        bid="99" if mode == "same_value" else "99.5",
        ask="101",
        fields=fields,
        when=old_time if mode == "equal" else old_time + timedelta(seconds=1),
    )
    if mode == "independent_clock":
        fields = second.metadata["field_observations"]
        fields["ask"]["event_timestamp"] = None
        fields["ask"]["event_timestamp_ns"] = None
    a, b = record(first, 10, kind="quote"), record(second, 20, kind="quote")
    if mode == "partial":
        # Omitted bid is no update. Destination retains the accepted original bid.
        b["observation"]["bid"] = None
        del b["observation"]["metadata"]["field_observations"]["bid"]
        b["observation"]["event_timestamp"] = b["observation"]["metadata"]["field_observations"][
            "ask"
        ]["event_timestamp"]
    if mode == "concurrent":
        await asyncio.gather(
            d.writer.commit(d.producer, d.batch([a], "quotes")),
            d.writer.commit(d.producer, d.batch([b], "quotes")),
        )
    else:
        for item in [b, a] if mode == "reverse" else [a, b]:
            await d.writer.commit(d.producer, d.batch([item], "quotes"))
    async with d.store.connection() as c:
        latest = Tick.model_validate_json(await c.fetchval("SELECT payload FROM latest_prices"))
        proof = await d.writer.quote_state(c, pair[0].id, pair[1].provider)
    assert latest.tick_id == second.tick_id
    assert latest.bid == (first.bid if mode == "partial" else second.bid)
    assert (
        latest.metadata["field_observations"]["ask"]["observation_id"]
        == second.metadata["field_observations"]["ask"]["observation_id"]
    )
    assert proof["materialized"] and proof["source_position"] == "20"


@pytest.fixture
async def sync(ingress, source, pair, tmp_path, tls_files, monkeypatch):  # noqa: F811
    i = ingress
    local, capture = source
    if ssl.SSLContext.__module__.endswith("truststore._api"):
        monkeypatch.setattr(
            ssl,
            "SSLContext",
            importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext,
        )
    async with local.connection() as c:
        epoch = await c.fetchval("SELECT source_epoch FROM source_capture_clock")
        await c.execute(
            await asyncio.to_thread(Path("scripts/sql/v11_source_sync_role.sql").read_text)
        )
        await c.execute("ALTER ROLE marketdata_v11_sync PASSWORD 'disposable_test'")
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_source_owners SET source_epoch=$1,ownership_generation=2", epoch
        )
        rows = await c.fetch(
            "SELECT instrument_id,provider,stream,source_epoch,ownership_generation "
            "FROM ingest_source_owners ORDER BY instrument_id,provider,stream"
        )
    manifest = [{k: str(v) if isinstance(v, UUID) else v for k, v in dict(r).items()} for r in rows]
    parsed = urlsplit(local.settings.database_url.get_secret_value())
    restricted = urlunsplit(
        parsed._replace(
            netloc=f"marketdata_v11_sync:disposable_test@{parsed.hostname}:{parsed.port}"
        )
    )
    (tmp_path / "dsn").write_text(restricted)
    (tmp_path / "token").write_text(i.token)
    worker_store = Store(
        Settings(
            _env_file=None,
            database_url=restricted,
            application_role="collector",
            source_capture_enabled=True,
        )
    )
    await worker_store.connect()
    try:
        tls_app = create_app(i.settings)
        async with worker_tls_server(tls_app, tls_files) as origin:
            i.app = tls_app
            config = SyncConfig(
                endpoint=origin,
                source_dsn_file=tmp_path / "dsn",
                token_file=tmp_path / "token",
                anchor_file=tmp_path / "anchor.json",
                ca_file=tls_files[0],
                producer_id=i.d.producer,
                session_id=i.d.session,
                session_generation=1,
                streams=manifest,
            )
            worker = SyncWorker(worker_store, config)
            try:
                async with worker.capture.consumer_lock() as connection:
                    await worker.local.claim(connection)
                    worker.state = await worker.local.initialize()
                    await worker.session()
                    yield worker, local, capture, i
            finally:
                await worker.http.close()
    finally:
        await worker_store.close()
        async with local.connection() as c:
            await c.execute("DROP OWNED BY marketdata_v11_sync")
            await c.execute("DROP ROLE marketdata_v11_sync")


@pytest.mark.integration
async def test_tls_candle_quote_correction_and_old_ack(sync, pair):
    w, local, capture, i = sync
    bar = make_candle(pair, realtime_status="simulated")
    await local.write_candles([bar], [])
    await local.write_quotes([quote(pair, last="100.1234567890123456789")])
    assert await w.stage("quotes")
    staged = await w.local.pending("quotes")
    await local.write_quotes([quote(pair, ask="101")])
    assert await w.deliver("quotes")
    assert len((await capture.pending_quotes())["records"]) == 1
    assert await w.stage("candles")
    assert await w.deliver("candles")
    assert (await capture.page())["records"] == []
    await w.stage("quotes")
    await w.deliver("quotes")
    assert (await capture.pending_quotes())["records"] == []
    corrected = bar.model_copy(
        update=dict(
            close=bar.open, received_timestamp=bar.received_timestamp + timedelta(seconds=1)
        )
    )
    await local.write_candles([corrected], [])
    await w.stage("candles")
    await w.deliver("candles")
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM candle_revisions") == 1
        latest = Tick.model_validate_json(await c.fetchval("SELECT payload FROM latest_prices"))
        assert latest.ask == 101 and str(latest.last) == "100.1234567890123456789"
    counts = (await w.local.get())["state"]["delivery_counts"]
    assert (
        counts["candle_completed"] == 1
        and counts["candle_corrected"] == 1
        and counts["quotes"] == 2
    )
    async with local.connection() as c:
        assert (
            bytes(
                await c.fetchval(
                    "SELECT payload FROM source_sync_batches WHERE batch_id=$1", staged["batch_id"]
                )
            )
            == staged["payload"]
        )
    # Actual audit loss is visible even when current OHLC still matches.
    async with i.d.store.connection() as c:
        await c.execute("DELETE FROM candle_revisions")
    grants = [s for s in w.config.streams if s.stream in {"candles", "quotes"}]
    w.reconcile_cursor = next(
        n for n, s in enumerate(grants) if s.instrument_id == pair[0].id and s.stream == "candles"
    )
    assert (await w.reconciliation())[0]["reason"] == "revision_history_mismatch"


@pytest.mark.integration
@pytest.mark.parametrize(
    "boundary",
    [
        "before_staging_commit",
        "after_staging_commit",
        "before_http",
        "after_http_before_receipt",
        "after_receipt_commit",
        "after_source_ack",
    ],
)
async def test_restart_at_delivery_boundaries(sync, pair, boundary, monkeypatch):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])

    class Crash(BaseException):
        pass

    async def crash(point):
        if point == boundary:
            raise Crash()

    monkeypatch.setattr(w, "checkpoint", crash)
    with pytest.raises(Crash):
        await w.stage("candles")
        await w.deliver("candles")
    restarted = SyncWorker(w.store, w.config)
    try:
        restarted.local = w.local
        restarted.capture = w.capture
        restarted.state = (await w.local.get())["state"]
        await restarted.session()
        if await restarted.local.pending("candles") is None:
            await restarted.stage("candles")
        await restarted.deliver("candles")
        assert (await capture.page())["records"] == []
        async with i.d.store.connection() as c:
            assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
            assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1
        await restarted.capture.cleanup()
    finally:
        await restarted.http.close()


@pytest.mark.integration
async def test_lost_upload_and_recovery_response_preserves_bytes(sync, pair, monkeypatch):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    original = await w.local.pending("candles")
    real = w.http.request
    lost = set()

    async def drop(method, path, payload=None, **kwargs):
        result = await real(method, path, payload, **kwargs)
        if path in {"/candles", "/session/recover"} and path not in lost:
            lost.add(path)
            raise SyncProblem("transport_unavailable", retryable=True, delay=0)
        return result

    monkeypatch.setattr(w.http, "request", drop)
    await w.deliver("candles")
    assert (await capture.page())["after"] == 0
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()-interval '1 second'"
        )
    w.lease_until = 0
    with pytest.raises(SyncProblem, match="transport_unavailable"):
        await w.session()
    pending = w.state["pending_recovery"]
    await w.session()
    assert w.state["session_generation"] == 2 and pending is not None
    await w.local.operation(
        "attempt", dict(batch_id=str(original["batch_id"]), code="test", delay=0)
    )
    await w.deliver("candles")
    assert (await capture.page())["records"] == []
    async with local.connection() as c:
        assert (
            bytes(
                await c.fetchval(
                    "SELECT payload FROM source_sync_batches WHERE batch_id=$1",
                    original["batch_id"],
                )
            )
            == original["payload"]
        )


@pytest.mark.integration
async def test_lock_competition_fence_loss_and_sql_permissions(sync):
    w, local, _, _ = sync
    with pytest.raises(CaptureConflict):
        async with w.capture.consumer_lock():
            pass
    async with w.store.connection() as c:
        for query in (
            "UPDATE source_capture_clock SET head=head+1",
            "DELETE FROM market_candles",
            "UPDATE source_quote_state SET acked_version=version",
            "UPDATE source_candle_outbox SET record='{}'",
            "TRUNCATE instruments",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await c.execute(query)
    async with local.connection() as c:
        await c.fetchval(
            "SELECT pg_terminate_backend($1)", w.local.lock_connection.get_server_pid()
        )
    with pytest.raises((CaptureConflict, asyncpg.PostgresError, asyncpg.InterfaceError)):
        await w.save(forbidden_after_lock_loss=True)


@pytest.mark.integration
async def test_bounded_bytes_partial_page_and_quote_gaps(sync, pair):
    w, local, capture, _ = sync
    w.config.max_body_bytes = 4096
    first = make_candle(pair, realtime_status="simulated")
    await local.write_candles([first], [])
    await local.write_quotes([quote(pair, last="100")])
    for n in range(1, 5):
        await local.write_candles(
            [
                make_candle(
                    pair,
                    start=first.bucket_start - timedelta(minutes=n),
                    realtime_status="simulated",
                    metadata={"padding": "x" * 1500, "volume_type": "base"},
                )
            ],
            [],
        )
    page = await capture.page()
    assert len(page["records"]) == 5 and int(page["records"][1]["source_position"]) == 3
    await w.stage("candles")
    batch = await w.local.pending("candles")
    assert len(batch["payload"]) <= 4096
    assert len(json.loads(batch["payload"])["records"]) < len(page["records"])
    await w.deliver("candles")
    remaining = await capture.page()
    assert remaining["after"] < page["through"] and remaining["records"]


@pytest.mark.integration
async def test_reconciliation_detects_missing_after_ack_and_repairs(sync, pair):
    w, local, _, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    await w.deliver("candles")
    await w.capture.cleanup()
    async with i.d.store.connection() as c:
        await c.execute("DELETE FROM market_candles")
    w.reconcile_cursor = next(n for n, s in enumerate(w.config.streams) if s.stream == "candles")
    # Explicitly select the stream corresponding to this instrument.
    grants = [s for s in w.config.streams if s.stream in {"candles", "quotes"}]
    w.reconcile_cursor = next(
        n for n, s in enumerate(grants) if s.instrument_id == pair[0].id and s.stream == "candles"
    )
    assert (await w.reconciliation(repair=True))[0]["reason"] == "missing"
    await w.stage("candles")
    await w.deliver("candles")
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1


def test_configuration_https_and_snapshot_byte_limits(tmp_path):
    base = dict(
        endpoint="http://company.example",
        source_dsn_file=tmp_path / "dsn",
        token_file=tmp_path / "token",
        anchor_file=tmp_path / "anchor",
        producer_id=uuid4(),
        session_id=uuid4(),
        session_generation=1,
        streams=[
            dict(
                instrument_id=uuid4(),
                provider="mock",
                stream="candles",
                source_epoch=uuid4(),
                ownership_generation=1,
            )
        ],
    )
    with pytest.raises(ValueError):
        SyncConfig(**base)
    from marketdata.reconciliation import ReconcileRequest

    request = dict(
        protocol_version=True,
        session_id=str(base["session_id"]),
        session_generation=1,
        keys=base["streams"],
    )
    with pytest.raises(ValueError):
        ReconcileRequest.model_validate(request)
    request["protocol_version"] = 1
    request["keys"][0]["bucket_start"] = 1
    with pytest.raises(ValueError):
        ReconcileRequest.model_validate(request)


def test_candle_reconciliation_hash_preserves_arbitrary_decimal_precision(pair):
    from marketdata.reconciliation import candle_fingerprint

    original = make_candle(
        pair, realtime_status="simulated", open="100.123456789012345678901234567891"
    ).model_dump(mode="json")
    different = copy.deepcopy(original)
    different["open"] = "100.123456789012345678901234567892"
    assert candle_fingerprint(original) != candle_fingerprint(different)
    same = copy.deepcopy(original)
    same["open"] += "00"
    assert candle_fingerprint(original) == candle_fingerprint(same)


@pytest.mark.parametrize(
    "name,value",
    [
        ("value", "garbage"),
        ("event_timestamp_ns", True),
        ("observation_id", 123),
        ("received_timestamp", None),
    ],
)
def test_malformed_snapshot_provenance_is_rejected(pair, name, value):
    from marketdata.quote_snapshots import materialize

    item = snapshot(pair, last="100")
    item.metadata["field_observations"]["last"][name] = value
    with pytest.raises(ValueError):
        materialize(item)


@pytest.mark.integration
@pytest.mark.parametrize("status", [429, 503])
async def test_http_backoff_retains_pending_bytes(sync, pair, monkeypatch, status):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    old = await w.local.pending("candles")
    original = i.app.state.writer.commit

    async def busy(*args, **kwargs):
        raise Rejection(status, "rate_limited" if status == 429 else "ingestion_busy")

    monkeypatch.setattr(i.app.state.writer, "commit", busy)
    assert not await w.deliver("candles")
    retry = await w.local.pending("candles")
    assert retry["attempts"] == 1 and retry["payload"] == old["payload"] and not retry["ready"]
    assert (await capture.page())["after"] == 0
    monkeypatch.setattr(i.app.state.writer, "commit", original)


@pytest.mark.integration
@pytest.mark.parametrize("action", ["revoked", "key_rotated", "handover", "unknown_session"])
async def test_worker_authorization_fail_closed(sync, pair, action):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    async with i.d.store.connection() as c:
        if action == "revoked":
            await c.execute("UPDATE ingest_producers SET enabled=FALSE")
        elif action == "key_rotated":
            await c.execute("UPDATE ingest_producers SET credential_key_id='replacement-key'")
        elif action == "unknown_session":
            await c.execute(
                "UPDATE ingest_producers SET session_id=gen_random_uuid(),session_generation=2"
            )
        else:
            other = uuid4()
            await c.execute(
                """INSERT INTO ingest_producers(producer_id,label,credential_key_id,
                session_id,session_generation,session_expires_at) VALUES($1,'other',$2,$3,1,
                clock_timestamp()+interval '1 hour')""",
                other,
                str(other),
                uuid4(),
            )
            await c.execute(
                "UPDATE ingest_source_owners SET producer_id=$1,ownership_generation=3", other
            )
    w.lease_until = 0
    with pytest.raises(SyncProblem):
        await w.session()
    assert (await capture.page())["after"] == 0
    assert (await w.local.pending("candles"))["receipt"] is None


@pytest.mark.integration
async def test_recovered_session_reenvelopes_only_uncommitted_batch(sync, pair, monkeypatch):
    w, local, capture, i = sync
    await local.write_quotes([quote(pair, bid="99")])
    await w.stage("quotes")
    old = await w.local.pending("quotes")
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()-interval '1 second'"
        )
    w.lease_until = 0
    await w.session()
    await w.deliver("quotes")
    new = await w.local.pending("quotes")
    assert new["batch_id"] != old["batch_id"]
    assert json.loads(new["payload"])["records"] == json.loads(old["payload"])["records"]
    await w.deliver("quotes")
    assert not (await capture.pending_quotes())["records"]
    await local.write_quotes([quote(pair, last="100")])
    assert await w.stage("quotes")
    second = await w.local.pending("quotes")
    original = w.http.request

    async def disconnected(*args, **kwargs):
        raise SyncProblem("transport_unavailable", retryable=True)

    monkeypatch.setattr(w.http, "request", disconnected)
    assert not await w.deliver("quotes")
    retry = await w.local.pending("quotes")
    assert retry["payload"] == second["payload"] and retry["attempts"] == 1
    assert retry["error_code"] == "transport_unavailable" and not retry["ready"]
    assert (await capture.pending_quotes())["records"][0]["source_position"] == "2"
    monkeypatch.setattr(w.http, "request", original)
    # A delivery attempt can legitimately back off; run the real persisted schedule,
    # without forcing next_attempt or rewriting the immutable batch to obtain success.
    async with asyncio.timeout(30):
        while await w.local.pending("quotes"):
            await w.session()
            await w.deliver("quotes")
            await asyncio.sleep(0.1)
    assert not (await capture.pending_quotes())["records"]
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT source_position FROM ingest_quote_heads") == 2


@pytest.mark.integration
async def test_coverage_status_preserve_capture_and_expiry(sync, pair):
    w, local, _, i = sync
    bar = make_candle(pair, realtime_status="simulated")
    await local.write_candles(
        [bar], [(bar.instrument_id, bar.provider, bar.bucket_start, bar.bucket_end)]
    )
    await w.stage("candles")
    await w.deliver("candles")
    grants = [s for s in w.config.streams if s.stream == "coverage"]
    await w.save(
        coverage_index=next(n for n, s in enumerate(grants) if s.instrument_id == bar.instrument_id)
    )
    assert await w.stage("coverage")
    await w.deliver("coverage")
    health = ProviderHealth(
        provider=pair[1].provider,
        state="healthy",
        transport="connected",
        symbols={
            pair[0].canonical_symbol: SymbolHealth(
                canonical_symbol=pair[0].canonical_symbol, provider_symbol=pair[1].provider_symbol
            )
        },
        updated_at=utc_now(),
    )
    await local.publish_collector_status(uuid4(), True, True, 0, [health])
    grants = [s for s in w.config.streams if s.stream == "status"]
    await w.save(
        status_index=next(n for n, s in enumerate(grants) if s.instrument_id == bar.instrument_id)
    )
    assert await w.stage("status")
    await w.deliver("status")
    async with local.connection() as c:
        source_status = await c.fetchrow("SELECT * FROM collector_status")
    async with i.d.store.connection() as c:
        status = await c.fetchrow("SELECT * FROM ingest_source_status")
        assert status["captured_at"] == source_status["updated_at"]
        assert status["expires_at"] == source_status["expires_at"]
        assert not await c.fetchval(
            "SELECT missing @> $1::timestamptz FROM candle_coverage", bar.bucket_start
        )
    # Session recovery cannot resubmit captured pre-recovery status as a new heartbeat.
    await w.save(recovered_at=(utc_now() + timedelta(seconds=1)).isoformat())
    await w.save(
        status_index=next(n for n, s in enumerate(grants) if s.instrument_id == bar.instrument_id)
    )
    assert not await w.stage("status")


@pytest.mark.integration
@pytest.mark.parametrize("stream", ["coverage", "status"])
async def test_metadata_round_robin_skips_absent_and_unchanged_streams(sync, registry, stream):
    w, local, _, _ = sync
    w.config.metadata_max_records = 1
    grants = [s for s in w.config.streams if s.stream == stream]
    assert len(grants) > 2
    selected = [
        next(p for p in registry.subscriptions(g.provider) if p[0].id == g.instrument_id)
        for g in (grants[0], grants[-1])
    ]
    if stream == "coverage":
        for pair in selected:
            bar = make_candle(pair, realtime_status="simulated")
            await local.write_candles(
                [bar], [(bar.instrument_id, bar.provider, bar.bucket_start, bar.bucket_end)]
            )
    else:
        health = ProviderHealth(
            provider=selected[0][1].provider,
            state="healthy",
            transport="connected",
            symbols={
                pair[0].canonical_symbol: SymbolHealth(
                    canonical_symbol=pair[0].canonical_symbol,
                    provider_symbol=pair[1].provider_symbol,
                )
                for pair in selected
            },
            updated_at=utc_now(),
        )
        await local.publish_collector_status(uuid4(), True, True, 0, [health])
    await w.save(**{stream + "_index": 0})
    staged = []
    for _ in grants:
        if await w.stage(stream):
            batch = await w.local.pending(stream)
            staged.append(
                json.loads(batch["payload"])["records"][0]["observation"]["instrument_id"]
            )
            await w.deliver(stream)
    assert staged == [str(grants[0].instrument_id), str(grants[-1].instrument_id)]
    # Recover the persisted round-robin state: unchanged first and absent middle
    # streams still allow later streams to be examined on the next pass.
    w.state = (await w.local.get())["state"]
    assert w.state[stream + "_index"] == len(grants)
    for _ in grants:
        assert not await w.stage(stream)
    assert (await w.local.get())["state"][stream + "_index"] == len(grants)


@pytest.mark.integration
async def test_status_batches_cover_all_grants_without_extending_expiry(sync, registry):
    w, local, _, i = sync
    w.config.metadata_max_records = 7
    grants = [s for s in w.config.streams if s.stream == "status"]
    health = ProviderHealth(
        provider="mock",
        state="healthy",
        transport="connected",
        symbols={
            p[0].canonical_symbol: SymbolHealth(
                canonical_symbol=p[0].canonical_symbol, provider_symbol=p[1].provider_symbol
            )
            for p in registry.subscriptions("mock")
        },
        updated_at=utc_now(),
    )
    await local.publish_collector_status(uuid4(), True, True, 0, [health])
    async with local.connection() as c:
        captured = await c.fetchrow("SELECT updated_at,expires_at FROM collector_status")
    seen = set()
    for _ in range((len(grants) + 6) // 7):
        assert await w.stage("status")
        batch = await w.local.pending("status")
        records = json.loads(batch["payload"])["records"]
        assert 1 <= len(records) <= 7
        for entry in records:
            observed = entry["observation"]
            assert observed["instrument_id"] not in seen
            seen.add(observed["instrument_id"])
            assert observed["captured_at"] == captured["updated_at"].isoformat()
            assert observed["expires_at"] == captured["expires_at"].isoformat()
        assert await w.deliver("status")
    assert seen == {str(g.instrument_id) for g in grants}
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_source_status") == len(grants)
        assert (
            await c.fetchval("SELECT min(expires_at) FROM ingest_source_status")
            == captured["expires_at"]
        )
    assert not await w.stage("status")


@pytest.mark.integration
async def test_metadata_staging_failure_rolls_back_clock_hashes_and_cursor(
    sync, registry, monkeypatch
):
    w, local, _, _ = sync
    for pair in registry.subscriptions("mock")[:3]:
        bar = make_candle(pair, realtime_status="simulated")
        await local.write_candles(
            [bar], [(bar.instrument_id, bar.provider, bar.bucket_start, bar.bucket_end)]
        )
    async with local.connection() as c:
        head = await c.fetchval("SELECT head FROM source_capture_clock")
        state = await c.fetchval("SELECT state FROM source_sync_owner")

    async def fail(*args, **kwargs):
        raise RuntimeError("injected staging failure")

    monkeypatch.setattr(w.local, "stage", fail)
    with pytest.raises(RuntimeError, match="injected staging failure"):
        await w.stage("coverage")
    async with local.connection() as c:
        assert await c.fetchval("SELECT head FROM source_capture_clock") == head
        assert await c.fetchval("SELECT state FROM source_sync_owner") == state
        assert await c.fetchval("SELECT count(*) FROM source_sync_batches") == 0


@pytest.mark.integration
async def test_metadata_clock_observes_concurrent_unrelated_quote_commit(
    sync, registry, monkeypatch
):
    w, local, _, _ = sync
    grants = [s for s in w.config.streams if s.stream == "status"]
    w.config.metadata_max_records = 1
    target = next(p for p in registry.subscriptions("mock") if p[0].id == grants[0].instrument_id)
    other = next(p for p in registry.subscriptions("mock") if p[0].id != target[0].id)
    await local.publish_collector_status(
        uuid4(),
        True,
        True,
        0,
        [
            ProviderHealth(
                provider="mock",
                symbols={
                    target[0].canonical_symbol: SymbolHealth(
                        canonical_symbol=target[0].canonical_symbol,
                        provider_symbol=target[1].provider_symbol,
                    )
                },
            )
        ],
    )
    original = w.store.lock_sources

    async def interleave(c, keys):
        await original(c, keys)
        # Another key commits after our first snapshot-producing query, before
        # this transaction locks the shared event-position allocator.
        await local.write_quotes([quote(other, last="100")])

    monkeypatch.setattr(w.store, "lock_sources", interleave)
    assert await w.stage("status")
    batch = json.loads((await w.local.pending("status"))["payload"])
    async with local.connection() as c:
        version = await c.fetchval(
            "SELECT version FROM source_quote_state WHERE instrument_id=$1", other[0].id
        )
    assert int(batch["records"][0]["source_position"]) > version


@pytest.mark.integration
async def test_source_restore_anchor_and_remote_highwater_rejected(sync, pair):
    w, local, _, i = sync
    await local.write_quotes([quote(pair, last="100")])
    await w.stage("quotes")
    await w.deliver("quotes")
    value = json.loads(w.config.anchor_file.read_text())
    value["head"] += 10
    w.local.write_anchor(value)
    with pytest.raises(CaptureConflict, match="restore"):
        await w.local.initialize()
    value["head"] -= 10
    w.local.write_anchor(value)
    with pytest.raises(SyncProblem, match="source_restore_high_water"):
        await w.validate_remote(
            dict(
                protocol_version=1,
                producer_id=str(w.config.producer_id),
                destination_id=value["destination_id"],
                results=[dict(high_water="9999999")],
            )
        )


@pytest.mark.integration
async def test_latest_quote_reconciliation_resnapshot_preserves_field_clocks(sync, pair):
    w, local, capture, i = sync
    await local.write_quotes([quote(pair, last="100", bid="99")])
    original = (await capture.pending_quotes())["records"][0]
    await w.stage("quotes")
    await w.deliver("quotes")
    grants = [s for s in w.config.streams if s.stream in {"candles", "quotes"}]
    quote_cursor = next(
        n for n, s in enumerate(grants) if s.instrument_id == pair[0].id and s.stream == "quotes"
    )
    # Matching derived IDs alone do not prove actual financial/field provenance parity.
    # Simulate a damaged restored payload with an intact ordering head and identity.
    async with i.d.store.connection() as c:
        for table in ("latest_prices", "ingest_quote_heads"):
            await c.execute(
                f"UPDATE {table} SET payload=jsonb_set(payload,'{{bid}}','\"98\"') "
                "WHERE instrument_id=$1 AND provider=$2",
                pair[0].id,
                pair[1].provider,
            )
    w.reconcile_cursor = quote_cursor
    assert (await w.reconciliation())[0]["reason"] == "quote_parity_mismatch"
    async with i.d.store.connection() as c:
        await c.execute("DELETE FROM latest_prices")
        # Restore the accepted head for the independent missing-materialization repair.
        await c.execute(
            "UPDATE ingest_quote_heads SET payload=$1::jsonb "
            "WHERE instrument_id=$2 AND provider=$3",
            json.dumps(original["observation"]),
            pair[0].id,
            pair[1].provider,
        )
    w.reconcile_cursor = quote_cursor
    assert await w.reconciliation(repair=True)
    pending = (await capture.pending_quotes())["records"][0]
    assert (
        pending["observation"]["metadata"]["field_observations"]
        == original["observation"]["metadata"]["field_observations"]
    )
    await w.stage("quotes")
    await w.deliver("quotes")
    assert (await capture.pending_quotes())["records"] == []


@pytest.mark.integration
async def test_commit_fence_rolls_back_inflight_local_transaction(sync):
    w, local, _, _ = sync
    async with local.connection() as admin:
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            async with w.local.transaction() as c:
                await c.execute("SELECT source_sync_state('state','{\"must_rollback\":true}')")
                await admin.fetchval(
                    "SELECT pg_terminate_backend($1)", w.local.lock_connection.get_server_pid()
                )
                await asyncio.sleep(0.05)
        assert not json.loads(await admin.fetchval("SELECT state FROM source_sync_owner")).get(
            "must_rollback"
        )


@pytest.mark.integration
async def test_restricted_enrollment_and_source_budget(sync, pair):
    w, local, capture, _ = sync
    # Representative history committed with capture disabled before opt-in enrollment.
    local.settings.source_capture_enabled = False
    bar = make_candle(pair, realtime_status="simulated")
    await local.write_candles([bar], [])
    await local.write_quotes([quote(pair, last="100")])
    local.settings.source_capture_enabled = True
    assert await w.capture.seed_candles(pair[0].id, pair[1].provider) == 1
    assert await w.capture.seed_candles(pair[0].id, pair[1].provider) == 0
    assert await w.capture.seed_quotes() == 1
    assert await w.capture.seed_quotes() == 0
    local.settings.source_capture_max_rows = 1
    from marketdata.source_capture import CaptureBackpressure

    with pytest.raises(CaptureBackpressure):
        await local.write_candles(
            [
                make_candle(
                    pair, start=bar.bucket_start - timedelta(minutes=1), realtime_status="simulated"
                )
            ],
            [],
        )
    assert len((await capture.page())["records"]) == 1
    # Cleanup cannot remove pending rows before a verified acknowledgment.
    assert await w.capture.cleanup() == 0


@pytest.mark.integration
async def test_destination_new_epoch_restarts_position_order(destination, pair):  # noqa: F811
    d = destination
    one = snapshot(pair, last="100")
    await d.writer.commit(d.producer, d.batch([record(one, 9999, kind="quote")], "quotes"))
    epoch = uuid4()
    async with d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_source_owners SET source_epoch=$1,ownership_generation=2", epoch
        )
    two = snapshot(pair, last="101")
    await d.writer.commit(
        d.producer, d.batch([record(two, 1, kind="quote")], "quotes", source_epoch=str(epoch))
    )
    async with d.store.connection() as c:
        assert await c.fetchval("SELECT observation_id FROM ingest_quote_heads") == two.tick_id


@pytest.mark.integration
@pytest.mark.parametrize(
    "boundary",
    [
        "after_staging_commit",
        "after_http_before_receipt",
        "after_receipt_commit",
        "after_source_ack",
    ],
)
async def test_real_worker_process_crash_and_restart(sync, pair, tmp_path, boundary):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    config = tmp_path / "worker-process.json"
    config.write_text(w.config.model_dump_json())
    owner = w.local.lock_connection
    await owner.fetchval("SELECT pg_advisory_unlock(hashtextextended('marketdata:source-sync',0))")
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "tests/process_sync_worker.py",
        str(config),
        boundary,
        str(tmp_path / "marker"),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        assert await asyncio.wait_for(child.wait(), 30) == 81
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()
    assert await owner.fetchval(
        "SELECT pg_try_advisory_lock(hashtextextended('marketdata:source-sync',0))"
    )
    await w.local.claim(owner)
    w.state = (await w.local.get())["state"]
    # A restarted process has a fresh TLS pool and no trusted monotonic lease timer.
    await w.http.close()
    w.http = SyncHTTP(w.config)
    w.lease_until = 0
    await w.session()
    await w.deliver("candles")
    assert not (await capture.page())["records"]
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1


@pytest.mark.integration
async def test_competing_worker_processes_cannot_claim_or_mutate(sync, pair, tmp_path):
    w, local, _, _ = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    config = tmp_path / "worker-process.json"
    config.write_text(w.config.model_dump_json())
    marker = tmp_path / "process-owning"
    owner = w.local.lock_connection
    await owner.fetchval("SELECT pg_advisory_unlock(hashtextextended('marketdata:source-sync',0))")
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "tests/process_sync_worker.py",
        str(config),
        "after_staging_commit",
        str(marker),
        "hold",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(20):
            while not marker.exists():  # noqa: ASYNC110, ASYNC240
                assert child.returncode is None
                await asyncio.sleep(0.05)
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
            await w.save(stale_process_must_not_write=True)
        competitor = await asyncio.create_subprocess_exec(
            sys.executable,
            "tests/process_sync_worker.py",
            str(config),
            "never",
            str(tmp_path / "competitor"),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        assert await asyncio.wait_for(competitor.wait(), 20) != 0
    finally:
        child.kill()
        await child.wait()
        async with asyncio.timeout(10):
            while not await owner.fetchval(  # noqa: ASYNC110
                "SELECT pg_try_advisory_lock(hashtextextended('marketdata:source-sync',0))"
            ):
                await asyncio.sleep(0.02)
        await w.local.claim(owner)


@pytest.mark.integration
async def test_token_rotation_reload_and_untrusted_tls_rejection(sync, pair):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    before = await w.local.pending("candles")
    bad = SyncHTTP(w.config.model_copy(update=dict(ca_file=None)))
    try:
        with pytest.raises(SyncProblem, match="transport_unavailable"):
            await bad.request("GET", "/health")
    finally:
        await bad.close()
    key = "rotated_" + uuid4().hex
    token = key + "." + secrets.token_urlsafe(32)
    import hashlib

    i.credential.write_text(
        json.dumps(
            [
                dict(
                    producer_id=str(w.config.producer_id),
                    key_id=key,
                    token_sha256=hashlib.sha256(token.encode()).hexdigest(),
                )
            ]
        )
    )
    i.app.state.credentials = Credentials(i.credential)
    async with i.d.store.connection() as c:
        await c.execute("UPDATE ingest_producers SET credential_key_id=$1", key)
    with pytest.raises(SyncProblem):
        await w.http.request("GET", "/health")
    w.config.token_file.write_text(token)
    assert await w.deliver("candles")
    assert not (await capture.page())["records"]
    async with local.connection() as c:
        assert (
            bytes(
                await c.fetchval(
                    "SELECT payload FROM source_sync_batches WHERE batch_id=$1", before["batch_id"]
                )
            )
            == before["payload"]
        )


@pytest.mark.integration
async def test_0009_to_0010_additive_preserves_committed_candles(destination, pair):  # noqa: F811
    from test_destination_migrations import migrate

    d = destination
    url = d.store.settings.database_url.get_secret_value()
    await migrate(url, "downgrade", "0009")
    try:
        raw = d.batch([record(make_candle(pair, realtime_status="simulated"))])
        receipt = await d.writer.commit(d.producer, raw)
        async with d.store.connection() as c:
            before = await c.fetch("SELECT * FROM market_candles")
        await migrate(url, "upgrade", "head")
        assert await d.writer.receipt(d.producer, UUID(receipt["batch_id"])) == receipt
        async with d.store.connection() as c:
            assert await c.fetch("SELECT * FROM market_candles") == before
            assert await c.fetchval("SELECT version_num FROM alembic_version") == "0010"
    finally:
        await migrate(url, "upgrade", "head")


@pytest.mark.integration
async def test_older_primary_and_unchanged_last_clock(destination, pair):  # noqa: F811
    d = destination
    old = utc_now() - timedelta(seconds=30)
    first = snapshot(pair, bid="99", when=old)
    second = snapshot(
        pair,
        last="100",
        fields=first.metadata["field_observations"],
        when=old + timedelta(seconds=1),
    )
    older = (old - timedelta(hours=1)).isoformat()
    second.metadata["field_observations"]["last"]["event_timestamp"] = older
    second = second.model_copy(update=dict(event_timestamp=old - timedelta(hours=1)))
    await d.writer.commit(d.producer, d.batch([record(first, 1, kind="quote")], "quotes"))
    await d.writer.commit(d.producer, d.batch([record(second, 2, kind="quote")], "quotes"))
    third = snapshot(
        pair,
        ask="101",
        fields=second.metadata["field_observations"],
        when=old + timedelta(seconds=2),
    )
    await d.writer.commit(d.producer, d.batch([record(third, 3, kind="quote")], "quotes"))
    async with d.store.connection() as c:
        latest = Tick.model_validate_json(await c.fetchval("SELECT payload FROM latest_prices"))
    assert latest.event_timestamp == second.event_timestamp
    assert (
        latest.metadata["field_observations"]["last"]
        == second.metadata["field_observations"]["last"]
    )


@pytest.mark.integration
async def test_source_sc0001_upgrade_preserves_capture(store, registry, pair):
    import os

    from test_destination_migrations import migrate

    from marketdata.source_capture import SourceCapture

    database = "m3b_upgrade_" + uuid4().hex + "_test"
    url = urlunsplit(
        urlsplit(store.settings.database_url.get_secret_value())._replace(path="/" + database)
    )
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
        for revision in ["sc0001", "head"]:
            result = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, "-m", "alembic", "-c", "alembic-source.ini", "upgrade", revision],
                env=dict(os.environ, DATABASE_URL=url),
                capture_output=True,
                text=True,
                timeout=45,
            )
            assert result.returncode == 0, result.stderr
            if revision == "sc0001":
                await local.connect()
                await local.register(registry)
                await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
                await local.write_quotes([quote(pair, last="100")])
                capture = SourceCapture(local)
                before = (await capture.page(), await capture.pending_quotes())
            else:
                assert (await capture.page(), await capture.pending_quotes()) == before
                async with local.connection() as c:
                    assert (
                        await c.fetchval("SELECT version_num FROM source_capture_version")
                        == "sc0002"
                    )
    finally:
        await local.close()
        async with store.connection() as c:
            await c.execute(f'DROP DATABASE "{database}"')


@pytest.mark.integration
async def test_lock_loss_during_active_tls_upload_cannot_ack(sync, pair, monkeypatch):
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    original = i.app.state.writer.commit

    async def lose_after_commit(*args, **kwargs):
        receipt = await original(*args, **kwargs)
        async with local.connection() as c:
            await c.fetchval(
                "SELECT pg_terminate_backend($1)", w.local.lock_connection.get_server_pid()
            )
        await asyncio.sleep(0.05)
        return receipt

    monkeypatch.setattr(i.app.state.writer, "commit", lose_after_commit)
    with pytest.raises((CaptureConflict, asyncpg.InterfaceError, asyncpg.PostgresError)):
        await w.deliver("candles")
    assert (await capture.page())["after"] == 0
    async with local.connection() as c:
        assert await c.fetchval("SELECT phase FROM source_sync_batches") == "staged"


@pytest.mark.integration
async def test_invalid_legacy_volume_retained_and_diagnosed(sync, pair):
    w, local, capture, _ = sync
    local.settings.source_capture_enabled = False
    bar = make_candle(pair, realtime_status="simulated", metadata={"source_volume_raw": "10"})
    await local.write_candles([bar], [])
    local.settings.source_capture_enabled = True
    grants = [s for s in w.config.streams if s.stream == "candles"]
    w.enroll_cursor = next(n for n, s in enumerate(grants) if s.instrument_id == pair[0].id)
    with pytest.raises(SyncProblem, match="historical_preflight_failed"):
        await w.enroll()
    assert not (await capture.page())["records"]
    assert w.state["historical_preflight"]["issues"]
    async with local.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
        assert await c.fetchval("SELECT count(*) FROM source_candle_exports") == 0


@pytest.mark.integration
async def test_bounded_backlog_quotes_fairness_and_restart_progress(sync, pair):
    w, local, capture, _ = sync
    w.config.max_records = 7
    first = make_candle(pair, realtime_status="simulated")
    bars = [
        make_candle(
            pair, start=first.bucket_start - timedelta(minutes=n), realtime_status="simulated"
        )
        for n in range(125)
    ]
    await local.write_candles(bars, [])
    for n in range(3):
        await local.write_quotes([quote(pair, last=str(100 + n))])
        await w.cycle()
        assert not (await capture.pending_quotes())["records"]
        assert len((await capture.page(limit=500))["records"]) == 125 - 7 * (n + 1)
    # Durable partial range remains queued; no wall-clock cursor skips old history.
    assert (await capture.page())["after"] < 125
    assert (await w.local.get())["state"]["reconciliation_index"] > 0


@asynccontextmanager
async def destination_process(  # noqa: F811 - TLS fixture remains registered
    i, certificates, directory, *, recovery_mode="normal", upload_boundary=None
):
    import httpx

    directory.mkdir()
    port_file = directory / "port"
    marker = directory / "marker"
    config = directory / "config.json"
    config.write_text(
        json.dumps(
            dict(
                database_url=i.settings.database_url.get_secret_value(),
                credentials=str(i.credential),
                mode=recovery_mode,
                port_file=str(port_file),
                marker=str(marker),
                cert=str(certificates[0]),
                key=str(certificates[1]),
                upload_boundary=upload_boundary,
            )
        )
    )
    helper = (
        "tests/process_sync_destination.py"
        if upload_boundary
        else "tests/process_recovery_worker.py"
    )
    child = await asyncio.create_subprocess_exec(
        sys.executable, helper, str(config), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    try:
        async with asyncio.timeout(20):
            while not port_file.exists():  # noqa: ASYNC110, ASYNC240
                assert child.returncode is None
                await asyncio.sleep(0.02)
            origin = "https://127.0.0.1:" + port_file.read_text()
            async with httpx.AsyncClient(
                verify=ssl.create_default_context(cafile=str(certificates[0]))
            ) as client:
                while True:
                    try:
                        if (await client.get(origin + "/livez", timeout=0.5)).status_code == 200:
                            break
                    except httpx.TransportError:
                        await asyncio.sleep(0.02)
        yield child, origin, marker
    finally:
        if child.returncode is None:
            child.kill()
        await child.wait()


@pytest.mark.integration
@pytest.mark.parametrize("boundary", ["before", "after"])
async def test_worker_survives_destination_restart_during_upload(
    sync,
    pair,
    tmp_path,
    tls_files,  # noqa: F811
    boundary,
):  # noqa: F811
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    await w.stage("candles")
    old = await w.local.pending("candles")
    async with destination_process(i, tls_files, tmp_path / "paused", upload_boundary=boundary) as (
        child,
        origin,
        marker,
    ):
        await w.http.close()
        w.http = SyncHTTP(w.config.model_copy(update=dict(endpoint=origin)))
        pending = asyncio.create_task(w.deliver("candles"))
        try:
            async with asyncio.timeout(20):
                while not marker.exists():  # noqa: ASYNC110, ASYNC240
                    assert not pending.done()
                    await asyncio.sleep(0.02)
            child.kill()
            await child.wait()
            assert not await pending
        finally:
            if not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
    assert (await capture.page())["after"] == 0
    await w.local.operation(
        "attempt", dict(batch_id=str(old["batch_id"]), code="test_restart", delay=0)
    )
    async with destination_process(i, tls_files, tmp_path / "restarted") as (_, origin, _):
        await w.http.close()
        w.http = SyncHTTP(w.config.model_copy(update=dict(endpoint=origin)))
        assert await w.deliver("candles")
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1
        assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 1
    assert not (await capture.page())["records"]


@pytest.mark.integration
@pytest.mark.parametrize("boundary", ["before", "after"])
async def test_worker_recovers_same_operation_after_server_process_restart(
    sync,
    pair,
    tmp_path,
    tls_files,  # noqa: F811
    boundary,
):  # noqa: F811
    w, local, capture, i = sync
    await local.write_candles([make_candle(pair, realtime_status="simulated")], [])
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()-interval '1 second'"
        )
    w.lease_until = 0
    async with destination_process(
        i, tls_files, tmp_path / "recovering", recovery_mode=boundary
    ) as (child, origin, marker):
        await w.http.close()
        w.http = SyncHTTP(w.config.model_copy(update=dict(endpoint=origin)))
        pending = asyncio.create_task(w.session())
        try:
            async with asyncio.timeout(20):
                while not marker.exists():  # noqa: ASYNC110, ASYNC240
                    assert not pending.done()
                    await asyncio.sleep(0.02)
            saved = w.state["pending_recovery"]
            child.kill()
            await child.wait()
            with pytest.raises(SyncProblem):
                await pending
        finally:
            if not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
    async with destination_process(i, tls_files, tmp_path / "restarted") as (_, origin, _):
        await w.http.close()
        w.http = SyncHTTP(w.config.model_copy(update=dict(endpoint=origin)))
        assert w.state["pending_recovery"] == saved
        await w.session()
    assert w.state["session_generation"] == 2 and w.state["pending_recovery"] is None
    assert (await capture.page())["after"] == 0
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 1
