"""Same-producer expiry recovery through real, restricted PostgreSQL transactions."""

import asyncio
import copy
import hashlib
import importlib
import json
import secrets
import ssl
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from pydantic import ValidationError
from test_destination_ingestion import destination, record  # noqa: F401
from test_destination_migrations import migrate
from test_ingest_http import ingress, tls_files, upload  # noqa: F401
from test_market_stream import bar

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.ingest_api import create_app
from marketdata.ingest_auth import Credentials
from marketdata.session_recovery import RecoveryRequest

PATH = "/api/ingest/v1/session/recover"


@pytest.fixture
async def recovery(ingress):  # noqa: F811 - shared approved runtime fixture
    i = ingress
    admitted = await i.client.post(
        "/api/ingest/v1/session",
        json=dict(protocol_version=1, session_id=str(i.d.session), session_generation=1),
    )
    assert admitted.status_code == 200
    streams = [
        {k: v for k, v in row.items() if k != "allowed_statuses"}
        for row in admitted.json()["streams"]
    ]
    request = dict(
        protocol_version=1,
        recovery_id=str(uuid4()),
        previous_session_id=str(i.d.session),
        previous_session_generation=1,
        streams=streams,
    )
    return i, request


async def expire(i):
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE producer_id=$1",
            i.d.producer,
        )


async def state(i):
    async with i.d.store.connection() as c:
        row = await c.fetchrow(
            "SELECT session_id,session_generation,session_expires_at,enabled "
            "FROM ingest_producers WHERE producer_id=$1",
            i.d.producer,
        )
        return dict(row)


def test_recovery_openapi_is_typed_and_read_contract_stays_separate():
    app = create_app(
        Settings(_env_file=None, application_role="ingest", ingest_credentials_file="unused")
    )
    schema = app.openapi()
    assert schema["paths"][PATH]["post"]["security"] == [{"ProducerBearer": []}]
    assert schema["paths"][PATH]["post"]["requestBody"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/RecoveryRequest"}
    assert schema["components"]["schemas"]["RecoveryStream"]["additionalProperties"] is False
    assert not any(path.startswith("/api/v1/") for path in schema["paths"])
    assert schema == json.loads(Path("docs/fastapi-v1.1-ingest-openapi.json").read_text())

    def check_refs(value):
        if isinstance(value, dict):
            if "$ref" in value:
                assert (
                    value["$ref"].removeprefix("#/components/schemas/")
                    in schema["components"]["schemas"]
                )
            for item in value.values():
                check_refs(item)
        elif isinstance(value, list):
            for item in value:
                check_refs(item)

    check_refs(schema)


@pytest.mark.parametrize(
    "change",
    [
        {"streams": []},
        {"protocol_version": True},
        {"previous_session_generation": 1.0},
        {"previous_session_generation": 0},
        {"producer_id": str(uuid4())},
    ],
)
def test_recovery_model_rejects_coercion_and_identity_override(change):
    body = dict(
        protocol_version=1,
        recovery_id=str(uuid4()),
        previous_session_id=str(uuid4()),
        previous_session_generation=1,
        streams=[
            dict(
                instrument_id=str(uuid4()),
                provider="mock",
                stream="candles",
                source_epoch=str(uuid4()),
                ownership_generation=1,
            )
        ],
    )
    body.update(change)
    with pytest.raises(ValidationError):
        RecoveryRequest.model_validate(body)


@pytest.mark.integration
async def test_expired_recovery_preserves_epoch_receipts_delayed_batches_and_fences_old_session(
    recovery, pair
):
    i, request = recovery
    committed = i.d.batch([record(bar(pair), 1)])
    ack = await upload(i.client, committed)
    assert ack.status_code == 200
    pending_record = record(
        bar(pair, minutes_ago=10).model_copy(
            update={"received_timestamp": utc_now() - timedelta(minutes=8)}
        ),
        2,
    )
    pending = i.d.batch([pending_record])
    original_bytes = bytes(pending)
    active = await i.client.post(PATH, json=request)
    assert active.status_code == 409 and active.json()["error"]["code"] == "producer_session_active"
    await expire(i)
    async with i.d.store.connection() as c:
        owners = await c.fetch(
            "SELECT * FROM ingest_source_owners ORDER BY instrument_id,provider,stream"
        )
    recovered = await i.client.post(PATH, json=request)
    assert recovered.status_code == 200, recovered.text
    receipt = recovered.json()
    assert receipt["session_generation"] == 2 and receipt["session_id"] != str(i.d.session)
    assert {r["source_epoch"] for r in receipt["streams"]} == {str(i.d.epoch)}
    assert (await upload(i.client, pending)).status_code == 409
    assert (await upload(i.client, committed)).status_code == 409
    assert (
        await i.client.get("/api/ingest/v1/receipts/" + ack.json()["batch_id"])
    ).json() == ack.json()
    fresh = i.d.batch([pending_record], session_id=receipt["session_id"], session_generation=2)
    assert (await upload(i.client, fresh)).status_code == 200
    duplicate = i.d.batch(
        json.loads(committed)["records"], session_id=receipt["session_id"], session_generation=2
    )
    assert (await upload(i.client, duplicate)).json()["records"][0]["outcome"] == "duplicate"
    assert (
        pending == original_bytes
        and json.loads(fresh)["batch_id"] != json.loads(pending)["batch_id"]
    )
    async with i.d.store.connection() as c:
        assert (
            await c.fetch(
                "SELECT * FROM ingest_source_owners ORDER BY instrument_id,provider,stream"
            )
            == owners
        )
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 2
        assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 1


@pytest.mark.integration
async def test_competing_recoveries_have_one_winner_and_same_request_race_replays(recovery):
    i, request = recovery
    await expire(i)
    attempts = [dict(request, recovery_id=str(uuid4())) for _ in range(6)]
    responses = await asyncio.gather(*[i.client.post(PATH, json=r) for r in attempts])
    assert sorted(r.status_code for r in responses) == [200] + [409] * 5
    winner = next(n for n, r in enumerate(responses) if r.status_code == 200)
    repeats = await asyncio.gather(*[i.client.post(PATH, json=attempts[winner]) for _ in range(4)])
    assert all(r.status_code == 200 and r.json() == responses[winner].json() for r in repeats)
    await expire(i)
    loser = next(n for n in range(6) if n != winner)
    assert (await i.client.post(PATH, json=attempts[loser])).status_code == 409
    assert (await state(i))["session_generation"] == 2
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 1


@pytest.mark.integration
async def test_lost_recovery_response_replay_new_pool_expired_replay_and_obsolete_retry(
    recovery, monkeypatch
):
    import marketdata.ingest_api as api

    i, request = recovery
    await expire(i)
    original = api.recover_session

    async def lose(*args):
        await original(*args)
        raise OSError("do not expose transport or secret details")

    monkeypatch.setattr(api, "recover_session", lose)
    lost = await i.client.post(PATH, json=request)
    assert lost.status_code == 503 and "secret" not in lost.text
    monkeypatch.setattr(api, "recover_session", original)
    app = create_app(i.settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://restarted.test",
            headers={"Authorization": "Bearer " + i.token},
        ) as client,
    ):
        recovered = await client.post(PATH, json=request)
        assert recovered.status_code == 200
        prior = recovered.json()
        await expire(i)
        replay = await client.post(PATH, json=request)
        assert replay.status_code == 200 and replay.json() == prior
        assert (await state(i))["session_expires_at"] < utc_now()
        next_request = dict(
            request,
            recovery_id=str(uuid4()),
            previous_session_id=prior["session_id"],
            previous_session_generation=2,
        )
        next_reply = await client.post(PATH, json=next_request)
        assert next_reply.status_code == 200 and next_reply.json()["session_generation"] == 3
        stale = await client.post(PATH, json=request)
        assert stale.status_code == 409 and stale.json()["error"]["code"] == "recovery_obsolete"
        assert next_reply.json()["session_id"] not in stale.text


@pytest.mark.integration
async def test_recovery_identity_conflict_canonical_reorder_and_invalid_http(recovery):
    i, request = recovery
    await expire(i)
    first = await i.client.post(PATH, json=request)
    assert first.status_code == 200
    reordered = dict(request, streams=list(reversed(request["streams"])))
    assert (await i.client.post(PATH, json=reordered)).json() == first.json()
    changed = copy.deepcopy(request)
    changed["streams"][0]["source_epoch"] = str(uuid4())
    response = await i.client.post(PATH, json=changed)
    assert (
        response.status_code == 409
        and response.json()["error"]["code"] == "recovery_payload_conflict"
    )
    duplicate = dict(request, recovery_id=str(uuid4()), streams=[request["streams"][0]] * 2)
    assert (await i.client.post(PATH, json=duplicate)).status_code == 422
    assert (
        await i.client.post(
            PATH,
            content=b'{"recovery_id":1,"recovery_id":2}',
            headers={"Content-Type": "application/json"},
        )
    ).status_code == 422
    assert (
        await i.client.post(
            PATH, content=b" " * 262145, headers={"Content-Type": "application/json"}
        )
    ).status_code == 413
    assert (
        await i.client.post(PATH, json=request, headers={"Authorization": "Bearer bad"})
    ).status_code == 401
    assert (
        await i.client.post(PATH, json=request, headers={"Origin": "https://frontend.example.com"})
    ).status_code == 403


@pytest.mark.integration
@pytest.mark.parametrize(
    "change", ["disabled", "key_revoked", "new_session", "epoch", "ownership_generation"]
)
async def test_recovery_rejects_revocation_admin_fence_or_changed_stream(recovery, change):
    i, request = recovery
    await expire(i)
    async with i.d.store.connection() as c:
        statements = {
            "disabled": "UPDATE ingest_producers SET enabled=false",
            "key_revoked": "UPDATE ingest_producers SET credential_key_id='rotated-away'",
            "new_session": (
                "UPDATE ingest_producers SET session_id=gen_random_uuid(),session_generation=2"
            ),
            "epoch": (
                "UPDATE ingest_source_owners SET source_epoch=gen_random_uuid(),"
                "ownership_generation=2"
            ),
            "ownership_generation": "UPDATE ingest_source_owners SET ownership_generation=2",
        }
        await c.execute(statements[change])
    before = await state(i)
    response = await i.client.post(PATH, json=request)
    assert response.status_code in {401, 409}
    assert await state(i) == before
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 0


async def replacement(i):
    producer = uuid4()
    async with i.d.store.connection() as c:
        await c.execute(
            """INSERT INTO ingest_producers
            (producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)
            VALUES ($1,'replacement',$2,$3,1,clock_timestamp()+interval '120 seconds')""",
            producer,
            str(producer),
            uuid4(),
        )
    return producer


@pytest.mark.integration
@pytest.mark.parametrize("after_recovery", [False, True])
async def test_ownership_transfer_blocks_old_producer_and_saved_recovery_replay(
    recovery, after_recovery
):
    i, request = recovery
    await expire(i)
    if after_recovery:
        assert (await i.client.post(PATH, json=request)).status_code == 200
    other = await replacement(i)
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_source_owners SET producer_id=$1,ownership_generation=2", other
        )
    before = await state(i)
    denied = await i.client.post(PATH, json=request)
    assert (
        denied.status_code == 409 and denied.json()["error"]["code"] == "recovery_ownership_changed"
    )
    assert await state(i) == before


@pytest.mark.integration
async def test_recovery_serializes_with_stream_transfer_and_checks_post_lock_state(recovery):
    i, request = recovery
    other = await replacement(i)
    await expire(i)
    async with i.d.store.connection() as admin:
        transaction = admin.transaction()
        await transaction.start()
        stream = request["streams"][0]
        await admin.execute(
            "UPDATE ingest_source_owners SET producer_id=$1,ownership_generation=2 "
            "WHERE instrument_id=$2 AND provider=$3 AND stream=$4",
            other,
            UUID(stream["instrument_id"]),
            stream["provider"],
            stream["stream"],
        )
        pending = asyncio.create_task(i.client.post(PATH, json=request))
        try:
            async with i.d.store.connection() as observer, asyncio.timeout(5):
                while not await observer.fetchval(  # noqa: ASYNC110 - observe actual SQL lock
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                    "WHERE wait_event_type='Lock' AND query LIKE 'SELECT ingest_recover_session%')"
                ):
                    await asyncio.sleep(0.01)
            assert not pending.done()
        finally:
            await transaction.commit()
            denied = await pending
        assert denied.status_code == 409
    assert (await state(i))["session_generation"] == 1


@pytest.mark.integration
async def test_recovery_transaction_rollback_retry_and_restricted_permissions(recovery):
    i, request = recovery
    await expire(i)
    before = await state(i)
    async with i.app.state.store.connection() as c:
        with pytest.raises(asyncpg.DivisionByZeroError):
            async with c.transaction():
                receipt = json.loads(
                    await c.fetchval(
                        "SELECT ingest_recover_session($1,$2,$3)",
                        i.d.producer,
                        str(i.d.producer),
                        json.dumps(request),
                    )
                )
                assert receipt["session_generation"] == 2
                assert await state(i) == before
                await c.execute("SELECT 1/0")
        for sql in [
            "UPDATE ingest_producers SET enabled=true",
            "UPDATE ingest_source_owners SET ownership_generation=ownership_generation+1",
            "INSERT INTO ingest_session_recoveries SELECT * FROM ingest_session_recoveries",
            "SELECT * FROM ingest_session_recoveries",
        ]:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await c.execute(sql)
    assert await state(i) == before
    assert (await i.client.post(PATH, json=request)).status_code == 200


@pytest.mark.integration
async def test_populated_0008_upgrade_preserves_market_receipts_and_recovery_roundtrip(
    recovery, pair
):
    i, request = recovery
    payload = i.d.batch([record(bar(pair))])
    ack = await upload(i.client, payload)
    assert ack.status_code == 200
    url = i.d.store.settings.database_url.get_secret_value()
    # Close ingestion/read runtime's old-head pools before test-only schema roundtrip.
    await i.app.state.store.close()
    try:
        await migrate(url, "downgrade", "0008")
        async with i.d.store.connection() as c:
            before = {
                table: await c.fetch(f"SELECT * FROM {table}")
                for table in [
                    "ingest_producers",
                    "ingest_source_owners",
                    "ingest_batches",
                    "ingest_observations",
                    "market_candles",
                    "marketdata_events",
                    "marketdata_event_clock",
                ]
            }
        await migrate(url, "upgrade", "head")
        async with i.d.store.connection() as c:
            assert await c.fetchval("SELECT version_num FROM alembic_version") == "0010"
            for table, rows in before.items():
                assert await c.fetch(f"SELECT * FROM {table}") == rows
            await c.execute(
                "GRANT EXECUTE ON FUNCTION ingest_recover_session(UUID,TEXT,JSONB) "
                "TO marketdata_v11_ingest"
            )
        await i.app.state.store.connect()
        await expire(i)
        assert (await i.client.post(PATH, json=request)).status_code == 200
        assert (
            await i.client.get("/api/ingest/v1/receipts/" + ack.json()["batch_id"])
        ).json() == ack.json()
    finally:
        await migrate(url, "upgrade", "head")


@pytest.mark.integration
async def test_identical_first_recovery_requests_are_idempotent_under_concurrency(recovery):
    i, request = recovery
    await expire(i)
    replies = await asyncio.gather(*[i.client.post(PATH, json=request) for _ in range(6)])
    assert all(reply.status_code == 200 and reply.json() == replies[0].json() for reply in replies)
    assert (await state(i))["session_generation"] == 2


async def new_credential(i, producer, key_id):
    token = key_id + "." + secrets.token_urlsafe(32)
    await asyncio.to_thread(
        i.credential.write_text,
        json.dumps(
            [
                dict(
                    producer_id=str(producer),
                    key_id=key_id,
                    token_sha256=hashlib.sha256(token.encode()).hexdigest(),
                )
            ]
        ),
    )
    i.app.state.credentials = Credentials(i.credential)
    return {"Authorization": "Bearer " + token}


@pytest.mark.integration
async def test_authenticated_other_producer_cannot_recover_someone_elses_streams(recovery):
    i, request = recovery
    other = await replacement(i)
    headers = await new_credential(i, other, str(other))
    async with i.d.store.connection() as c:
        owner = await c.fetchrow(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE producer_id=$1 RETURNING session_id,session_generation",
            other,
        )
    forged = dict(
        request, previous_session_id=str(owner["session_id"]), previous_session_generation=1
    )
    before = await state(i)
    response = await i.client.post(PATH, json=forged, headers=headers)
    assert (
        response.status_code == 409
        and response.json()["error"]["code"] == "recovery_ownership_changed"
    )
    assert await state(i) == before
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 0


@pytest.mark.integration
async def test_key_rotation_revokes_retained_file_but_valid_new_key_can_replay_its_current_recovery(
    recovery,
):
    i, request = recovery
    await expire(i)
    prior = await i.client.post(PATH, json=request)
    assert prior.status_code == 200
    key = "rotated-machine-key"
    async with i.d.store.connection() as c:
        await c.execute("UPDATE ingest_producers SET credential_key_id=$1", key)
    # Old in-memory server credential definition cannot bypass the current database key.
    assert (await i.client.post(PATH, json=request)).status_code == 401
    headers = await new_credential(i, i.d.producer, key)
    replay = await i.client.post(PATH, json=request, headers=headers)
    assert replay.status_code == 200 and replay.json() == prior.json()
    assert (await state(i))["session_generation"] == 2


@pytest.mark.integration
async def test_recovery_rechecks_revocation_after_http_authentication(recovery, monkeypatch):
    import marketdata.ingest_api as api

    i, request = recovery
    await expire(i)
    original = api.recover_session

    async def revoke_then_recover(*args):
        async with i.d.store.connection() as c:
            await c.execute("UPDATE ingest_producers SET enabled=false")
        return await original(*args)

    monkeypatch.setattr(api, "recover_session", revoke_then_recover)
    reply = await i.client.post(PATH, json=request)
    assert reply.status_code == 403 and reply.json()["error"]["code"] == "producer_not_authorized"
    assert (await state(i))["session_generation"] == 1


@pytest.mark.integration
async def test_interrupted_uncommitted_recovery_connection_rolls_back_and_retry_recovers(recovery):
    i, request = recovery
    await expire(i)
    before = await state(i)
    connection = await asyncpg.connect(i.settings.database_url.get_secret_value())
    try:
        await connection.execute("BEGIN")
        result = json.loads(
            await connection.fetchval(
                "SELECT ingest_recover_session($1,$2,$3)",
                i.d.producer,
                str(i.d.producer),
                json.dumps(request),
            )
        )
        assert result["state"] == "committed"  # pending result is never an external acknowledgment
        terminated = asyncio.Event()
        connection.add_termination_listener(lambda _: terminated.set())
        pid = connection.get_server_pid()
        async with i.d.store.connection() as admin:
            assert await admin.fetchval("SELECT pg_terminate_backend($1)", pid)
        await asyncio.wait_for(terminated.wait(), 5)
        assert connection.is_closed()
    finally:
        await connection.close()
    assert await state(i) == before
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 0
    assert (await i.client.post(PATH, json=request)).status_code == 200


@pytest.mark.integration
@pytest.mark.parametrize("boundary", ["before", "after"])
async def test_actual_tls_ingestion_process_kill_and_restart_during_recovery(
    recovery,
    tls_files,  # noqa: F811 - imported fixture
    tmp_path,
    monkeypatch,
    boundary,  # noqa: F811 - shared TLS fixture
):
    i, request = recovery
    await expire(i)
    if ssl.SSLContext.__module__.endswith("truststore._api"):
        original = importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext
        monkeypatch.setattr(ssl, "SSLContext", original)
    context = ssl.create_default_context(cafile=str(tls_files[0]))

    async def start(mode):
        port_file, marker = tmp_path / (mode + "-port"), tmp_path / (mode + "-pending")
        config = tmp_path / (mode + "-config.json")
        await asyncio.to_thread(
            config.write_text,
            json.dumps(
                dict(
                    database_url=i.settings.database_url.get_secret_value(),
                    credentials=str(i.credential),
                    mode=mode,
                    port_file=str(port_file),
                    marker=str(marker),
                    cert=str(tls_files[0]),
                    key=str(tls_files[1]),
                )
            ),
        )
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "tests/process_recovery_worker.py",
            str(config),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            async with asyncio.timeout(15):
                while not await asyncio.to_thread(port_file.exists):  # noqa: ASYNC110
                    assert child.returncode is None
                    await asyncio.sleep(0.02)
                port = await asyncio.to_thread(port_file.read_text)
                base = "https://127.0.0.1:" + port
                async with httpx.AsyncClient(base_url=base, verify=context) as probe:
                    while True:
                        assert child.returncode is None
                        try:
                            if (await probe.get("/livez", timeout=0.5)).status_code == 200:
                                break
                        except httpx.TransportError:
                            await asyncio.sleep(0.02)
            return child, base, marker
        except BaseException:
            if child.returncode is None:
                child.kill()
            await child.wait()
            raise

    child, base, marker = await start(boundary)
    try:
        async with httpx.AsyncClient(
            base_url=base,
            verify=context,
            timeout=15,
            headers={"Authorization": "Bearer " + i.token},
        ) as client:
            pending = asyncio.create_task(client.post(PATH, json=request))
            try:
                async with asyncio.timeout(10):
                    while not await asyncio.to_thread(marker.exists):  # noqa: ASYNC110
                        assert child.returncode is None and not pending.done()
                        await asyncio.sleep(0.02)
                child.kill()
                await child.wait()
                with pytest.raises(httpx.TransportError):
                    await pending
            finally:
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
    finally:
        if child.returncode is None:
            child.kill()
        await child.wait()
    assert (await state(i))["session_generation"] == (1 if boundary == "before" else 2)
    restarted, base, _ = await start("normal")
    try:
        async with httpx.AsyncClient(
            base_url=base, verify=context, headers={"Authorization": "Bearer " + i.token}
        ) as client:
            result = await client.post(PATH, json=request)
            assert result.status_code == 200, result.text
            replay = await client.post(PATH, json=request)
            assert replay.status_code == 200 and replay.json() == result.json()
            assert result.json()["session_generation"] == 2
            async with i.d.store.connection() as c:
                assert await c.fetchval("SELECT count(*) FROM ingest_session_recoveries") == 1
    finally:
        if restarted.returncode is None:
            restarted.kill()
        await restarted.wait()
