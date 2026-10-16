"""Fixture data over authenticated HTTP/TLS into real disposable PostgreSQL only."""

import asyncio
import hashlib
import importlib
import json
import os
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
import uvicorn
from pydantic import ValidationError
from test_destination_ingestion import destination, record  # noqa: F401
from test_market_stream import HEADERS, ORIGIN, bar, receive, server, subscribe, ws_url
from websockets.asyncio.client import connect

from marketdata.api import create_app as read_app
from marketdata.config import Settings
from marketdata.domain import normalize, utc_now
from marketdata.ingest_api import create_app
from marketdata.ingest_auth import Credentials
from marketdata.persistence import Store
from marketdata.production import command
from marketdata.replication_models import canonical_bytes


@pytest.fixture
async def ingress(destination, tmp_path):  # noqa: F811 - imported pytest fixture
    d = destination
    key_id = str(d.producer)
    token = key_id + "." + secrets.token_urlsafe(32)
    credential = tmp_path / "credentials.json"
    credential.write_text(
        json.dumps(
            [
                dict(
                    producer_id=str(d.producer),
                    key_id=key_id,
                    token_sha256=hashlib.sha256(token.encode()).hexdigest(),
                )
            ]
        )
    )
    async with d.store.connection() as c:
        await c.execute(
            await asyncio.to_thread(Path("scripts/sql/v11_destination_roles.sql").read_text)
        )
        for role in ("marketdata_v11_ingest", "marketdata_v11_read"):
            await c.execute(f"ALTER ROLE {role} PASSWORD 'disposable_test'")
    parsed = urlsplit(d.store.settings.database_url.get_secret_value())
    url = urlunsplit(
        parsed._replace(
            netloc=f"marketdata_v11_ingest:disposable_test@{parsed.hostname}:{parsed.port}"
        )
    )
    config = Settings(
        _env_file=None,
        application_role="ingest",
        api_environment="production",
        database_url=url,
        ingest_credentials_file=credential,
    )
    reader_url = urlunsplit(
        parsed._replace(
            netloc=f"marketdata_v11_read:disposable_test@{parsed.hostname}:{parsed.port}"
        )
    )
    reader = Store(Settings(_env_file=None, database_url=reader_url), read_only=True)
    app = create_app(config)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="https://ingest.test",
                headers={"Authorization": "Bearer " + token},
            ) as client:
                yield SimpleNamespace(
                    d=d,
                    app=app,
                    client=client,
                    token=token,
                    settings=config,
                    credential=credential,
                    reader=reader,
                )
    finally:
        async with d.store.connection() as c:
            for role in ("marketdata_v11_ingest", "marketdata_v11_read"):
                await c.execute(f"DROP OWNED BY {role}")
                await c.execute(f"DROP ROLE {role}")


async def upload(client, payload, stream="candles", **headers):
    return await client.post(
        "/api/ingest/v1/" + stream,
        content=payload,
        headers={
            "Content-Type": "application/json",
            "X-Payload-SHA256": hashlib.sha256(payload).hexdigest(),
            **headers,
        },
    )


def test_explicit_factories_credentials_and_production_command(tmp_path, monkeypatch):
    monkeypatch.setattr(os, "environ", dict(os.environ))
    with pytest.raises(ValueError, match="APPLICATION_ROLE=ingest"):
        create_app(Settings(_env_file=None))
    with pytest.raises(ValidationError):
        Settings(_env_file=None, application_role="ingest")
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None, application_role="ingest", ingest_credentials_file="x", api_key="x" * 32
        )
    config = Settings(_env_file=None, application_role="ingest", ingest_credentials_file="x")
    with pytest.raises(ValueError, match="APPLICATION_ROLE=api"):
        read_app(config)
    path = tmp_path / "credentials.json"
    path.write_text('[{"key_id":"x","key_id":"x"}]')
    with pytest.raises(ValueError):
        Credentials(path)
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("DATABASE_URL_FILE", str(tmp_path / "dsn"))
    monkeypatch.setenv("INGEST_CREDENTIALS_FILE", str(path))
    (tmp_path / "dsn").write_text("postgresql://writer:disposable@localhost/m2_test")
    args = command("ingest")
    assert "marketdata.ingest_api:create_app" in args and "--no-access-log" in args
    assert "disposable" not in " ".join(args)
    monkeypatch.setenv("FORWARDED_ALLOW_IPS", "*")
    with pytest.raises(ValueError, match="explicit trusted proxy"):
        command("ingest")


def test_private_credential_tool_does_not_print_or_overwrite_secrets(tmp_path):
    producer = uuid4()
    directory = tmp_path / "new-credentials"
    args = [
        sys.executable,
        "scripts/create_ingest_credentials.py",
        "--directory",
        str(directory),
        "--producer-id",
        str(producer),
        "--key-id",
        "test-key",
    ]
    created = subprocess.run(args, capture_output=True, text=True, check=True)
    token = (directory / "ingest_token").read_text().strip()
    credentials = Credentials(directory / "ingest_credentials.json")
    assert credentials.authenticate("Bearer " + token).producer_id == producer
    assert token not in created.stdout + created.stderr
    duplicate = subprocess.run(args, capture_output=True, text=True)
    assert duplicate.returncode == 1
    assert (directory / "ingest_token").read_text().strip() == token


@pytest.mark.integration
async def test_https_auth_cross_role_and_sanitized_validation(ingress, pair):
    i = ingress
    payload = i.d.batch([record(bar(pair))])
    for headers in [
        {"Authorization": ""},
        HEADERS,
        {"Authorization": "Bearer " + i.token + "wrong"},
    ]:
        response = await upload(i.client, payload, **headers)
        assert response.status_code == 401
        assert "WWW-Authenticate" in response.headers
    response = await upload(i.client, payload, Origin=ORIGIN)
    assert response.status_code == 403 and "access-control-allow-origin" not in response.headers
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=i.app), base_url="http://x"
    ) as plain:
        response = await plain.post(
            "/api/ingest/v1/candles", headers={"X-Forwarded-Proto": "https"}
        )
        assert response.status_code == 400 and response.json()["error"]["code"] == "https_required"
        assert (await plain.get("/livez")).status_code == 200
    for content, extra, status, code in [
        (payload, {"X-Payload-SHA256": "0" * 64}, 400, "payload_hash_mismatch"),
        (payload, {"X-Payload-SHA256": ""}, 400, "payload_hash_required"),
        (payload, {"Content-Encoding": "gzip"}, 415, "unsupported_content_encoding"),
        (payload, {"Content-Type": "text/plain"}, 415, "json_required"),
        (b'{"secret":"never echo this","secret":true}', {}, 422, "invalid_batch"),
        (b'{"records":NaN}', {}, 422, "invalid_batch"),
        (b"\xff", {}, 422, "invalid_batch"),
        (b"[" * 2000, {}, 422, "invalid_batch"),
    ]:
        response = await upload(i.client, content, **extra)
        assert response.status_code == status, response.text
        assert response.json() == {"error": {"code": code, "retryable": False}}
    assert (await upload(i.client, payload, "quotes")).status_code == 422
    assert (await i.client.get("/api/v1/prices/latest")).status_code == 404
    assert (await i.client.get("/api/ingest/v1/health")).json()["database"] == "available"
    response = await i.client.get("/api/ingest/v1/receipts/not-a-uuid")
    assert response.status_code == 422 and "not-a-uuid" not in response.text
    async with server(i.reader) as (_, base), httpx.AsyncClient(base_url=base) as reader:
        assert (
            await reader.get(
                "/api/v1/prices/latest", headers={"Authorization": "Bearer " + i.token}
            )
        ).status_code == 403
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 0


@pytest.mark.integration
async def test_session_admission_renewal_expiry_rotation_and_revocation(ingress):
    i = ingress
    body = dict(protocol_version=1, session_id=str(i.d.session), session_generation=1)
    response = await i.client.post("/api/ingest/v1/session", json=body)
    assert response.status_code == 200 and response.json()["streams"]
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()+interval '30 seconds'"
        )
    renewed = await i.client.post("/api/ingest/v1/session/renew", json=body)
    assert renewed.status_code == 200
    async with i.d.store.connection() as c:
        assert await c.fetchval(
            "SELECT session_expires_at>clock_timestamp()+interval '100 seconds' "
            "FROM ingest_producers"
        )
        await c.execute(
            "UPDATE ingest_producers SET session_expires_at=clock_timestamp()-interval '1 second'"
        )
    assert (await i.client.post("/api/ingest/v1/session/renew", json=body)).status_code == 409
    async with i.d.store.connection() as c:
        await c.execute(
            "UPDATE ingest_producers SET session_id=$1,session_generation=2,"
            "session_expires_at=clock_timestamp()+interval '60 seconds'",
            uuid4(),
        )
    assert (await i.client.post("/api/ingest/v1/session", json=body)).status_code == 409
    assert (await i.client.post("/api/ingest/v1/session/renew", json=body)).status_code == 409
    async with i.d.store.connection() as c:
        await c.execute("UPDATE ingest_producers SET credential_key_id='revoked'")
    assert (await i.client.post("/api/ingest/v1/session", json=body)).status_code == 401


@pytest.mark.integration
async def test_http_atomic_retry_conflict_lost_ack_and_server_restart(ingress, pair, monkeypatch):
    i = ingress
    payload = i.d.batch([record(bar(pair))])
    saved_commit = i.app.state.writer.commit

    async def lost(*args, **kwargs):
        await saved_commit(*args, **kwargs)
        raise OSError("secret-connection-string-never-return")

    monkeypatch.setattr(i.app.state.writer, "commit", lost)
    response = await upload(i.client, payload)
    assert response.status_code == 503 and "secret" not in response.text
    batch_id = json.loads(payload)["batch_id"]
    prior = await i.client.get("/api/ingest/v1/receipts/" + batch_id)
    assert prior.status_code == 200 and prior.json()["state"] == "committed"
    monkeypatch.setattr(i.app.state.writer, "commit", saved_commit)
    app = create_app(i.settings)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://restarted.test",
            headers={"Authorization": "Bearer " + i.token},
        ) as client,
    ):
        assert (await upload(client, payload)).json() == prior.json()
        assert (await upload(client, payload + b" ")).status_code == 409
        repeated = await upload(client, i.d.batch(json.loads(payload)["records"]))
        assert repeated.json()["records"][0]["outcome"] == "duplicate"
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
        assert (
            await c.fetchval("SELECT count(*) FROM marketdata_events WHERE kind='candle_completed'")
            == 1
        )
        await c.execute("UPDATE ingest_producers SET session_id=$1,session_generation=2", uuid4())
    assert (await upload(i.client, payload)).status_code == 409
    assert (await i.client.get("/api/ingest/v1/receipts/" + batch_id)).json() == prior.json()
    async with i.d.store.connection() as c:
        await c.execute("UPDATE ingest_producers SET enabled=false")
    assert (await i.client.get("/api/ingest/v1/receipts/" + batch_id)).status_code == 401


@pytest.mark.integration
async def test_global_rate_limit_across_instances_and_restart(ingress):
    i = ingress
    apps = [
        create_app(i.settings.model_copy(update={"ingest_requests_per_minute": 3}))
        for _ in range(2)
    ]
    async with i.d.store.connection() as c:
        # Avoid a real minute boundary in this concurrency test by retrying a bounded window.
        await c.execute("DELETE FROM ingest_request_budgets")
    async with apps[0].router.lifespan_context(apps[0]), apps[1].router.lifespan_context(apps[1]):
        clients = [
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="https://x",
                headers={"Authorization": "Bearer " + i.token},
            )
            for app in apps
        ]
        try:
            results = await asyncio.gather(
                *[clients[n % 2].get("/api/ingest/v1/health") for n in range(8)]
            )
            assert sorted(r.status_code for r in results) == [200] * 3 + [429] * 5
            assert all(
                r.headers.get("Retry-After") == "60" for r in results if r.status_code == 429
            )
        finally:
            for client in clients:
                await client.aclose()
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT requests FROM ingest_request_budgets") == 3
        await c.execute(
            "UPDATE ingest_request_budgets SET window_start=window_start-interval '1 minute'"
        )
    assert (await i.client.get("/api/ingest/v1/health")).status_code == 200


@pytest.mark.integration
async def test_body_streaming_record_limits_and_timeout_rolls_back(ingress, pair, monkeypatch):
    i = ingress
    payload = i.d.batch([record(bar(pair))])
    app = create_app(
        i.settings.model_copy(
            update={
                "ingest_max_body_bytes": 1024,
                "ingest_max_records": 1,
                "ingest_request_timeout_seconds": 1,
            }
        )
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://x",
            headers={"Authorization": "Bearer " + i.token},
        ) as client,
    ):

        async def chunks():
            yield b" " * 512
            yield b" " * 513

        response = await client.post(
            "/api/ingest/v1/candles", content=chunks(), headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 413
        assert (await upload(client, b" " * 1025)).status_code == 413
        # Use the full configured body ceiling for record validation and transaction cancellation.
        app.state.store.settings.ingest_max_body_bytes = 2097152
        original = app.state.writer._commit
        entered = asyncio.Event()

        async def held(c, batch, digest):
            result = await original(c, batch, digest)
            entered.set()
            await asyncio.Event().wait()
            return result

        monkeypatch.setattr(app.state.writer, "_commit", held)
        response = await upload(client, payload)
        assert entered.is_set() and response.status_code == 503
        async with i.d.store.connection() as c:
            assert await c.fetchval("SELECT count(*) FROM ingest_batches") == 0
            assert await c.fetchval("SELECT count(*) FROM market_candles") == 0
            assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 0
        monkeypatch.setattr(app.state.writer, "_commit", original)
        response = await upload(client, i.d.batch([record(bar(pair)), record(bar(pair), 2)]))
        assert (
            response.status_code == 413
            and response.json()["error"]["code"] == "record_limit_exceeded"
        )
        assert (await upload(client, payload)).status_code == 200


@pytest.fixture(scope="session")
def tls_files(tmp_path_factory):
    openssl = shutil.which("openssl")
    if not openssl:
        pytest.skip("OpenSSL is required to generate disposable TLS certificates")
    directory = tmp_path_factory.mktemp("ingest-tls")
    cert, key = directory / "cert.pem", directory / "key.pem"
    config = directory / "openssl.cnf"
    config.write_text("[req]\ndistinguished_name=dn\n[dn]\n")
    subprocess.run(
        [
            openssl,
            "req",
            "-config",
            str(config),
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


@asynccontextmanager
async def tls_server(app, tls_files):
    cert, key = tls_files
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        listener.setblocking(False)
        runner = uvicorn.Server(
            uvicorn.Config(
                app,
                log_level="critical",
                access_log=False,
                ssl_certfile=str(cert),
                ssl_keyfile=str(key),
                proxy_headers=False,
            )
        )
        task = asyncio.create_task(runner.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not runner.started:  # noqa: ASYNC110 - test-owned readiness
                    assert not task.done()
                    await asyncio.sleep(0.01)
            yield f"https://127.0.0.1:{listener.getsockname()[1]}"
        finally:
            runner.should_exit = True
            await asyncio.wait_for(task, 10)


@pytest.mark.integration
async def test_verified_tls_candle_quote_corrections_rest_chart_and_ws(
    ingress, pair, tls_files, monkeypatch
):
    i = ingress
    # This workstation injects pip's client-only truststore SSLContext globally.
    # Use the original verifying stdlib context for the test-owned TLS server/client.
    if ssl.SSLContext.__module__.endswith("truststore._api"):
        original = importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext
        monkeypatch.setattr(ssl, "SSLContext", original)
    app = create_app(i.settings)
    async with (
        tls_server(app, tls_files) as origin,
        server(i.reader) as (_, base),
        connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws,
    ):
        await subscribe(ws, [pair[0].canonical_symbol])
        async with httpx.AsyncClient(base_url=origin) as untrusted:
            with pytest.raises(httpx.ConnectError):
                await untrusted.get("/livez")
        context = ssl.create_default_context(cafile=str(tls_files[0]))
        async with httpx.AsyncClient(
            base_url=origin, verify=context, headers={"Authorization": "Bearer " + i.token}
        ) as client:
            native = bar(pair)
            assert (await upload(client, i.d.batch([record(native)]))).status_code == 200
            assert (await receive(ws, "candle_completed"))["data"]["close"] == str(native.close)
            correction = native.model_copy(
                update={
                    "close": native.open,
                    "received_timestamp": native.received_timestamp + timedelta(seconds=1),
                }
            )
            assert (await upload(client, i.d.batch([record(correction, 3)]))).status_code == 200
            assert (await receive(ws, "candle_corrected"))["data"]["close"] == str(correction.close)
            older = native.model_copy(
                update={
                    "received_timestamp": native.received_timestamp + timedelta(milliseconds=500)
                }
            )
            stale = await upload(client, i.d.batch([record(older, 2)]))
            assert stale.json()["records"][0]["outcome"] == "superseded"
            quote = normalize(
                *pair,
                utc_now(),
                last="123.1234567890123456789",
                event_timestamp=utc_now() - timedelta(seconds=1),
            )
            assert (
                await upload(
                    client, i.d.batch([record(quote, 4, kind="quote")], stream="quotes"), "quotes"
                )
            ).status_code == 200
            assert (await receive(ws, "price_update"))["persisted"]
            old = quote.model_copy(
                update={
                    "tick_id": uuid4(),
                    "last": native.open,
                    "event_timestamp": quote.event_timestamp - timedelta(seconds=5),
                }
            )
            assert (
                await upload(
                    client, i.d.batch([record(old, 5, kind="quote")], stream="quotes"), "quotes"
                )
            ).status_code == 200
        params = dict(
            symbol=pair[0].canonical_symbol,
            provider=pair[1].provider,
            start=native.bucket_start.isoformat(),
            end=native.bucket_end.isoformat(),
        )
        async with httpx.AsyncClient(base_url=base, headers=HEADERS) as reader:
            candles = (await reader.get("/api/v1/candles", params=params)).json()["items"]
            chart = (await reader.get("/api/v1/dashboard/candles", params=params)).json()
            assert candles[0]["close"] == str(correction.close)
            assert chart == dict(
                a=[int(native.bucket_start.timestamp())],
                b=[float(native.open)],
                c=[float(correction.close)],
                d=[float(native.high)],
                e=[float(native.low)],
                f=[0],
                g=[float(native.volume)],
                h=[0.0],
            )
            latest = (await reader.get("/api/v1/prices/latest")).json()["items"]
            assert next(
                item for item in latest if item["canonical_symbol"] == pair[0].canonical_symbol
            )["last"] == str(quote.last)


@pytest.mark.integration
async def test_unsafe_startup_and_new_privilege_denials(ingress):
    i = ingress
    unsafe = create_app(
        i.settings.model_copy(update={"database_url": i.d.store.settings.database_url})
    )
    with pytest.raises(RuntimeError, match="startup rejected"):
        async with unsafe.router.lifespan_context(unsafe):
            pytest.fail("unsafe database identity accepted")
    async with i.app.state.store.connection() as c:
        for sql in [
            "UPDATE ingest_request_budgets SET requests=1",
            "UPDATE ingest_producers SET enabled=true",
            "DELETE FROM ingest_request_budgets",
        ]:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await c.execute(sql)


@pytest.mark.integration
async def test_captured_status_projection_is_source_specific_and_expires(ingress, pair):
    i = ingress
    captured = utc_now() - timedelta(seconds=1)
    status = dict(
        instrument_id=str(pair[0].id),
        canonical_symbol=pair[0].canonical_symbol,
        provider=pair[1].provider,
        provider_symbol=pair[1].provider_symbol,
        run_id=str(uuid4()),
        captured_at=captured.isoformat(),
        expires_at=(captured + timedelta(seconds=60)).isoformat(),
        running=True,
        database_healthy=True,
        queue_depth=7,
        health=dict(
            provider=pair[1].provider,
            updated_at=captured.isoformat(),
            state="healthy",
            transport="connected",
            subscription_restored=True,
            symbols={
                pair[0].canonical_symbol: dict(
                    canonical_symbol=pair[0].canonical_symbol,
                    provider_symbol=pair[1].provider_symbol,
                    last_observation_at=captured.isoformat(),
                    stale=False,
                    observed_in_session=True,
                    session_expectation="OPEN",
                    candle_state="healthy",
                )
            },
        ),
    )
    payload = i.d.batch([record(status, kind="status")], stream="status")
    assert (await upload(i.client, payload, "status")).status_code == 200
    async with (
        server(i.reader) as (_, base),
        connect(ws_url(base), origin=ORIGIN, additional_headers=HEADERS) as ws,
        httpx.AsyncClient(base_url=base, headers=HEADERS) as reader,
    ):
        await subscribe(ws, [pair[0].canonical_symbol])
        async with asyncio.timeout(5):
            while True:
                event = await receive(ws, "source_status")
                source = next(
                    item
                    for item in event["items"]
                    if item["canonical_symbol"] == pair[0].canonical_symbol
                )
                if source["provider_state"] == "healthy":
                    break
        assert source["session_expectation"] == "OPEN"
        health = (await reader.get("/api/v1/health")).json()
        # Other authorized sources have no captured status yet; a healthy single source
        # cannot promote global collector state or fabricate an available price.
        assert health["collector"] == "unknown" and health["data_freshness"] == "unavailable"
        await i.reader.connect()
        try:
            snapshot = await i.reader.collector_status()
            assert snapshot["updated_at"] == captured and snapshot["queue_depth"] == 7
            assert not snapshot["fresh"]
        finally:
            await i.reader.close()
        # A status captured during an outage may arrive after its original TTL.
        # Upload and exact replay must never renew that captured TTL.
        expired = dict(status)
        old_capture = utc_now() - timedelta(minutes=5)
        expired.update(
            captured_at=old_capture.isoformat(),
            expires_at=(old_capture + timedelta(seconds=60)).isoformat(),
        )
        expired_payload = i.d.batch([record(expired, 2, kind="status")], stream="status")
        first = await upload(i.client, expired_payload, "status")
        assert first.status_code == 200
        assert (await upload(i.client, expired_payload, "status")).json() == first.json()
        async with i.d.store.connection() as c:
            await c.execute(
                "UPDATE ingest_producers SET session_id=$1,session_generation=2", uuid4()
            )
        async with asyncio.timeout(5):
            while True:
                event = await receive(ws, "source_status")
                source = next(
                    item
                    for item in event["items"]
                    if item["canonical_symbol"] == pair[0].canonical_symbol
                )
                if source["provider_state"] == "disconnected":
                    break
        assert source["session_expectation"] == "UNKNOWN"
        assert (await reader.get("/api/v1/health")).status_code == 200


@pytest.mark.integration
async def test_coverage_http_identity_authorization_concurrency_and_backpressure(
    ingress, pair, monkeypatch
):
    i = ingress
    native = bar(pair)
    payload = i.d.batch([record(native)])
    responses = await asyncio.gather(*[upload(i.client, payload) for _ in range(4)])
    assert all(r.status_code == 200 for r in responses)
    assert all(r.json() == responses[0].json() for r in responses)
    coverage = dict(
        instrument_id=str(pair[0].id),
        canonical_symbol=pair[0].canonical_symbol,
        provider=pair[1].provider,
        provider_symbol=pair[1].provider_symbol,
        start=native.bucket_start.isoformat(),
        end=native.bucket_end.isoformat(),
    )
    response = await upload(
        i.client, i.d.batch([record(coverage, 2, kind="coverage")], stream="coverage"), "coverage"
    )
    assert response.status_code == 200
    assert (await i.d.store.candle_progress(pair[0].id, pair[1].provider))["missing_count"] == 0
    forged = json.loads(payload)
    forged["producer_id"] = str(uuid4())
    assert (await upload(i.client, canonical_bytes(forged))).status_code == 403
    async with i.d.store.connection() as c:
        await c.execute("DELETE FROM ingest_source_owners WHERE stream='candles'")
    assert (await upload(i.client, i.d.batch([record(native, 3)]))).status_code == 403
    async with i.d.store.connection() as c:
        assert await c.fetchval("SELECT count(*) FROM market_candles") == 1
    # Per-process concurrency is bounded before database/body work.
    app = create_app(i.settings.model_copy(update={"ingest_max_inflight": 1}))
    entered, release = asyncio.Event(), asyncio.Event()
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://x",
            headers={"Authorization": "Bearer " + i.token},
        ) as client,
    ):
        original = app.state.writer.commit

        async def held(*args, **kwargs):
            entered.set()
            await release.wait()
            return await original(*args, **kwargs)

        monkeypatch.setattr(app.state.writer, "commit", held)
        quote = normalize(*pair, utc_now(), last="123")
        pending = asyncio.create_task(
            upload(client, i.d.batch([record(quote, 4, kind="quote")], stream="quotes"), "quotes")
        )
        await asyncio.wait_for(entered.wait(), 5)
        try:
            busy = await client.get("/api/ingest/v1/health")
            assert busy.status_code == 503 and busy.json()["error"]["code"] == "ingestion_busy"
        finally:
            release.set()
        assert (await pending).status_code == 200


@pytest.mark.integration
async def test_wire_coercion_is_rejected(ingress, pair):
    i = ingress
    for field, value in [
        ("protocol_version", True),
        ("protocol_version", 1.0),
        ("received_timestamp", utc_now().timestamp()),
    ]:
        data = json.loads(i.d.batch([record(bar(pair))]))
        target = data["records"][0]["observation"] if field == "received_timestamp" else data
        target[field] = value
        response = await upload(i.client, canonical_bytes(data))
        assert response.status_code == 422 and response.json()["error"]["code"] == "invalid_batch"
