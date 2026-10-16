"""HTTPS-only producer API. No collector lifecycle or frontend routes."""

import asyncio
import hashlib
import hmac
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from marketdata import __version__
from marketdata.config import Settings
from marketdata.ingest_auth import Credentials, SessionRequest, session, strict_json
from marketdata.persistence import Store
from marketdata.reconciliation import ReconcileRequest, reconcile
from marketdata.replication import DestinationWriter, ReplicationError
from marketdata.replication_models import decode_batch
from marketdata.session_recovery import (
    MAX_RECOVERY_BYTES,
    RecoveryReceipt,
    RecoveryRequest,
    recover_session,
)

logger = logging.getLogger(__name__)
DATABASE_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError)
ERROR_STATUS = {
    "invalid_quote_snapshot": 422,
    "quote_snapshot_required": 409,
    "invalid_reconciliation": 422,
    "authentication_required": 401,
    "producer_not_authorized": 403,
    "source_not_authorized": 403,
    "source_status_not_authorized": 403,
    "batch_payload_conflict": 409,
    "observation_payload_conflict": 409,
    "source_version_conflict": 409,
    "producer_session_fenced": 409,
    "coverage_window_conflict": 409,
    "source_identity_conflict": 409,
    "collector_clock_ahead": 422,
    "source_clock_ahead": 422,
    "ownership_manifest_too_large": 413,
    "invalid_recovery": 422,
    "recovery_payload_conflict": 409,
    "recovery_ownership_changed": 409,
    "recovery_obsolete": 409,
    "producer_session_active": 409,
    "session_generation_exhausted": 409,
}


class Rejection(Exception):
    def __init__(self, status: int, code: str):
        self.status, self.code = status, code


def failure(status: int, code: str) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    if status in {429, 503}:
        headers["Retry-After"] = "60" if status == 429 else "2"
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    return JSONResponse(
        {"error": {"code": code, "retryable": status in {429, 503}}},
        status_code=status,
        headers=headers,
    )


async def body(request: Request, maximum: int) -> bytes:
    if request.headers.get("content-encoding", "identity").lower() != "identity":
        raise Rejection(415, "unsupported_content_encoding")
    if request.headers.get("content-type", "").lower() not in {
        "application/json",
        "application/json; charset=utf-8",
    }:
        raise Rejection(415, "json_required")
    lengths = request.headers.getlist("content-length")
    if len(lengths) > 1 or (lengths and not re.fullmatch(r"[0-9]{1,10}", lengths[0])):
        raise Rejection(400, "invalid_content_length")
    if lengths and int(lengths[0]) > maximum:
        raise Rejection(413, "payload_too_large")
    result = bytearray()
    async for chunk in request.stream():
        if len(result) + len(chunk) > maximum:
            raise Rejection(413, "payload_too_large")
        result.extend(chunk)
    if lengths and len(result) != int(lengths[0]):
        raise Rejection(400, "invalid_content_length")
    return bytes(result)


class IngestGuard:
    """One task owns admission, body reading and the transaction, including cancellation."""

    def __init__(self, app: ASGIApp, owner: FastAPI, settings: Settings, store: Store):
        self.app, self.owner, self.settings, self.store = app, owner, settings, store
        self.active = 0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope, receive)
        started = False

        async def guarded_send(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
                message["headers"] = [*message.get("headers", []), (b"cache-control", b"no-store")]
            await send(message)

        response = None
        # Trust only the ASGI scheme (direct TLS or Uvicorn's explicit proxy allowlist),
        # never raw Forwarded/X-Forwarded-Proto supplied to this application.
        if request.url.path == "/livez":
            await JSONResponse({"ingest": "alive"})(scope, receive, guarded_send)
            return
        if request.scope["scheme"] != "https":
            await failure(400, "https_required")(scope, receive, send)
            return
        if request.headers.get("origin") is not None:
            await failure(403, "browser_access_denied")(scope, receive, send)
            return
        if self.active >= self.settings.ingest_max_inflight:
            await failure(503, "ingestion_busy")(scope, receive, send)
            return
        self.active += 1
        try:
            async with asyncio.timeout(self.settings.ingest_request_timeout_seconds):
                headers = request.headers.getlist("authorization")
                if len(headers) != 1 or len(headers[0]) > 256:
                    raise ReplicationError("authentication_required")
                principal = self.owner.state.credentials.authenticate(headers[0])
                async with self.store.connection() as c:
                    admitted = await c.fetchval(
                        "SELECT ingest_admit_request($1,$2,$3)",
                        principal.producer_id,
                        principal.key_id,
                        self.settings.ingest_requests_per_minute,
                    )
                if admitted is None:
                    raise ReplicationError("authentication_required")
                if not admitted:
                    raise Rejection(429, "rate_limited")
                request.state.principal = principal
                await self.app(scope, receive, guarded_send)
        except Rejection as error:
            response = failure(error.status, error.code)
        except ReplicationError as error:
            status = ERROR_STATUS.get(error.code, 500)
            response = failure(status, error.code if status != 500 else "internal_error")
        except DATABASE_ERRORS as error:
            logger.warning(
                "ingestion database unavailable",
                extra={"context": {"category": type(error).__name__}},
            )
            response = failure(503, "database_unavailable")
        except Exception as error:
            logger.error(
                "ingestion request failed", extra={"context": {"category": type(error).__name__}}
            )
            response = failure(500, "internal_error")
        finally:
            self.active -= 1

        if response is not None and not started:
            await response(scope, receive, send)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    if settings.application_role != "ingest":
        raise ValueError("ingestion factory requires APPLICATION_ROLE=ingest")
    store = Store(settings)
    writer = DestinationWriter(store)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        try:
            assert settings.ingest_credentials_file is not None
            app.state.credentials = Credentials(settings.ingest_credentials_file)
            await store.connect()
            await writer.validate_permissions()
            async with store.connection() as c:
                permitted = await c.fetchval("""SELECT
                    has_function_privilege(current_user,
                        'ingest_admit_request(uuid,text,integer)','EXECUTE') AND
                    has_function_privilege(current_user,
                        'ingest_renew_session(uuid,text,uuid,bigint)','EXECUTE') AND NOT
                    has_table_privilege(current_user,'ingest_session_recoveries',
                        'INSERT,UPDATE,DELETE,TRUNCATE') AND
                    has_function_privilege(current_user,
                        'ingest_recover_session(uuid,text,jsonb)','EXECUTE') AND NOT
                    has_table_privilege(current_user,'ingest_request_budgets',
                        'INSERT,UPDATE,DELETE,TRUNCATE')""")
            if not permitted:
                raise RuntimeError("restricted runtime permissions required")
        except Exception as error:
            await store.close()
            logger.error(
                "ingestion startup rejected", extra={"context": {"category": type(error).__name__}}
            )
            raise RuntimeError("ingestion startup rejected; check private configuration") from None
        try:
            yield
        finally:
            await store.close()

    app = FastAPI(
        title="Market-data ingestion",
        version=__version__,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        redirect_slashes=False,
    )
    app.state.store, app.state.writer = store, writer
    app.add_middleware(IngestGuard, owner=app, settings=settings, store=store)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return failure(
            error.status_code, "not_found" if error.status_code == 404 else "invalid_request"
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, error: RequestValidationError) -> JSONResponse:
        return failure(422, "invalid_request")

    # Route exceptions must reach the guard without FastAPI's default input-echo errors.
    async def upload(request: Request, stream: str) -> dict[str, Any]:
        payload = await body(request, settings.ingest_max_body_bytes)
        hashes = request.headers.getlist("x-payload-sha256")
        if len(hashes) != 1 or not re.fullmatch(r"[0-9a-f]{64}", hashes[0]):
            raise Rejection(400, "payload_hash_required")
        if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), hashes[0]):
            raise Rejection(400, "payload_hash_mismatch")
        try:
            batch = decode_batch(payload)
        except (ValueError, RecursionError):
            raise Rejection(422, "invalid_batch") from None
        if batch.stream != stream:
            raise Rejection(422, "stream_mismatch")
        if len(batch.records) > settings.ingest_max_records:
            raise Rejection(413, "record_limit_exceeded")
        principal = request.state.principal
        return await writer.commit(
            principal.producer_id, payload, credential_key_id=principal.key_id
        )

    def handler(stream: str) -> Callable[[Request], Awaitable[dict[str, Any]]]:
        async def endpoint(request: Request) -> dict[str, Any]:
            return await upload(request, stream)

        return endpoint

    for stream in ("candles", "quotes", "coverage", "status"):
        app.add_api_route(
            "/api/ingest/v1/" + stream, handler(stream), methods=["POST"], name="upload_" + stream
        )

    @app.get("/api/ingest/v1/receipts/{batch_id}")
    async def receipt(request: Request, batch_id: UUID) -> dict[str, Any]:
        principal = request.state.principal
        # Serialize key revocation with lookup, including after session rotation.
        async with store.connection() as c, c.transaction():
            producer = await c.fetchrow(
                "SELECT * FROM ingest_lock_producer($1)", principal.producer_id
            )
            if (
                not producer
                or not producer["enabled"]
                or producer["credential_key_id"] != principal.key_id
            ):
                raise ReplicationError("producer_not_authorized")
            value = await c.fetchval(
                "SELECT receipt FROM ingest_batches WHERE producer_id=$1 AND batch_id=$2",
                principal.producer_id,
                batch_id,
            )
        if value is None:
            raise Rejection(404, "receipt_not_found")
        return dict(strict_json(value.encode("utf-8")))

    async def session_request(request: Request, renew: bool) -> dict[str, Any]:
        payload = await body(request, 2048)
        try:
            parsed = SessionRequest.model_validate(strict_json(payload))
        except (ValueError, RecursionError):
            raise Rejection(422, "invalid_session") from None
        return await session(store, request.state.principal, parsed, renew=renew)

    @app.post("/api/ingest/v1/session")
    async def admit_session(request: Request) -> dict[str, Any]:
        return await session_request(request, False)

    @app.post("/api/ingest/v1/session/renew")
    async def renew_session(request: Request) -> dict[str, Any]:
        return await session_request(request, True)

    @app.post(
        "/api/ingest/v1/session/recover",
        response_model=RecoveryReceipt,
        openapi_extra={
            "security": [{"ProducerBearer": []}],
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {"schema": {"$ref": "#/components/schemas/RecoveryRequest"}}
                },
            },
        },
        responses={
            status: {"description": description}
            for status, description in {
                401: "Authentication required",
                403: "Producer not authorized",
                409: "Session, ownership or recovery request fenced",
                413: "Bounded recovery request exceeded",
                422: "Invalid recovery request",
                429: "Producer request budget exhausted",
                503: "Retry after temporary failure",
            }.items()
        },
    )
    async def recover(request: Request) -> dict[str, Any]:
        payload = await body(request, min(MAX_RECOVERY_BYTES, settings.ingest_max_body_bytes))
        try:
            parsed = RecoveryRequest.model_validate(strict_json(payload))
        except (ValueError, RecursionError):
            raise Rejection(422, "invalid_recovery") from None
        return await recover_session(store, request.state.principal, parsed)

    @app.get("/api/ingest/v1/health")
    async def health() -> dict[str, str]:
        await store.ping()
        return {"ingest": "ready", "database": "available"}

    @app.post("/api/ingest/v1/reconcile")
    async def reconcile_state(request: Request) -> dict[str, Any]:
        payload = await body(request, min(262144, settings.ingest_max_body_bytes))
        try:
            parsed = ReconcileRequest.model_validate(strict_json(payload))
        except (ValueError, RecursionError):
            raise Rejection(422, "invalid_reconciliation") from None
        return await reconcile(writer, request.state.principal, parsed)

    # Request parsing remains bounded/strict above; advertise its typed wire schema too.
    original_openapi = app.openapi

    def ingestion_openapi() -> dict[str, Any]:
        schema = original_openapi()
        request_schema = RecoveryRequest.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
        components = schema.setdefault("components", {})
        schemas = components.setdefault("schemas", {})
        for name, value in request_schema.pop("$defs", {}).items():
            schemas.setdefault(name, value)
        schemas["RecoveryRequest"] = request_schema
        reconciliation_schema = ReconcileRequest.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
        schemas.update(reconciliation_schema.pop("$defs", {}))
        schemas["ReconcileRequest"] = reconciliation_schema
        schema["paths"]["/api/ingest/v1/reconcile"]["post"].update(
            security=[{"ProducerBearer": []}],
            requestBody={
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ReconcileRequest"}
                    }
                },
            },
        )
        components.setdefault("securitySchemes", {})["ProducerBearer"] = {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "key_id.secret",
        }
        return schema

    app.openapi_schema = ingestion_openapi()

    return app
