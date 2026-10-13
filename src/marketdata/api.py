from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

import anyio
import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.routing import APIRoute
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field, ValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from marketdata.api_v1 import create_router
from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import DataStatus, Instrument, ProviderInstrument, Tick, utc, utc_now
from marketdata.fanout import Fanout
from marketdata.ingestion import IngestionService
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import INTERVALS, Store
from marketdata.providers.base import ProviderHealth

logger = logging.getLogger(__name__)
Limit = Annotated[int, Query(ge=1, le=1000)]


class InstrumentResponse(BaseModel):
    id: UUID
    instrument: Instrument
    mappings: list[ProviderInstrument] = Field(default_factory=list)


class TickPage(BaseModel):
    items: list[Tick]
    next_cursor: str | None = None


class LatestPrice(Tick):
    price_type: Literal["quote"] = "quote"
    age_ms: int
    stale: bool
    stale_after_seconds: float
    provider_state: str | None = None


class LatestPage(BaseModel):
    items: list[LatestPrice | LatestCandlePrice]
    next_after: str | None = None


class LatestCandlePrice(BaseModel):
    instrument_id: UUID
    canonical_symbol: str
    provider: str
    provider_symbol: str
    last: Decimal
    realtime_status: DataStatus
    received_timestamp: datetime
    price_basis: Literal["candle_close"] = "candle_close"
    interval: Literal["1m"] = "1m"
    bucket_start: datetime
    bucket_end: datetime
    age_ms: int
    stale: bool
    stale_after_seconds: float
    provider_state: str | None = None


class Candle(BaseModel):
    bucket: datetime
    provider: str
    realtime_status: DataStatus
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    observations: int
    complete: bool = False
    price_basis: str | None = None
    source_status: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    bucket_start: datetime | None = None
    bucket_end: datetime | None = None
    instrument_id: UUID | None = None
    canonical_symbol: str | None = None
    provider_symbol: str | None = None
    received_timestamp: datetime | None = None


class CandlePage(BaseModel):
    items: list[Candle]
    interval: str
    basis: Literal["last", "mid", "native"]
    next_start: datetime | None = None
    next_after_status: DataStatus | None = None


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    database: bool
    database_state: Literal["healthy", "degraded"]
    ingestion_running: bool
    queue_depth: int
    provider_states: dict[str, str]
    provider_transports: dict[str, str]
    ingestion_state: Literal["running", "stopped", "unknown"] = "unknown"
    ingestion_database_state: Literal["healthy", "degraded", "unknown"] = "unknown"
    streaming_mode: Literal["local", "unavailable"] = "unavailable"


class Command(BaseModel):
    model_config = {"extra": "forbid"}
    action: Literal["subscribe", "unsubscribe"]
    symbols: list[str] = Field(min_length=1, max_length=5000)


def encode_cursor(tick: Tick) -> str:
    raw = json.dumps([tick.storage_timestamp.isoformat(), str(tick.tick_id)]).encode()
    return base64.urlsafe_b64encode(raw).decode()


def decode_cursor(value: str | None) -> tuple[datetime, UUID] | None:
    if value is None:
        return None
    try:
        if len(value) > 256:
            raise ValueError
        timestamp, tick_id = json.loads(base64.b64decode(value, altchars=b"-_", validate=True))
        return utc(datetime.fromisoformat(timestamp)), UUID(tick_id)
    except (ValueError, TypeError, json.JSONDecodeError):
        raise HTTPException(422, "invalid history cursor") from None


def history_window(
    start: datetime | None, end: datetime | None, max_days: int
) -> tuple[datetime, datetime]:
    try:
        end = utc(end) if end else utc_now()
        start = utc(start) if start else end - timedelta(hours=1)
    except ValueError:
        raise HTTPException(422, "timestamps must include timezone") from None
    if not start < end or end - start > timedelta(days=max_days):
        raise HTTPException(422, f"history range must be positive and <= {max_days} days")
    return start, end


def authorized(value: str | None, settings: Settings) -> bool:
    if settings.api_key is None:
        return True
    scheme, _, token = (value or "").partition(" ")
    return scheme.lower() == "bearer" and hmac.compare_digest(
        token.encode(), settings.api_key.get_secret_value().encode()
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    """Database-only factory. No configuration can enable ingestion here."""
    settings = settings or Settings()
    if settings.application_role != "api":
        raise ValueError("API factory requires APPLICATION_ROLE=api")
    return _create_app(settings, combined=False)


def create_combined_app(settings: Settings | None = None) -> FastAPI:
    """Explicit single-worker development compatibility, including local fanout."""
    settings = settings or Settings()
    if settings.application_role != "combined":
        raise ValueError("combined factory requires APPLICATION_ROLE=combined")
    return _create_app(settings, combined=True)


def _create_app(settings: Settings, *, combined: bool) -> FastAPI:
    metrics = Metrics()
    store = Store(settings, read_only=not combined)
    fanout = Fanout(settings.websocket_queue_capacity, settings.websocket_max_clients, metrics)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        nonlocal store
        configure_logging(settings.log_level)
        if combined:
            from marketdata.collector import collector_lifespan

            async with collector_lifespan(settings, metrics=metrics, fanout=fanout) as runtime:
                store = runtime.store
                app.state.service = runtime.service
                app.state.registry = runtime.registry
                app.state.store = store
                yield
        else:
            app.state.store = store
            try:
                await store.connect()
                yield
            finally:
                await store.close()

    app = FastAPI(
        title="Multi-asset market data",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.state.service = None
    # Initialized before lifespan too, for tooling and tests; no connection is opened here.
    app.state.store = store

    def error_response(
        request: Request, status: int, message: str, headers: Mapping[str, str] | None = None
    ) -> JSONResponse:
        codes = {
            401: "authentication_required",
            403: "access_denied",
            404: "not_found",
            422: "invalid_request",
            503: "database_unavailable",
        }
        body = (
            {"error": {"code": codes.get(status, "request_failed"), "message": message}}
            if request.url.path.startswith("/api/v1/")
            else {"detail": message}
        )
        return JSONResponse(body, status_code=status, headers=headers)

    @app.middleware("http")
    async def authenticate(request: Request, call_next: Any) -> Response:
        if request.url.path not in {
            "/health",
            "/docs",
            "/redoc",
            "/openapi.json",
            "/docs/oauth2-redirect",
        } and not authorized(request.headers.get("authorization"), settings):
            header = request.headers.get("authorization", "")
            invalid = header[:7].lower() == "bearer " and bool(header[7:])
            return error_response(
                request,
                403 if invalid else 401,
                "invalid credentials" if invalid else "authentication required",
                {"WWW-Authenticate": "Bearer"} if not invalid else None,
            )
        try:
            if request.url.path.startswith("/api/v1/"):
                async with asyncio.timeout(settings.db_timeout_seconds):
                    response: Response = await call_next(request)
            else:
                response = await call_next(request)
        except TimeoutError:
            response = error_response(request, 503, "database temporarily unavailable")
        metrics.api_requests.labels(request.method, str(response.status_code)).inc()
        return response

    # Outermost middleware so preflight and 401/403/503 responses have CORS headers.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.api_cors_origins,
        allow_methods=["GET"],
        allow_headers=["Authorization", "Content-Type"],
        allow_credentials=False,
        expose_headers=["WWW-Authenticate"],
        max_age=600,
    )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error: StarletteHTTPException) -> JSONResponse:
        return error_response(request, error.status_code, str(error.detail), error.headers)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error: RequestValidationError) -> JSONResponse:
        if request.url.path.startswith("/api/v1/"):
            return error_response(request, 422, "invalid query parameters")
        from fastapi.exception_handlers import request_validation_exception_handler

        return await request_validation_exception_handler(request, error)

    async def database_error(request: Request, error: Exception) -> JSONResponse:
        logger.error(
            "query database unavailable", extra={"context": {"category": type(error).__name__}}
        )
        return error_response(request, 503, "database temporarily unavailable")

    for exception_type in (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
        app.add_exception_handler(exception_type, database_error)

    async def provider_healths() -> list[ProviderHealth]:
        service: IngestionService | None = app.state.service
        if service is not None:
            return [await p.health_check() for p in service.providers]
        snapshot = await store.collector_status()
        return snapshot["providers"] if snapshot else []

    @app.get("/health", response_model=Health, openapi_extra={"security": []})
    async def health(response: Response) -> Health:
        service: IngestionService | None = app.state.service
        database = False
        running = False
        queue_depth = 0
        healths: list[ProviderHealth] = []
        ingestion_state: Literal["running", "stopped", "unknown"] = "unknown"
        ingestion_database: Literal["healthy", "degraded", "unknown"] = "unknown"
        try:
            async with asyncio.timeout(settings.db_timeout_seconds):
                database = await store.ping()
                if service is not None:
                    ingestion_database = (
                        "healthy"
                        if service.database_healthy
                        and all(c.database_healthy for c in service.candle_services.values())
                        else "degraded"
                    )
                    running = service.writer_task is not None and not service.writer_task.done()
                    ingestion_state = "running" if running else "stopped"
                    queue_depth = service.queue.qsize()
                    healths = await provider_healths()
                else:
                    snapshot = await store.collector_status()
                    if snapshot:
                        running = snapshot["fresh"] and snapshot["running"]
                        ingestion_state = (
                            ("running" if running else "stopped")
                            if snapshot["fresh"]
                            else "unknown"
                        )
                        ingestion_database = (
                            ("healthy" if snapshot["database_healthy"] else "degraded")
                            if snapshot["fresh"]
                            else "unknown"
                        )
                        queue_depth = snapshot["queue_depth"]
                        healths = snapshot["providers"]
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError, RuntimeError):
            database = False
        states: dict[str, str] = {h.provider: h.state for h in healths}
        metrics.api_database.set(int(database))
        # API readiness is independent of collector/feed readiness.
        ok = database
        if combined:
            ok = (
                ok
                and running
                and ingestion_database == "healthy"
                and all(s == "healthy" for s in states.values())
            )
        if not ok:
            response.status_code = 503
        return Health(
            status="ok" if ok else "degraded",
            database=database,
            database_state="healthy" if database else "degraded",
            ingestion_running=running,
            queue_depth=queue_depth,
            provider_states=states,
            provider_transports={h.provider: h.transport for h in healths},
            ingestion_state=ingestion_state,
            ingestion_database_state=ingestion_database,
            streaming_mode="local" if combined else "unavailable",
        )

    @app.get("/providers", response_model=list[ProviderHealth])
    async def provider_health() -> list[ProviderHealth]:
        return await provider_healths()

    @app.get("/metrics", include_in_schema=False)
    async def prometheus() -> Response:
        if app.state.service is not None:
            for p in app.state.service.providers:
                p.refresh_health()
        return Response(
            generate_latest(metrics.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
        )

    @app.get("/instruments", response_model=list[InstrumentResponse])
    async def instruments(limit: Limit = 100, after: str = "") -> list[InstrumentResponse]:
        rows = await store.instruments(limit, after)
        return [
            InstrumentResponse(id=r.pop("id"), instrument=Instrument.model_validate(r))
            for r in rows
        ]

    async def require_instrument(symbol: str) -> dict[str, Any]:
        instrument = await store.instrument(symbol)
        if instrument is None:
            raise HTTPException(404, "unknown canonical symbol")
        return instrument

    @app.get("/instruments/{symbol}", response_model=InstrumentResponse)
    async def instrument(symbol: str) -> InstrumentResponse:
        row = await require_instrument(symbol)
        return InstrumentResponse(
            id=row.pop("id"),
            mappings=row.pop("mappings"),
            instrument=Instrument.model_validate(row),
        )

    async def latest_prices(
        rows: list[Tick | MarketCandle],
    ) -> list[LatestPrice | LatestCandlePrice]:
        healths = await provider_healths()
        now = utc_now()
        items: list[LatestPrice | LatestCandlePrice] = []
        for tick in rows:
            health = next(
                (
                    h
                    for h in healths
                    if h.provider == tick.provider
                    or (
                        h.provider.startswith(tick.provider + "_")
                        and tick.canonical_symbol in h.symbols
                        and h.symbols[tick.canonical_symbol].provider_symbol == tick.provider_symbol
                    )
                ),
                None,
            )
            symbol_health = health.symbols.get(tick.canonical_symbol) if health else None
            if isinstance(tick, MarketCandle):
                age_ms = max(0, int((now - tick.bucket_end).total_seconds() * 1000))
                items.append(
                    LatestCandlePrice(
                        instrument_id=tick.instrument_id,
                        canonical_symbol=tick.canonical_symbol,
                        provider=tick.provider,
                        provider_symbol=tick.provider_symbol,
                        last=tick.close,
                        realtime_status=tick.realtime_status,
                        received_timestamp=tick.received_timestamp,
                        bucket_start=tick.bucket_start,
                        bucket_end=tick.bucket_end,
                        age_ms=age_ms,
                        stale_after_seconds=60 + settings.candle_fetch_grace_seconds,
                        stale=age_ms > (60 + settings.candle_fetch_grace_seconds) * 1000
                        or symbol_health is None
                        or symbol_health.stale,
                        provider_state=health.state if health else None,
                    )
                )
                continue
            threshold = (
                symbol_health.stale_after_seconds if symbol_health else settings.stale_seconds
            )
            age_ms = max(0, int((now - tick.received_timestamp).total_seconds() * 1000))
            items.append(
                LatestPrice(
                    **tick.model_dump(),
                    age_ms=age_ms,
                    stale_after_seconds=threshold,
                    stale=age_ms > threshold * 1000 or symbol_health is None or symbol_health.stale,
                    provider_state=health.state if health else None,
                )
            )
        return items

    @app.get("/prices/latest", response_model=LatestPage, deprecated=True)
    async def latest(
        provider: str | None = None, limit: Limit = 100, after: str = ""
    ) -> LatestPage:
        rows = await store.latest(None, provider, limit + 1, after)
        items = rows[:limit]
        next_after = (
            items[-1].canonical_symbol + "/" + items[-1].provider if len(rows) > limit else None
        )
        return LatestPage(items=await latest_prices(items), next_after=next_after)

    @app.get(
        "/prices/latest/{symbol}",
        response_model=list[LatestPrice | LatestCandlePrice],
        deprecated=True,
    )
    async def latest_symbol(
        symbol: str, provider: str | None = None
    ) -> list[LatestPrice | LatestCandlePrice]:
        await require_instrument(symbol)
        return await latest_prices(await store.latest(symbol, provider, 1000))

    @app.get("/ticks/{symbol}", response_model=TickPage)
    async def ticks(
        symbol: str,
        start: datetime | None = None,
        end: datetime | None = None,
        provider: str | None = None,
        limit: Limit = 100,
        cursor: str | None = None,
    ) -> TickPage:
        row = await require_instrument(symbol)
        start, end = history_window(start, end, 1)
        rows = await store.ticks(
            UUID(row["id"]), provider, start, end, limit + 1, decode_cursor(cursor)
        )
        items = rows[:limit]
        return TickPage(
            items=items, next_cursor=encode_cursor(items[-1]) if len(rows) > limit else None
        )

    @app.get("/candles/{symbol}", response_model=CandlePage)
    async def candles(
        symbol: str,
        provider: str,
        interval: str = "1m",
        basis: Literal["last", "mid", "native"] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: Limit = 100,
        after_status: DataStatus | None = None,
    ) -> CandlePage:
        basis = basis or ("native" if await store.native_provider(provider) else "last")
        if interval not in INTERVALS or (basis == "native" and interval == "1s"):
            raise HTTPException(422, "unsupported interval")
        # Full, UTC epoch-aligned buckets only; avoids mode-dependent partial candles.
        seconds = {"1s": 1, "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}[interval]
        if end is None:
            end = datetime.fromtimestamp((int(utc_now().timestamp()) // seconds + 1) * seconds, UTC)
        if start is None:
            start = end - timedelta(seconds=max(seconds, 3600))
        start, end = history_window(
            start, end, 31 if basis == "native" or store.has_aggregates else 1
        )
        if any(t.microsecond or int(t.timestamp()) % seconds for t in (start, end)):
            raise HTTPException(422, "candle start/end must align to UTC interval boundaries")
        row = await require_instrument(symbol)
        if basis == "native":
            rows = await store.native_candles(
                UUID(row["id"]), provider, start, end, interval, limit + 1, after_status
            )
        else:
            rows = await store.candles(
                UUID(row["id"]), provider, start, end, interval, basis, limit + 1, after_status
            )
        items = [
            Candle.model_validate(
                dict(
                    r,
                    complete=r["bucket"] + timedelta(seconds=seconds) <= utc_now()
                    and (basis != "native" or r["observations"] == seconds // 60),
                    bucket_start=r["bucket"],
                    bucket_end=r["bucket"] + timedelta(seconds=seconds),
                    canonical_symbol=symbol,
                )
            )
            for r in rows[:limit]
        ]
        next_start = items[-1].bucket + timedelta(seconds=seconds) if len(rows) > limit else None
        next_after_status = None
        if len(rows) > limit and rows[limit]["bucket"] == items[-1].bucket:
            next_start = items[-1].bucket
            next_after_status = items[-1].realtime_status
        return CandlePage(
            items=items,
            interval=interval,
            basis=basis,
            next_start=next_start,
            next_after_status=next_after_status,
        )

    @app.get("/futures/{exchange}/{root}/{alias}", response_model=InstrumentResponse)
    async def resolve_future(
        exchange: str, root: str, alias: Literal["front", "next"], at: datetime | None = None
    ) -> InstrumentResponse:
        try:
            registry = app.state.registry if combined else await store.registry()
            resolved = registry.resolve(
                exchange, root, utc(at) if at else utc_now(), next_contract=alias == "next"
            )
        except ValueError:
            raise HTTPException(
                422, "explicit roll schedules and aware timestamp required"
            ) from None
        except LookupError:
            raise HTTPException(404, "no eligible concrete contract") from None
        return InstrumentResponse(id=resolved.id, instrument=resolved)

    @app.websocket("/ws/prices")
    async def prices_socket(websocket: WebSocket) -> None:
        if not authorized(websocket.headers.get("authorization"), settings):
            await websocket.close(code=1008)
            return
        if not combined:
            await websocket.accept()
            await websocket.close(
                code=1013, reason="cross-process streaming unavailable; query REST history"
            )
            return
        try:
            client = fanout.register()
        except OverflowError:
            await websocket.close(code=1013)
            return
        await websocket.accept()
        lock = asyncio.Lock()

        async def send(payload: dict[str, Any]) -> None:
            async with lock, asyncio.timeout(settings.websocket_send_timeout_seconds):
                await websocket.send_json(payload)

        async def commands() -> None:
            while True:
                raw = await websocket.receive_text()
                if len(raw) > 16384:
                    await websocket.close(code=1009)
                    return
                try:
                    command = Command.model_validate_json(raw)
                    if any(s not in app.state.registry.by_symbol for s in command.symbols):
                        raise ValueError("unknown symbol")
                    requested = set(command.symbols)
                    updated = (
                        client.symbols | requested
                        if command.action == "subscribe"
                        else client.symbols - requested
                    )
                    if len(updated) > settings.websocket_max_symbols:
                        raise ValueError("subscription capacity exceeded")
                    client.symbols = updated
                    await send({"type": "subscribed", "symbols": sorted(updated)})
                except (ValidationError, ValueError):
                    await send(
                        {
                            "type": "error",
                            "message": "invalid command, unknown symbol or subscription limit",
                        }
                    )

        async def ticks_out() -> None:
            while True:
                tick = await client.queue.get()
                if tick.canonical_symbol in client.symbols:
                    await send(
                        dict(
                            tick.model_dump(mode="json"),
                            type="candle" if isinstance(tick, MarketCandle) else "tick",
                            symbol=tick.canonical_symbol,
                            persisted=isinstance(tick, MarketCandle),
                        )
                    )

        async def overflow() -> None:
            await client.overflow.wait()
            async with lock:
                await websocket.close(
                    code=1013, reason="slow consumer; reconnect and query history"
                )

        try:
            async with anyio.create_task_group() as group:

                async def run(operation: Any) -> None:
                    try:
                        await operation()
                    except (WebSocketDisconnect, TimeoutError, RuntimeError):
                        pass
                    finally:
                        group.cancel_scope.cancel()

                for operation in (commands, ticks_out, overflow):
                    group.start_soon(run, operation)
        finally:
            fanout.unregister(client)
            with anyio.move_on_after(1, shield=True):
                if websocket.client_state.name == "CONNECTED":
                    try:
                        await websocket.close()
                    except RuntimeError:
                        pass

    for route in app.routes:
        if isinstance(route, APIRoute) and route.path != "/health":
            route.openapi_extra = {"security": [{"CompanyBearer": []}]}
    app.include_router(create_router(settings))
    return app
