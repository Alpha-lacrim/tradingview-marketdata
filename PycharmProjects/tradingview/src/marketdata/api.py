from __future__ import annotations

import asyncio
import base64
import hmac
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

import anyio
import asyncpg
from fastapi import FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field, ValidationError

from marketdata.config import Settings
from marketdata.domain import DataStatus, Instrument, ProviderInstrument, Tick, utc, utc_now
from marketdata.fanout import Fanout
from marketdata.ingestion import IngestionService
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import INTERVALS, Store
from marketdata.providers.base import ProviderHealth
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry

logger = logging.getLogger(__name__)
Limit = Annotated[int, Query(ge=1, le=1000)]


class InstrumentResponse(BaseModel):
    id: UUID
    instrument: Instrument
    mappings: list[ProviderInstrument] = Field(default_factory=list)


class TickPage(BaseModel):
    items: list[Tick]
    next_cursor: str | None = None


class LatestPage(BaseModel):
    items: list[Tick]
    next_after: str | None = None


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


class CandlePage(BaseModel):
    items: list[Candle]
    interval: str
    basis: Literal["last", "mid"]
    next_start: datetime | None = None
    next_after_status: DataStatus | None = None


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    database: bool
    ingestion_running: bool
    queue_depth: int
    provider_states: dict[str, str]


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
    return hmac.compare_digest(value or "", "Bearer " + settings.api_key.get_secret_value())


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    metrics = Metrics()
    store = Store(settings)
    fanout = Fanout(settings.websocket_queue_capacity, settings.websocket_max_clients, metrics)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level)
        registry = settings.selected_registry(Registry.load(settings.registry_file))
        providers = await build_providers(settings, registry, metrics)
        service = IngestionService(settings, store, providers, fanout, metrics)
        app.state.service, app.state.registry, app.state.store = service, registry, store
        try:
            await store.connect()
            await store.register(registry)
            await service.start()
            yield
        finally:
            await service.stop()
            await store.close()

    app = FastAPI(title="Multi-asset market data", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def authenticate(request: Request, call_next: Any) -> Response:
        if request.url.path != "/health" and not authorized(
            request.headers.get("authorization"), settings
        ):
            return JSONResponse({"detail": "authentication required"}, status_code=401)
        response: Response = await call_next(request)
        return response

    async def database_error(request: Request, error: Exception) -> JSONResponse:
        logger.error(
            "query database unavailable", extra={"context": {"category": type(error).__name__}}
        )
        return JSONResponse({"detail": "database temporarily unavailable"}, status_code=503)

    for exception_type in (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
        app.add_exception_handler(exception_type, database_error)

    @app.get("/health", response_model=Health)
    async def health(response: Response) -> Health:
        service: IngestionService = app.state.service
        try:
            async with asyncio.timeout(settings.db_timeout_seconds):
                database = await store.ping() and service.database_healthy
        except Exception:
            database = False
        running = service.writer_task is not None and not service.writer_task.done()
        states: dict[str, str] = {p.name: p.health.state for p in service.providers}
        ok = database and running and all(s == "healthy" for s in states.values())
        if not ok:
            response.status_code = 503
        return Health(
            status="ok" if ok else "degraded",
            database=database,
            ingestion_running=running,
            queue_depth=service.queue.qsize(),
            provider_states=states,
        )

    @app.get("/providers", response_model=list[ProviderHealth])
    async def provider_health() -> list[ProviderHealth]:
        return [await p.health_check() for p in app.state.service.providers]

    @app.get("/metrics", include_in_schema=False)
    async def prometheus() -> Response:
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

    @app.get("/prices/latest", response_model=LatestPage)
    async def latest(
        provider: str | None = None, limit: Limit = 100, after: str = ""
    ) -> LatestPage:
        rows = await store.latest(None, provider, limit + 1, after)
        items = rows[:limit]
        next_after = (
            items[-1].canonical_symbol + "/" + items[-1].provider if len(rows) > limit else None
        )
        return LatestPage(items=items, next_after=next_after)

    @app.get("/prices/latest/{symbol}", response_model=list[Tick])
    async def latest_symbol(symbol: str, provider: str | None = None) -> list[Tick]:
        await require_instrument(symbol)
        return await store.latest(symbol, provider, 1000)

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
        basis: Literal["last", "mid"] = "last",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: Limit = 100,
        after_status: DataStatus | None = None,
    ) -> CandlePage:
        if interval not in INTERVALS:
            raise HTTPException(422, "unsupported interval")
        # Full, UTC epoch-aligned buckets only; avoids mode-dependent partial candles.
        seconds = {"1s": 1, "1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}[interval]
        if end is None:
            end = datetime.fromtimestamp((int(utc_now().timestamp()) // seconds + 1) * seconds, UTC)
        if start is None:
            start = end - timedelta(seconds=max(seconds, 3600))
        start, end = history_window(start, end, 31 if store.has_aggregates else 1)
        if any(t.microsecond or int(t.timestamp()) % seconds for t in (start, end)):
            raise HTTPException(422, "candle start/end must align to UTC interval boundaries")
        row = await require_instrument(symbol)
        rows = await store.candles(
            UUID(row["id"]), provider, start, end, interval, basis, limit + 1, after_status
        )
        items = [
            Candle.model_validate(
                dict(r, complete=r["bucket"] + timedelta(seconds=seconds) <= utc_now())
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
            resolved = app.state.registry.resolve(
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
                            type="tick",
                            symbol=tick.canonical_symbol,
                            persisted=False,
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

    return app
