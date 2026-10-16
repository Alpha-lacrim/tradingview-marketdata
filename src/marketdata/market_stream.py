"""One bounded journal reader per API worker, with independent downstream queues."""

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

import anyio
import asyncpg
from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ValidationError

from marketdata.api_models import ErrorResponse
from marketdata.api_v1 import completed_candle, identity, price_item
from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import Instrument, Tick, utc_now
from marketdata.events import decode_watermark, encode_watermark, event_page, persisted_snapshot
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.ws_models import (
    CandleEvent,
    ChangePage,
    Heartbeat,
    MarketCommand,
    PriceEvent,
    PriceSnapshot,
    SocketError,
    SourceState,
    SourceStatus,
    SubscriptionAck,
)

logger = logging.getLogger(__name__)
DATABASE_ERRORS = (
    asyncpg.PostgresError,
    asyncpg.InterfaceError,
    OSError,
    TimeoutError,
    RuntimeError,
)


def render_event(row: dict[str, Any], epoch: Any, settings: Settings) -> PriceEvent | CandleEvent:
    payload = json.loads(row["payload"])
    source = dict(
        instrument=Instrument.model_validate_json(row["instrument"]),
        provider=row["provider"],
        provider_symbol=payload["provider_symbol"],
        tick=None,
        candle=None,
    )
    common = dict(
        server_timestamp=utc_now(),
        cursor=encode_watermark(epoch, row["id"]),
        event_id=str(row["id"]),
        recorded_at=row["recorded_at"],
    )
    if row["kind"] == "price_update":
        source["tick"] = Tick.model_validate(payload)
        return PriceEvent(**common, data=price_item(source, utc_now(), settings))
    bar = MarketCandle.model_validate(payload)
    return CandleEvent(
        **common,
        type=row["kind"],
        data=completed_candle(
            source, dict(bar.model_dump(), bucket=bar.bucket_start, observations=1), "1m"
        ),
    )


@dataclass(eq=False)
class MarketClient:
    queue: asyncio.Queue[str]
    # New symbols start at the snapshot barrier; existing ones keep their own barrier.
    symbols: dict[str, int] = field(default_factory=dict)
    failed: asyncio.Event = field(default_factory=asyncio.Event)
    close_reason: str = "slow consumer; REST resynchronization required"


class MarketHub:
    def __init__(self, store: Store, settings: Settings, metrics: Metrics):
        self.store, self.settings, self.metrics = store, settings, metrics
        self.clients: set[MarketClient] = set()
        self.lock = asyncio.Lock()
        self.cursor: str | None = None
        self.database: Literal["available", "unavailable"] = "unavailable"
        self.task: asyncio.Task[None] | None = None
        self.queue_high_water = 0

    def start(self) -> None:
        self.task = asyncio.create_task(self.run(), name="marketdata-journal-reader")
        self.task.add_done_callback(self.reader_done)

    def reader_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error(
                "marketdata reader failed", extra={"context": {"category": type(error).__name__}}
            )
            for client in self.clients:
                client.close_reason = "journal unavailable; REST resynchronization required"
                client.failed.set()

    async def stop(self) -> None:
        for client in self.clients:
            client.close_reason = "API restarting; REST resynchronization required"
            client.failed.set()
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self.task

    def register(self) -> MarketClient:
        if len(self.clients) >= self.settings.websocket_max_clients:
            raise OverflowError
        if not self.clients:
            self.cursor = None
        client = MarketClient(asyncio.Queue(maxsize=self.settings.websocket_queue_capacity))
        self.clients.add(client)
        return client

    def enqueue(self, client: MarketClient, message: BaseModel) -> None:
        if client.failed.is_set():
            return
        raw = message.model_dump_json()
        if len(raw.encode()) > self.settings.websocket_message_max_bytes:
            client.close_reason = "message capacity exceeded; REST resynchronization required"
            client.failed.set()
            return
        try:
            client.queue.put_nowait(raw)
            self.queue_high_water = max(self.queue_high_water, client.queue.qsize())
            self.metrics.api_ws_queue.labels("high_water").set(self.queue_high_water)
        except asyncio.QueueFull:
            self.metrics.ws_dropped.inc()
            client.failed.set()

    def progress(self, client: MarketClient) -> str | None:
        if self.cursor is None:
            return None
        _, position = decode_watermark(self.cursor)
        return self.cursor if position >= max(client.symbols.values(), default=0) else None

    async def reconcile(self) -> bool:
        async with self.lock, asyncio.timeout(self.settings.db_timeout_seconds):
            page = await event_page(
                self.store, self.cursor, self.settings.marketdata_event_batch_size
            )
            for row in page["rows"]:
                event = render_event(row, page["epoch"], self.settings)
                for client in self.clients:
                    barrier = client.symbols.get(row["canonical_symbol"])
                    if barrier is not None and row["id"] > barrier:
                        self.enqueue(client, event)
            self.cursor = page["cursor"]
            self.database = "available"
            return bool(page["has_more"])

    async def run(self) -> None:
        next_status = 0.0
        loop = asyncio.get_running_loop()
        while True:
            if not self.clients:
                await asyncio.sleep(self.settings.marketdata_poll_seconds)
                continue
            more = False
            try:
                more = await self.reconcile()
            except HTTPException as error:
                if error.status_code != 409:
                    raise
                async with self.lock:
                    for client in self.clients:
                        self.enqueue(
                            client,
                            SocketError(
                                type="resync_required",
                                server_timestamp=utc_now(),
                                code="cursor_expired",
                                message="refresh loaded REST history",
                            ),
                        )
                        client.close_reason = (
                            "journal cursor expired; REST resynchronization required"
                        )
                        client.failed.set()
                    # Current state will be initialized by the next successful reconciliation.
                    self.cursor = None
            except DATABASE_ERRORS as error:
                if self.database != "unavailable":
                    logger.warning(
                        "marketdata reconciliation unavailable",
                        extra={"context": {"category": type(error).__name__}},
                    )
                self.database = "unavailable"
            if loop.time() >= next_status:
                await self.statuses()
                next_status = loop.time() + self.settings.websocket_heartbeat_seconds
            # A bounded page and a scheduling point prevent a large catch-up starving commands.
            await asyncio.sleep(0 if more else self.settings.marketdata_poll_seconds)

    async def statuses(self) -> None:
        async with self.lock:
            active = [c for c in self.clients if not c.failed.is_set()]
            if not active:
                return
            symbols = sorted({s for c in active for s in c.symbols})
            rows: list[dict[str, Any]] = []
            snapshot = None
            try:
                async with asyncio.timeout(self.settings.db_timeout_seconds):
                    snapshot = await self.store.collector_status()
                    # Chunk the union, not one query per client. More source mappings than
                    # the configured snapshot bound cause explicit resync for affected clients.
                    for offset in range(0, len(symbols), 100):
                        part = await persisted_snapshot(
                            self.store,
                            symbols[offset : offset + 100],
                            self.settings.websocket_snapshot_max_rows,
                        )
                        rows.extend(part["rows"])
            except (*DATABASE_ERRORS, HTTPException):
                self.database = "unavailable"
            now = utc_now()
            for client in active:
                selected = [r for r in rows if r["instrument"].canonical_symbol in client.symbols]
                items = []
                for row in selected:
                    price = price_item(row, now, self.settings)
                    health = (
                        next(
                            (
                                p
                                for p in snapshot["providers"]
                                if p.provider == row["provider"]
                                or (
                                    p.provider.startswith(row["provider"] + "_")
                                    and price.canonical_symbol in p.symbols
                                )
                            ),
                            None,
                        )
                        if snapshot
                        else None
                    )
                    symbol_health = health.symbols.get(price.canonical_symbol) if health else None
                    source_fresh = bool(snapshot and snapshot["fresh"])
                    if snapshot and "source_health" in snapshot:
                        key = (price.canonical_symbol, row["provider"])
                        health = snapshot["source_health"].get(key)
                        symbol_health = (
                            health.symbols.get(price.canonical_symbol) if health else None
                        )
                        source_fresh = snapshot["source_fresh"].get(key, False)
                    items.append(
                        SourceState(
                            **identity(row),
                            freshness=price.freshness,
                            quote_freshness=price.quote_freshness,
                            age_ms=price.age_ms,
                            price_type=price.price_type,
                            price_basis=price.price_basis,
                            realtime_quote_available=price.realtime_quote_available,
                            source_timestamp=price.source_timestamp,
                            received_timestamp=price.received_timestamp,
                            provider_state=health.state if health else "unknown",
                            transport=health.transport if health else "unknown",
                            session_expectation=symbol_health.session_expectation
                            if symbol_health and source_fresh
                            else "UNKNOWN",
                            candle_state=symbol_health.candle_state
                            if symbol_health and source_fresh
                            else None,
                        )
                    )
                batches = [
                    items[i : i + self.settings.websocket_snapshot_batch_size]
                    for i in range(0, len(items), self.settings.websocket_snapshot_batch_size)
                ] or [[]]
                for index, batch in enumerate(batches):
                    self.enqueue(
                        client,
                        SourceStatus(
                            server_timestamp=now,
                            database=self.database,
                            collector=("running" if snapshot["running"] else "stopped")
                            if snapshot and snapshot["fresh"]
                            else "unknown",
                            batch=index,
                            final=index == len(batches) - 1,
                            items=batch,
                        ),
                    )
                self.enqueue(
                    client,
                    Heartbeat(
                        server_timestamp=now, database=self.database, cursor=self.progress(client)
                    ),
                )

    async def command(self, client: MarketClient, command: MarketCommand) -> None:
        if command.action == "disconnect":
            return
        async with self.lock, asyncio.timeout(self.settings.db_timeout_seconds):
            now = utc_now()
            requested = set(command.symbols)
            current = set(client.symbols)
            updated = current | requested if command.action == "subscribe" else current - requested
            if command.action == "ping":
                self.enqueue(
                    client,
                    Heartbeat(
                        server_timestamp=now,
                        cursor=self.progress(client),
                        database=self.database,
                        request_id=command.request_id,
                    ),
                )
                return
            if command.action == "snapshot":
                updated = current
                if requested - current:
                    raise HTTPException(422, "snapshot symbols must be subscribed")
            if len(updated) > min(100, self.settings.websocket_max_symbols):
                raise HTTPException(422, "subscription capacity exceeded")
            total = sum(len(c.symbols) for c in self.clients if c is not client) + len(updated)
            if total > self.settings.websocket_max_total_subscriptions:
                raise HTTPException(422, "worker subscription capacity exceeded")
            targets = requested if command.action == "subscribe" else requested or current
            if command.action == "unsubscribe":
                client.symbols = {s: b for s, b in client.symbols.items() if s in updated}
                self.enqueue(
                    client,
                    SubscriptionAck(
                        server_timestamp=now,
                        action="unsubscribe",
                        symbols=sorted(updated),
                        cursor=self.progress(client),
                        request_id=command.request_id,
                    ),
                )
                return
            if not targets:
                raise HTTPException(422, "no subscribed symbols")
            snapshot = await persisted_snapshot(
                self.store, sorted(targets), self.settings.websocket_snapshot_max_rows
            )
            if self.cursor is None:
                self.cursor = snapshot["cursor"]
            _, barrier = decode_watermark(snapshot["cursor"])
            for symbol in updated:
                client.symbols.setdefault(symbol, barrier)
            self.enqueue(
                client,
                SubscriptionAck(
                    server_timestamp=now,
                    action=command.action,
                    symbols=sorted(updated),
                    cursor=self.cursor,
                    snapshot_cursor=snapshot["cursor"],
                    request_id=command.request_id,
                ),
            )
            items = [price_item(r, now, self.settings) for r in snapshot["rows"]]
            batches = [
                items[i : i + self.settings.websocket_snapshot_batch_size]
                for i in range(0, len(items), self.settings.websocket_snapshot_batch_size)
            ]
            for index, batch in enumerate(batches):
                self.enqueue(
                    client,
                    PriceSnapshot(
                        server_timestamp=now,
                        request_id=command.request_id,
                        snapshot_cursor=snapshot["cursor"],
                        batch=index,
                        final=index == len(batches) - 1,
                        items=batch,
                    ),
                )


def create_stream_router(settings: Settings) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1",
        responses={code: {"model": ErrorResponse} for code in (401, 403, 409, 422, 503)},
    )

    @router.get("/marketdata/changes", response_model=ChangePage)
    async def changes(
        request: Request,
        after: str | None = Query(default=None, max_length=128),
        until: str | None = Query(default=None, max_length=128),
        limit: int = Query(default=250, ge=1, le=1000),
    ) -> ChangePage:
        if limit > settings.api_max_rows:
            raise HTTPException(422, "page capacity exceeded")
        page = await event_page(request.app.state.store, after, limit, until)
        return ChangePage(
            items=[render_event(r, page["epoch"], settings) for r in page["rows"]],
            cursor=page["cursor"],
            until=page["until"],
            has_more=page["has_more"],
        )

    @router.websocket("/ws/marketdata")
    async def marketdata_socket(websocket: WebSocket) -> None:
        from marketdata.api import authorized

        # Origin is required even with a valid service token. The trusted BFF must forward
        # the validated browser origin, never arbitrary user-supplied authorization.
        if (
            not authorized(websocket.headers.get("authorization"), settings)
            or websocket.headers.get("origin") not in settings.websocket_allowed_origins
        ):
            await websocket.close(code=1008, reason="authentication or origin denied")
            return
        hub: MarketHub = websocket.app.state.market_hub
        try:
            async with hub.lock:
                client = hub.register()
        except OverflowError:
            await websocket.accept()
            await websocket.close(code=1013, reason="worker client capacity exceeded")
            return
        try:
            await websocket.accept()
        except BaseException:
            # A failed/cancelled handshake must not consume an admission slot. No await
            # is needed for this synchronous set cleanup, even under cancellation.
            hub.clients.discard(client)
            raise

        async def sender() -> None:
            while True:
                raw = await client.queue.get()
                async with asyncio.timeout(settings.websocket_send_timeout_seconds):
                    await websocket.send_text(raw)

        async def receiver() -> None:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    return
                raw = message.get("text")
                if raw is None:
                    await websocket.close(code=1003, reason="JSON text commands required")
                    return
                if len(raw.encode()) > settings.websocket_message_max_bytes:
                    await websocket.close(code=1009, reason="command too large")
                    return
                command = None
                try:
                    command = MarketCommand.model_validate_json(raw)
                    if command.action == "disconnect":
                        await websocket.close(code=1000, reason="client disconnect")
                        return
                    await hub.command(client, command)
                except (ValidationError, HTTPException) as error:
                    hub.enqueue(
                        client,
                        SocketError(
                            server_timestamp=utc_now(),
                            code="invalid_command"
                            if isinstance(error, ValidationError)
                            else "unknown_symbol"
                            if error.status_code == 404
                            else "subscription_limit",
                            message="invalid command"
                            if isinstance(error, ValidationError)
                            else str(error.detail),
                            request_id=command.request_id if command else None,
                        ),
                    )
                except DATABASE_ERRORS:
                    hub.enqueue(
                        client,
                        SocketError(
                            server_timestamp=utc_now(),
                            code="database_unavailable",
                            message="database temporarily unavailable",
                            request_id=command.request_id if command else None,
                        ),
                    )

        async def failed() -> None:
            await client.failed.wait()
            # Group cancellation interrupts a blocked send before final close.

        close_code, close_reason = 1000, ""
        try:
            async with anyio.create_task_group() as group:

                async def run(operation: Any) -> None:
                    nonlocal close_code, close_reason
                    try:
                        await operation()
                    except TimeoutError:
                        close_code, close_reason = (
                            1013,
                            "send timeout; REST resynchronization required",
                        )
                    except (WebSocketDisconnect, RuntimeError, OSError):
                        pass
                    finally:
                        group.cancel_scope.cancel()

                for operation in (sender, receiver, failed):
                    group.start_soon(run, operation)
        finally:
            async with hub.lock:
                hub.clients.discard(client)
            if client.failed.is_set():
                close_code, close_reason = 1013, client.close_reason
            with anyio.move_on_after(settings.websocket_send_timeout_seconds, shield=True):
                if websocket.application_state.name == "CONNECTED":
                    with contextlib.suppress(RuntimeError, OSError, WebSocketDisconnect):
                        await websocket.close(code=close_code, reason=close_reason)

    return router
