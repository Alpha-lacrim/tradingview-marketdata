"""Collector process ownership, independently of FastAPI."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg
from prometheus_client import start_http_server

from marketdata.config import Settings
from marketdata.fanout import Fanout
from marketdata.ingestion import IngestionService
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import Store
from marketdata.providers.candles import CandleProvider
from marketdata.providers.factory import build_providers
from marketdata.registry import Registry

logger = logging.getLogger(__name__)
# Database-scoped session lock; only collector owners acquire it, never API workers.
COLLECTOR_LOCK = 0x4D41524B4554


@dataclass
class Collector:
    store: Store
    service: IngestionService
    registry: Registry
    metrics: Metrics
    run_id: UUID
    status_task: asyncio.Task[None] | None = None

    async def publish_status(self, running: bool) -> None:
        healths = [await p.health_check() for p in self.service.providers]
        if self.store.settings.tradingview_storage_mode == "dual":
            candle_healths = [
                health
                for provider, health in zip(self.service.providers, healths, strict=True)
                if isinstance(provider, CandleProvider)
            ]
            for provider, health in zip(self.service.providers, healths, strict=True):
                if isinstance(provider, CandleProvider):
                    continue
                for symbol, quote in health.symbols.items():
                    candle = next(
                        (h.symbols[symbol] for h in candle_healths if symbol in h.symbols), None
                    )
                    if candle is not None:
                        quote.session_expectation = candle.session_expectation
                        quote.candle_state = candle.candle_state
            # Released v1.0 resolves the first matching provider/prefix per source.
            # Publish continuous quote health first, with separate candle diagnostics.
            healths = [h for h in healths if h.storage_mode != "candles"] + candle_healths
        await self.store.publish_collector_status(
            self.run_id,
            running,
            self.service.database_healthy
            and all(c.database_healthy for c in self.service.candle_services.values()),
            self.service.queue.qsize(),
            healths,
        )

    async def publish_loop(self, ownership_lost: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    ownership_lost.wait(), self.store.settings.collector_status_interval_seconds
                )
            except TimeoutError:
                pass
            if ownership_lost.is_set():
                logger.error("collector ownership connection lost; stopping ingestion")
                await self.service.stop()
                raise RuntimeError("collector ownership connection lost; restart required")
            try:
                await self.publish_status(True)
            except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError) as error:
                logger.error(
                    "collector status unavailable",
                    extra={"context": {"category": type(error).__name__}},
                )


@asynccontextmanager
async def collector_lifespan(
    settings: Settings, *, metrics: Metrics | None = None, fanout: Fanout | None = None
) -> AsyncIterator[Collector]:
    if settings.application_role not in {"collector", "combined"}:
        raise ValueError("collector requires APPLICATION_ROLE=collector or combined")
    configure_logging(settings.log_level)
    metrics = metrics or Metrics()
    fanout = fanout or Fanout(
        settings.websocket_queue_capacity, settings.websocket_max_clients, metrics
    )
    store = Store(settings)
    runtime: Collector | None = None
    try:
        await store.connect()
        # Hold a dedicated pool connection for the lifetime of this collector.
        async with store.connection() as lock:
            ownership_lost = asyncio.Event()
            lock.add_termination_listener(lambda _: ownership_lost.set())
            if not await lock.fetchval("SELECT pg_try_advisory_lock($1)", COLLECTOR_LOCK):
                raise RuntimeError("another collector already owns this database")
            try:
                registry = settings.selected_registry(Registry.load(settings.registry_file))
                providers = await build_providers(settings, registry, metrics)
                store.candle_providers = sorted(
                    {
                        m.provider
                        for p in providers
                        if isinstance(p, CandleProvider)
                        for _, m in p.subscriptions
                    }
                )
                service = IngestionService(settings, store, providers, fanout, metrics)
                runtime = Collector(store, service, registry, metrics, uuid4())
                await store.register(registry, candle_providers=store.candle_providers)
                if settings.tradingview_storage_mode == "dual":
                    # Combined-development legacy reads use the same persisted
                    # freshness selection as the independent v1.0 API consumer.
                    store.candle_providers = None
                await runtime.publish_status(True)
                await service.start()
                runtime.status_task = asyncio.create_task(
                    runtime.publish_loop(ownership_lost), name="collector_status"
                )
                yield runtime
            finally:
                if runtime is not None:
                    if runtime.status_task:
                        runtime.status_task.cancel()
                        await asyncio.gather(runtime.status_task, return_exceptions=True)
                    try:
                        await runtime.service.stop()
                    finally:
                        try:
                            await runtime.publish_status(False)
                        except (
                            asyncpg.PostgresError,
                            asyncpg.InterfaceError,
                            OSError,
                            TimeoutError,
                        ):
                            logger.error("final collector status unavailable")
                if not ownership_lost.is_set():
                    try:
                        await lock.execute("SELECT pg_advisory_unlock($1)", COLLECTOR_LOCK)
                    except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
                        logger.error("collector lock connection unavailable during cleanup")
    finally:
        await store.close()


async def run_collector(settings: Settings, stop: asyncio.Event) -> None:
    async with collector_lifespan(settings) as runtime:
        server = thread = None
        waiter = asyncio.create_task(stop.wait(), name="collector_shutdown")
        try:
            if settings.collector_metrics_port:
                server, thread = start_http_server(
                    settings.collector_metrics_port,
                    addr=settings.collector_metrics_host,
                    registry=runtime.metrics.registry,
                )
            tasks = [
                task
                for task in (
                    runtime.status_task,
                    runtime.service.writer_task,
                    runtime.service.monitor_task,
                )
                if task is not None
            ]
            done, _ = await asyncio.wait([waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
            for task in done - {waiter}:
                await task
                raise RuntimeError("collector background task exited unexpectedly")
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            if server is not None:
                await asyncio.to_thread(server.shutdown)
                server.server_close()
            if thread is not None:
                await asyncio.to_thread(thread.join, 2)


async def main() -> None:
    settings = Settings()
    if settings.application_role != "collector":
        raise ValueError("set APPLICATION_ROLE=collector for the collector entry point")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    previous = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stop.set))
    try:
        await run_collector(settings, stop)
    finally:
        for signum, handler in previous.items():
            with suppress(ValueError):
                signal.signal(signum, handler)


if __name__ == "__main__":
    asyncio.run(main())
