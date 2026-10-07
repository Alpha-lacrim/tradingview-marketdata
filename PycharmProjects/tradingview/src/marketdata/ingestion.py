from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import Counter
from contextlib import suppress
from datetime import datetime
from typing import Protocol, cast

from marketdata.candle_ingestion import CandleIngestion, CandleStore
from marketdata.config import Settings
from marketdata.domain import Tick, utc_now
from marketdata.fanout import Fanout
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider, PermanentProviderError
from marketdata.providers.candles import CandleProvider

logger = logging.getLogger(__name__)


class TickWriter(Protocol):
    async def write(self, ticks: list[Tick]) -> int: ...


class IngestionService:
    def __init__(
        self,
        settings: Settings,
        store: TickWriter,
        providers: list[MarketDataProvider],
        fanout: Fanout,
        metrics: Metrics,
    ):
        self.settings, self.store, self.providers = settings, store, providers
        self.fanout, self.metrics = fanout, metrics
        self.queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=settings.queue_capacity)
        self.provider_tasks: list[asyncio.Task[None]] = []
        self.writer_task: asyncio.Task[None] | None = None
        self.monitor_task: asyncio.Task[None] | None = None
        self.stopping = False
        self.pending: list[Tick] = []
        self.database_healthy = True
        self.candle_services = {
            p: CandleIngestion(settings, cast(CandleStore, store), p, fanout)
            for p in providers
            if isinstance(p, CandleProvider)
        }
        self.drop_log_times: dict[tuple[str, str], float] = {}
        self.unlogged_drops: Counter[tuple[str, str]] = Counter()
        for provider in providers:
            provider.configure_freshness(settings.stale_seconds)

    async def start(self) -> None:
        self.provider_tasks = [
            asyncio.create_task(
                self.candle_services[p].run()
                if p in self.candle_services
                else self.run_provider(p),
                name=p.name,
            )
            for p in self.providers
        ]
        self.writer_task = asyncio.create_task(self.write_batches(), name="batch_writer")
        self.monitor_task = asyncio.create_task(self.monitor(), name="feed_monitor")

    def drop(self, ticks: list[Tick], reason: str) -> None:
        for provider, count in Counter(t.provider for t in ticks).items():
            self.metrics.dropped.labels(provider, reason).inc(count)
            key = provider, reason
            self.unlogged_drops[key] += count
            now = time.monotonic()
            if now - self.drop_log_times.get(key, -10) < 1:
                continue
            self.drop_log_times[key] = now
            logged_count = self.unlogged_drops.pop(key)
            logger.error(
                "ticks dropped",
                extra={
                    "context": {
                        "provider": provider,
                        "reason": reason,
                        "count_since_previous_log": logged_count,
                        "queue_depth": self.queue.qsize(),
                    }
                },
            )

    def accept(self, tick: Tick) -> None:
        self.metrics.received.labels(tick.provider).inc()
        # WebSocket updates are explicitly pre-persistence observations.
        self.fanout.publish(tick)
        try:
            self.queue.put_nowait(tick)
        except asyncio.QueueFull:
            self.drop([tick], "persistence_queue_full")
        self.metrics.queue_depth.set(self.queue.qsize())

    async def run_provider(self, provider: MarketDataProvider) -> None:
        attempt = 0
        while not self.stopping:
            successful = 0
            try:
                provider.connection_started(reconnecting=bool(provider.health.reconnect_count))
                async with asyncio.timeout(self.settings.stale_seconds):
                    await provider.connect()
                if provider.health.transport != "connected":
                    provider.transport_connected()
                    provider.subscriptions_requested()
                iterator = provider.stream_ticks().__aiter__()
                while not self.stopping:
                    # Monitor separately sees heartbeats even when a quiet market has no ticks.
                    tick = await anext(iterator)
                    provider.observed(tick)
                    self.accept(tick)
                    successful += 1
                break
            except PermanentProviderError as error:
                provider.transition("degraded")
                provider.health.last_error = str(error)  # Only our static, sanitized categories.
                provider.health.error_count += 1
                logger.error(
                    "provider requires operator intervention",
                    extra={
                        "context": {
                            "provider": provider.name,
                            "category": provider.health.last_error,
                        }
                    },
                )
                break
            except (Exception, asyncio.CancelledError) as error:
                if isinstance(error, asyncio.CancelledError) and (
                    self.stopping or not provider.transport_timed_out
                ):
                    raise
                provider.transition("reconnecting")
                provider.health.error_count += 1
                if not provider.transport_timed_out:
                    provider.health.last_error = type(error).__name__
                provider.health.reconnect_count += 1
                provider.health.consecutive_reconnect_failures += 1
                self.metrics.reconnects.labels(provider.name).inc()
                logger.warning(
                    "provider reconnect",
                    extra={
                        "context": {
                            "provider": provider.name,
                            "category": type(error).__name__,
                            "attempt": attempt + 1,
                        }
                    },
                )
            finally:
                try:
                    async with asyncio.timeout(6):
                        await provider.disconnect()
                except Exception as error:
                    logger.warning(
                        "provider close failed",
                        extra={
                            "context": {"provider": provider.name, "category": type(error).__name__}
                        },
                    )
                provider.transport_disconnected()
                if self.stopping:
                    provider.transition("disconnected")
                elif provider.health.state == "reconnecting":
                    provider.health.transport = "reconnecting"
            if successful >= 10:
                attempt = 0
            delay = min(
                self.settings.reconnect_max_seconds,
                self.settings.reconnect_min_seconds * 2 ** min(attempt, 16),
            )
            attempt += 1
            await asyncio.sleep(
                random.uniform(delay, min(self.settings.reconnect_max_seconds, delay * 1.2))
            )
        if self.stopping:
            provider.transition("disconnected")

    async def monitor(self) -> None:
        while True:
            await asyncio.sleep(1)
            await self.monitor_once()

    async def monitor_once(self, now: datetime | None = None) -> None:
        now = now or utc_now()
        for provider in self.providers:
            provider.refresh_health(now)
            if not isinstance(provider, CandleProvider) and provider.transport_expired(now):
                connection_id = provider.health.connection_id
                reconnect_count = provider.health.reconnect_count
                # Only the owned runner reconnects. The watchdog closes once per session.
                logger.warning(
                    "provider transport timed out",
                    extra={
                        "context": {
                            "provider": provider.name,
                            "category": "transport_frame_timeout",
                        }
                    },
                )
                try:
                    async with asyncio.timeout(6):
                        await provider.disconnect()
                except Exception:
                    logger.warning(
                        "stale feed close failed", extra={"context": {"provider": provider.name}}
                    )
                    # A failed close must not strand the runner in an endless receive.
                    index = self.providers.index(provider)
                    if (
                        index < len(self.provider_tasks)
                        and provider.health.connection_id == connection_id
                        and provider.health.reconnect_count == reconnect_count
                    ):
                        self.provider_tasks[index].cancel()

    async def write_batches(self) -> None:
        while not self.stopping or not self.queue.empty():
            self.pending = []
            deadline = asyncio.get_running_loop().time() + self.settings.batch_flush_seconds
            while len(self.pending) < self.settings.batch_size:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    self.pending.append(await asyncio.wait_for(self.queue.get(), remaining))
                except TimeoutError:
                    break
            self.metrics.queue_depth.set(self.queue.qsize())
            if not self.pending:
                continue
            attempt = 0
            while True:
                try:
                    async with asyncio.timeout(self.settings.db_timeout_seconds):
                        with self.metrics.batch_latency.time():
                            count = await self.store.write(self.pending)
                    self.metrics.persisted.inc(count)
                    self.database_healthy = True
                    for _ in self.pending:
                        self.queue.task_done()
                    self.pending = []
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.database_healthy = False
                    self.metrics.db_errors.inc()
                    logger.error(
                        "batch persistence failed; retained for retry",
                        extra={
                            "context": {
                                "category": type(error).__name__,
                                "batch_size": len(self.pending),
                                "queue_depth": self.queue.qsize(),
                                "attempt": attempt + 1,
                            }
                        },
                    )
                    attempt += 1
                    await asyncio.sleep(random.uniform(0.1, min(5, 0.2 * 2 ** min(attempt, 6))))

    async def stop(self) -> None:
        self.stopping = True
        for task in self.provider_tasks:
            task.cancel()
        await asyncio.gather(*self.provider_tasks, return_exceptions=True)
        if self.monitor_task:
            self.monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.monitor_task
        if self.writer_task:
            try:
                await asyncio.wait_for(
                    asyncio.shield(self.writer_task), self.settings.shutdown_timeout_seconds
                )
            except TimeoutError:
                self.writer_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self.writer_task
                self.drop(self.pending, "shutdown_deadline")
                self.pending = []
                remaining: list[Tick] = []
                while not self.queue.empty():
                    remaining.append(self.queue.get_nowait())
                self.drop(remaining, "shutdown_deadline")
        self.metrics.queue_depth.set(self.queue.qsize())
