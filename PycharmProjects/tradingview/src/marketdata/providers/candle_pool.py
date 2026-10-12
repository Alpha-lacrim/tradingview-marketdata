"""Bounded connection workers behind ONE CandleIngestion scheduler.

Limits are supplied by a provider integration, never inferred from observed access.
Workers must use RequestBudget for each outstanding request and publish completed_results
incrementally if partial results are to survive a cycle deadline.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentConnectionError, PermanentProviderError, Subscription
from marketdata.providers.candles import CandleProvider

Result = list[MarketCandle] | Exception
Job = tuple[Subscription, datetime, datetime, int]


@dataclass(frozen=True)
class PoolLimits:
    """Integration's verified ceilings (synthetic integrations declare simulated limits)."""

    connections: int
    instruments_per_connection: int
    total_instruments: int
    concurrent_requests: int
    requests_per_connection: int = 12


class RequestBudget:
    def __init__(self, limit: int, metrics: Metrics, provider: str):
        self.slots = asyncio.BoundedSemaphore(limit)
        self.active = self.peak = 0
        self.metrics, self.provider = metrics, provider

    async def acquire(self) -> None:
        await self.slots.acquire()
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.metrics.candle_inflight.labels(self.provider).set(self.active)

    def release(self) -> None:
        self.active -= 1
        self.slots.release()
        self.metrics.candle_inflight.labels(self.provider).set(self.active)


@dataclass
class Worker:
    provider: CandleProvider
    retry_at: float = 0
    attempts: int = 0


class CandleConnectionPool(CandleProvider):
    handles_transport_backoff = True

    def __init__(
        self,
        name: str,
        settings: Settings,
        metrics: Metrics,
        factory: Callable[[str], CandleProvider],
        limits: PoolLimits,
    ):
        super().__init__(name, metrics)
        self.settings = settings
        self.grace = settings.candle_fetch_grace_seconds
        self.backfill_minutes = settings.candle_backfill_minutes
        self.limits = limits
        if (
            min(
                limits.connections,
                limits.instruments_per_connection,
                limits.total_instruments,
                limits.concurrent_requests,
                limits.requests_per_connection,
            )
            < 1
            or settings.candle_pool_connections > limits.connections
            or settings.candle_pool_worker_capacity > limits.instruments_per_connection
            or settings.candle_pool_max_instruments > limits.total_instruments
            or settings.candle_pool_concurrency > limits.concurrent_requests
        ):
            raise ValueError("candle pool exceeds declared provider limits")
        self.workers = [
            Worker(factory(f"w{n:02d}")) for n in range(settings.candle_pool_connections)
        ]
        if any(w.provider.name != name for w in self.workers):
            raise ValueError("pool workers must have the same provider identity")
        if any(not w.provider.supports_request_budget for w in self.workers):
            raise ValueError("pool workers must enforce the shared request budget")
        if any(
            getattr(w.provider, "effective_concurrency", 1) > limits.requests_per_connection
            for w in self.workers
        ):
            raise ValueError("worker concurrency exceeds declared provider limits")
        if len({id(w.provider) for w in self.workers}) != len(self.workers):
            raise ValueError("pool factory must create independent workers")
        for n, worker in enumerate(self.workers):
            worker.provider.worker_id = f"w{n:02d}"
        self.owners: dict[str, int] = {}
        self.tasks: list[asyncio.Task[None]] = []
        self.opened = False
        self.retry_at = 0.0
        self.stopped: PermanentConnectionError | None = None
        self.effective_concurrency = settings.candle_pool_concurrency

    def refresh_health(self, now: datetime | None = None) -> None:
        self.metrics.candle_pool_sockets.labels(self.name).set(
            sum(w.provider.health.transport == "connected" for w in self.workers)
        )
        stats = self.health.candle_retrieval
        if self.request_budget is not None:
            stats.update(
                active_requests=self.request_budget.active, peak_inflight=self.request_budget.peak
            )
        messages = [
            w.provider.health.last_message_at
            for w in self.workers
            if w.provider.health.last_message_at is not None
        ]
        if messages:
            self.health.last_message_at = max(messages)
        workers = stats.get("workers")
        if isinstance(workers, dict):
            for worker in self.workers:
                p = worker.provider
                if p.worker_id in workers:
                    workers[p.worker_id].update(
                        transport=p.health.transport,
                        health=p.health.state,
                        connection_id=p.health.connection_id,
                        last_error=p.health.last_error,
                        last_message_at=p.health.last_message_at,
                        backoff_remaining_seconds=max(0, worker.retry_at - time.monotonic()),
                        retrieval=p.health.candle_retrieval,
                    )
        super().refresh_health(now)

    async def subscribe(self, subscriptions: list[Subscription]) -> None:
        if self.opened or self.tasks:
            raise RuntimeError("cannot replace ownership during collection")
        symbols = [i.canonical_symbol for i, _ in subscriptions]
        identities = [(i.id, m.provider) for i, m in subscriptions]
        sources = [(m.provider, m.provider_symbol) for _, m in subscriptions]
        capacity = self.settings.candle_pool_worker_capacity
        if (
            len(set(symbols)) != len(symbols)
            or len(set(identities)) != len(identities)
            or len(set(sources)) != len(sources)
            or len(symbols)
            > min(self.settings.candle_pool_max_instruments, capacity * len(self.workers))
        ):
            raise ValueError("unique instruments required within bounded pool capacity")
        # Sorted round robin balances sizes, reproducible regardless of input order.
        assignments: list[list[Subscription]] = [[] for _ in self.workers]
        self.owners = {}
        for n, pair in enumerate(sorted(subscriptions, key=lambda p: p[0].canonical_symbol)):
            owner = n % len(self.workers)
            assignments[owner].append(pair)
            self.owners[pair[0].canonical_symbol] = owner
        for worker, pairs in zip(self.workers, assignments, strict=True):
            await worker.provider.subscribe(pairs)
        await super().subscribe(subscriptions)
        self.health.candle_retrieval = {"ownership": dict(self.owners)}

    async def connect(self) -> None:
        if self.opened or self.tasks:
            raise RuntimeError("candle pool already collecting")
        if self.stopped:
            raise self.stopped
        self.opened = True
        self.connection_started()

    async def _close(self, provider: CandleProvider) -> str | None:
        try:
            async with asyncio.timeout(self.settings.candle_pool_cleanup_seconds):
                await provider.disconnect()
        except Exception as error:
            self.health.last_error = f"worker_cleanup_{type(error).__name__}"
            self.health.error_count += 1
            # Uncertain resource disposal must not be followed by replacement sockets.
            self.stopped = PermanentConnectionError("candle_worker_cleanup_failed")
            return self.health.last_error
        return None

    async def disconnect(self) -> None:
        tasks = list(self.tasks)
        for task in tasks:
            if not task.cancelling():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Worker tasks own cleanup; also close idle/partially established workers.
        await asyncio.gather(*(self._close(w.provider) for w in self.workers))
        self.opened = False
        self.transport_disconnected()

    async def fetch_completed(
        self, pair: Subscription, start: datetime, end: datetime, count: int
    ) -> list[MarketCandle]:
        result = (await self.fetch_many([(pair, start, end, count)]))[pair[0].canonical_symbol]
        if isinstance(result, Exception):
            raise result
        return result

    async def fetch_many(self, requests: list[Job]) -> dict[str, Result]:
        if not self.opened or self.tasks:
            raise RuntimeError("pool must be connected and have one collection owner")
        symbols = [p[0].canonical_symbol for p, _, _, _ in requests]
        registered = {i.canonical_symbol: pair for pair in self.subscriptions for i in [pair[0]]}
        if len(set(symbols)) != len(symbols) or any(
            pair != registered.get(pair[0].canonical_symbol) for pair, _, _, _ in requests
        ):
            raise ValueError("pool jobs must have unique registered identities")
        assigned: list[list[Job]] = [[] for _ in self.workers]
        for job in requests:
            assigned[self.owners[job[0][0].canonical_symbol]].append(job)
        self.timings.clear()
        self.completed_results.clear()
        budget = RequestBudget(self.effective_concurrency, self.metrics, self.name)
        self.request_budget = budget
        self.health.requested_symbol_count = len(requests)
        started = time.perf_counter()
        deadline = asyncio.get_running_loop().time() + self.settings.candle_pool_deadline_seconds
        stats: dict[str, Any] = dict(
            strategy="connection_pool",
            jobs=len(requests),
            sockets=sum(bool(jobs) for jobs in assigned),
            configured_connections=len(self.workers),
            window=self.effective_concurrency,
            peak_inflight=0,
            ownership=dict(self.owners),
            workers={},
            errors={},
            timeouts=0,
            pending_futures=0,
            sessions_current=0,
        )
        self.health.candle_retrieval = stats
        self.metrics.candle_window.labels(self.name).set(self.effective_concurrency)
        self.metrics.candle_instrument_count.labels(self.name).set(len(requests))

        async def collect(index: int, jobs: list[Job]) -> None:
            worker = self.workers[index]
            p = worker.provider
            p.request_budget = budget
            p.completed_results.clear()
            p.timings.clear()
            p.health.candle_retrieval = {}
            worker_stats: dict[str, Any] = dict(
                worker_id=p.worker_id,
                jobs=len(jobs),
                state="connecting",
                error=None,
                phase="handshake",
                deadline_exceeded=False,
            )
            stats["workers"][p.worker_id] = worker_stats
            timer = time.perf_counter()
            result: dict[str, Result] = {}
            try:
                if self.stopped:
                    raise self.stopped
                if time.monotonic() < max(worker.retry_at, self.retry_at):
                    raise ConnectionError("candle_worker_backoff")
                async with asyncio.timeout_at(deadline):
                    await p.connect()
                    worker_stats["state"] = "collecting"
                    worker_stats["phase"] = "requests"
                    self.transport_connected()
                    result = await p.fetch_many(jobs)
            except Exception as error:
                worker_stats["error"] = type(error).__name__
                worker_stats["deadline_exceeded"] = (
                    isinstance(error, TimeoutError)
                    and asyncio.get_running_loop().time() >= deadline
                )
                result = dict(p.completed_results)
                result.update(
                    {
                        pair[0].canonical_symbol: error
                        for pair, _, _, _ in jobs
                        if pair[0].canonical_symbol not in result
                    }
                )
            finally:
                worker_stats["cleanup_error"] = await self._close(p)
                # Merge only this worker's owned identities/windows, even if an
                # adapter accidentally returns another worker's key or candle.
                checked: dict[str, Result] = {}
                for pair, start, end, _ in jobs:
                    i, m = pair
                    value = result.get(i.canonical_symbol, ConnectionError("missing_worker_result"))
                    if isinstance(value, list) and any(
                        c.instrument_id != i.id
                        or c.canonical_symbol != i.canonical_symbol
                        or c.provider != m.provider
                        or c.provider_symbol != m.provider_symbol
                        or c.bucket_start < start
                        or c.bucket_end > end
                        for c in value
                    ):
                        value = ValueError("worker_candle_identity_or_window_mismatch")
                    checked[i.canonical_symbol] = value
                result = checked
                self.timings.update(
                    {s: {**t, "worker_id": p.worker_id} for s, t in p.timings.items()}
                )
                self.session_info.update(p.session_info)
                for pair, _, _, _ in jobs:
                    symbol = pair[0].canonical_symbol
                    source = p.health.symbols[symbol]
                    target = self.health.symbols[symbol]
                    for field in (
                        "last_fetch_started_at",
                        "last_fetch_completed_at",
                        "last_successful_fetch_at",
                    ):
                        setattr(target, field, getattr(source, field))
                worker_stats.update(
                    duration_seconds=time.perf_counter() - timer,
                    state="disconnected",
                    retrieval=p.health.candle_retrieval,
                    successful_requests=sum(isinstance(v, list) for v in result.values()),
                )
                self.completed_results.update(result)
            errors = [r for r in result.values() if isinstance(r, Exception)]
            worker_stats["failed_requests"] = len(errors)
            if errors:
                p.health.last_error = type(errors[0]).__name__
                p.health.error_count += len(errors)
                p.transition("degraded")
            permanent = next((e for e in errors if isinstance(e, PermanentConnectionError)), None)
            if permanent:
                self.stopped = permanent
            transport = [
                e
                for e in errors
                if not isinstance(
                    e, (PermanentProviderError, ValueError, KeyError, TypeError, IndexError)
                )
            ]
            # Worker-owned network backoff; no in-cycle reconnect or work reassignment.
            if transport and not all(str(e) == "candle_worker_backoff" for e in transport):
                worker.attempts += 1
                delay = min(
                    self.settings.reconnect_max_seconds,
                    self.settings.reconnect_min_seconds * 2 ** min(worker.attempts, 16),
                )
                worker.retry_at = time.monotonic() + random.uniform(delay, delay * 1.2)
            elif not errors:
                worker.attempts = 0
                worker.retry_at = 0
            # Explicit HTTP 429 is provider-wide: do not create replacement sockets.
            if any(
                getattr(getattr(e, "response", None), "status_code", None) == 429 for e in errors
            ):
                self.retry_at = max(self.retry_at, worker.retry_at)
                self.effective_concurrency = max(1, self.effective_concurrency // 2)
            worker_stats.update(
                retry_at_monotonic=worker.retry_at, reconnect_attempts=worker.attempts
            )

        self.tasks = [
            asyncio.create_task(collect(n, jobs), name=f"candle-pool-w{n:02d}")
            for n, jobs in enumerate(assigned)
            if jobs
        ]
        try:
            await asyncio.gather(*self.tasks)
        finally:
            for task in self.tasks:
                if not task.done():
                    if not task.cancelling():
                        task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()
            stats.update(
                peak_inflight=budget.peak,
                active_requests=budget.active,
                duration_seconds=time.perf_counter() - started,
                successful_requests=sum(
                    isinstance(v, list) for v in self.completed_results.values()
                ),
            )
            for worker in self.workers:
                if worker.provider.worker_id not in stats["workers"]:
                    continue
                retrieval: dict[str, Any] = dict(worker.provider.health.candle_retrieval)
                for key in ("timeouts", "sessions_current", "pending_futures"):
                    stats[key] += retrieval.get(key, 0)
                for key, value in dict(retrieval.get("errors", {})).items():
                    stats["errors"][key] = stats["errors"].get(key, 0) + value
            self.transport_disconnected()
            stats["next_window"] = self.effective_concurrency
            stats["requests_per_second"] = stats["successful_requests"] / max(
                0.001, stats["duration_seconds"]
            )
            if stats["workers"]:
                stats["slowest_worker"] = max(
                    stats["workers"],
                    key=lambda worker_id: stats["workers"][worker_id]["duration_seconds"],
                )
        return dict(self.completed_results)
