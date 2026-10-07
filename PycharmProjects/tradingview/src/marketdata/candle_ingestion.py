"""Minute-aligned, bounded native-candle collection with durable gap recovery."""

import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from marketdata.candles import MarketCandle, expected_minute, next_collection
from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.fanout import Fanout
from marketdata.providers.base import PermanentConnectionError, PermanentProviderError
from marketdata.providers.candles import CandleProvider

logger = logging.getLogger(__name__)


class CandleStore(Protocol):
    async def candle_progress(self, instrument_id: UUID, provider: str) -> dict[str, Any]: ...
    async def write_candles(
        self,
        candles: list[MarketCandle],
        coverage: list[tuple[UUID, str, datetime, datetime]],
    ) -> int: ...


class CandleIngestion:
    def __init__(
        self, settings: Settings, store: CandleStore, provider: CandleProvider, fanout: Fanout
    ):
        self.settings, self.store, self.provider, self.fanout = settings, store, provider, fanout
        self.database_healthy = True
        self.pending: list[MarketCandle] = []
        self.disabled: set[str] = set()
        self.retry_at = 0.0
        self.reconnect_attempt = 0

    async def cycle(self, now: datetime | None = None) -> None:
        p, settings = self.provider, self.settings
        now = now or utc_now()
        expected = expected_minute(now, settings.candle_fetch_grace_seconds)
        end = expected + timedelta(minutes=1)
        p.metrics.candle_lateness.labels(p.name).set(max(0, (now - end).total_seconds() - p.grace))
        coverage: list[tuple[UUID, str, datetime, datetime]] = []
        self.pending = []
        backfilled = 0
        try:
            for pair in p.subscriptions:
                instrument, mapping = pair
                state = await self.store.candle_progress(instrument.id, mapping.provider)
                p.progress[instrument.canonical_symbol] = state
                beginning = state.get("coverage_start", expected)
                if end <= beginning:
                    continue  # Wall clock moved behind the durable coverage start.
                coverage.append((instrument.id, mapping.provider, beginning, end))
                if instrument.canonical_symbol in self.disabled:
                    continue
                if time.monotonic() < self.retry_at:
                    p.failures.add(instrument.canonical_symbol)
                    continue
                # Request missing completed minutes. Older gaps remain explicit if
                # beyond this bounded history window; no synthetic gap filling.
                last = state.get("last_persisted_minute")
                requested = state.get("earliest_gap") or (
                    last + timedelta(minutes=1) if last else expected
                )
                start = max(
                    beginning,
                    requested,
                    end - timedelta(minutes=settings.candle_backfill_minutes),
                )
                if start >= end:
                    continue
                count = min(
                    settings.candle_backfill_minutes + 1,
                    max(3, int((end - start).total_seconds() // 60) + 1),
                )
                try:
                    if p.health.transport != "connected":
                        await p.connect()
                    candles = await p.fetch_completed(pair, start, end, count)
                    # Defensive boundary independent of the transport/parser.
                    if any(
                        c.bucket_start < start
                        or c.bucket_end > end
                        or c.instrument_id != instrument.id
                        or c.provider != mapping.provider
                        for c in candles
                    ):
                        raise ValueError("candle collection identity/window mismatch")
                    checked = state.get("coverage_end", beginning)
                    gaps = state.get("missing_ranges", [])
                    candles = [
                        c
                        for c in candles
                        if c.bucket_start >= checked
                        or any(lo <= c.bucket_start < hi for lo, hi in gaps)
                    ]
                    self.pending.extend(candles)
                    backfilled += sum(c.bucket_start < expected for c in candles)
                    p.failures.discard(instrument.canonical_symbol)
                    self.reconnect_attempt = 0
                    p.health.consecutive_reconnect_failures = 0
                    if candles:
                        p.health.symbols[instrument.canonical_symbol].source_update_mode = candles[
                            -1
                        ].source_status
                except Exception as error:
                    if isinstance(error, (ValueError, KeyError, TypeError, IndexError)):
                        p.metrics.parse_failures.labels(p.name).inc()
                    p.failures.add(instrument.canonical_symbol)
                    p.metrics.candles.labels(p.name, "fetch_failures").inc()
                    p.health.error_count += 1
                    p.health.last_error = type(error).__name__
                    if isinstance(error, PermanentConnectionError):
                        self.disabled.update(i.canonical_symbol for i, _ in p.subscriptions)
                        p.failures.update(self.disabled)
                    elif isinstance(error, PermanentProviderError):
                        self.disabled.add(instrument.canonical_symbol)
                    else:
                        p.health.reconnect_count += 1
                        p.metrics.reconnects.labels(p.name).inc()
                        if not isinstance(error, (ValueError, KeyError, TypeError, IndexError)):
                            delay = min(
                                settings.reconnect_max_seconds,
                                settings.reconnect_min_seconds
                                * 2 ** min(self.reconnect_attempt, 16),
                            )
                            self.retry_at = time.monotonic() + random.uniform(delay, delay * 1.2)
                            self.reconnect_attempt += 1
                            p.health.consecutive_reconnect_failures += 1
                    logger.error(
                        "candle fetch failed; minute remains a gap",
                        extra={
                            "context": {
                                "provider": p.name,
                                "symbol": instrument.canonical_symbol,
                                "category": type(error).__name__,
                                "operator_intervention": instrument.canonical_symbol
                                in self.disabled,
                            }
                        },
                    )
                    await p.disconnect()
        finally:
            await p.disconnect()
        attempt = 0
        while True:
            try:
                with p.metrics.candle_latency.labels("write").time():
                    changed = await self.store.write_candles(self.pending, coverage)
                self.database_healthy = True
                p.metrics.candles.labels(p.name, "persisted").inc(changed)
                p.metrics.candles.labels(p.name, "upserted").inc(changed)
                p.metrics.candles.labels(p.name, "duplicates").inc(len(self.pending) - changed)
                p.metrics.candles.labels(p.name, "backfilled").inc(backfilled)
                for candle in self.pending:
                    item = p.health.symbols[candle.canonical_symbol]
                    item.last_observation_at = candle.received_timestamp
                    item.observation_count += 1
                    p.health.instrument_last_success[candle.canonical_symbol] = (
                        candle.received_timestamp
                    )
                    p.health.last_success_at = max(
                        p.health.last_success_at or candle.received_timestamp,
                        candle.received_timestamp,
                    )
                    self.fanout.publish(candle)
                persisted, self.pending = self.pending, []
                for i, m in p.subscriptions:
                    state = await self.store.candle_progress(i.id, m.provider)
                    old_state = p.progress.get(i.canonical_symbol, {})
                    checked = old_state.get("coverage_end", state.get("coverage_start", end))
                    new_expected = max(0, int((end - checked).total_seconds() // 60))
                    supplied = sum(
                        c.instrument_id == i.id and c.bucket_start >= checked for c in persisted
                    )
                    p.metrics.candles.labels(p.name, "gaps_detected").inc(
                        max(0, new_expected - supplied)
                    )
                    p.progress[i.canonical_symbol] = state
                p.refresh_health()
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.database_healthy = False
                p.metrics.db_errors.inc()
                logger.error(
                    "candle batch retained during database outage",
                    extra={
                        "context": {
                            "provider": p.name,
                            "category": type(error).__name__,
                            "batch_size": len(self.pending),
                        }
                    },
                )
                attempt += 1
                await asyncio.sleep(random.uniform(0.1, min(5, 0.2 * 2 ** min(attempt, 6))))

    async def run(self) -> None:
        # Startup immediately recovers the latest FULLY closed minute, even mid-minute.
        try:
            while True:
                try:
                    await self.cycle()
                except Exception as error:
                    self.database_healthy = False
                    self.provider.health.last_error = type(error).__name__
                    logger.error(
                        "candle collection cycle failed",
                        extra={
                            "context": {
                                "provider": self.provider.name,
                                "category": type(error).__name__,
                            }
                        },
                    )
                # Recompute wall-clock target; bounded sleeps notice wall-clock changes.
                target = next_collection(utc_now(), self.settings.candle_fetch_grace_seconds)
                while True:
                    remaining = (target - utc_now()).total_seconds()
                    if remaining <= 0:
                        break
                    await asyncio.sleep(min(remaining, 30))
                    if (target - utc_now()).total_seconds() > 90:
                        target = next_collection(
                            utc_now(), self.settings.candle_fetch_grace_seconds
                        )
        finally:
            if self.pending:
                self.provider.metrics.candles.labels(
                    self.provider.name, "shutdown_unpersisted"
                ).inc(len(self.pending))
                logger.error(
                    "unpersisted candle batch on shutdown; native history can recover",
                    extra={"context": {"provider": self.provider.name, "count": len(self.pending)}},
                )
            await self.provider.disconnect()
