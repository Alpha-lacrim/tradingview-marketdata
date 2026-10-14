"""UTC-minute collection, correction overlap and bounded native-history recovery."""

import asyncio
import logging
import math
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
Coverage = (
    tuple[UUID, str, datetime, datetime] | tuple[UUID, str, datetime, datetime, dict[str, Any]]
)


def is_backfill(
    candle: MarketCandle, state: dict[str, Any], expected: datetime, end: datetime, startup: bool
) -> bool:
    checked = state.get("coverage_end", end)
    known_missing = any(
        lo <= candle.bucket_start < hi
        for lo, hi in state.get("missing_ranges", []) + state.get("unknown_ranges", [])
    )
    recovery = bool(state) and (startup or end - checked > timedelta(minutes=1))
    return known_missing or (
        candle.bucket_start >= checked and (candle.bucket_start < expected or recovery)
    )


class CandleStore(Protocol):
    async def candle_progress(self, instrument_id: UUID, provider: str) -> dict[str, Any]: ...
    async def write_candles(self, candles: list[MarketCandle], coverage: list[Coverage]) -> int: ...


class CandleIngestion:
    def __init__(
        self, settings: Settings, store: CandleStore, provider: CandleProvider, fanout: Fanout
    ):
        self.settings, self.store, self.provider, self.fanout = settings, store, provider, fanout
        self.database_healthy = True
        self.pending: list[MarketCandle] = []
        self.disabled: set[str] = set()
        self.recovering: set[str] = set()
        self.retry_at = 0.0
        self.reconnect_attempt = 0
        self.last_attempted_end: datetime | None = None
        self.overrun_reported = False
        provider.backfill_minutes = settings.candle_backfill_minutes

    def log(self, message: str, level: int = logging.INFO, **context: Any) -> None:
        logger.log(level, message, extra={"context": {"provider": self.provider.name, **context}})

    def observe_overrun(self, deadline: datetime, now: datetime) -> bool:
        if now < deadline:
            return False
        if not self.overrun_reported:
            self.overrun_reported = True
            self.provider.metrics.candle_overruns.labels(self.provider.name).inc()
            self.log(
                "candle cycle overrun; finish bounded work then recover at next UTC target",
                level=logging.WARNING,
                next_target=deadline,
                observed_at=now,
            )
        return True

    async def cycle(self, now: datetime | None = None) -> None:
        p, settings = self.provider, self.settings
        now = now or utc_now()
        expected = expected_minute(now, settings.candle_fetch_grace_seconds)
        end = expected + timedelta(minutes=1)
        if self.last_attempted_end is not None and end <= self.last_attempted_end:
            return  # One attempt per closed minute, including clock regressions.
        startup = self.last_attempted_end is None
        self.overrun_reported = False
        self.last_attempted_end = end
        target = end + timedelta(seconds=p.grace)
        p.timings.clear()
        p.health.candle_retrieval = {}
        started, timer = utc_now(), time.perf_counter()
        summary: dict[str, Any] = dict(
            trigger="startup" if startup else "minute",
            bucket_end=end.isoformat(),
            scheduler_target_time=target.isoformat(),
            scheduler_actual_wake_time=now.isoformat(),
            fetch_cycle_started_at=started.isoformat(),
            scheduler_lateness_seconds=max(0, (now - target).total_seconds()),
            grace_to_fetch_start_seconds=max(0, (started - target).total_seconds()),
            collection_budget_seconds=60
            - settings.candle_fetch_grace_seconds
            - settings.candle_cycle_safety_margin_seconds,
        )
        p.metrics.candle_cycle.labels(p.name).inc()
        p.metrics.candle_lateness.labels(p.name).set(summary["scheduler_lateness_seconds"])
        self.log("candle cycle started", **summary)
        if not startup and summary["scheduler_lateness_seconds"] > 5:
            self.log("candle scheduler unusually late", level=logging.WARNING, **summary)
        requests = []
        coverage: list[Coverage] = []
        states: dict[str, dict[str, Any]] = {}
        # Read actual durable state before any upstream interaction. A DB outage
        # cannot create a false in-memory acknowledgement; next cycle rereads it.
        try:
            for pair in p.subscriptions:
                i, m = pair
                state = await self.store.candle_progress(i.id, m.provider)
                states[i.canonical_symbol] = state
                p.progress[i.canonical_symbol] = state
                if i.canonical_symbol not in p.session_info and state.get("session_info"):
                    p.session_info[i.canonical_symbol] = state["session_info"]
                beginning = state.get("coverage_start", expected)
                if end <= beginning:
                    continue
                if i.canonical_symbol in self.disabled or time.monotonic() < self.retry_at:
                    p.failures.add(i.canonical_symbol)
                    continue
                last = state.get("last_persisted_minute")
                candidates = [
                    value
                    for value in (
                        state.get("earliest_gap"),
                        state.get("earliest_unknown"),
                        last + timedelta(minutes=1)
                        if last
                        else state.get("coverage_end", expected),
                    )
                    if value is not None
                ]
                requested = min(candidates)
                if (
                    state.get("missing_count")
                    or state.get("unknown_count")
                    or (state and end - state["coverage_end"] > timedelta(minutes=1))
                ) and i.canonical_symbol not in self.recovering:
                    self.recovering.add(i.canonical_symbol)
                    self.log(
                        "native candle backfill started",
                        canonical_symbol=i.canonical_symbol,
                        requested_start=requested,
                        horizon_minutes=settings.candle_backfill_minutes,
                    )
                overlap = end - timedelta(minutes=settings.candle_correction_lookback_minutes)
                start = max(
                    beginning,
                    min(requested, overlap),
                    end - timedelta(minutes=settings.candle_backfill_minutes),
                )
                count = min(
                    settings.candle_backfill_minutes + 1,
                    max(3, int((end - start).total_seconds() // 60) + 1),
                )
                requests.append((pair, start, end, count))
        except Exception:
            self.database_healthy = False
            p.metrics.db_errors.inc()
            self.log("candle database unavailable before reconciliation", level=logging.ERROR)
            raise
        self.pending = []
        results: dict[str, list[MarketCandle] | Exception] = {}
        try:
            if requests:
                await p.connect()
                results = await p.fetch_many(requests)
        except Exception as error:
            results = {pair[0].canonical_symbol: error for pair, _, _, _ in requests}
        finally:
            await p.disconnect()
        fetch_finished = utc_now()
        summary.update(
            fetch_cycle_finished_at=fetch_finished.isoformat(),
            fetch_cycle_duration_seconds=max(0, time.perf_counter() - timer),
            instruments=dict(p.timings),
            fetch_failures=sum(isinstance(r, Exception) for r in results.values()),
            requested_instruments=len(requests),
            configured_instruments=len(p.subscriptions),
            coverage_only=not requests,
        )
        network_failed = False
        for pair, start, _, _ in requests:
            i, m = pair
            symbol = i.canonical_symbol
            result = results.get(symbol, ConnectionError("missing_candle_result"))
            if not isinstance(result, Exception) and any(
                c.bucket_start < start
                or c.bucket_end > end
                or c.instrument_id != i.id
                or c.provider != m.provider
                or c.provider_symbol != m.provider_symbol
                for c in result
            ):
                result = ValueError("candle collection identity/window mismatch")
            if isinstance(result, Exception):
                p.failures.add(symbol)
                p.metrics.candles.labels(p.name, "fetch_failures").inc()
                p.health.error_count += 1
                p.health.last_error = type(result).__name__
                if isinstance(result, PermanentConnectionError):
                    self.disabled.update(i.canonical_symbol for i, _ in p.subscriptions)
                elif isinstance(result, PermanentProviderError):
                    self.disabled.add(symbol)
                elif not isinstance(result, (ValueError, KeyError, TypeError, IndexError)):
                    network_failed = True
                    if not p.handles_transport_backoff:
                        self.reconnect_attempt += 1
                        delay = min(
                            settings.reconnect_max_seconds,
                            settings.reconnect_min_seconds * 2 ** min(self.reconnect_attempt, 16),
                        )
                        self.retry_at = time.monotonic() + random.uniform(delay, delay * 1.2)
                else:
                    p.metrics.parse_failures.labels(p.name).inc()
                self.log(
                    "candle fetch failed",
                    level=logging.ERROR,
                    canonical_symbol=symbol,
                    category=type(result).__name__,
                )
            else:
                self.pending.extend(
                    c.model_copy(
                        update={
                            "metadata": {
                                **c.metadata,
                                "backfilled": is_backfill(
                                    c, states[symbol], expected, end, startup
                                ),
                            }
                        }
                    )
                    for c in result
                )
                p.failures.discard(symbol)
                if result:
                    p.health.symbols[symbol].source_update_mode = result[-1].source_status
        if network_failed:
            p.health.reconnect_count += 1
            p.metrics.reconnects.labels(p.name).inc()
            p.health.consecutive_reconnect_failures += 1
        if requests and not p.failures:
            p.health.consecutive_reconnect_failures = 0
            self.reconnect_attempt = 0
            self.retry_at = 0
        for i, m in p.subscriptions:
            state = states[i.canonical_symbol]
            beginning = state.get("coverage_start", expected)
            if beginning < end:
                coverage.append(
                    (i.id, m.provider, beginning, end, p.session_info.get(i.canonical_symbol, {}))
                )
        current = [c for c in self.pending if c.bucket_start == expected]
        activated = sum(
            states[i.canonical_symbol].get("coverage_start", expected) <= expected
            for i, _ in p.subscriptions
        )
        summary["activated_current_instruments"] = activated
        summary["completed_current_instruments"] = len(current)
        summary["complete_current_minute"] = len(current) == activated
        if current:
            receipts = [
                datetime.fromisoformat(
                    p.timings[c.canonical_symbol]["completed_candle_available_at"]
                )
                if p.timings.get(c.canonical_symbol, {}).get("completed_candle_available_at")
                else c.received_timestamp
                for c in current
            ]
            first, last = min(receipts), max(receipts)
            ordered = sorted(receipts)
            summary.update(
                close_to_first_receive_seconds=max(0, (first - end).total_seconds()),
                close_to_last_receive_seconds=max(0, (last - end).total_seconds()),
                first_to_last_receive_seconds=max(0, (last - first).total_seconds()),
                close_to_p50_receive_seconds=max(
                    0, (ordered[math.ceil(len(ordered) * 0.5) - 1] - end).total_seconds()
                ),
                close_to_p95_receive_seconds=max(
                    0, (ordered[math.ceil(len(ordered) * 0.95) - 1] - end).total_seconds()
                ),
            )
        attempt = 0
        while True:
            write_start, write_timer = utc_now(), time.perf_counter()
            try:
                changed = await self.store.write_candles(self.pending, coverage)
                break
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.database_healthy = False
                p.metrics.db_errors.inc()
                if attempt == 0:
                    self.log(
                        "candle database unavailable; bounded batch retained",
                        level=logging.ERROR,
                        category=type(error).__name__,
                        batch_size=len(self.pending),
                    )
                attempt += 1
                await asyncio.sleep(random.uniform(0.1, min(5, 0.2 * 2 ** min(attempt, 6))))
        committed = utc_now()
        if not self.database_healthy:
            self.log("candle database recovered; durable reconciliation resumes next minute")
        self.database_healthy = True
        summary.update(
            db_write_started_at=write_start.isoformat(),
            db_commit_completed_at=committed.isoformat(),
            db_write_seconds=max(0, time.perf_counter() - write_timer),
            close_to_commit_seconds=max(0, (committed - end).total_seconds()),
            db_failures=attempt,
        )
        for stage in (
            "scheduler_lateness",
            "grace_to_fetch_start",
            "fetch_cycle_duration",
            "close_to_first_receive",
            "close_to_last_receive",
            "first_to_last_receive",
            "close_to_p50_receive",
            "close_to_p95_receive",
            "db_write",
            "close_to_commit",
        ):
            if stage + "_seconds" in summary:
                p.metrics.candle_timing.labels(p.name, stage).observe(summary[stage + "_seconds"])
        details = getattr(self.store, "last_candle_write", {})
        corrections = details.get("corrections", [])
        summary.update(
            candles_received=len(self.pending),
            candles_changed=changed,
            backfilled=sum(bool(c.metadata.get("backfilled")) for c in self.pending),
            corrections=len(corrections),
        )
        p.metrics.candles.labels(p.name, "corrections").inc(len(corrections))
        for revision in corrections:
            self.log("native candle correction discovered", **revision)
            for i, _ in p.subscriptions:
                if i.id == revision["instrument_id"]:
                    p.health.symbols[i.canonical_symbol].last_correction_at = revision[
                        "discovered_at"
                    ]
        p.metrics.candles.labels(p.name, "persisted").inc(changed)
        p.metrics.candles.labels(p.name, "upserted").inc(changed)
        p.metrics.candles.labels(p.name, "duplicates").inc(len(self.pending) - changed)
        keys = details.get("changed_keys")
        # Large native-history recovery must not perform a linear search per candle.
        changed_keys = set(keys) if keys is not None else None
        for candle in details.get("changed_candles", self.pending):
            if (
                changed_keys is not None
                and (candle.instrument_id, candle.provider, candle.bucket_start) not in changed_keys
            ):
                continue
            state = states[candle.canonical_symbol]
            if is_backfill(candle, state, expected, end, startup):
                p.metrics.candles.labels(p.name, "backfilled").inc()
            item = p.health.symbols[candle.canonical_symbol]
            item.last_persisted_at = committed
            item.last_observation_at = candle.received_timestamp
            item.observation_count += 1
            p.health.instrument_last_success[candle.canonical_symbol] = candle.received_timestamp
            p.health.last_success_at = committed
            self.fanout.publish(candle)
        self.pending = []
        for i, m in p.subscriptions:
            try:
                state = await self.store.candle_progress(i.id, m.provider)
            except Exception:
                self.database_healthy = False
                p.metrics.db_errors.inc()
                raise
            old = states[i.canonical_symbol]
            old_count, new_count = old.get("missing_count", 0), state.get("missing_count", 0)
            overlapping_missing = sum(
                max(0, int((min(a_hi, b_hi) - max(a_lo, b_lo)).total_seconds() // 60))
                for a_lo, a_hi in old.get("missing_ranges", [])
                for b_lo, b_hi in state.get("missing_ranges", [])
            )
            for operation, count in (
                ("gap_open", new_count - overlapping_missing),
                ("gap_close", old_count - overlapping_missing),
            ):
                if count:
                    p.metrics.candles.labels(p.name, operation).inc(count)
                    self.log(
                        operation,
                        canonical_symbol=i.canonical_symbol,
                        minutes=count,
                        unresolved_expected_gaps=new_count,
                    )
            p.progress[i.canonical_symbol] = state
            if (
                i.canonical_symbol in self.recovering
                and not state.get("missing_count")
                and not state.get("unknown_count")
            ):
                self.recovering.discard(i.canonical_symbol)
                self.log("native candle backfill completed", canonical_symbol=i.canonical_symbol)
        summary.update(
            retrieval=dict(p.health.candle_retrieval),
            cycle_duration_seconds=max(0, time.perf_counter() - timer),
            overrun=self.observe_overrun(target + timedelta(minutes=1), utc_now()),
        )
        p.metrics.candle_timing.labels(p.name, "cycle_duration").observe(
            summary["cycle_duration_seconds"]
        )
        if summary["cycle_duration_seconds"] > summary["collection_budget_seconds"]:
            self.log(
                "candle minute safety budget exceeded",
                level=logging.WARNING,
                duration=summary["cycle_duration_seconds"],
                budget=summary["collection_budget_seconds"],
            )
        p.health.last_candle_cycle = summary
        p.refresh_health()
        self.log("candle cycle completed", **summary)

    async def run(self) -> None:
        try:
            while True:
                deadline = next_collection(utc_now(), self.settings.candle_fetch_grace_seconds)
                alarm: asyncio.TimerHandle | None = None

                def watch_overrun(target: datetime = deadline) -> None:
                    nonlocal alarm
                    if not self.observe_overrun(target, utc_now()):
                        alarm = asyncio.get_running_loop().call_later(
                            min(30, max(0.001, (target - utc_now()).total_seconds())), watch_overrun
                        )

                alarm = asyncio.get_running_loop().call_later(
                    min(30, max(0.001, (deadline - utc_now()).total_seconds())), watch_overrun
                )
                try:
                    await self.cycle()
                except Exception as error:
                    self.provider.failures.update(
                        i.canonical_symbol for i, _ in self.provider.subscriptions
                    )
                    self.provider.health.last_error = type(error).__name__
                    self.log(
                        "candle cycle failed", level=logging.ERROR, category=type(error).__name__
                    )
                finally:
                    if alarm:
                        alarm.cancel()
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
                self.log(
                    "unpersisted candle batch on shutdown; recent native history may recover",
                    level=logging.ERROR,
                    count=len(self.pending),
                )
            await self.provider.disconnect()
