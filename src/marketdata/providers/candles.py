"""Optional completed-candle capability; tick adapters remain unchanged."""

import logging
from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from marketdata.candles import MarketCandle, expected_minute
from marketdata.domain import Tick, utc_now
from marketdata.providers.base import MarketDataProvider, Subscription
from marketdata.sessions import Expectation, classify_ranges, expectation

if TYPE_CHECKING:
    from marketdata.providers.candle_pool import RequestBudget


class CandleProvider(MarketDataProvider):
    grace: float = 2
    backfill_minutes: int = 300
    handles_transport_backoff: bool = False
    supports_request_budget: bool = False

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.health.storage_mode = "candles"
        self.progress: dict[str, dict[str, Any]] = {}
        self.failures: set[str] = set()
        self.timings: dict[str, dict[str, Any]] = {}
        self.reported_states: dict[str, tuple[str, int]] = {}
        self.session_info: dict[str, dict[str, Any]] = {}
        self.worker_id = ""
        self.request_budget: RequestBudget | None = None
        self.completed_results: dict[str, list[MarketCandle] | Exception] = {}

    @abstractmethod
    async def fetch_completed(
        self, pair: Subscription, start: datetime, end: datetime, count: int
    ) -> list[MarketCandle]: ...

    async def fetch_many(
        self, requests: list[tuple[Subscription, datetime, datetime, int]]
    ) -> dict[str, list[MarketCandle] | Exception]:
        result: dict[str, list[MarketCandle] | Exception] = {}
        for pair, start, end, count in requests:
            try:
                result[pair[0].canonical_symbol] = await self.fetch_completed(
                    pair, start, end, count
                )
            except Exception as error:
                result[pair[0].canonical_symbol] = error
        return result

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        raise RuntimeError("candle provider has no tick stream")
        yield  # pragma: no cover

    def refresh_health(self, now: datetime | None = None) -> None:
        now = now or utc_now()
        if self.worker_id:
            # Pool owns provider-wide persisted coverage and gauges. A connection
            # worker must not overwrite them with its partial subscription set.
            self.health.updated_at = now
            return
        h = self.health
        expected = expected_minute(now, self.grace)
        h.expected_closed_minute = expected
        for symbol, item in h.symbols.items():
            state = self.progress.get(symbol, {})
            latest = state.get("last_persisted_minute")
            item.last_persisted_minute = latest
            info = self.session_info.get(symbol, state.get("session_info", {}))
            current = Expectation.EXPECTED if latest == expected else expectation(info, expected)
            item.session_expectation = current.value
            item.last_correction_at = state.get("last_correction_at", item.last_correction_at)
            item.unknown_minutes = state.get("unknown_count", 0)
            item.not_expected_minutes = state.get("not_expected_count", 0)
            checked = state.get("coverage_end", expected + timedelta(minutes=1))
            unchecked = classify_ranges(info, checked, expected + timedelta(minutes=1))

            def minutes(ranges: list[tuple[datetime, datetime]]) -> int:
                return sum(max(0, int((hi - lo).total_seconds() // 60)) for lo, hi in ranges)

            item.missing_candle_count = state.get("missing_count", 0) + minutes(
                unchecked["EXPECTED"]
            )
            item.unknown_minutes += minutes(unchecked["UNKNOWN"])
            # Closed schedules retain the latest expected completed bucket. Search
            # at most 8 days, sufficient for supported weekly sessions, not holidays.
            latest_expected: datetime | None = latest
            if current == Expectation.EXPECTED:
                latest_expected = expected
            elif current == Expectation.NOT_EXPECTED:
                from zoneinfo import ZoneInfo

                from marketdata.sessions import session_windows

                local_day = expected.astimezone(ZoneInfo(info["timezone"])).date()
                candidates = []
                for offset in range(8):
                    for opening, closing in (
                        session_windows(info, local_day - timedelta(days=offset)) or []
                    ):
                        candidate = min(expected, closing - timedelta(minutes=1))
                        if candidate >= opening:
                            candidates.append(candidate)
                latest_expected = max(candidates) if candidates else latest
            item.latest_expected_completed_bucket = latest_expected
            pending_ranges = state.get("missing_ranges", []) + unchecked["EXPECTED"]
            item.minutes_behind = (
                sum(
                    max(
                        0,
                        int(
                            (
                                min(hi, expected + timedelta(minutes=1))
                                - max(lo, latest + timedelta(minutes=1))
                            ).total_seconds()
                            // 60
                        ),
                    )
                    for lo, hi in pending_ranges
                )
                if latest
                else None
            )
            horizon = expected + timedelta(minutes=1 - self.backfill_minutes)
            gaps = state.get("missing_ranges", []) + unchecked["EXPECTED"]
            item.oldest_unresolved_expected_gap = min((lo for lo, _ in gaps), default=None)
            item.recoverable_missing_count = sum(
                max(0, int((hi - max(lo, horizon)).total_seconds() // 60)) for lo, hi in gaps
            )
            item.outside_horizon_missing_count = max(
                0, item.missing_candle_count - item.recoverable_missing_count
            )
            item.backfill_horizon_minutes = self.backfill_minutes
            prior = self.reported_states.get(symbol)
            reported = (current.value, item.outside_horizon_missing_count)
            if prior is None or prior[0] != reported[0]:
                logging.getLogger(__name__).info(
                    "candle session expectation changed",
                    extra={
                        "context": {
                            "provider": self.name,
                            "canonical_symbol": symbol,
                            "session_expectation": current.value,
                        }
                    },
                )
            if item.outside_horizon_missing_count and (prior is None or reported[1] != prior[1]):
                logging.getLogger(__name__).warning(
                    "candle gaps outside native recovery horizon",
                    extra={
                        "context": {
                            "provider": self.name,
                            "canonical_symbol": symbol,
                            "outside_horizon_missing_count": reported[1],
                            "oldest_gap": item.oldest_unresolved_expected_gap,
                        }
                    },
                )
            self.reported_states[symbol] = reported
            item.stale = (
                latest is None
                or latest > expected
                or bool(item.minutes_behind)
                or bool(item.missing_candle_count)
                or bool(item.unknown_minutes)
                or current == Expectation.UNKNOWN
                or symbol in self.failures
            )
            item.candle_state = (
                "unknown"
                if item.unknown_minutes or current == Expectation.UNKNOWN
                else "degraded"
                if item.stale
                else "closed"
                if current == Expectation.NOT_EXPECTED
                else "healthy"
            )
            self.metrics.candle_behind.labels(self.name, symbol).set(
                item.minutes_behind if item.minutes_behind is not None else -1
            )
            for classification in Expectation:
                self.metrics.candle_expectation.labels(self.name, symbol, classification.value).set(
                    int(classification == current)
                )
            item.observed_in_session = latest is not None
            item.age_ms = (
                max(0, int((now - latest - timedelta(minutes=1)).total_seconds() * 1000))
                if latest
                else None
            )
            item.stale_after_seconds = 60 + self.grace
            self.metrics.candle_age.labels(self.name, symbol).set(
                item.age_ms / 1000 if item.age_ms is not None else -1
            )
            self.metrics.candle_gaps.labels(self.name, symbol).set(item.missing_candle_count)
        h.configured_symbol_count = len(h.symbols)
        h.stale_symbols = [s for s, item in h.symbols.items() if item.stale]
        h.stale_symbol_count = len(h.stale_symbols)
        h.fresh_symbol_count = h.configured_symbol_count - h.stale_symbol_count
        h.subscription_restored = bool(h.symbols) and all(
            i.observed_in_session for i in h.symbols.values()
        )
        h.degradation_reasons = ["missing_or_unpersisted_candles"] if h.stale_symbols else []
        if any(i.unknown_minutes or i.session_expectation == "UNKNOWN" for i in h.symbols.values()):
            h.degradation_reasons.append("unknown_session_coverage")
        if self.failures:
            h.degradation_reasons.append("candle_fetch_failed")
        # Polling transports intentionally disconnect between minute collections.
        self.transition("degraded" if h.degradation_reasons else "healthy")
        h.updated_at = now
        self.metrics.connection_up.labels(self.name).set(int(h.transport == "connected"))
        self.metrics.feed_healthy.labels(self.name).set(int(h.state == "healthy"))
