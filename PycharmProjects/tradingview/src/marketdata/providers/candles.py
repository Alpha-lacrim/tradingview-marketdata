"""Optional completed-candle capability; tick adapters remain unchanged."""

from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from marketdata.candles import MarketCandle, expected_minute
from marketdata.domain import Tick, utc_now
from marketdata.providers.base import MarketDataProvider, Subscription


class CandleProvider(MarketDataProvider):
    grace: float = 2

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.health.storage_mode = "candles"
        self.progress: dict[str, dict[str, Any]] = {}
        self.failures: set[str] = set()

    @abstractmethod
    async def fetch_completed(
        self, pair: Subscription, start: datetime, end: datetime, count: int
    ) -> list[MarketCandle]: ...

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        raise RuntimeError("candle provider has no tick stream")
        yield  # pragma: no cover

    def refresh_health(self, now: datetime | None = None) -> None:
        now = now or utc_now()
        h = self.health
        expected = expected_minute(now, self.grace)
        h.expected_closed_minute = expected
        for symbol, item in h.symbols.items():
            state = self.progress.get(symbol, {})
            latest = state.get("last_persisted_minute")
            item.last_persisted_minute = latest
            item.minutes_behind = (
                max(0, int((expected - latest).total_seconds() // 60)) if latest else None
            )
            checked = state.get("coverage_end", expected + timedelta(minutes=1))
            unchecked = max(
                0, int((expected + timedelta(minutes=1) - checked).total_seconds() // 60)
            )
            item.missing_candle_count = state.get("missing_count", 0) + unchecked
            item.stale = (
                latest is None
                or bool(item.minutes_behind)
                or latest > expected
                or bool(item.missing_candle_count)
                or symbol in self.failures
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
        if self.failures:
            h.degradation_reasons.append("candle_fetch_failed")
        # Polling transports intentionally disconnect between minute collections.
        self.transition("degraded" if h.degradation_reasons else "healthy")
        h.updated_at = now
        self.metrics.connection_up.labels(self.name).set(int(h.transport == "connected"))
        self.metrics.feed_healthy.labels(self.name).set(int(h.state == "healthy"))
