from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field

from marketdata.domain import Instrument, ProviderInstrument, Tick, utc_now
from marketdata.observability import Metrics

Subscription = tuple[Instrument, ProviderInstrument]
logger = logging.getLogger(__name__)
HealthState = Literal[
    "disconnected",
    "connecting",
    "connected",
    "subscribing",
    "healthy",
    "degraded",
    "reconnecting",
    "stale",
]


class PermanentProviderError(Exception):
    """Invalid credentials, entitlement or subscription: requires operator action."""


class PermanentConnectionError(PermanentProviderError):
    """Provider-wide rejection; stop all subscriptions until operator intervention."""


class SymbolHealth(BaseModel):
    canonical_symbol: str
    provider_symbol: str
    last_observation_at: datetime | None = None
    age_ms: int | None = None
    observation_count: int = 0
    stale: bool = True
    stale_after_seconds: float = 30
    observed_in_session: bool = False
    source_update_mode: str | None = None
    last_rejection_reason: str | None = None
    rejected: bool = False
    last_persisted_minute: datetime | None = None
    minutes_behind: int | None = None
    missing_candle_count: int = 0


class ProviderHealth(BaseModel):
    provider: str
    state: HealthState = "disconnected"
    transport: Literal["disconnected", "connecting", "connected", "reconnecting"] = "disconnected"
    connection_id: str | None = None  # Local identifier only, never an upstream/auth session.
    last_message_at: datetime | None = None  # Last application frame, including heartbeats.
    last_success_at: datetime | None = None  # Last valid normalized market observation.
    reconnect_count: int = 0
    consecutive_reconnect_failures: int = 0
    error_count: int = 0
    last_error: str | None = None
    connected_at: datetime | None = None
    last_disconnect_at: datetime | None = None
    last_close_code: int | None = None
    last_close_reason: str | None = None  # Sanitized category, never arbitrary upstream text.
    configured_symbol_count: int = 0
    requested_symbol_count: int = 0
    fresh_symbol_count: int = 0
    stale_symbol_count: int = 0
    subscription_restored: bool = False
    degradation_reasons: list[str] = Field(default_factory=list)
    symbols: dict[str, SymbolHealth] = Field(default_factory=dict)
    instrument_last_success: dict[str, datetime | None] = Field(default_factory=dict)
    stale_symbols: list[str] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=utc_now)
    storage_mode: str = "ticks"
    expected_closed_minute: datetime | None = None


class MarketDataProvider(ABC):
    def __init__(self, name: str, metrics: Metrics):
        self.name = name
        self.metrics = metrics
        self.subscriptions: list[Subscription] = []
        self.health = ProviderHealth(provider=name)
        self.snapshots: dict[str, Tick] = {}
        self.stale_seconds = 30.0
        self.frame_timeout_seconds = 30.0
        self.observation_timeout_seconds = 30.0
        self.symbol_stale_seconds: dict[str, float] = {}
        self.transport_timed_out = False
        self.parse_degraded = False
        self._last_parse_log = -float("inf")

    def configure_freshness(self, stale_seconds: float) -> None:
        self.stale_seconds = stale_seconds
        self.frame_timeout_seconds = stale_seconds
        self.observation_timeout_seconds = stale_seconds

    async def subscribe(self, subscriptions: list[Subscription]) -> None:
        """Replace the desired subscription set; effective on next connection."""
        self.subscriptions = list(subscriptions)
        previous = self.health.symbols
        self.health.symbols = {
            i.canonical_symbol: previous.get(i.canonical_symbol)
            or SymbolHealth(canonical_symbol=i.canonical_symbol, provider_symbol=m.provider_symbol)
            for i, m in subscriptions
        }
        self.health.instrument_last_success = {
            symbol: item.last_observation_at for symbol, item in self.health.symbols.items()
        }
        self.refresh_health()

    async def unsubscribe(self, symbols: list[str]) -> None:
        await self.subscribe(
            [p for p in self.subscriptions if p[0].canonical_symbol not in symbols]
        )
        for symbol in symbols:
            self.snapshots.pop(symbol, None)

    async def get_snapshot(self) -> list[Tick]:
        """Locally observed ticks only. An empty cache means no observation yet."""
        return list(self.snapshots.values())

    async def health_check(self) -> ProviderHealth:
        self.refresh_health()
        return self.health.model_copy(deep=True)

    def transition(self, state: HealthState) -> None:
        if self.health.state == state:
            return
        self.health.state = state
        logger.info(
            "provider state changed",
            extra={
                "context": {
                    "provider": self.name,
                    "state": state,
                    "fresh": self.health.fresh_symbol_count,
                    "stale": self.health.stale_symbol_count,
                    "reasons": self.health.degradation_reasons,
                }
            },
        )

    def connection_started(self, reconnecting: bool = False) -> None:
        self.health.transport = "reconnecting" if reconnecting else "connecting"
        self.transition("reconnecting" if reconnecting else "connecting")
        self.health.requested_symbol_count = 0
        self.health.subscription_restored = False
        self.transport_timed_out = False
        self.parse_degraded = False
        for item in self.health.symbols.values():
            item.observed_in_session = False
            item.rejected = False
        self.refresh_health()

    def transport_connected(self, now: datetime | None = None) -> None:
        h = self.health
        h.transport = "connected"
        h.connection_id = uuid4().hex
        h.connected_at = now or utc_now()
        h.last_message_at = None
        self.transition("connected")
        logger.info(
            "provider connection established",
            extra={
                "context": {
                    "provider": self.name,
                    "connection_id": h.connection_id,
                }
            },
        )
        if h.reconnect_count:
            logger.info("provider reconnect succeeded", extra={"context": {"provider": self.name}})
        self.refresh_health(h.connected_at)

    def subscriptions_requested(self) -> None:
        self.health.requested_symbol_count = len(self.subscriptions)
        self.transition("subscribing")
        self.refresh_health()

    def transport_disconnected(self, now: datetime | None = None) -> None:
        if self.health.transport == "connected":
            self.health.last_disconnect_at = now or utc_now()
        self.health.transport = "disconnected"
        if self.health.state not in ("degraded", "reconnecting"):
            self.transition("disconnected")
        self.refresh_health(now)

    def refresh_health(self, now: datetime | None = None) -> None:
        now = now or utc_now()
        h = self.health
        connected = h.transport == "connected"
        for symbol, item in h.symbols.items():
            item.stale_after_seconds = self.symbol_stale_seconds.get(
                item.provider_symbol, self.stale_seconds
            )
            item.age_ms = (
                max(0, int((now - item.last_observation_at).total_seconds() * 1000))
                if item.last_observation_at
                else None
            )
            stale = (
                not connected
                or not item.observed_in_session
                or item.rejected
                or item.age_ms is None
                or item.age_ms > item.stale_after_seconds * 1000
            )
            if stale != item.stale:
                logger.info(
                    "symbol became stale" if stale else "symbol recovered",
                    extra={
                        "context": {
                            "provider": self.name,
                            "symbol": symbol,
                            "provider_symbol": item.provider_symbol,
                        }
                    },
                )
            item.stale = stale
            self.metrics.symbol_age.labels(self.name, symbol).set(
                item.age_ms / 1000 if item.age_ms is not None else -1
            )
            self.metrics.symbol_fresh.labels(self.name, symbol).set(int(not stale))
        h.configured_symbol_count = len(h.symbols)
        h.stale_symbols = [s for s, item in h.symbols.items() if item.stale]
        h.stale_symbol_count = len(h.stale_symbols)
        h.fresh_symbol_count = h.configured_symbol_count - h.stale_symbol_count
        if connected:
            elapsed = (now - (h.connected_at or now)).total_seconds()
            last = max(h.last_success_at or h.connected_at or now, h.connected_at or now)
            no_market = (now - last).total_seconds() > self.observation_timeout_seconds
            reasons = []
            if no_market:
                reasons.append("no_valid_market_observations")
                if h.state != "degraded":
                    h.last_error = "no_valid_market_observations"
            if any(item.rejected for item in h.symbols.values()):
                reasons.append("source_rejected")
            if self.parse_degraded:
                reasons.append("malformed_payload")
            if h.stale_symbols and (
                h.subscription_restored
                or any(
                    elapsed > item.stale_after_seconds for item in h.symbols.values() if item.stale
                )
            ):
                reasons.append("stale_symbols")
            h.degradation_reasons = reasons
            if h.configured_symbol_count and all(i.observed_in_session for i in h.symbols.values()):
                if not h.subscription_restored:
                    h.subscription_restored = True
                    logger.info(
                        "subscription restoration complete",
                        extra={
                            "context": {
                                "provider": self.name,
                                "symbols": h.configured_symbol_count,
                            }
                        },
                    )
            if reasons:
                self.transition("degraded")
            elif h.configured_symbol_count and not h.stale_symbols:
                self.transition("healthy")
                h.consecutive_reconnect_failures = 0
            elif h.requested_symbol_count:
                self.transition("subscribing")
        h.updated_at = now
        self.metrics.connection_up.labels(self.name).set(int(connected))
        self.metrics.connection_age.labels(self.name).set(
            max(0, (now - h.connected_at).total_seconds()) if connected and h.connected_at else 0
        )
        self.metrics.feed_healthy.labels(self.name).set(int(connected and h.state == "healthy"))
        for state, count in (
            ("configured", h.configured_symbol_count),
            ("fresh", h.fresh_symbol_count),
            ("stale", h.stale_symbol_count),
        ):
            self.metrics.symbol_count.labels(self.name, state).set(count)

    def transport_expired(self, now: datetime) -> bool:
        h = self.health
        if h.transport != "connected" or h.connected_at is None or self.transport_timed_out:
            return False
        if (
            now - (h.last_message_at or h.connected_at)
        ).total_seconds() <= self.frame_timeout_seconds:
            return False
        self.transport_timed_out = True
        h.last_error = "transport_frame_timeout"
        h.error_count += 1
        h.degradation_reasons = ["transport_frame_timeout"]
        self.metrics.transport_timeouts.labels(self.name).inc()
        self.transition("reconnecting")
        self.transport_disconnected(now)
        return True

    def message_received(self, received: datetime | None = None) -> None:
        self.metrics.messages.labels(self.name).inc()
        self.health.last_message_at = received or utc_now()
        self.health.updated_at = self.health.last_message_at

    def observed(self, tick: Tick) -> None:
        self.snapshots[tick.canonical_symbol] = tick
        h = self.health
        h.last_success_at = tick.received_timestamp
        h.instrument_last_success[tick.canonical_symbol] = tick.received_timestamp
        item = h.symbols[tick.canonical_symbol]
        item.last_observation_at = tick.received_timestamp
        item.observation_count += 1
        item.observed_in_session = True
        self.metrics.symbol_observations.labels(self.name, tick.canonical_symbol).inc()
        self.parse_degraded = False
        self.refresh_health(tick.received_timestamp)

    def malformed(self) -> None:
        self.metrics.parse_failures.labels(self.name).inc()
        self.health.error_count += 1
        self.health.last_error = "malformed_payload"
        self.parse_degraded = True
        self.transition("degraded")
        self.refresh_health()
        now = time.monotonic()
        if now - self._last_parse_log >= 30:
            self._last_parse_log = now
            logger.warning("provider malformed message", extra={"context": {"provider": self.name}})

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    def stream_ticks(self) -> AsyncIterator[Tick]: ...
