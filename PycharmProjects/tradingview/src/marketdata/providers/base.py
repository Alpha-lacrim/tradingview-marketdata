from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from marketdata.domain import Instrument, ProviderInstrument, Tick, utc_now
from marketdata.observability import Metrics

Subscription = tuple[Instrument, ProviderInstrument]
logger = logging.getLogger(__name__)


class PermanentProviderError(Exception):
    """Invalid credentials, entitlement or subscription: requires operator action."""


class ProviderHealth(BaseModel):
    provider: str
    state: Literal["healthy", "stale", "degraded", "reconnecting", "disconnected"] = "disconnected"
    last_message_at: datetime | None = None
    last_success_at: datetime | None = None
    reconnect_count: int = 0
    error_count: int = 0
    last_error: str | None = None
    connected_at: datetime | None = None
    instrument_last_success: dict[str, datetime | None] = Field(default_factory=dict)
    stale_symbols: list[str] = Field(default_factory=list)
    updated_at: datetime = Field(default_factory=utc_now)


class MarketDataProvider(ABC):
    def __init__(self, name: str, metrics: Metrics):
        self.name = name
        self.metrics = metrics
        self.subscriptions: list[Subscription] = []
        self.health = ProviderHealth(provider=name)
        self.snapshots: dict[str, Tick] = {}

    async def subscribe(self, subscriptions: list[Subscription]) -> None:
        """Replace the desired subscription set; effective on next connection."""
        self.subscriptions = list(subscriptions)
        self.health.instrument_last_success = {
            i.canonical_symbol: self.health.instrument_last_success.get(i.canonical_symbol)
            for i, _ in subscriptions
        }

    async def unsubscribe(self, symbols: list[str]) -> None:
        self.subscriptions = [
            pair for pair in self.subscriptions if pair[0].canonical_symbol not in symbols
        ]
        for symbol in symbols:
            self.snapshots.pop(symbol, None)
            self.health.instrument_last_success.pop(symbol, None)

    async def get_snapshot(self) -> list[Tick]:
        """Locally observed ticks only. An empty cache means no observation yet."""
        return list(self.snapshots.values())

    async def health_check(self) -> ProviderHealth:
        return self.health.model_copy(deep=True)

    def message_received(self, received: datetime | None = None) -> None:
        self.metrics.messages.labels(self.name).inc()
        self.health.last_message_at = received or utc_now()
        self.health.updated_at = utc_now()

    def observed(self, tick: Tick) -> None:
        self.snapshots[tick.canonical_symbol] = tick
        self.health.last_success_at = tick.received_timestamp
        self.health.instrument_last_success[tick.canonical_symbol] = tick.received_timestamp
        if tick.canonical_symbol in self.health.stale_symbols:
            self.health.stale_symbols.remove(tick.canonical_symbol)
        self.health.state = "healthy"
        self.health.updated_at = utc_now()

    def malformed(self) -> None:
        self.metrics.parse_failures.labels(self.name).inc()
        self.health.error_count += 1
        self.health.last_error = "malformed_payload"
        self.health.state = "degraded"
        logger.warning("provider malformed message", extra={"context": {"provider": self.name}})

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    def stream_ticks(self) -> AsyncIterator[Tick]: ...
