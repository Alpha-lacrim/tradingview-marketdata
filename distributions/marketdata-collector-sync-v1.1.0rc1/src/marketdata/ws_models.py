"""Versioned market-data WebSocket schemas; JSON schema is exportable via TypeAdapter."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

from marketdata.api_models import CompletedCandle, PriceItem, SourceIdentity

Symbol = Annotated[str, Field(min_length=3, max_length=100, pattern=r"^[A-Z0-9_:.-]+$")]


class MarketCommand(BaseModel):
    model_config = {"extra": "forbid", "strict": True}
    action: Literal["subscribe", "unsubscribe", "snapshot", "ping", "disconnect"]
    symbols: list[Symbol] = Field(default_factory=list, max_length=100)
    request_id: str | None = Field(default=None, max_length=64)

    @model_validator(mode="after")
    def valid(self) -> "MarketCommand":
        if self.action in ("subscribe", "unsubscribe") and not self.symbols:
            raise ValueError("symbols required")
        if self.action in ("ping", "disconnect") and self.symbols:
            raise ValueError("symbols not allowed")
        return self


class Envelope(BaseModel):
    version: Literal[1] = 1
    server_timestamp: datetime


class PriceEvent(Envelope):
    type: Literal["price_update"] = "price_update"
    cursor: str
    event_id: str
    recorded_at: datetime
    persisted: Literal[True] = True
    data: PriceItem


class CandleEvent(Envelope):
    type: Literal["candle_completed", "candle_corrected"]
    cursor: str
    event_id: str
    recorded_at: datetime
    persisted: Literal[True] = True
    data: CompletedCandle


class PriceSnapshot(Envelope):
    type: Literal["price_snapshot"] = "price_snapshot"
    request_id: str | None = None
    snapshot_cursor: str
    batch: int
    final: bool
    items: list[PriceItem]


class SubscriptionAck(Envelope):
    type: Literal["subscription_ack"] = "subscription_ack"
    action: Literal["subscribe", "unsubscribe", "snapshot"]
    request_id: str | None = None
    symbols: list[str]
    cursor: str | None
    snapshot_cursor: str | None = None


class SourceState(SourceIdentity):
    freshness: Literal["fresh", "stale", "unavailable"]
    quote_freshness: Literal["fresh", "stale", "unavailable"]
    age_ms: int | None
    price_type: Literal["quote", "candle_close", "unavailable"]
    price_basis: str
    realtime_quote_available: bool
    source_timestamp: datetime | None
    received_timestamp: datetime | None
    provider_state: str
    transport: str
    session_expectation: str
    candle_state: str | None


class SourceStatus(Envelope):
    type: Literal["source_status"] = "source_status"
    collector: Literal["running", "stopped", "unknown"]
    database: Literal["available", "unavailable"]
    batch: int
    final: bool
    items: list[SourceState]


class Heartbeat(Envelope):
    type: Literal["heartbeat"] = "heartbeat"
    cursor: str | None
    database: Literal["available", "unavailable"]
    request_id: str | None = None


class SocketError(Envelope):
    type: Literal["error", "resync_required"] = "error"
    code: str
    message: str
    request_id: str | None = None


MarketEvent = Annotated[
    PriceEvent
    | CandleEvent
    | PriceSnapshot
    | SubscriptionAck
    | SourceStatus
    | Heartbeat
    | SocketError,
    Field(discriminator="type"),
]


class ChangePage(BaseModel):
    items: list[PriceEvent | CandleEvent]
    cursor: str
    until: str
    has_more: bool
