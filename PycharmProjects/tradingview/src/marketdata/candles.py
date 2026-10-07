"""Completed native candles and UTC scheduling (independent of provider transport)."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from marketdata.domain import DataStatus, Model, utc


def minute_start(now: datetime) -> datetime:
    return utc(now).replace(second=0, microsecond=0)


def expected_minute(now: datetime, grace: float) -> datetime:
    return minute_start(utc(now) - timedelta(seconds=grace)) - timedelta(minutes=1)


def next_collection(now: datetime, grace: float) -> datetime:
    target = minute_start(now) + timedelta(seconds=grace)
    return target if target > utc(now) else target + timedelta(minutes=1)


class MarketCandle(Model):
    instrument_id: UUID
    canonical_symbol: str
    provider: str
    provider_symbol: str
    interval: Literal["1m"] = "1m"
    bucket_start: datetime
    bucket_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None
    price_basis: Literal["native"] = "native"
    realtime_status: DataStatus
    source_status: str
    received_timestamp: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("open", "high", "low", "close", "volume", mode="before")
    @classmethod
    def exact(cls, value: Any) -> Any:
        if isinstance(value, (float, bool)):
            raise ValueError("financial inputs must preserve decimals")
        return value

    @field_validator("bucket_start", "bucket_end", "received_timestamp")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        return utc(value)

    @model_validator(mode="after")
    def completed(self) -> Self:
        if self.bucket_start != minute_start(self.bucket_start):
            raise ValueError("minute-aligned bucket required")
        if self.bucket_end != self.bucket_start + timedelta(minutes=1):
            raise ValueError("one minute bucket required")
        if self.bucket_end > self.received_timestamp:
            raise ValueError("forming candles cannot be finalized")
        if not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high:
            raise ValueError("invalid OHLC bounds")
        if self.volume is not None and self.volume < 0:
            raise ValueError("negative candle volume")
        return self
