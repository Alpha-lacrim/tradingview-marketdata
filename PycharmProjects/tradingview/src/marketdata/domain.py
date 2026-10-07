from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware timestamp required")
    return value.astimezone(UTC)


def from_ns(value: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value // 1000)


def parse_timestamp_ns(value: str) -> tuple[datetime, int]:
    match = re.fullmatch(r"(.+T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})", value)
    if match is None:
        raise ValueError("expected timezone-aware RFC3339 timestamp, up to nanosecond precision")
    seconds, fraction, zone = match.groups()
    whole = utc(datetime.fromisoformat(seconds + zone))
    delta = whole - datetime(1970, 1, 1, tzinfo=UTC)
    ns = (delta.days * 86400 + delta.seconds) * 1000000000 + int((fraction or "").ljust(9, "0"))
    return from_ns(ns), ns


class AssetClass(StrEnum):
    CRYPTO = "crypto"
    FOREX = "forex"
    METALS = "metals"
    ENERGY = "energy"
    FUTURES = "futures"


class InstrumentType(StrEnum):
    SPOT = "spot"
    OTC = "otc"
    CFD = "cfd"
    FUTURE = "future"
    PERPETUAL = "perpetual"
    INDEX = "index"
    REFERENCE = "reference"


class DataStatus(StrEnum):
    REALTIME = "realtime"
    DELAYED = "delayed"
    INDICATIVE = "indicative"
    SIMULATED = "simulated"
    END_OF_DAY = "end_of_day"
    UNKNOWN = "unknown"


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class Instrument(Model):
    canonical_symbol: str = Field(pattern=r"^[A-Z0-9_:.-]{3,100}$")
    display_name: str
    asset_class: AssetClass
    instrument_type: InstrumentType
    exchange: str
    currency: str
    base_asset: str | None = None
    quote_asset: str | None = None
    root_symbol: str | None = None
    contract_code: str | None = None
    contract_month: date | None = None
    expiry: datetime | None = None
    roll_at: datetime | None = None
    tick_size: Decimal | None = Field(default=None, gt=0)
    multiplier: Decimal | None = Field(default=None, gt=0)
    active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("tick_size", "multiplier", mode="before")
    @classmethod
    def decimal_inputs(cls, value: Any) -> Any:
        if isinstance(value, float):
            raise ValueError("financial values require Decimal, integer or decimal string")
        return value

    @property
    def id(self) -> UUID:
        return uuid5(NAMESPACE_URL, "marketdata:instrument:" + self.canonical_symbol)

    @field_validator("expiry", "roll_at")
    @classmethod
    def timestamps(cls, value: datetime | None) -> datetime | None:
        return utc(value) if value is not None else None

    @model_validator(mode="after")
    def future_identity(self) -> Self:
        if self.instrument_type == InstrumentType.FUTURE:
            if not all((self.root_symbol, self.contract_code, self.contract_month, self.expiry)):
                raise ValueError(
                    "futures require root, concrete code, month and authoritative expiry"
                )
            if not self.canonical_symbol.startswith("FUTURES:"):
                raise ValueError("futures require FUTURES: canonical identity")
            if self.canonical_symbol.endswith((":FRONT", ":NEXT")):
                raise ValueError("logical futures aliases cannot be persisted as instruments")
            if self.contract_month and self.contract_month.day != 1:
                raise ValueError("contract_month must be the first day of its delivery month")
            if self.roll_at and self.expiry and self.roll_at > self.expiry:
                raise ValueError("roll_at must not follow expiry")
        elif any(
            (self.root_symbol, self.contract_code, self.contract_month, self.expiry, self.roll_at)
        ):
            raise ValueError("contract metadata is only valid for concrete futures")
        return self


class ProviderInstrument(Model):
    provider: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    provider_symbol: str = Field(min_length=1, max_length=100)
    realtime_status: DataStatus = DataStatus.UNKNOWN
    data_delay_seconds: int | None = Field(default=None, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def status_consistency(self) -> Self:
        if self.provider == "mock" and self.realtime_status != DataStatus.SIMULATED:
            raise ValueError("mock mappings must be visibly simulated")
        if self.realtime_status == DataStatus.REALTIME and self.data_delay_seconds:
            raise ValueError("delayed mapping cannot claim realtime")
        return self


class RegistryEntry(Model):
    instrument: Instrument
    mappings: list[ProviderInstrument] = Field(min_length=1)


class Tick(Model):
    tick_id: UUID = Field(default_factory=uuid4)
    instrument_id: UUID
    canonical_symbol: str
    provider_symbol: str
    asset_class: AssetClass
    instrument_type: InstrumentType
    provider: str
    exchange: str
    realtime_status: DataStatus
    event_timestamp: datetime | None = None
    event_timestamp_ns: int | None = Field(default=None, ge=0, le=9223372036854775807)
    received_timestamp: datetime
    bid: Decimal | None = None
    ask: Decimal | None = None
    last: Decimal | None = None
    bid_size: Decimal | None = Field(default=None, ge=0)
    ask_size: Decimal | None = Field(default=None, ge=0)
    last_size: Decimal | None = Field(default=None, ge=0)
    volume: Decimal | None = Field(default=None, ge=0)
    sequence_number: int | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "bid", "ask", "last", "bid_size", "ask_size", "last_size", "volume", mode="before"
    )
    @classmethod
    def decimal_inputs(cls, value: Any) -> Any:
        if isinstance(value, float):
            raise ValueError("financial values require Decimal, integer or decimal string")
        return value

    @field_validator("event_timestamp", "received_timestamp")
    @classmethod
    def timestamps(cls, value: datetime | None) -> datetime | None:
        return utc(value) if value is not None else None

    @model_validator(mode="after")
    def observation(self) -> Self:
        if self.bid is None and self.ask is None and self.last is None:
            raise ValueError("tick must contain an observed price")
        if self.provider == "mock" and self.realtime_status != DataStatus.SIMULATED:
            raise ValueError("mock ticks must be simulated")
        if self.event_timestamp_ns is not None:
            if self.event_timestamp != from_ns(self.event_timestamp_ns):
                raise ValueError("event_timestamp must match truncated event_timestamp_ns")
        return self

    @property
    def storage_timestamp(self) -> datetime:
        return self.event_timestamp or self.received_timestamp


def normalize(
    instrument: Instrument, mapping: ProviderInstrument, received: datetime, **fields: Any
) -> Tick:
    return Tick(
        instrument_id=instrument.id,
        canonical_symbol=instrument.canonical_symbol,
        provider_symbol=mapping.provider_symbol,
        asset_class=instrument.asset_class,
        instrument_type=instrument.instrument_type,
        provider=mapping.provider,
        exchange=instrument.exchange,
        realtime_status=mapping.realtime_status,
        received_timestamp=received,
        **fields,
    )
