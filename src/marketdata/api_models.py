"""Version 1 public contracts. Decimal values serialize as JSON strings."""

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from marketdata.domain import AssetClass, DataStatus, InstrumentType

HistoricalInterval = Literal["1m", "5m", "15m", "1h", "1d"]
Freshness = Literal["fresh", "stale", "unavailable"]


class SourceIdentity(BaseModel):
    instrument_id: UUID
    canonical_symbol: str
    provider: str
    provider_symbol: str = Field(description="Exact persisted source symbol; never rewritten")
    venue: str
    broker: str | None = Field(description="Configured FX broker namespace; null if absent")


class InstrumentItem(SourceIdentity):
    display_name: str | None
    asset_class: AssetClass
    instrument_type: InstrumentType
    currency: str | None
    active: bool
    realtime_status: DataStatus
    data_delay_seconds: int | None
    supported_historical_intervals: list[HistoricalInterval]
    availability: Literal["persisted", "configured_unverified"]


class InstrumentPage(BaseModel):
    items: list[InstrumentItem]
    next_after: str | None = None


class PriceItem(SourceIdentity):
    last: Decimal | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    price_type: Literal["quote", "candle_close", "unavailable"]
    price_basis: Literal["last", "bid_ask", "bid", "ask", "candle_close", "unavailable"]
    source_timestamp: datetime | None = None
    received_timestamp: datetime | None = None
    freshness_basis: Literal["source_and_receipt", "receipt", "unavailable"]
    age_ms: int | None = None
    freshness: Freshness
    stale_after_seconds: float | None = None
    clock_skew: bool = False
    realtime_status: DataStatus
    quote_freshness: Freshness
    realtime_quote_available: bool
    bucket_start: datetime | None = None
    bucket_end: datetime | None = None


class PricePage(BaseModel):
    items: list[PriceItem]
    as_of: datetime
    next_after: str | None = None


class CompletedCandle(SourceIdentity):
    interval: HistoricalInterval
    origin: Literal["native", "derived"]
    base_interval: Literal["1m"] = "1m"
    price_basis: Literal["native"] = "native"
    bucket_start: datetime
    bucket_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = Field(description="Source volume only; null if units are unavailable")
    volume_type: str | None = None
    realtime_status: DataStatus
    source_status: str
    received_timestamp: datetime
    observations: int
    expected_observations: int
    complete: bool = Field(
        description="All base minutes exist; elapsed partial buckets stay visible"
    )


class HistoricalCandlePage(BaseModel):
    items: list[CompletedCandle]
    interval: HistoricalInterval
    start: datetime
    end: datetime
    as_of: datetime
    next_cursor: str | None = None


class LatestCandleItem(SourceIdentity):
    candle: CompletedCandle | None
    availability: Literal["available", "unavailable"]


class LatestCandlePage(BaseModel):
    items: list[LatestCandleItem]
    as_of: datetime
    next_after: str | None = None


class GapRange(BaseModel):
    start: datetime
    end: datetime


class CoverageItem(SourceIdentity):
    start: datetime
    end: datetime
    tracked_start: datetime | None
    tracked_end: datetime | None
    expected_minutes: int
    persisted_completed_candles: int
    initially_missing_candles: int | None = Field(
        default=None, description="Null: the current schema has no complete initial-missing ledger"
    )
    recovered_candles: int = Field(description="Persisted minutes carrying backfilled provenance")
    unresolved_expected_gaps: int
    not_expected_minutes: int
    unknown_minutes: int
    latest_completed_bucket: datetime | None
    latest_persisted_bucket: datetime | None
    coverage_state: Literal["complete", "gaps", "unknown"]
    missing_ranges: list[GapRange]
    missing_ranges_truncated: bool


class CoveragePage(BaseModel):
    items: list[CoverageItem]
    start: datetime
    end: datetime
    as_of: datetime
    next_after: str | None = None


class ServiceHealth(BaseModel):
    api: Literal["available"] = "available"
    database: Literal["available", "unavailable"]
    status: Literal["ok", "degraded"]
    as_of: datetime
    collector: Literal["running", "stopped", "unknown"]
    collector_updated_at: datetime | None = None
    data_freshness: Literal["fresh", "stale", "unavailable", "unknown"]
    historical_coverage: Literal["complete", "gaps", "unknown"]
    streaming_mode: Literal["database", "unavailable"] = "database"
    websocket_delivery: Literal["idle", "operational", "degraded", "unknown"] = "unknown"
    provider_states: dict[str, str] = Field(default_factory=dict)
    source_count: int = 0
    fresh_sources: int = 0
    stale_sources: int = 0
    unavailable_sources: int = 0
    summary_truncated: bool = False


class ErrorBody(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorBody
