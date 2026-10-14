"""Database-backed public REST routes; no provider or collector lifecycle ownership."""

import base64
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.security import HTTPBearer
from pydantic import BaseModel, ValidationError, field_validator

from marketdata.api_models import (
    CompletedCandle,
    CoverageItem,
    CoveragePage,
    ErrorResponse,
    GapRange,
    HistoricalCandlePage,
    HistoricalInterval,
    InstrumentItem,
    InstrumentPage,
    LatestCandleItem,
    LatestCandlePage,
    PriceItem,
    PricePage,
    ServiceHealth,
)
from marketdata.candles import MarketCandle, minute_start
from marketdata.config import Settings
from marketdata.domain import AssetClass, DataStatus, Tick, utc, utc_now
from marketdata.persistence import Store
from marketdata.price_selection import observation_age, select_price, stale_threshold

Symbol = Annotated[str, Query(min_length=3, max_length=100, pattern=r"^[A-Z0-9_:.-]+$")]
Provider = Annotated[str, Query(min_length=1, max_length=100, pattern=r"^[a-z][a-z0-9_]*$")]
SourceSymbol = Annotated[str, Query(min_length=1, max_length=100)]
Symbols = Annotated[list[Symbol] | None, Query(max_length=100)]
Limit = Annotated[int, Query(ge=1, le=1000)]
After = Annotated[str, Query(max_length=220)]
SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 86400}
HISTORICAL_INTERVALS: list[HistoricalInterval] = ["1m", "5m", "15m", "1h", "1d"]


class HistoryCursor(BaseModel):
    model_config = {"extra": "forbid"}
    symbol: str
    provider: str
    provider_symbol: str
    interval: HistoricalInterval
    start: datetime
    end: datetime
    as_of: datetime
    bucket: datetime
    status: DataStatus

    @field_validator("start", "end", "as_of", "bucket")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        return utc(value)


def identity(row: dict[str, Any]) -> dict[str, Any]:
    instrument = row["instrument"]
    return dict(
        instrument_id=instrument.id,
        canonical_symbol=instrument.canonical_symbol,
        provider=row["provider"],
        provider_symbol=row["provider_symbol"],
        venue=instrument.exchange,
        broker=instrument.broker,
    )


def page_after(rows: list[dict[str, Any]], limit: int) -> str | None:
    if len(rows) <= limit:
        return None
    return (
        str(rows[limit - 1]["instrument"].canonical_symbol) + "/" + str(rows[limit - 1]["provider"])
    )


def price_item(row: dict[str, Any], now: datetime, settings: Settings) -> PriceItem:
    tick: Tick | None = row["tick"]
    candle: MarketCandle | None = row["candle"]
    observation = select_price(tick, candle, now, settings)
    quote_freshness: Literal["fresh", "stale", "unavailable"] = "unavailable"
    if tick is not None:
        age, skew = observation_age(tick, now)
        quote_freshness = (
            "fresh" if not skew and age <= stale_threshold(tick, settings) * 1000 else "stale"
        )
    common = dict(
        identity(row),
        quote_freshness=quote_freshness,
        realtime_quote_available=quote_freshness == "fresh"
        and tick is not None
        and tick.realtime_status == DataStatus.REALTIME,
    )
    if observation is None:
        return PriceItem(
            **common,
            price_type="unavailable",
            price_basis="unavailable",
            realtime_status=DataStatus.UNKNOWN,
            freshness="unavailable",
            freshness_basis="unavailable",
        )
    age, skew = observation_age(observation, now)
    threshold = stale_threshold(observation, settings)
    freshness = "stale" if skew or age > threshold * 1000 else "fresh"
    common.update(
        age_ms=age,
        clock_skew=skew,
        freshness=freshness,
        stale_after_seconds=threshold,
        realtime_status=observation.realtime_status,
        received_timestamp=observation.received_timestamp,
    )
    if isinstance(observation, MarketCandle):
        return PriceItem(
            **common,
            last=observation.close,
            price_type="candle_close",
            price_basis="candle_close",
            source_timestamp=observation.bucket_end,
            freshness_basis="source_and_receipt",
            bucket_start=observation.bucket_start,
            bucket_end=observation.bucket_end,
        )
    basis: Literal["last", "bid_ask", "bid", "ask"] = (
        "last"
        if observation.last is not None
        else "bid_ask"
        if observation.bid is not None and observation.ask is not None
        else "bid"
        if observation.bid is not None
        else "ask"
    )
    return PriceItem(
        **common,
        last=observation.last,
        bid=observation.bid,
        ask=observation.ask,
        price_type="quote",
        price_basis=basis,
        source_timestamp=observation.event_timestamp,
        freshness_basis="source_and_receipt" if observation.event_timestamp else "receipt",
    )


def completed_candle(
    row: dict[str, Any], data: dict[str, Any], interval: HistoricalInterval
) -> CompletedCandle:
    seconds = SECONDS[interval]
    metadata = data["metadata"]
    return CompletedCandle(
        **identity(row),
        interval=interval,
        origin="native" if interval == "1m" else "derived",
        bucket_start=data["bucket"],
        bucket_end=data["bucket"] + timedelta(seconds=seconds),
        open=data["open"],
        high=data["high"],
        low=data["low"],
        close=data["close"],
        volume=data["volume"],
        volume_type=metadata.get("volume_type") if data["volume"] is not None else None,
        realtime_status=data["realtime_status"],
        source_status=data["source_status"],
        received_timestamp=data["received_timestamp"],
        observations=data["observations"],
        expected_observations=seconds // 60,
        complete=data["observations"] == seconds // 60,
    )


def window(
    start: datetime | None,
    end: datetime | None,
    now: datetime,
    seconds: int,
    max_days: int,
    default_days: int = 1,
) -> tuple[datetime, datetime]:
    try:
        end = (
            utc(end)
            if end is not None
            else datetime.fromtimestamp(int(now.timestamp()) // seconds * seconds, UTC)
        )
        start = utc(start) if start is not None else end - timedelta(days=default_days)
    except ValueError:
        raise HTTPException(422, "timestamps must include timezone") from None
    if start >= end or end - start > timedelta(days=max_days):
        raise HTTPException(422, f"range must be positive and <= {max_days} days")
    if any(t.microsecond or int(t.timestamp()) % seconds for t in (start, end)):
        raise HTTPException(422, "start/end must align to UTC interval boundaries")
    return start, end


def create_router(settings: Settings) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1",
        tags=["Market data v1"],
        dependencies=[Depends(HTTPBearer(auto_error=False, scheme_name="CompanyBearer"))],
        responses={code: {"model": ErrorResponse} for code in (401, 403, 404, 422, 503)},
    )

    def store(request: Request) -> Store:
        result: Store = request.app.state.store
        return result

    async def sources(
        request: Request,
        symbols: list[str] | None,
        provider: str | None,
        provider_symbol: str | None,
        asset_class: str | None,
        limit: int,
        after: str,
    ) -> list[dict[str, Any]]:
        if limit > settings.api_max_rows:
            raise HTTPException(422, f"limit must be <= {settings.api_max_rows}")
        if symbols is not None:
            if not symbols or len(symbols) > 100:
                raise HTTPException(422, "request 1..100 canonical symbols")
            if set(symbols) - await store(request).known_symbols(symbols):
                raise HTTPException(404, "unknown canonical symbol")
        return await store(request).source_records(
            symbols, provider, provider_symbol, asset_class, limit + 1, after
        )

    @router.get("/instruments", response_model=InstrumentPage)
    async def instruments(
        request: Request,
        asset_class: AssetClass | None = None,
        provider: Provider | None = None,
        provider_symbol: SourceSymbol | None = None,
        limit: Limit = 100,
        after: After = "",
    ) -> InstrumentPage:
        """List persisted configured source identities, including unverified mappings."""
        rows = await sources(request, None, provider, provider_symbol, asset_class, limit, after)
        return InstrumentPage(
            items=[
                InstrumentItem(
                    **identity(r),
                    display_name=r["instrument"].display_name or None,
                    asset_class=r["instrument"].asset_class,
                    instrument_type=r["instrument"].instrument_type,
                    currency=r["instrument"].currency or None,
                    active=r["instrument"].active,
                    realtime_status=r["realtime_status"],
                    data_delay_seconds=r["data_delay_seconds"],
                    supported_historical_intervals=HISTORICAL_INTERVALS
                    if r["native_history"]
                    else [],
                    availability="persisted"
                    if r["tick"] or r["candle"]
                    else "configured_unverified",
                )
                for r in rows[:limit]
            ],
            next_after=page_after(rows, limit),
        )

    @router.get("/prices/latest", response_model=PricePage)
    async def prices(
        request: Request,
        symbols: Symbols = None,
        provider: Provider | None = None,
        provider_symbol: SourceSymbol | None = None,
        limit: Limit = 100,
        after: After = "",
    ) -> PricePage:
        """Prefer fresh persisted quotes; label candle-close fallback and unavailable data."""
        rows = await sources(request, symbols, provider, provider_symbol, None, limit, after)
        now = utc_now()
        return PricePage(
            items=[price_item(r, now, settings) for r in rows[:limit]],
            as_of=now,
            next_after=page_after(rows, limit),
        )

    @router.get("/candles", response_model=HistoricalCandlePage)
    async def candles(
        request: Request,
        symbol: Symbol,
        provider: Provider,
        provider_symbol: SourceSymbol | None = None,
        interval: HistoricalInterval = "1m",
        start: datetime | None = None,
        end: datetime | None = None,
        limit: Limit = 100,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
    ) -> HistoricalCandlePage:
        """Read native 1m candles or UTC rollups, keeping incomplete elapsed rollups explicit."""
        rows = await sources(request, [symbol], provider, provider_symbol, None, limit, "")
        if not rows:
            raise HTTPException(404, "unknown source mapping")
        row = rows[0]
        now = utc_now()
        position = None
        if cursor:
            try:
                saved = HistoryCursor.model_validate_json(
                    base64.b64decode(cursor, altchars=b"-_", validate=True)
                )
                if (saved.symbol, saved.provider, saved.provider_symbol, saved.interval) != (
                    symbol,
                    provider,
                    row["provider_symbol"],
                    interval,
                ) or saved.as_of > now:
                    raise ValueError
                if (
                    start is not None
                    and utc(start) != saved.start
                    or end is not None
                    and utc(end) != saved.end
                ):
                    raise ValueError
                start, end, now = saved.start, saved.end, saved.as_of
                if not saved.start <= saved.bucket < saved.end:
                    raise ValueError
                position = (saved.bucket, saved.status.value)
            except (ValueError, TypeError, ValidationError):
                raise HTTPException(422, "invalid or mismatched candle cursor") from None
        start, end = window(start, end, now, SECONDS[interval], settings.api_history_max_days)
        records = await store(request).native_candles(
            row["instrument"].id,
            provider,
            start,
            end,
            interval,
            limit + 1,
            as_of=now,
            provider_symbol=row["provider_symbol"],
            cursor=position,
        )
        items = [completed_candle(row, r, interval) for r in records[:limit]]
        next_cursor = None
        if len(records) > limit:
            saved = HistoryCursor(
                symbol=symbol,
                provider=provider,
                provider_symbol=row["provider_symbol"],
                interval=interval,
                start=start,
                end=end,
                as_of=now,
                bucket=items[-1].bucket_start,
                status=items[-1].realtime_status,
            )
            next_cursor = base64.urlsafe_b64encode(saved.model_dump_json().encode()).decode()
        return HistoricalCandlePage(
            items=items, interval=interval, start=start, end=end, as_of=now, next_cursor=next_cursor
        )

    @router.get("/candles/latest", response_model=LatestCandlePage)
    async def latest_candles(
        request: Request,
        symbols: Symbols = None,
        provider: Provider | None = None,
        provider_symbol: SourceSymbol | None = None,
        limit: Limit = 100,
        after: After = "",
    ) -> LatestCandlePage:
        """Latest persisted completed native 1m candle per source; no quote substitution."""
        rows = await sources(request, symbols, provider, provider_symbol, None, limit, after)
        now = utc_now()
        items = []
        for row in rows[:limit]:
            c = row["candle"]
            candle = None
            if c is not None and c.bucket_end <= now:
                candle = completed_candle(
                    row, dict(c.model_dump(), bucket=c.bucket_start, observations=1), "1m"
                )
            items.append(
                LatestCandleItem(
                    **identity(row),
                    candle=candle,
                    availability="available" if candle else "unavailable",
                )
            )
        return LatestCandlePage(items=items, as_of=now, next_after=page_after(rows, limit))

    @router.get("/coverage", response_model=CoveragePage)
    async def coverage(
        request: Request,
        symbols: Symbols = None,
        provider: Provider | None = None,
        provider_symbol: SourceSymbol | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: Limit = 100,
        after: After = "",
    ) -> CoveragePage:
        """Bounded completeness counts and stored gaps; untracked time stays UNKNOWN."""
        now = utc_now()
        start, end = window(start, end, now, 60, settings.api_history_max_days)
        if end > minute_start(now):
            raise HTTPException(422, "coverage end must exclude the forming minute")
        rows = await sources(request, symbols, provider, provider_symbol, None, limit, after)
        states = await store(request).coverage_summary(
            [(r["instrument"].id, r["provider"]) for r in rows[:limit]], start, end, now
        )
        by_identity = {(r["instrument_id"], r["provider"]): r for r in states}
        items = []
        for row in rows[:limit]:
            state = by_identity[(row["instrument"].id, row["provider"])]
            missing, unknown = state["missing_count"], state["unknown_count"]
            items.append(
                CoverageItem(
                    **identity(row),
                    start=start,
                    end=end,
                    tracked_start=state["coverage_start"],
                    tracked_end=state["coverage_end"],
                    expected_minutes=int((end - start).total_seconds() // 60)
                    - state["not_expected_count"]
                    - unknown,
                    persisted_completed_candles=state["persisted"],
                    recovered_candles=state["recovered"],
                    unresolved_expected_gaps=missing,
                    not_expected_minutes=state["not_expected_count"],
                    unknown_minutes=unknown,
                    latest_completed_bucket=state["last_completed"],
                    latest_persisted_bucket=state["last_persisted"],
                    coverage_state="gaps" if missing else "unknown" if unknown else "complete",
                    missing_ranges=[
                        GapRange(start=g.lower, end=g.upper) for g in state["missing_ranges"]
                    ],
                    missing_ranges_truncated=state["missing_range_count"] > 100,
                )
            )
        return CoveragePage(
            items=items, start=start, end=end, as_of=now, next_after=page_after(rows, limit)
        )

    @router.get(
        "/health",
        response_model=ServiceHealth,
        responses={
            503: {
                "model": ServiceHealth | ErrorResponse,
                "description": "Unavailable database or query deadline",
            }
        },
    )
    async def health(request: Request, response: Response) -> ServiceHealth:
        """API/database readiness is independent of source connectivity or collector heartbeat."""
        now = utc_now()
        try:
            await store(request).ping()
            snapshot = await store(request).collector_status()
            rows = await store(request).source_records(None, None, None, None, 1001)
            counts = {"fresh": 0, "stale": 0, "unavailable": 0}
            for row in rows[:1000]:
                counts[price_item(row, now, settings).freshness] += 1
            state: Literal["fresh", "stale", "unavailable", "unknown"] = (
                "unknown"
                if len(rows) > 1000
                else "stale"
                if counts["stale"]
                else ("unavailable" if counts["unavailable"] or not rows else "fresh")
            )
            coverage_state = await store(request).coverage_health()
            if getattr(request.app.state, "metrics", None) is not None:
                request.app.state.metrics.api_database.set(1)
            hub = getattr(request.app.state, "market_hub", None)
            delivery: Literal["idle", "operational", "degraded", "unknown"] = "unknown"
            if hub is not None:
                delivery = (
                    "degraded"
                    if hub.task is None or hub.task.done()
                    else "idle"
                    if not hub.clients
                    else "operational"
                    if hub.database == "available"
                    else "degraded"
                )
            return ServiceHealth(
                database="available",
                status="ok",
                as_of=now,
                collector=("running" if snapshot["running"] else "stopped")
                if snapshot and snapshot["fresh"]
                else "unknown",
                collector_updated_at=snapshot["updated_at"] if snapshot else None,
                data_freshness=state,
                historical_coverage=coverage_state,
                websocket_delivery=delivery,
                provider_states={p.provider: p.state for p in snapshot["providers"]}
                if snapshot
                else {},
                source_count=min(len(rows), 1000),
                fresh_sources=counts["fresh"],
                stale_sources=counts["stale"],
                unavailable_sources=counts["unavailable"],
                summary_truncated=len(rows) > 1000,
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError, RuntimeError):
            if getattr(request.app.state, "metrics", None) is not None:
                request.app.state.metrics.api_database.set(0)
            response.status_code = 503
            return ServiceHealth(
                database="unavailable",
                status="degraded",
                streaming_mode="unavailable",
                as_of=now,
                collector="unknown",
                data_freshness="unknown",
                historical_coverage="unknown",
                websocket_delivery="degraded",
            )

    return router
