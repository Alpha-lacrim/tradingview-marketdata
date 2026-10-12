"""Native guest chart bars, verified against served JS and captured 1m responses.

See docs/tradingview.md. No quote sampling, login, cookies or browser automation.
"""

import asyncio
import re
from datetime import datetime, timedelta
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.typing import Origin

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import DataStatus, InstrumentType, from_ns
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentConnectionError, PermanentProviderError, Subscription
from marketdata.providers.candles import CandleProvider
from marketdata.providers.tradingview import (
    MAX_FRAME_UNITS,
    FrameDecoder,
    encode_message,
    observed_decimal,
)


def validate_source(info: dict[str, Any], pair: Subscription) -> None:
    instrument, mapping = pair
    if (
        info.get("full_name") != mapping.provider_symbol
        or info.get("type") != mapping.metadata.get("source_type")
        or str(info.get("listed_exchange", "")).casefold() != instrument.exchange.casefold()
        or info.get("currency_code") != instrument.currency
        or info.get("bar_transform") != "none"
        or info.get("bar_fillgaps") is not False
        or info.get("has_intraday") is not True
        or info.get("bar_source") != "trade"
    ):
        raise PermanentProviderError("tradingview_candle_identity_or_semantics_unverified")


def parse_bar(
    values: list[Any],
    pair: Subscription,
    info: dict[str, Any],
    received: datetime,
    source_status: str,
) -> MarketCandle:
    validate_source(info, pair)
    if source_status not in ("streaming", "realtime"):
        raise PermanentProviderError("tradingview_candles_not_realtime")
    if len(values) != 6:
        raise ValueError("expected timestamp/O/H/L/C/volume vector")
    timestamp = observed_decimal(values[0])
    if timestamp is None or timestamp % 60:
        raise ValueError("unaligned candle timestamp")
    start = from_ns(int(timestamp) * 1_000_000_000)
    instrument, mapping = pair
    prices = [observed_decimal(v) for v in values[1:5]]
    if any(v is None for v in prices):
        raise ValueError("missing OHLC price")
    opening, high, low, close = prices
    assert opening is not None and high is not None and low is not None and close is not None
    return MarketCandle(
        instrument_id=instrument.id,
        canonical_symbol=instrument.canonical_symbol,
        provider=mapping.provider,
        provider_symbol=mapping.provider_symbol,
        bucket_start=start,
        bucket_end=start + timedelta(minutes=1),
        open=opening,
        high=high,
        low=low,
        close=close,
        volume=observed_decimal(values[5]) if info.get("volume_type") in ("base", "tick") else None,
        realtime_status=DataStatus.REALTIME,
        source_status="streaming" if source_status == "streaming" else "realtime",
        received_timestamp=received,
        metadata={
            "provenance": "native_upstream_chart",
            "resolution": "1",
            "bar_source": info["bar_source"],
            "bar_transform": info["bar_transform"],
            "bar_fillgaps": info["bar_fillgaps"],
            "source_timezone": info.get("timezone"),
            "source_update_period_seconds": info.get("observed_update_period_seconds"),
            "volume_type": info.get("volume_type"),
            "volume_additive": info.get("volume_type") in ("base", "tick"),
            "price_semantics": (
                "native chart OHLC; broker/reference methodology not independently verified"
            ),
            "completion": "bucket_end <= collection cutoff; series_completed is request completion",
        },
    )


class TradingViewCandleProvider(CandleProvider):
    supports_request_budget = True

    def __init__(self, settings: Settings, metrics: Metrics, *, experimental_pool: bool = False):
        super().__init__("tradingview_guest", metrics)
        self.settings = settings
        self.grace = settings.candle_fetch_grace_seconds
        self.socket: ClientConnection | None = None
        self.decoder = FrameDecoder()
        self.connection_generation = 0
        self.request_serial = 0
        self.fetching = False
        self.effective_concurrency = settings.tradingview_candle_concurrency
        self.job_limit = 100  # Local safeguard, not a documented upstream limit.
        # Explicit experimental harness opt-in; the app factory retains its default.
        # This authorizes a local experiment, not a claim of upstream permitted limits.
        self.experimental_pool = experimental_pool

    async def connect(self) -> None:
        if (
            self.settings.candle_pool_connections != 1 and not self.experimental_pool
        ) or self.job_limit > 100:
            raise PermanentConnectionError("tradingview_live_pool_permissions_unverified")
        if self.socket is not None:
            raise RuntimeError("candle socket already connected")
        if not 1 <= len(self.subscriptions) <= 100:
            raise PermanentConnectionError("tradingview_local_subscription_limit")
        for instrument, mapping in self.subscriptions:
            if (
                instrument.instrument_type
                not in (
                    InstrumentType.SPOT,
                    InstrumentType.OTC,
                    InstrumentType.REFERENCE,
                    InstrumentType.CFD,
                )
                or not re.fullmatch(r"[A-Z0-9_]+:[A-Z0-9_.-]+", mapping.provider_symbol)
                or mapping.provider_symbol.split(":", 1)[0] != instrument.exchange.upper()
            ):
                raise PermanentConnectionError("tradingview_identity_or_product_unsupported")
        self.connection_started(reconnecting=bool(self.health.reconnect_count))
        self.connection_generation += 1
        try:
            self.socket = await connect(
                self.settings.tradingview_ws_url,
                origin=Origin("https://www.tradingview.com"),
                open_timeout=10,
                close_timeout=3,
                ping_interval=20,
                ping_timeout=20,
                max_queue=16,
                max_size=MAX_FRAME_UNITS,
            )
        except InvalidStatus as error:
            if error.response.status_code == 429:
                self.metrics.tradingview_throttle.labels("http_429").inc()
                self.effective_concurrency = max(1, self.effective_concurrency // 2)
            if error.response.status_code in (400, 401, 403, 451):
                raise PermanentConnectionError("tradingview_access_rejected") from None
            raise
        self.decoder = FrameDecoder()
        self.transport_connected()
        self.metrics.tradingview_sockets.set(1)
        try:
            async with asyncio.timeout(self.settings.candle_request_timeout_seconds):
                await self.socket.send(
                    encode_message("set_auth_token", ["unauthorized_user_token"])
                )
        except ConnectionClosed as error:
            self.metrics.tradingview_disconnects.inc()
            self.metrics.tradingview_throttle.labels("connection").inc()
            self.metrics.candle_request_errors.labels("connection").inc()
            self.effective_concurrency = max(1, self.effective_concurrency // 2)
            self.health.last_close_code = getattr(error.rcvd, "code", None)
            self.health.last_close_reason = "upstream_disconnect"
            raise
        self.subscriptions_requested()

    async def disconnect(self) -> None:
        self.connection_generation += 1
        socket, self.socket = self.socket, None
        try:
            if socket:
                await socket.close()
        finally:
            self.metrics.tradingview_sockets.set(0)
            self.transport_disconnected()

    def _parse_native_bar(
        self,
        values: list[Any],
        pair: Subscription,
        info: dict[str, Any],
        received: datetime,
        source_status: str,
    ) -> MarketCandle:
        return parse_bar(values, pair, info, received, source_status)

    async def fetch_completed(
        self, pair: Subscription, start: datetime, end: datetime, count: int
    ) -> list[MarketCandle]:
        results = await self.fetch_many([(pair, start, end, count)])
        result = results[pair[0].canonical_symbol]
        if isinstance(result, Exception):
            raise result
        return result

    async def fetch_many(
        self, requests: list[tuple[Subscription, datetime, datetime, int]]
    ) -> dict[str, list[MarketCandle] | Exception]:
        from marketdata.providers.tradingview_pipeline import retrieve

        return await retrieve(self, requests)
