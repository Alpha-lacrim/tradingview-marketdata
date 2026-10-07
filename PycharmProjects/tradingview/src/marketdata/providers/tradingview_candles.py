"""Native guest chart bars, verified against served JS and captured 1m responses.

See docs/tradingview.md. No quote sampling, login, cookies or browser automation.
"""

import asyncio
import json
import re
import secrets
from contextlib import suppress
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.typing import Origin

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import DataStatus, InstrumentType, from_ns, utc_now
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
        source_status=source_status,
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
    def __init__(self, settings: Settings, metrics: Metrics):
        super().__init__("tradingview_guest", metrics)
        self.settings = settings
        self.grace = settings.candle_fetch_grace_seconds
        self.socket: ClientConnection | None = None
        self.decoder = FrameDecoder()

    async def connect(self) -> None:
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
            if error.response.status_code in (400, 401, 403, 451):
                raise PermanentConnectionError("tradingview_access_rejected") from None
            raise
        self.decoder = FrameDecoder()
        self.transport_connected()
        await self.socket.send(encode_message("set_auth_token", ["unauthorized_user_token"]))
        self.subscriptions_requested()

    async def disconnect(self) -> None:
        socket, self.socket = self.socket, None
        try:
            if socket:
                await socket.close()
        finally:
            self.transport_disconnected()

    async def fetch_completed(
        self,
        pair: Subscription,
        start: datetime,
        end: datetime,
        count: int,
    ) -> list[MarketCandle]:
        socket = self.socket
        if socket is None:
            raise ConnectionError("candle_transport_disconnected")
        session = "cs_" + secrets.token_hex(6)
        info: dict[str, Any] = {}
        bars: dict[Decimal, list[Any]] = {}
        self.metrics.candles.labels(self.name, "requested").inc()
        with self.metrics.candle_latency.labels("request").time():
            async with asyncio.timeout(self.settings.candle_request_timeout_seconds):
                try:
                    for method, params in (
                        ("chart_create_session", [session, ""]),
                        ("resolve_symbol", [session, "symbol_1", pair[1].provider_symbol]),
                        ("create_series", [session, "s1", "s1", "symbol_1", "1", count, ""]),
                    ):
                        await socket.send(encode_message(method, params))
                    while True:
                        received = utc_now()
                        raw = await socket.recv()
                        received = utc_now()
                        self.health.last_message_at = received
                        for body in self.decoder.feed(raw):  # type: ignore[arg-type]
                            if body.startswith("~h~"):
                                await socket.send(f"~m~{len(body)}~m~{body}")
                                continue
                            message = json.loads(body, parse_float=Decimal)
                            if not isinstance(message, dict):
                                raise ValueError("chart message must be an object")
                            kind, p = message.get("m"), message.get("p", [])
                            if kind in ("critical_error", "protocol_error"):
                                raise PermanentConnectionError(
                                    "tradingview_chart_protocol_rejected"
                                )
                            if not p or p[0] != session:
                                continue
                            if kind in ("symbol_error", "series_error"):
                                raise PermanentProviderError("tradingview_chart_unavailable")
                            if kind == "symbol_resolved":
                                if p[1] != "symbol_1":
                                    raise ValueError("unexpected resolved symbol identifier")
                                info = p[2]
                                validate_source(info, pair)
                            if kind in ("timescale_update", "du"):
                                for row in p[1].get("s1", {}).get("s", []):
                                    values = row["v"]
                                    key = observed_decimal(values[0])
                                    if key is None:
                                        raise ValueError("missing bar time")
                                    bars[key] = values
                                if len(bars) > count + 2:
                                    raise ValueError("candle response exceeds requested bound")
                            if kind == "series_completed":
                                if p[1] != "s1" or p[3] != "s1":
                                    raise ValueError("unexpected chart series/turnaround")
                                status = p[2]
                                if len(p) > 4 and isinstance(p[4], dict):
                                    period = observed_decimal(p[4].get("rt_update_period"))
                                    if period is not None:
                                        info["observed_update_period_seconds"] = str(period)
                                validate_source(info, pair)
                                if status not in ("streaming", "realtime"):
                                    raise PermanentProviderError("tradingview_candles_not_realtime")
                                cutoff = min(end, received - timedelta(seconds=self.grace))
                                result = [
                                    parse_bar(v, pair, info, received, status)
                                    for ts, v in sorted(bars.items())
                                    if start <= from_ns(int(ts) * 1_000_000_000)
                                    and from_ns(int(ts) * 1_000_000_000) + timedelta(minutes=1)
                                    <= cutoff
                                ]
                                self.metrics.candles.labels(self.name, "received").inc(len(result))
                                return result
                finally:
                    with suppress(ConnectionClosed):
                        await socket.send(encode_message("chart_delete_session", [session]))
