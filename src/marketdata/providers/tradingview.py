"""Experimental guest quotes: observed public-site protocol, not a supported vendor API.

Protocol evidence, sampled-feed semantics and access limitations are in
docs/tradingview.md. No browser profiles, cookies or authenticated tokens are used.
"""

from __future__ import annotations

import json
import logging
import re
import secrets
import time
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Any

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import InvalidStatus
from websockets.typing import Origin

from marketdata.config import Settings
from marketdata.domain import DataStatus, InstrumentType, Tick, from_ns, normalize, utc_now
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider, PermanentProviderError, Subscription

logger = logging.getLogger(__name__)
MAX_FRAME_UNITS = 262144
FIELDS = (
    "lp",
    "lp_time",
    "bid",
    "ask",
    "volume",
    "update_mode",
    "update_mode_seconds",
    "type",
    "exchange",
    "description",
    "currency_code",
    "short_name",
)
IDENTITY_FIELDS = ("type", "exchange", "currency_code")
METADATA_FIELDS = (*IDENTITY_FIELDS, "update_mode", "update_mode_seconds")


def encode_message(method: str, parameters: list[Any]) -> str:
    # ASCII escaping makes Python and JavaScript string lengths agree.
    body = json.dumps({"m": method, "p": parameters}, separators=(",", ":"), ensure_ascii=True)
    if len(body) > MAX_FRAME_UNITS:
        raise ValueError("tradingview outgoing frame exceeds local limit")
    return f"~m~{len(body)}~m~{body}"


class FrameDecoder:
    """Bounded incremental framing; the site's length unit is UTF-16 code units."""

    def __init__(self) -> None:
        self.buffer = bytearray()

    def feed(self, raw: str) -> list[str]:
        if not isinstance(raw, str):
            raise ValueError("tradingview requires text frames")
        self.buffer.extend(raw.encode("utf-16-le"))
        if len(self.buffer) > 2 * MAX_FRAME_UNITS + 64:
            raise ValueError("tradingview frame buffer exceeds local limit")
        marker = "~m~".encode("utf-16-le")
        messages: list[str] = []
        while self.buffer:
            if len(self.buffer) < len(marker):
                break
            if not self.buffer.startswith(marker):
                raise ValueError("tradingview malformed frame prefix")
            end = self.buffer.find(marker, len(marker))
            if end == -1:
                if len(self.buffer) > 32:
                    raise ValueError("tradingview malformed frame length")
                break
            length = self.buffer[len(marker) : end].decode("utf-16-le")
            if not re.fullmatch(r"[0-9]{1,8}", length):
                raise ValueError("tradingview malformed frame length")
            units = int(length)
            if not 1 <= units <= MAX_FRAME_UNITS:
                raise ValueError("tradingview frame exceeds local limit")
            start = end + len(marker)
            stop = start + units * 2
            if len(self.buffer) < stop:
                break
            messages.append(self.buffer[start:stop].decode("utf-16-le"))
            del self.buffer[:stop]
        return messages


def observed_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("tradingview numeric input must preserve decimals")
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("tradingview nonfinite value")
    return result


def parse_quote(values: dict[str, Any], pair: Subscription, received: datetime) -> Tick:
    """Only CURRENT delta prices; metadata validation is owned by the adapter."""
    instrument, mapping = pair
    last = observed_decimal(values.get("lp"))
    timestamp = observed_decimal(values.get("lp_time")) if last is not None else None
    ns = None
    if timestamp is not None:
        scaled = timestamp * 1000000000
        if scaled != scaled.to_integral_value():
            raise ValueError("tradingview timestamp precision exceeds nanoseconds")
        ns = int(scaled)
    return normalize(
        instrument,
        mapping.model_copy(update={"realtime_status": DataStatus.REALTIME}),
        received,
        event_timestamp=from_ns(ns) if ns is not None else None,
        event_timestamp_ns=ns,
        bid=observed_decimal(values.get("bid")),
        ask=observed_decimal(values.get("ask")),
        last=last,
        volume=observed_decimal(values.get("volume")),
        metadata={
            "transport": "experimental_tradingview_guest",
            "source_symbol": mapping.provider_symbol,
            "price_semantics": "upstream quote delta, not a verified execution",
            "timestamp_semantics": "lp_time applies to last only; bid/ask time unavailable",
            "volume_semantics": "upstream reported volume; window unverified",
        },
    )


class TradingViewGuestProvider(MarketDataProvider):
    def __init__(self, settings: Settings, metrics: Metrics):
        super().__init__("tradingview_guest", metrics)
        self.settings = settings
        self.socket: ClientConnection | None = None
        self.session = ""
        self.decoder = FrameDecoder()
        self.source_metadata: dict[str, dict[str, Any]] = {}
        self.rejected: dict[str, str] = {}
        self.awaiting_metadata: set[str] = set()
        self._last_unknown_log = -float("inf")
        self.configure_freshness(settings.stale_seconds)

    def configure_freshness(self, stale_seconds: float) -> None:
        super().configure_freshness(stale_seconds)
        self.frame_timeout_seconds = self.settings.tradingview_frame_timeout_seconds
        self.observation_timeout_seconds = self.settings.tradingview_observation_timeout_seconds
        self.symbol_stale_seconds = self.settings.tradingview_symbol_stale_seconds

    async def subscribe(self, subscriptions: list[Subscription]) -> None:
        configured = {m.provider_symbol for _, m in subscriptions}
        if self.symbol_stale_seconds.keys() - configured:
            raise ValueError("TradingView stale threshold contains an unconfigured source symbol")
        await super().subscribe(subscriptions)

    async def connect(self) -> None:
        self.connection_started(reconnecting=bool(self.health.reconnect_count))
        # Local operating bound, not a claim about TradingView's subscription limit.
        if not 1 <= len(self.subscriptions) <= 100:
            raise PermanentProviderError("tradingview_local_subscription_limit")
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
                or mapping.metadata.get("source_type") not in ("spot", "forex", "commodity", "cfd")
            ):
                raise PermanentProviderError("tradingview_identity_or_product_unsupported")
        self.source_metadata.clear()
        self.rejected.clear()
        self.awaiting_metadata.clear()
        self.decoder = FrameDecoder()
        self.session = "qs_" + secrets.token_hex(6)
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
                raise PermanentProviderError("tradingview_access_rejected") from None
            raise
        self.transport_connected()
        self.transition("subscribing")
        for method, params in (
            # This is the public site's literal guest token, not an account credential.
            ("set_auth_token", ["unauthorized_user_token"]),
            ("quote_create_session", [self.session]),
            ("quote_set_fields", [self.session, *FIELDS]),
            (
                "quote_add_symbols",
                [self.session, *(m.provider_symbol for _, m in self.subscriptions)],
            ),
        ):
            await self.socket.send(encode_message(method, params))
        self.subscriptions_requested()
        logger.warning(
            "experimental unsupported TradingView guest protocol enabled",
            extra={"context": {"provider": self.name, "subscriptions": len(self.subscriptions)}},
        )

    async def disconnect(self) -> None:
        socket, self.socket = self.socket, None
        if socket is not None:
            try:
                await socket.close()
            finally:
                self.health.last_close_code = getattr(socket, "close_code", None)
                # Remote close text may contain sensitive internals. Keep only a category.
                self.health.last_close_reason = (
                    "remote_close_reason_present" if getattr(socket, "close_reason", None) else None
                )
                self.transport_disconnected()

    def reject(self, symbol: str, reason: str) -> None:
        if symbol in self.rejected:
            return
        self.rejected[symbol] = reason
        self.health.error_count += 1
        self.health.last_error = reason
        item = next(i for i in self.health.symbols.values() if i.provider_symbol == symbol)
        item.last_rejection_reason = reason
        item.rejected = True
        self.transition("degraded")
        self.refresh_health()
        logger.error(
            "TradingView symbol rejected; other subscriptions continue",
            extra={"context": {"provider": self.name, "provider_symbol": symbol, "reason": reason}},
        )

    def quote_tick(self, quote: dict[str, Any], received: datetime) -> Tick | None:
        symbol = quote["n"]
        pair = next((p for p in self.subscriptions if p[1].provider_symbol == symbol), None)
        if pair is None:
            return None  # A removed subscription can have in-flight responses.
        values = quote.get("v", {})
        if not isinstance(values, dict):
            raise ValueError("tradingview quote values must be an object")
        has_price = any(values.get(field) is not None for field in ("lp", "bid", "ask"))
        if quote.get("s") != "ok":
            self.reject(symbol, "tradingview_symbol_unavailable")
        metadata = self.source_metadata.setdefault(symbol, {})
        metadata.update({key: values[key] for key in METADATA_FIELDS if key in values})
        mode = metadata.get("update_mode")
        item = self.health.symbols[pair[0].canonical_symbol]
        if mode is not None:
            item.source_update_mode = (
                str(mode)
                if mode in ("streaming", "realtime", "delayed", "endofday", "snapshot", "forbidden")
                else "unrecognized"
            )
        if mode is not None and mode not in ("streaming", "realtime"):
            self.reject(symbol, "tradingview_not_realtime")
        delay = observed_decimal(metadata.get("update_mode_seconds"))
        if delay is not None and delay != 0:
            self.reject(symbol, "tradingview_not_realtime")
        instrument, mapping = pair
        # FXCM chart symbols use FX:, while captured quote identity reports FXCM.
        # Keep the exact source symbol and require the broker's reported quote venue.
        quote_exchange = (
            "FXCM"
            if instrument.exchange == "FX" and mapping.provider_symbol.startswith("FX:")
            else instrument.exchange
        )
        if all(key in metadata for key in IDENTITY_FIELDS) and (
            metadata["type"] != mapping.metadata["source_type"]
            or str(metadata["exchange"]).casefold() != quote_exchange.casefold()
            or metadata["currency_code"] != instrument.currency
        ):
            self.reject(symbol, "tradingview_source_identity_mismatch")
        if symbol in self.rejected:
            if has_price:
                self.metrics.dropped.labels(self.name, "source_rejected").inc()
                self.metrics.rejections.labels(self.name, self.rejected[symbol]).inc()
            return None
        if not has_price:
            return None
        if mode not in ("streaming", "realtime") or not all(
            key in metadata for key in IDENTITY_FIELDS
        ):
            self.metrics.dropped.labels(self.name, "source_metadata_unverified").inc()
            item.last_rejection_reason = "source_metadata_unverified"
            self.metrics.rejections.labels(self.name, "source_metadata_unverified").inc()
            if symbol not in self.awaiting_metadata:
                self.awaiting_metadata.add(symbol)
                logger.warning(
                    "quote discarded until source identity and realtime status are verified",
                    extra={"context": {"provider": self.name, "provider_symbol": symbol}},
                )
            return None
        tick = parse_quote(values, pair, received)
        return tick.model_copy(update={"metadata": dict(tick.metadata, upstream=dict(metadata))})

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        socket = self.socket
        if socket is None:
            raise RuntimeError("not connected")
        async for raw in socket:
            received = utc_now()
            self.message_received(received)
            try:
                messages = self.decoder.feed(raw)  # type: ignore[arg-type]
            except (ValueError, UnicodeError):
                self.malformed()
                raise ConnectionError("tradingview_framing_error") from None
            for body in messages:
                if body.startswith("~h~"):
                    await socket.send(f"~m~{len(body)}~m~{body}")
                    continue
                try:
                    data = json.loads(body, parse_float=Decimal)
                    if not isinstance(data, dict):
                        raise ValueError("tradingview message must be an object")
                    method = data.get("m")
                    if method in ("critical_error", "protocol_error"):
                        raise PermanentProviderError("tradingview_protocol_rejected")
                    if method != "qsd":
                        if "session_id" not in data:
                            self.metrics.unknown_messages.labels(self.name).inc()
                            now = time.monotonic()
                            if now - self._last_unknown_log >= 30:
                                self._last_unknown_log = now
                                logger.warning(
                                    "unknown TradingView message observed",
                                    extra={"context": {"provider": self.name}},
                                )
                        continue
                    parameters = data["p"]
                    if parameters[0] != self.session:
                        raise ValueError("tradingview wrong session")
                    tick = self.quote_tick(parameters[1], received)
                except (KeyError, IndexError, TypeError, ValueError, ArithmeticError):
                    self.malformed()
                    continue
                if tick is not None:
                    yield tick
