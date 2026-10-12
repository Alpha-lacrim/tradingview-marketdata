import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

import httpx
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import InvalidStatus

from marketdata.config import Settings
from marketdata.domain import (
    DataStatus,
    Instrument,
    InstrumentType,
    ProviderInstrument,
    Tick,
    from_ns,
    normalize,
    utc_now,
)
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider, PermanentProviderError

logger = logging.getLogger(__name__)


def parse_trade(
    message: dict[str, object],
    instrument: Instrument,
    mapping: ProviderInstrument,
    received: datetime,
) -> Tick:
    if message["e"] != "trade" or message["s"] != mapping.provider_symbol:
        raise ValueError("unexpected trade event/symbol")
    trade_id = int(str(message["t"]))
    timestamp_ns = int(str(message["T"])) * 1000000
    return normalize(
        instrument,
        mapping,
        received,
        tick_id=uuid5(NAMESPACE_URL, f"binance:trade:{mapping.provider_symbol}:{trade_id}"),
        event_timestamp=from_ns(timestamp_ns),
        event_timestamp_ns=timestamp_ns,
        last=Decimal(str(message["p"])),
        last_size=Decimal(str(message["q"])),
        sequence_number=trade_id,
        metadata={"stream": "trade", "event_time_ms": message["E"]},
    )


class BinanceProvider(MarketDataProvider):
    def __init__(self, settings: Settings, metrics: Metrics):
        super().__init__("binance", metrics)
        self.settings = settings
        self.socket: ClientConnection | None = None
        self.last_trade_ids: dict[str, int] = {}

    async def connect(self) -> None:
        if not 1 <= len(self.subscriptions) <= 1024:
            raise PermanentProviderError("binance_subscription_count")
        symbols = [m.provider_symbol for _, m in self.subscriptions]
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(
                self.settings.binance_rest_url + "/api/v3/exchangeInfo",
                params={"symbols": json.dumps(symbols)},
            )
            if response.status_code in (400, 401, 403, 451):
                raise PermanentProviderError("binance_unavailable_or_invalid_symbol")
            response.raise_for_status()
            available = {s["symbol"]: s for s in response.json()["symbols"]}
        for instrument, mapping in self.subscriptions:
            info = available.get(mapping.provider_symbol)
            if info is None or info["status"] != "TRADING":
                raise PermanentProviderError("binance_instrument_not_trading")
            if (
                instrument.instrument_type != InstrumentType.SPOT
                or instrument.exchange != "BINANCE"
                or instrument.base_asset != info["baseAsset"]
                or instrument.quote_asset != info["quoteAsset"]
                or mapping.realtime_status != DataStatus.REALTIME
            ):
                raise PermanentProviderError("binance_identity_mismatch")
        streams = "/".join(s.lower() + "@trade" for s in symbols)
        try:
            self.socket = await connect(
                self.settings.binance_ws_url.rstrip("/") + "/stream?streams=" + streams,
                open_timeout=10,
                close_timeout=5,
                ping_interval=20,
                ping_timeout=20,
                max_queue=32,
                max_size=65536,
            )
        except InvalidStatus as error:
            if error.response.status_code in (400, 401, 403, 451):
                raise PermanentProviderError("binance_websocket_rejected") from None
            raise

    async def disconnect(self) -> None:
        if self.socket is not None:
            await self.socket.close()
            self.socket = None

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        if self.socket is None:
            raise RuntimeError("not connected")
        mappings = {m.provider_symbol: (i, m) for i, m in self.subscriptions}
        async for raw in self.socket:
            received = utc_now()
            self.message_received(received)
            try:
                data = json.loads(raw)["data"]
                if data.get("e") == "serverShutdown":
                    raise ConnectionError("scheduled_shutdown")
                instrument, mapping = mappings[data["s"]]
                tick = parse_trade(data, instrument, mapping, received)
            except (KeyError, TypeError, ValueError, ArithmeticError):
                self.malformed()
                continue
            sequence = tick.sequence_number
            previous = self.last_trade_ids.get(tick.provider_symbol)
            if previous is not None and sequence is not None and sequence > previous + 1:
                self.metrics.sequence_gaps.labels(self.name).inc()
                logger.error(
                    "trade ID gap; history may be incomplete",
                    extra={
                        "context": {
                            "provider": self.name,
                            "symbol": tick.canonical_symbol,
                            "previous": previous,
                            "current": sequence,
                        }
                    },
                )
            if sequence is not None:
                self.last_trade_ids[tick.provider_symbol] = max(previous or sequence, sequence)
            yield tick
