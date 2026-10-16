import json
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from marketdata.config import Settings
from marketdata.domain import (
    DataStatus,
    Instrument,
    InstrumentType,
    ProviderInstrument,
    Tick,
    normalize,
    parse_timestamp_ns,
    utc_now,
)
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider, PermanentProviderError


def parse_price(
    message: dict[str, Any], instrument: Instrument, mapping: ProviderInstrument, received: datetime
) -> Tick:
    if message["type"] != "PRICE" or message["instrument"] != mapping.provider_symbol:
        raise ValueError("unexpected price/symbol")
    bids = message["bids"]
    asks = message["asks"]
    bid = max(bids, key=lambda p: Decimal(p["price"])) if bids else None
    ask = min(asks, key=lambda p: Decimal(p["price"])) if asks else None
    event_timestamp, ns = parse_timestamp_ns(message["time"])
    return normalize(
        instrument,
        mapping,
        received,
        event_timestamp=event_timestamp,
        event_timestamp_ns=ns,
        bid=Decimal(bid["price"]) if bid else None,
        ask=Decimal(ask["price"]) if ask else None,
        bid_size=Decimal(str(bid["liquidity"])) if bid else None,
        ask_size=Decimal(str(ask["liquidity"])) if ask else None,
        metadata={
            "upstream_time": message["time"],
            "tradeable": message.get("tradeable"),
            "status": message.get("status"),
            "sampled": True,
        },
    )


class OandaProvider(MarketDataProvider):
    def __init__(self, settings: Settings, metrics: Metrics):
        super().__init__("oanda", metrics)
        self.settings = settings
        self.client: httpx.AsyncClient | None = None

    @property
    def rest_url(self) -> str:
        return (
            "https://api-fxtrade.oanda.com"
            if self.settings.oanda_environment == "live"
            else "https://api-fxpractice.oanda.com"
        )

    @property
    def stream_url(self) -> str:
        return (
            "https://stream-fxtrade.oanda.com"
            if self.settings.oanda_environment == "live"
            else "https://stream-fxpractice.oanda.com"
        )

    def account_path(self) -> str:
        account = self.settings.oanda_account_id
        if account is None:
            raise PermanentProviderError("oanda_credentials_required")
        value = account.get_secret_value()
        if not value or any(c not in "0123456789-" for c in value):
            raise PermanentProviderError("oanda_account_id_invalid")
        return "/v3/accounts/" + value

    async def discover(self) -> list[dict[str, Any]]:
        if self.client is None:
            token = self.settings.oanda_api_token
            if token is None or not token.get_secret_value():
                raise PermanentProviderError("oanda_credentials_required")
            self.client = httpx.AsyncClient(
                headers={"Authorization": "Bearer " + token.get_secret_value()},
                timeout=httpx.Timeout(10, read=self.settings.stale_seconds),
            )
        response = await self.client.get(self.rest_url + self.account_path() + "/instruments")
        self.check_status(response)
        instruments: list[dict[str, Any]] = response.json()["instruments"]
        return instruments

    @staticmethod
    def check_status(response: httpx.Response) -> None:
        if response.status_code in (400, 401, 403, 404):
            raise PermanentProviderError("oanda_authentication_or_subscription_rejected")
        response.raise_for_status()

    async def connect(self) -> None:
        discovered = {i["name"]: i for i in await self.discover()}
        for instrument, mapping in self.subscriptions:
            info = discovered.get(mapping.provider_symbol)
            expected = {
                "CURRENCY": {InstrumentType.OTC},
                "METAL": {InstrumentType.OTC},
                "CFD": {InstrumentType.CFD},
            }
            if (
                info is None
                or instrument.instrument_type not in expected.get(info["type"], set())
                or instrument.exchange != "OANDA"
            ):
                raise PermanentProviderError("oanda_identity_or_availability_mismatch")
            required = (
                DataStatus.SIMULATED
                if self.settings.oanda_environment == "practice"
                else DataStatus.INDICATIVE
            )
            if mapping.realtime_status != required:
                raise PermanentProviderError("oanda_status_must_match_account_environment")

    async def disconnect(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        if self.client is None:
            raise RuntimeError("not connected")
        mappings = {m.provider_symbol: (i, m) for i, m in self.subscriptions}
        async with self.client.stream(
            "GET",
            self.stream_url + self.account_path() + "/pricing/stream",
            params={"instruments": ",".join(mappings), "snapshot": "true"},
        ) as response:
            self.check_status(response)
            async for line in bounded_lines(response):
                if not line:
                    continue
                received = utc_now()
                self.message_received(received)
                try:
                    data = json.loads(line)
                    if data["type"] == "HEARTBEAT":
                        continue
                    instrument, mapping = mappings[data["instrument"]]
                    yield parse_price(data, instrument, mapping, received)
                except (KeyError, TypeError, ValueError, ArithmeticError):
                    self.malformed()


async def bounded_lines(response: httpx.Response) -> AsyncIterator[str]:
    pending = b""
    async for chunk in response.aiter_bytes():
        pending += chunk
        while b"\n" in pending:
            line, pending = pending.split(b"\n", 1)
            if len(line) > 131072:
                raise ConnectionError("oanda_message_size_limit")
            yield line.decode("utf-8")
        if len(pending) > 131072:
            raise ConnectionError("oanda_message_size_limit")
    if pending:
        yield pending.decode("utf-8")
