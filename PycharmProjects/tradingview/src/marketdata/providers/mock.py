import asyncio
import random
from collections.abc import AsyncIterator
from decimal import Decimal

from marketdata.domain import Tick, normalize, utc_now
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider


class MockMarketDataProvider(MarketDataProvider):
    def __init__(self, metrics: Metrics, ticks_per_second: float = 100, seed: int = 42):
        super().__init__("mock", metrics)
        self.ticks_per_second = ticks_per_second
        self.random = random.Random(seed)
        self.connected = False
        self.prices: dict[str, Decimal] = {}
        self.sequence = 0

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        if not self.subscriptions:
            return
        index = 0
        started = asyncio.get_running_loop().time()
        burst_size = max(1, min(100, int(self.ticks_per_second / 100)))
        while self.connected:
            instrument, mapping = self.subscriptions[index % len(self.subscriptions)]
            index += 1
            symbol = instrument.canonical_symbol
            step = instrument.tick_size or Decimal("0.00001")
            initial = Decimal(str(mapping.metadata.get("simulation_initial_price", "100")))
            price = self.prices.get(symbol, initial) + step * self.random.choice([-1, 0, 1])
            self.prices[symbol] = max(price, step * 2)
            now = utc_now()
            self.message_received(now)
            self.sequence += 1
            yield normalize(
                instrument,
                mapping,
                now,
                event_timestamp=now,
                bid=self.prices[symbol] - step,
                ask=self.prices[symbol] + step,
                last=self.prices[symbol],
                last_size=Decimal(self.random.randint(1, 10)),
                sequence_number=self.sequence,
                metadata={"synthetic": True, "sequence_scope": "mock_session"},
            )
            if index % burst_size == 0:
                due = started + index / self.ticks_per_second
                await asyncio.sleep(max(0, due - asyncio.get_running_loop().time()))
