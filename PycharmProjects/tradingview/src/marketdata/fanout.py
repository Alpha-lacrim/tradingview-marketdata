import asyncio
from dataclasses import dataclass, field

from marketdata.domain import Tick
from marketdata.observability import Metrics


@dataclass(eq=False)
class Subscriber:
    queue: asyncio.Queue[Tick]
    symbols: set[str] = field(default_factory=set)
    overflow: asyncio.Event = field(default_factory=asyncio.Event)


class Fanout:
    def __init__(self, capacity: int, max_clients: int, metrics: Metrics):
        self.capacity = capacity
        self.max_clients = max_clients
        self.metrics = metrics
        self.clients: set[Subscriber] = set()

    def register(self) -> Subscriber:
        if len(self.clients) >= self.max_clients:
            raise OverflowError("websocket capacity reached")
        subscriber = Subscriber(asyncio.Queue(maxsize=self.capacity))
        self.clients.add(subscriber)
        return subscriber

    def unregister(self, subscriber: Subscriber) -> None:
        self.clients.discard(subscriber)

    def publish(self, tick: Tick) -> None:
        for client in self.clients:
            if tick.canonical_symbol in client.symbols and not client.overflow.is_set():
                try:
                    client.queue.put_nowait(tick)
                except asyncio.QueueFull:
                    self.metrics.ws_dropped.inc()
                    client.overflow.set()
