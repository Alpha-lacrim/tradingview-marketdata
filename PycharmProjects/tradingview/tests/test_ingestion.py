import asyncio
from datetime import UTC, datetime

from marketdata.config import Settings
from marketdata.domain import normalize
from marketdata.fanout import Fanout
from marketdata.ingestion import IngestionService
from marketdata.observability import Metrics
from marketdata.providers.base import PermanentProviderError
from marketdata.providers.mock import MockMarketDataProvider


class Writer:
    def __init__(self, failures=0):
        self.failures = failures
        self.batches = []
        self.written = asyncio.Event()

    async def write(self, ticks):
        if self.failures:
            self.failures -= 1
            raise ConnectionError("temporary")
        self.batches.append(list(ticks))
        self.written.set()
        return len(ticks)


def service(writer, providers=None, **kwargs):
    settings = Settings(
        _env_file=None,
        batch_size=2,
        queue_capacity=2,
        batch_flush_seconds=0.01,
        shutdown_timeout_seconds=1,
        **kwargs,
    )
    metrics = Metrics()
    fanout = Fanout(1, 2, metrics)
    return IngestionService(settings, writer, providers or [], fanout, metrics)


async def test_outage_retains_batch_and_receive_does_not_block(pair):
    writer = Writer(failures=1)
    ingestion = service(writer)
    tick = normalize(*pair, datetime.now(UTC), last="1")
    await ingestion.start()
    ingestion.accept(tick)
    async with asyncio.timeout(2):
        await ingestion.queue.join()
    await ingestion.stop()
    assert writer.batches == [[tick]]
    assert ingestion.metrics.db_errors._value.get() == 1
    assert ingestion.database_healthy


async def test_queue_full_is_visible_and_shutdown_drains(pair):
    ingestion = service(Writer())
    tick = normalize(*pair, datetime.now(UTC), last="1")
    for _ in range(3):
        ingestion.accept(tick)
    assert ingestion.queue.qsize() == 2
    assert ingestion.metrics.dropped.labels("mock", "persistence_queue_full")._value.get() == 1
    await ingestion.start()
    await ingestion.stop()
    assert sum(map(len, ingestion.store.batches)) == 2


async def test_shutdown_deadline_reports_all_unpersisted_ticks(pair):
    ingestion = service(Writer(failures=100))
    ingestion.settings = Settings(
        _env_file=None,
        batch_size=2,
        queue_capacity=2,
        batch_flush_seconds=0.01,
        shutdown_timeout_seconds=0.05,
    )
    for _ in range(2):
        ingestion.accept(normalize(*pair, datetime.now(UTC), last="1"))
    await ingestion.start()
    await asyncio.sleep(0.02)
    await ingestion.stop()
    assert ingestion.metrics.dropped.labels("mock", "shutdown_deadline")._value.get() == 2


def test_slow_websocket_consumer_is_disconnected(pair):
    ingestion = service(Writer())
    client = ingestion.fanout.register()
    client.symbols.add(pair[0].canonical_symbol)
    tick = normalize(*pair, datetime.now(UTC), last="1")
    ingestion.fanout.publish(tick)
    ingestion.fanout.publish(tick)
    assert client.overflow.is_set()
    assert ingestion.metrics.ws_dropped._value.get() == 1
    assert client.queue.qsize() == 1


class DisconnectOnce(MockMarketDataProvider):
    def __init__(self, metrics):
        super().__init__(metrics, ticks_per_second=1000)
        self.connections = 0

    async def connect(self):
        self.connections += 1
        if self.connections == 1:
            raise ConnectionError("disconnect")
        await super().connect()


async def test_reconnect_restores_subscriptions(pair):
    provider = DisconnectOnce(Metrics())
    await provider.subscribe([pair])
    ingestion = service(Writer(), [provider])
    await ingestion.start()
    await asyncio.wait_for(ingestion.store.written.wait(), 3)
    await ingestion.stop()
    assert provider.connections == 2
    assert provider.health.reconnect_count == 1
    assert provider.subscriptions == [pair]


class BadCredentials(MockMarketDataProvider):
    async def connect(self):
        raise PermanentProviderError("authentication")


async def test_permanent_failure_does_not_stop_other_provider(pair):
    bad = BadCredentials(Metrics())
    good = MockMarketDataProvider(Metrics(), ticks_per_second=1000)
    await good.subscribe([pair])
    ingestion = service(Writer(), [bad, good])
    await ingestion.start()
    await asyncio.sleep(0.05)
    assert bad.health.state == "degraded"
    assert bad.health.reconnect_count == 0
    assert ingestion.store.batches
    await ingestion.stop()
