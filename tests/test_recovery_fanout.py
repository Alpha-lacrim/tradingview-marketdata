"""A native-history recovery may acknowledge tens of thousands of changed keys."""

from datetime import timedelta
from pathlib import Path

import pytest
from test_native_candles import NOW, Replay

from marketdata.candle_ingestion import CandleIngestion
from marketdata.config import Settings
from marketdata.fanout import Fanout
from marketdata.registry import Registry


class NoLinearSearch(list):
    def __contains__(self, value):
        raise AssertionError("per-candle linear search in durable acknowledgements")


@pytest.mark.parametrize("acknowledge", [True, False])
async def test_large_recovery_acknowledgements_keep_fanout_exact_without_linear_search(acknowledge):
    pair = Registry.load(Path("config/instruments.tradingview.json")).subscriptions(
        "tradingview_guest"
    )[0]
    end = NOW.replace(second=0)
    beginning = end - timedelta(minutes=300)

    class MemoryStore:
        def __init__(self):
            self.state = dict(coverage_start=beginning, coverage_end=beginning)
            self.last_candle_write = {}

        async def candle_progress(self, instrument_id, provider):
            return self.state

        async def write_candles(self, candles, coverage):
            keys = [(c.instrument_id, c.provider, c.bucket_start) for c in candles]
            self.last_candle_write = dict(
                changed_keys=NoLinearSearch(keys if acknowledge else []),
                changed_candles=candles,
            )
            self.state = dict(
                coverage_start=beginning,
                coverage_end=end,
                last_persisted_minute=end - timedelta(minutes=1),
            )
            return len(keys) if acknowledge else 0

    provider = Replay()
    await provider.subscribe([pair])
    fanout = Fanout(1000, 1, provider.metrics)
    client = fanout.register()
    client.symbols.add(pair[0].canonical_symbol)
    service = CandleIngestion(Settings(_env_file=None), MemoryStore(), provider, fanout)
    await service.cycle(NOW)
    assert client.queue.qsize() == (300 if acknowledge else 0)
    assert not client.overflow.is_set() and not service.pending
    if acknowledge:
        delivered = [client.queue.get_nowait() for _ in range(300)]
        assert len({c.bucket_start for c in delivered}) == 300
        assert all(c.metadata["backfilled"] for c in delivered)
        assert delivered[0].bucket_start == beginning and delivered[-1].bucket_end == end
