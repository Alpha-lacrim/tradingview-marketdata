"""Injected provider doubles for isolated qualification; never contacts TradingView."""

import asyncio
import json
from datetime import datetime
from pathlib import Path

from marketdata.candles import MarketCandle
from marketdata.domain import Tick, utc_now
from marketdata.observability import Metrics
from marketdata.persistence import Store
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.candles import CandleProvider


def record(path: Path, kind: str, **values):
    with path.open("a", encoding="utf-8") as output:
        output.write(
            json.dumps(dict(kind=kind, observed_at=utc_now().isoformat(), **values), default=str)
            + "\n"
        )


class InjectedQuotes(MarketDataProvider):
    persist_tick_history = False

    def __init__(self, path: Path, trace: Path, metrics: Metrics):
        super().__init__("tradingview_guest_quotes_00", metrics)
        self.path, self.trace, self.offset, self.connected = path, trace, 0, False
        self.health.storage_mode = "quotes"

    async def connect(self):
        self.connected = True

    async def disconnect(self):
        self.connected = False

    def read(self):
        with self.path.open(encoding="utf-8") as incoming:
            incoming.seek(self.offset)
            rows = []
            for _ in range(1000):
                position = incoming.tell()
                line = incoming.readline()
                if not line or not line.endswith("\n"):
                    incoming.seek(position)
                    break
                rows.append(Tick.model_validate_json(line))
            self.offset = incoming.tell()
        return rows

    async def stream_ticks(self):
        while self.connected:
            for tick in await asyncio.to_thread(self.read):
                if tick.realtime_status.value != "simulated":
                    raise ValueError("injected provider refuses non-simulated input")
                self.message_received()
                record(
                    self.trace,
                    "injected_quote_read",
                    observation_id=tick.tick_id,
                    source_timestamp=tick.event_timestamp,
                    received_timestamp=tick.received_timestamp,
                    instrument_id=tick.instrument_id,
                )
                yield tick
            await asyncio.sleep(0.02)


class InjectedCandles(CandleProvider):
    def __init__(self, path: Path, trace: Path, metrics: Metrics):
        super().__init__("tradingview_guest", metrics)
        self.path, self.trace = path, trace

    async def connect(self):
        self.transport_connected()

    async def disconnect(self):
        self.transport_disconnected()

    async def fetch_completed(self, pair, start: datetime, end: datetime, count: int):
        self.transport_connected()
        rows = await asyncio.to_thread(self.path.read_text, encoding="utf-8")
        bars = [MarketCandle.model_validate(v) for v in json.loads(rows)]
        selected = [
            b for b in bars if b.instrument_id == pair[0].id and start <= b.bucket_start < end
        ]
        if any(b.realtime_status != "simulated" for b in selected):
            raise ValueError("injected candle provider refuses genuine-data labels")
        self.session_info[pair[0].canonical_symbol] = {"session": "24x7", "timezone": "Etc/UTC"}
        record(
            self.trace,
            "injected_native_fetch",
            instrument_id=pair[0].id,
            start=start,
            end=end,
            returned=len(selected),
        )
        return selected


def tracing_store(trace: Path):
    class TracingStore(Store):
        async def write_quotes(self, ticks):
            started = utc_now()
            count = await super().write_quotes(ticks)
            record(
                trace,
                "local_quote_commit_observed",
                started_at=started,
                count=count,
                observation_ids=[str(t.tick_id) for t in ticks],
            )
            return count

        async def write_candles(self, candles, coverage):
            started = utc_now()
            count = await super().write_candles(candles, coverage)
            record(
                trace,
                "local_candle_commit_observed",
                started_at=started,
                count=count,
                keys=[
                    dict(
                        instrument_id=str(b.instrument_id),
                        bucket_start=b.bucket_start.isoformat(),
                        received_timestamp=b.received_timestamp.isoformat(),
                    )
                    for b in self.last_candle_write["changed_candles"]
                ],
            )
            return count

    return TracingStore
