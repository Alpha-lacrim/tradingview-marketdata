"""Synthetic guest-protocol load/fault harness. NEVER contacts an upstream service.

Synthetic identities are isolated from production registries. Any persistence benchmark
must use a disposable *_test database and marks candles simulated.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any

from marketdata.domain import AssetClass, DataStatus, Instrument, InstrumentType, ProviderInstrument
from marketdata.providers.base import Subscription
from marketdata.providers.tradingview import FrameDecoder, encode_message


def synthetic_pairs(count: int) -> list[Subscription]:
    return [
        (
            Instrument(
                canonical_symbol=f"BENCH:S{index:04d}",
                display_name="SIMULATED benchmark",
                asset_class=AssetClass.CRYPTO,
                instrument_type=InstrumentType.SPOT,
                exchange="BENCH",
                currency="USD",
                metadata={"simulated": True},
            ),
            ProviderInstrument(
                provider="benchmark",
                provider_symbol=f"BENCH:S{index:04d}",
                realtime_status=DataStatus.SIMULATED,
                metadata={"source_type": "crypto"},
            ),
        )
        for index in range(count)
    ]


class SyntheticChartSocket:
    """Deterministic independent delayed replies, bounded queue and owned emitter tasks."""

    def __init__(
        self,
        end: datetime,
        latency: float = 0.05,
        *,
        delays: dict[int, float] | None = None,
        drop: set[int] | None = None,
        duplicate: bool = False,
    ):
        self.end, self.latency = end, latency
        self.delays, self.drop = delays or {}, drop or set()
        self.duplicate = duplicate
        self.messages: asyncio.Queue[str | Exception] = asyncio.Queue(maxsize=64)
        self.sessions: dict[str, str] = {}
        self.tasks: dict[str, asyncio.Task[None]] = {}
        self.created: list[str] = []
        self.requests: list[tuple[str, str, int]] = []
        self.completed_order: list[int] = []
        self.max_active = 0
        self.closed = False
        self.receives = 0
        self.request_events: dict[int, asyncio.Event] = {}

    async def wait_for_requests(self, count: int) -> None:
        if len(self.requests) < count:
            await self.request_events.setdefault(count, asyncio.Event()).wait()

    async def send(self, raw: str) -> None:
        if self.closed:
            raise ConnectionError("synthetic_socket_closed")
        body = FrameDecoder().feed(raw)[0]
        if body.startswith("~h~"):
            return
        message = json.loads(body)
        kind, args = message["m"], message["p"]
        session = args[0]
        if kind == "chart_create_session":
            self.sessions[session] = ""
            self.created.append(session)
            self.max_active = max(self.max_active, len(self.sessions))
        elif kind == "resolve_symbol":
            self.sessions[session] = args[2]
        elif kind == "create_series":
            source = self.sessions[session]
            index = int(source.rsplit("S", 1)[-1])
            self.requests.append((session, source, args[5]))
            for threshold, event in self.request_events.items():
                if len(self.requests) >= threshold:
                    event.set()
            if index not in self.drop:
                self.tasks[session] = asyncio.create_task(
                    self.emit(session, source, index, args[5]), name="synthetic-chart-emitter"
                )
        elif kind == "chart_delete_session":
            self.sessions.pop(session, None)
            task = self.tasks.pop(session, None)
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def emit(self, session: str, source: str, index: int, count: int) -> None:
        await asyncio.sleep(self.delays.get(index, self.latency * (0.7 + (index % 5) * 0.15)))
        info: dict[str, Any] = dict(
            full_name=source,
            type="crypto",
            listed_exchange="BENCH",
            currency_code="USD",
            bar_transform="none",
            bar_fillgaps=False,
            has_intraday=True,
            bar_source="trade",
            timezone="Etc/UTC",
            session="24x7",
            volume_type="base",
        )
        # Includes one forming bar, deliberately: real parser must exclude it.
        rows = [
            dict(
                v=[
                    int((self.end - timedelta(minutes=offset)).timestamp()),
                    str(100 + index),
                    str(102 + index),
                    str(99 + index),
                    str(101 + index),
                    "10",
                ]
            )
            for offset in range(count - 1, -1, -1)
        ]
        for kind, args in (
            ("symbol_resolved", [session, "symbol_1", info]),
            ("timescale_update", [session, {"s1": {"s": rows}}]),
            ("series_completed", [session, "s1", "streaming", "s1"]),
        ):
            await self.messages.put(encode_message(kind, args))
            if self.duplicate:
                await self.messages.put(encode_message(kind, args))
        self.completed_order.append(index)

    async def recv(self) -> str:
        self.receives += 1
        if self.closed and self.messages.empty():
            raise ConnectionError("synthetic_socket_closed")
        value = await self.messages.get()
        if isinstance(value, Exception):
            raise value
        return value

    async def close(self) -> None:
        self.closed = True
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.tasks.clear()
        self.sessions.clear()
        if not self.messages.full():
            self.messages.put_nowait(ConnectionError("synthetic_socket_closed"))
