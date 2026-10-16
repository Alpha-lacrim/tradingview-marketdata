"""Benchmark-only chart workers: no network connector, simulated identities and results."""

import asyncio
from datetime import datetime, timedelta
from typing import Any

from marketdata.candle_benchmark import SyntheticChartSocket
from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import DataStatus
from marketdata.observability import Metrics
from marketdata.providers.base import Subscription
from marketdata.providers.tradingview import FrameDecoder
from marketdata.providers.tradingview_candles import TradingViewCandleProvider


class SyntheticCandleProvider(TradingViewCandleProvider):
    def __init__(
        self,
        settings: Settings,
        metrics: Metrics,
        end: datetime,
        *,
        latency: float = 0.05,
        handshake_seconds: float = 0.03,
        job_limit: int = 4096,
        **socket_options: Any,
    ):
        super().__init__(settings, metrics)
        self.name = self.health.provider = "benchmark"
        self.end, self.latency = end, latency
        self.handshake_seconds = handshake_seconds
        self.socket_options = socket_options
        self.socket_options.setdefault("missing_bucket", end - timedelta(minutes=1))
        self.job_limit = job_limit
        self.fail_handshake = False
        self.connected = asyncio.Event()
        self.sockets: list[SyntheticChartSocket] = []

    async def connect(self) -> None:
        if self.socket is not None:
            raise RuntimeError("synthetic worker already connected")
        if not 1 <= len(self.subscriptions) <= self.job_limit or any(
            not i.metadata.get("simulated") or m.realtime_status != DataStatus.SIMULATED
            for i, m in self.subscriptions
        ):
            raise ValueError("synthetic transport requires bounded simulated subscriptions")
        self.connection_generation += 1
        self.connection_started()
        await asyncio.sleep(self.handshake_seconds)
        if self.fail_handshake:
            raise TimeoutError("synthetic_handshake_failure")
        socket = SyntheticChartSocket(self.end, self.latency, **self.socket_options)
        self.sockets.append(socket)
        self.socket = socket  # type: ignore[assignment]
        self.decoder = FrameDecoder()
        self.transport_connected()
        self.connected.set()

    def _parse_native_bar(
        self,
        values: list[Any],
        pair: Subscription,
        info: dict[str, Any],
        received: datetime,
        source_status: str,
    ) -> MarketCandle:
        candle = super()._parse_native_bar(values, pair, info, received, source_status)
        return candle.model_copy(
            update={
                "realtime_status": DataStatus.SIMULATED,
                "metadata": {**candle.metadata, "provenance": "simulated_benchmark"},
            }
        )
