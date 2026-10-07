"""Owned single-reader guest chart pipeline with a bounded sliding request window.

One series per temporary chart session. Shared-session multi-series was rejected live;
no alternative connections or access workaround is attempted here.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from websockets.exceptions import ConnectionClosed

from marketdata.candles import MarketCandle
from marketdata.domain import from_ns, utc_now
from marketdata.providers.base import PermanentConnectionError, PermanentProviderError, Subscription
from marketdata.providers.tradingview import encode_message, observed_decimal
from marketdata.providers.tradingview_candles import parse_bar, validate_source

if TYPE_CHECKING:
    from marketdata.providers.tradingview_candles import TradingViewCandleProvider

logger = logging.getLogger(__name__)
Result = list[MarketCandle] | Exception
Job = tuple[Subscription, datetime, datetime, int]


@dataclass
class Request:
    pair: Subscription
    start: datetime
    end: datetime
    count: int
    future: asyncio.Future[Result]
    started: datetime = field(default_factory=utc_now)
    timer: float = field(default_factory=time.perf_counter)
    info: dict[str, Any] = field(default_factory=dict)
    bars: dict[Decimal, list[Any]] = field(default_factory=dict)
    first: datetime | None = None
    available: datetime | None = None
    parsed: datetime | None = None
    finished_timer: float | None = None


async def retrieve(p: TradingViewCandleProvider, jobs: list[Job]) -> dict[str, Result]:
    socket, generation, decoder = p.socket, p.connection_generation, p.decoder
    if socket is None:
        raise ConnectionError("candle_transport_disconnected")
    symbols = [pair[0].canonical_symbol for pair, _, _, _ in jobs]
    if len(symbols) != len(set(symbols)) or len(jobs) > 100:
        raise ValueError("unique candle jobs required; local cap is 100")
    if any(
        end <= start
        or end - start > timedelta(minutes=p.settings.candle_backfill_minutes)
        or not 1 <= count <= p.settings.candle_backfill_minutes + 1
        or pair[0].canonical_symbol not in p.health.symbols
        for pair, start, end, count in jobs
    ):
        raise ValueError("candle jobs exceed configured history/identity bounds")
    if p.fetching:
        raise RuntimeError("candle retrieval already in progress")
    p.fetching = True
    p.timings.clear()
    active: dict[str, Request] = {}
    results: dict[str, Result] = {}
    waiting = iter(jobs)
    timeout = p.settings.candle_request_timeout_seconds
    window = p.effective_concurrency
    stats: dict[str, Any] = dict(
        connection_generation=generation,
        window=window,
        peak_inflight=0,
        strategy="per_request",
        sockets=1,
        jobs=len(jobs),
        timeouts=0,
        ignored_responses=0,
        errors={},
    )
    p.health.candle_retrieval = stats
    p.metrics.candle_window.labels(p.name).set(window)
    p.metrics.candle_instrument_count.labels(p.name).set(len(jobs))

    def error_seen(error: Exception) -> None:
        category = (
            "timeout"
            if isinstance(error, TimeoutError)
            else "protocol"
            if isinstance(error, PermanentConnectionError)
            else "rejected"
            if isinstance(error, PermanentProviderError)
            else "malformed"
            if isinstance(error, (ValueError, KeyError, TypeError, IndexError))
            else "connection"
        )
        stats["errors"][category] = stats["errors"].get(category, 0) + 1
        p.metrics.candle_request_errors.labels(category).inc()
        if category == "timeout":
            stats["timeouts"] += 1
            p.metrics.candle_request_timeouts.labels(p.name).inc()
        if category in ("timeout", "protocol", "connection"):
            p.metrics.tradingview_throttle.labels(category).inc()
            # Static configured optimum, decrease-only on overload symptoms. Never evade/retry
            # on extra sockets. Operator restart/configuration is required to increase again.
            p.effective_concurrency = max(1, min(p.effective_concurrency, window // 2))

    def remember(state: Request, result: Result) -> None:
        symbol = state.pair[0].canonical_symbol
        parsed = state.parsed or utc_now()
        duration = max(0, (state.finished_timer or time.perf_counter()) - state.timer)
        p.timings[symbol] = dict(
            requested_start=state.start.isoformat(),
            requested_end=state.end.isoformat(),
            requested_bars=state.count,
            request_started_at=state.started.isoformat(),
            first_relevant_message_at=state.first.isoformat() if state.first else None,
            completed_candle_available_at=state.available.isoformat() if state.available else None,
            parser_completed_at=parsed.isoformat(),
            duration_seconds=duration,
        )
        item = p.health.symbols[symbol]
        item.last_fetch_completed_at = parsed
        if isinstance(result, Exception):
            error_seen(result)
        else:
            item.last_successful_fetch_at = parsed
        results[symbol] = result
        p.metrics.candle_symbol_latency.labels(p.name, symbol).observe(duration)
        if duration > 5:
            logger.warning(
                "slow candle retrieval",
                extra={
                    "context": dict(
                        provider=p.name,
                        canonical_symbol=symbol,
                        duration=duration,
                        window=window,
                    )
                },
            )

    def current_generation() -> None:
        if p.socket is not socket or p.connection_generation != generation:
            raise ConnectionError("stale_candle_connection_generation")

    async def send(method: str, params: list[Any]) -> None:
        current_generation()
        async with asyncio.timeout(timeout):
            await socket.send(encode_message(method, params))

    async def dispatch() -> bool:
        job = next(waiting, None)
        if job is None:
            return False
        pair, start, end, count = job
        p.request_serial += 1
        # Identifiers are client-assigned in the observed serializer. Never reused across
        # batches or connection generations; even a delayed old frame cannot route to a new job.
        session = f"cs_{generation:04x}{p.request_serial:08x}"
        state = Request(pair, start, end, count, asyncio.get_running_loop().create_future())
        active[session] = state
        p.health.symbols[pair[0].canonical_symbol].last_fetch_started_at = state.started
        stats["peak_inflight"] = max(stats["peak_inflight"], len(active))
        p.metrics.candle_inflight.labels(p.name).set(len(active))
        p.metrics.candles.labels(p.name, "requested").inc()
        async with asyncio.timeout(timeout):
            await send("chart_create_session", [session, ""])
            await send("resolve_symbol", [session, "symbol_1", pair[1].provider_symbol])
            await send("create_series", [session, "s1", "s1", "symbol_1", "1", count, ""])
        return True

    async def receive() -> None:
        while True:
            current_generation()
            raw = await socket.recv()
            current_generation()
            received = utc_now()
            p.health.last_message_at = received
            for body in decoder.feed(raw):  # type: ignore[arg-type]
                if body.startswith("~h~"):
                    await socket.send(f"~m~{len(body)}~m~{body}")
                    continue
                message = json.loads(body, parse_float=Decimal)
                if not isinstance(message, dict):
                    raise ValueError("chart message must be an object")
                kind, params = message.get("m"), message.get("p", [])
                if kind in ("critical_error", "protocol_error"):
                    raise PermanentConnectionError("tradingview_chart_protocol_rejected")
                state = active.get(params[0]) if isinstance(params, list) and params else None
                if state is None or state.future.done():
                    stats["ignored_responses"] += 1
                    p.metrics.candle_ignored.labels(
                        "unmatched" if state is None else "duplicate"
                    ).inc()
                    continue
                state.first = state.first or received
                try:
                    if time.perf_counter() - state.timer >= timeout:
                        raise TimeoutError("candle_request_timeout")
                    if kind in ("symbol_error", "series_error"):
                        raise PermanentProviderError("tradingview_chart_unavailable")
                    if kind == "symbol_resolved":
                        if params[1] != "symbol_1":
                            raise ValueError("unexpected resolved symbol identifier")
                        state.info.update(params[2])
                        validate_source(state.info, state.pair)
                        info = {
                            k: state.info[k]
                            for k in (
                                "session",
                                "timezone",
                                "session_holidays",
                                "corrections",
                                "subsessions",
                                "subsession_id",
                            )
                            if k in state.info
                        }
                        source = state.pair[1].provider_symbol
                        if source.startswith("OANDA:"):
                            info["uncertain_break"] = (
                                "1659-1805"
                                if source in ("OANDA:XAUUSD", "OANDA:XAGUSD")
                                else "1659-1705"
                            )
                        p.session_info[state.pair[0].canonical_symbol] = info
                    if kind in ("timescale_update", "du"):
                        series = params[1]
                        if set(series) - {"s1"}:
                            raise ValueError("unexpected chart series identifier")
                        for row in series.get("s1", {}).get("s", []):
                            values = row["v"]
                            key = observed_decimal(values[0])
                            if key is None:
                                raise ValueError("missing bar time")
                            state.bars[key] = values
                            if key == Decimal(int(state.end.timestamp())) - 60:
                                state.available = received
                        if len(state.bars) > state.count + 2:
                            raise ValueError("candle response exceeds requested bound")
                    if kind == "series_completed":
                        if params[1] != "s1" or params[3] != "s1":
                            raise ValueError("unexpected chart series/turnaround")
                        status = params[2]
                        validate_source(state.info, state.pair)
                        if status not in ("streaming", "realtime"):
                            raise PermanentProviderError("tradingview_candles_not_realtime")
                        if len(params) > 4 and isinstance(params[4], dict):
                            state.info["observed_update_period_seconds"] = str(
                                params[4].get("rt_update_period")
                            )
                        cutoff = min(state.end, received - timedelta(seconds=p.grace))
                        parse_timer = time.perf_counter()
                        result = [
                            parse_bar(v, state.pair, state.info, received, status)
                            for ts, v in sorted(state.bars.items())
                            if state.start <= from_ns(int(ts) * 1_000_000_000)
                            and from_ns(int(ts) * 1_000_000_000) + timedelta(minutes=1) <= cutoff
                        ]
                        state.parsed = utc_now()
                        state.finished_timer = time.perf_counter()
                        p.metrics.candle_timing.labels(p.name, "parser").observe(
                            max(0, time.perf_counter() - parse_timer)
                        )
                        p.metrics.candles.labels(p.name, "received").inc(len(result))
                        state.future.set_result(result)
                except (
                    ValueError,
                    KeyError,
                    TypeError,
                    IndexError,
                    TimeoutError,
                    PermanentProviderError,
                ) as error:
                    state.parsed = utc_now()
                    state.finished_timer = time.perf_counter()
                    state.future.set_result(error)

    receiver = asyncio.create_task(receive(), name="tradingview-candle-receiver")
    try:
        for _ in range(window):
            if not await dispatch():
                break
        while active:
            remaining = min(timeout - (time.perf_counter() - s.timer) for s in active.values())
            waiters: list[asyncio.Future[Any]] = [receiver, *(s.future for s in active.values())]
            await asyncio.wait(
                waiters,
                timeout=max(0, remaining),
                return_when=asyncio.FIRST_COMPLETED,
            )
            finished = [
                session
                for session, s in active.items()
                if s.future.done() or time.perf_counter() - s.timer >= timeout
            ]
            for session in finished:
                state = active.pop(session)
                result = (
                    state.future.result()
                    if state.future.done()
                    else TimeoutError("candle_request_timeout")
                )
                if not state.future.done():
                    state.future.cancel()
                remember(state, result)
                p.metrics.candle_inflight.labels(p.name).set(len(active))
                await send("chart_delete_session", [session])
            if receiver.done() and (active or len(results) < len(jobs)):
                # Save completed requests first, including completions in the last frame
                # before a disconnect. Unfinished work fails without discarding valid siblings.
                receiver.result()
                raise ConnectionError("candle_receiver_stopped")
            for _ in finished:
                if not await dispatch():
                    break
    except Exception as error:
        if isinstance(error, (ConnectionClosed, ConnectionError, OSError)):
            p.metrics.tradingview_disconnects.inc()
            p.health.last_close_code = getattr(getattr(error, "rcvd", None), "code", None)
            p.health.last_close_reason = "upstream_disconnect"
        for state in active.values():
            result = state.future.result() if state.future.done() else error
            remember(state, result)
        for symbol in symbols:
            if symbol not in results:
                results[symbol] = error
                error_seen(error)
        if isinstance(error, PermanentConnectionError):
            results = {symbol: error for symbol in symbols}
    finally:
        receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
        for state in active.values():
            state.future.cancel()
        # Bounded cleanup even on cancellation; never leave a detached receiver/future.
        try:
            async with asyncio.timeout(min(1, timeout)):
                for session in active:
                    await socket.send(encode_message("chart_delete_session", [session]))
        except (ConnectionClosed, OSError, TimeoutError):
            pass
        p.metrics.candle_inflight.labels(p.name).set(0)
        p.metrics.candle_window.labels(p.name).set(p.effective_concurrency)
        stats["next_window"] = p.effective_concurrency
        p.fetching = False
    return results
