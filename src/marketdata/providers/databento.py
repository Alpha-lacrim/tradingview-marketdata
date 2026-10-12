from __future__ import annotations

import asyncio
import logging
import queue
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from typing import Any

from marketdata.config import Settings
from marketdata.domain import (
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
UNDEF_PRICE = 9223372036854775807


def fixed_price(value: int) -> Decimal | None:
    return None if value == UNDEF_PRICE else Decimal(value) / Decimal(1000000000)


def parse_mbp1(
    record: Any, instrument: Instrument, mapping: ProviderInstrument, received: datetime
) -> Tick:
    level = record.levels[0]
    bid, ask = fixed_price(level.bid_px), fixed_price(level.ask_px)
    trade = str(record.action) == "T"
    return normalize(
        instrument,
        mapping,
        received,
        event_timestamp=from_ns(record.ts_event),
        event_timestamp_ns=record.ts_event,
        bid=bid,
        ask=ask,
        bid_size=Decimal(level.bid_sz) if bid is not None else None,
        ask_size=Decimal(level.ask_sz) if ask is not None else None,
        last=fixed_price(record.price) if trade else None,
        last_size=Decimal(record.size) if trade else None,
        sequence_number=record.sequence,
        metadata={
            "dataset": mapping.metadata["dataset"],
            "schema": "mbp-1",
            "publisher_id": record.publisher_id,
            "upstream_instrument_id": record.instrument_id,
            "upstream_received_ns": record.ts_recv,
            "flags": record.flags,
            "action": str(record.action),
            "sequence_scope": "venue_filtered",
        },
    )


class DatabentoProvider(MarketDataProvider):
    """One session per dataset, bounded SDK callback bridge, raw concrete contracts.

    The SDK callback runs on its own thread. Never schedule a callback per tick
    onto asyncio's unbounded ready queue. Drain this fixed-size queue instead.
    """

    def __init__(self, settings: Settings, metrics: Metrics, dataset: str):
        super().__init__("databento_" + dataset.lower().replace(".", "_"), metrics)
        self.settings = settings
        self.dataset = dataset
        self.client: Any = None
        self.records: queue.Queue[tuple[Any, datetime]] = queue.Queue(
            maxsize=settings.queue_capacity
        )
        self.callback_error: str | None = None
        self.verified: set[int] = set()

    def on_record(self, record: Any) -> None:
        try:
            self.records.put_nowait((record, utc_now()))
        except queue.Full:
            self.metrics.dropped.labels(self.name, "sdk_bridge_full").inc()
            self.callback_error = "sdk_bridge_full"

    def on_exception(self, error: Exception) -> None:
        # The exception text may contain credentials. Expose its category only.
        self.callback_error = type(error).__name__

    async def connect(self) -> None:
        try:
            import databento as db
        except ImportError:
            raise PermanentProviderError("install_databento_extra") from None
        token = self.settings.databento_api_key
        if token is None or not token.get_secret_value():
            raise PermanentProviderError("databento_credentials_required")
        for i, m in self.subscriptions:
            if (
                i.instrument_type != InstrumentType.FUTURE
                or not i.tick_size
                or not i.multiplier
                or m.metadata.get("dataset") != self.dataset
                or not isinstance(m.metadata.get("instrument_id"), int)
                or not m.metadata.get("definition_source")
                or m.provider_symbol.endswith(".FUT")
                or ".c." in m.provider_symbol
                or ".v." in m.provider_symbol
            ):
                raise PermanentProviderError("concrete_verified_futures_metadata_required")
        self.records = queue.Queue(maxsize=self.settings.queue_capacity)
        self.verified.clear()
        self.callback_error = None
        self.client = db.Live(
            key=token.get_secret_value(),
            heartbeat_interval_s=5,
            reconnect_policy="none",
            slow_reader_behavior="warn",
        )
        self.client.add_callback(self.on_record, self.on_exception)
        symbols = [m.provider_symbol for _, m in self.subscriptions]
        try:
            # Definition replay is official; ticks remain live only. No automatic paid
            # historical API calls or continuous-contract subscriptions are made.
            await asyncio.to_thread(
                self.client.subscribe,
                dataset=self.dataset,
                schema="definition",
                stype_in="raw_symbol",
                symbols=symbols,
                start=0,
            )
            await asyncio.to_thread(
                self.client.subscribe,
                dataset=self.dataset,
                schema="mbp-1",
                stype_in="raw_symbol",
                symbols=symbols,
            )
            self.client.start()
        except ValueError:
            raise PermanentProviderError("databento_authentication_or_configuration") from None

    async def disconnect(self) -> None:
        if self.client is not None:
            self.client.terminate()
            try:
                await self.client.wait_for_close(timeout=5)
            except Exception:
                logger.warning(
                    "SDK close ended with an error", extra={"context": {"provider": self.name}}
                )
            self.client = None
        discarded = 0
        while not self.records.empty():
            record, _ = self.records.get_nowait()
            if hasattr(record, "levels"):
                discarded += 1
        if discarded:
            self.metrics.dropped.labels(self.name, "sdk_bridge_discarded").inc(discarded)
            logger.error(
                "unprocessed SDK observations discarded",
                extra={"context": {"provider": self.name, "count": discarded}},
            )

    async def stream_ticks(self) -> AsyncIterator[Tick]:
        import databento as db

        mappings = {m.metadata["instrument_id"]: (i, m) for i, m in self.subscriptions}
        while self.client is not None:
            if self.callback_error:
                raise ConnectionError(self.callback_error)
            try:
                record, received = self.records.get_nowait()
            except queue.Empty:
                if not self.client.is_connected():
                    # Includes authentication/entitlement error responses not emitted as records.
                    try:
                        await self.client.wait_for_close(timeout=1)
                    except Exception:
                        # ErrorMsg records were handled above. A bare socket close is
                        # transient even though the SDK reports it with an exception.
                        raise ConnectionError("databento_session_disconnected") from None
                    raise ConnectionError("databento_disconnected") from None
                await asyncio.sleep(0.001)
                continue
            self.message_received(received)
            if isinstance(record, db.ErrorMsg):
                # Official guidance: error disconnects require operator intervention.
                raise PermanentProviderError(f"databento_error_code_{record.code}")
            if isinstance(record, db.SystemMsg):
                if record.code == 2:
                    self.health.state = "degraded"
                    logger.warning(
                        "Databento slow reader warning", extra={"context": {"provider": self.name}}
                    )
                continue
            if isinstance(record, db.InstrumentDefMsg) and record.instrument_id in mappings:
                instrument, mapping = mappings[record.instrument_id]
                if instrument.contract_month is None:
                    raise PermanentProviderError("databento_missing_maturity")
                if (
                    record.raw_symbol != mapping.provider_symbol
                    or str(record.instrument_class) != "F"
                    or record.leg_count != 0
                    or record.asset != instrument.root_symbol
                    or record.exchange != instrument.exchange
                    or record.currency != instrument.currency
                    or record.maturity_year != instrument.contract_month.year
                    or record.maturity_month != instrument.contract_month.month
                    or from_ns(record.expiration) != instrument.expiry
                    or fixed_price(record.min_price_increment) != instrument.tick_size
                    or fixed_price(record.unit_of_measure_qty) != instrument.multiplier
                ):
                    raise PermanentProviderError("databento_definition_mismatch")
                self.verified.add(record.instrument_id)
                continue
            if not isinstance(record, db.MBP1Msg):
                continue
            if record.instrument_id not in mappings:
                raise PermanentProviderError("databento_unexpected_contract_id")
            if record.instrument_id not in self.verified or record.flags & 4:
                reason = (
                    "unverified_definition"
                    if record.instrument_id not in self.verified
                    else "upstream_gap"
                )
                self.metrics.dropped.labels(self.name, reason).inc()
                if reason == "upstream_gap":
                    self.metrics.sequence_gaps.labels(self.name).inc()
                logger.error(
                    "futures observation rejected",
                    extra={"context": {"provider": self.name, "reason": reason}},
                )
                continue
            try:
                instrument, mapping = mappings[record.instrument_id]
                yield parse_mbp1(record, instrument, mapping, received)
            except (KeyError, TypeError, ValueError, ArithmeticError):
                self.malformed()
