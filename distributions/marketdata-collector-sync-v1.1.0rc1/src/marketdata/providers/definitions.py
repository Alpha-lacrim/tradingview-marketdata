"""Authoritative Databento definition import; never infer maturity from expiry."""

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from marketdata.domain import (
    AssetClass,
    DataStatus,
    Instrument,
    InstrumentType,
    ProviderInstrument,
    RegistryEntry,
    from_ns,
)
from marketdata.providers.databento import fixed_price


def definition_entry(
    record: Any,
    dataset: str,
    asset_class: AssetClass,
    source: str,
    roll_at: datetime | None = None,
    status: DataStatus = DataStatus.UNKNOWN,
) -> RegistryEntry:
    if str(record.instrument_class) != "F" or record.leg_count != 0:
        raise ValueError("only outright exchange-traded futures definitions are supported")
    tick_size = fixed_price(record.min_price_increment)
    multiplier = fixed_price(record.unit_of_measure_qty)
    if not tick_size or tick_size <= 0 or not multiplier or multiplier <= 0:
        raise ValueError("authoritative constant tick size and contract size required")
    if not record.exchange or not record.currency or not record.asset or not source:
        raise ValueError("exchange, currency, root and definition provenance required")
    month = date(record.maturity_year, record.maturity_month, 1)
    instrument = Instrument(
        canonical_symbol=f"FUTURES:{record.exchange}:{record.asset}:{month:%Y%m}",
        display_name=record.raw_symbol,
        asset_class=asset_class,
        instrument_type=InstrumentType.FUTURE,
        exchange=record.exchange,
        currency=record.currency,
        root_symbol=record.asset,
        contract_code=record.raw_symbol,
        contract_month=month,
        expiry=from_ns(record.expiration),
        roll_at=roll_at,
        tick_size=Decimal(tick_size),
        multiplier=Decimal(multiplier),
        metadata={
            "unit_of_measure": record.unit_of_measure,
            "definition_source": source,
            "definition_event_ns": record.ts_event,
        },
    )
    mapping = ProviderInstrument(
        provider="databento",
        provider_symbol=record.raw_symbol,
        realtime_status=status,
        metadata={
            "dataset": dataset,
            "instrument_id": record.instrument_id,
            "definition_source": source,
            "publisher_id": record.publisher_id,
        },
    )
    return RegistryEntry(instrument=instrument, mappings=[mapping])
