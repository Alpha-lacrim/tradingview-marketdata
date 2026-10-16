from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from pydantic import TypeAdapter

from marketdata.domain import Instrument, InstrumentType, ProviderInstrument, RegistryEntry, utc


def parse_contract_month(code: str) -> tuple[str, int, int]:
    """Parse unambiguous CME-style month codes with FOUR digit years only.

    One/two digit provider years must be resolved through authoritative metadata.
    This helper is never used to invent expiry or a provider symbol.
    """
    match = re.fullmatch(r"([A-Z0-9]+?)([FGHJKMNQUVXZ])(\d{4})", code)
    if match is None:
        raise ValueError("expected root + month letter + four-digit year")
    root, month, year = match.groups()
    return root, int(year), "FGHJKMNQUVXZ".index(month) + 1


class Registry:
    def __init__(self, entries: list[RegistryEntry]):
        self.entries = entries
        self.by_symbol: dict[str, Instrument] = {}
        self.by_provider: dict[tuple[str, str], tuple[Instrument, ProviderInstrument]] = {}
        for entry in entries:
            instrument = entry.instrument
            if instrument.canonical_symbol in self.by_symbol:
                raise ValueError("duplicate canonical instrument")
            self.by_symbol[instrument.canonical_symbol] = instrument
            seen: set[str] = set()
            for mapping in entry.mappings:
                key = mapping.provider, mapping.provider_symbol
                if key in self.by_provider or mapping.provider in seen:
                    raise ValueError("ambiguous provider instrument mapping")
                seen.add(mapping.provider)
                self.by_provider[key] = instrument, mapping

    @classmethod
    def load(cls, path: Path) -> Registry:
        return cls(
            TypeAdapter(list[RegistryEntry]).validate_python(json.loads(path.read_text("utf-8")))
        )

    def subscriptions(self, provider: str) -> list[tuple[Instrument, ProviderInstrument]]:
        return [pair for (name, _), pair in self.by_provider.items() if name == provider]

    def active_contracts(self, exchange: str, root: str, at: datetime) -> list[Instrument]:
        at = utc(at)
        return sorted(
            (
                i
                for i in self.by_symbol.values()
                if i.instrument_type == InstrumentType.FUTURE
                and i.active
                and i.exchange == exchange
                and i.root_symbol == root
                and i.expiry is not None
                and i.expiry > at
            ),
            key=lambda i: (i.contract_month, i.canonical_symbol),
        )

    def resolve(
        self, exchange: str, root: str, at: datetime, *, next_contract: bool = False
    ) -> Instrument:
        contracts = self.active_contracts(exchange, root, at)
        # Explicit roll schedules are required; expiry is never an implicit roll policy.
        if any(i.roll_at is None for i in contracts):
            raise ValueError("front/next resolution requires explicit roll_at for all candidates")
        candidates = [i for i in contracts if i.roll_at is not None and i.roll_at > utc(at)]
        index = 1 if next_contract else 0
        if len(candidates) <= index:
            raise LookupError("no eligible concrete contract")
        return candidates[index]
