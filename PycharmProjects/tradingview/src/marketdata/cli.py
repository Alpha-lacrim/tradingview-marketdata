"""Explicit operator-driven discovery. Never runs automatically during ingestion."""

import argparse
import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from marketdata.config import Settings
from marketdata.domain import (
    AssetClass,
    DataStatus,
    Instrument,
    InstrumentType,
    ProviderInstrument,
    RegistryEntry,
)
from marketdata.observability import Metrics
from marketdata.providers.definitions import definition_entry
from marketdata.providers.oanda import OandaProvider
from marketdata.registry import Registry


def oanda_entry(info: dict[str, Any], environment: str, cfd_asset: AssetClass) -> RegistryEntry:
    kind = info["type"]
    asset = {"CURRENCY": AssetClass.FOREX, "METAL": AssetClass.METALS, "CFD": cfd_asset}[kind]
    instrument_type = InstrumentType.CFD if kind == "CFD" else InstrumentType.OTC
    prefix = {"CURRENCY": "FX", "METAL": "SPOT", "CFD": "CFD"}[kind]
    name = info["name"]
    base, quote = name.rsplit("_", 1)
    return RegistryEntry(
        instrument=Instrument(
            canonical_symbol=f"{prefix}:OANDA:{name.replace('_', '')}",
            display_name=info["displayName"],
            asset_class=asset,
            instrument_type=instrument_type,
            exchange="OANDA",
            currency=quote,
            base_asset=base,
            quote_asset=quote,
            metadata={"oanda_type": kind, "pricing": "account_specific_otc"},
        ),
        mappings=[
            ProviderInstrument(
                provider="oanda",
                provider_symbol=name,
                realtime_status=DataStatus.SIMULATED
                if environment == "practice"
                else DataStatus.INDICATIVE,
            )
        ],
    )


async def discover_oanda(args: argparse.Namespace) -> None:
    settings = Settings()
    provider = OandaProvider(settings, Metrics())
    try:
        instruments = await provider.discover()
    finally:
        await provider.disconnect()
    if not args.output:
        # Instrument definitions only; never account details or credentials.
        print(json.dumps(instruments, indent=2))
        return
    names = set(args.symbols.split(","))
    selected = [i for i in instruments if i["name"] in names]
    if {i["name"] for i in selected} != names:
        raise ValueError("one or more requested instruments are unavailable in this account")
    entries = [
        oanda_entry(i, settings.oanda_environment, AssetClass(args.cfd_asset_class))
        for i in selected
    ]
    await asyncio.to_thread(write_registry, Path(args.output), entries)


def write_registry(path: Path, entries: list[RegistryEntry]) -> None:
    with path.open("x", encoding="utf-8") as file:
        json.dump([e.model_dump(mode="json") for e in entries], file, indent=2)
        file.write("\n")


def import_databento(args: argparse.Namespace) -> None:
    import databento as db

    store = db.DBNStore.from_file(args.file)
    dataset = store.metadata.dataset
    symbols = set(args.symbols.split(","))
    latest: dict[str, Any] = {}
    for record in store:
        if isinstance(record, db.InstrumentDefMsg) and record.raw_symbol in symbols:
            previous = latest.get(record.raw_symbol)
            if previous is None or record.ts_event >= previous.ts_event:
                latest[record.raw_symbol] = record
    if set(latest) != symbols:
        raise ValueError("requested concrete symbols were not found in the definition file")
    rolls = json.loads(Path(args.rolls).read_text("utf-8")) if args.rolls else {}
    entries = [
        definition_entry(
            record,
            dataset,
            AssetClass(args.asset_class),
            args.source,
            datetime.fromisoformat(rolls[symbol]) if symbol in rolls else None,
            DataStatus(args.status),
        )
        for symbol, record in sorted(latest.items())
    ]
    Registry(entries)  # Reject ambiguous same-root/month contracts rather than merge them.
    write_registry(Path(args.output), entries)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    oanda = commands.add_parser(
        "discover-oanda", help="list account-available instrument definitions"
    )
    oanda.add_argument("--symbols", default="", help="comma-separated names returned by discovery")
    oanda.add_argument("--output", help="write a new registry (never overwrite)")
    oanda.add_argument("--cfd-asset-class", choices=[a.value for a in AssetClass], default="energy")
    futures = commands.add_parser(
        "import-databento", help="import an already acquired definition DBN file offline"
    )
    futures.add_argument("--file", required=True, type=Path)
    futures.add_argument("--symbols", required=True, help="concrete raw symbols in the file")
    futures.add_argument("--asset-class", required=True, choices=["metals", "energy", "futures"])
    futures.add_argument(
        "--source", required=True, help="non-secret provenance, e.g. export date and dataset"
    )
    futures.add_argument(
        "--rolls", type=Path, help="JSON object: raw_symbol -> explicit aware roll_at"
    )
    futures.add_argument("--status", default="unknown", choices=["realtime", "delayed", "unknown"])
    futures.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "discover-oanda":
        asyncio.run(discover_oanda(args))
    else:
        import_databento(args)


if __name__ == "__main__":
    main()
