"""Regenerate clearly synthetic instruments. Never use these contracts for live subscriptions."""

import json
from pathlib import Path

entries = []


def add(symbol, name, asset, kind, exchange, currency, price, step, **extra):
    instrument = dict(
        canonical_symbol=symbol,
        display_name=name,
        asset_class=asset,
        instrument_type=kind,
        exchange=exchange,
        currency=currency,
        tick_size=step,
        metadata={"simulation_fixture": True},
        **extra,
    )
    entries.append(
        dict(
            instrument=instrument,
            mappings=[
                dict(
                    provider="mock",
                    provider_symbol=symbol,
                    realtime_status="simulated",
                    metadata={"simulation_initial_price": price},
                )
            ],
        )
    )


for base, price, step in [
    ("BTC", "60000", "0.01"),
    ("ETH", "3000", "0.01"),
    ("SOL", "150", "0.01"),
    ("XRP", "0.5", "0.0001"),
]:
    add(
        f"CRYPTO:{base}USDT",
        f"{base}/USDT (simulation)",
        "crypto",
        "spot",
        "BINANCE",
        "USDT",
        price,
        step,
        base_asset=base,
        quote_asset="USDT",
    )
for pair, price in [
    ("EURUSD", "1.1"),
    ("GBPUSD", "1.3"),
    ("USDJPY", "150"),
    ("USDCHF", "0.9"),
    ("AUDUSD", "0.65"),
    ("USDCAD", "1.35"),
    ("NZDUSD", "0.6"),
    ("EURGBP", "0.85"),
]:
    add(
        f"FX:SIM:{pair}",
        pair + " (simulation)",
        "forex",
        "otc",
        "SIM",
        pair[3:],
        price,
        "0.001" if pair.endswith("JPY") else "0.00001",
        base_asset=pair[:3],
        quote_asset=pair[3:],
    )
for base, price, step in [("XAU", "2000", "0.01"), ("XAG", "25", "0.001")]:
    add(
        f"SPOT:SIM:{base}USD",
        base + "/USD (simulation)",
        "metals",
        "spot",
        "SIM",
        "USD",
        price,
        step,
        base_asset=base,
        quote_asset="USD",
    )
for root, price in [("WTI", "75"), ("BRENT", "80")]:
    add(
        f"CFD:SIM:{root}USD",
        root + " CFD (simulation)",
        "energy",
        "cfd",
        "SIM",
        "USD",
        price,
        "0.01",
        base_asset=root,
        quote_asset="USD",
    )
for root, name, asset, price, step, multiplier in [
    ("GC", "Gold", "metals", "2005", "0.1", "100"),
    ("SI", "Silver", "metals", "25.1", "0.005", "5000"),
    ("CL", "WTI", "energy", "76", "0.01", "1000"),
    ("BRENT", "Brent", "energy", "81", "0.01", "1000"),
    ("ES", "E-mini S&P 500", "futures", "5000", "0.25", "50"),
    ("NQ", "E-mini Nasdaq-100", "futures", "18000", "0.25", "20"),
    ("YM", "Dow", "futures", "38000", "1", "5"),
]:
    for month, expiry, roll in [
        ("202712", "2027-12-20T00:00:00Z", "2027-12-10T00:00:00Z"),
        ("202803", "2028-03-20T00:00:00Z", "2028-03-10T00:00:00Z"),
    ]:
        add(
            f"FUTURES:SIM:{root}:{month}",
            name + " futures (simulation)",
            asset,
            "future",
            "SIM",
            "USD",
            price,
            step,
            root_symbol=root,
            contract_code=f"SIM-{root}-{month}",
            contract_month=f"{month[:4]}-{month[4:]}-01",
            expiry=expiry,
            roll_at=roll,
            multiplier=multiplier,
        )
Path("config").mkdir(exist_ok=True)
Path("config/instruments.mock.json").write_text(
    json.dumps(entries, indent=2) + "\n", encoding="utf-8"
)

live = []
for entry in entries[:4]:
    instrument = dict(entry["instrument"])
    instrument.pop("tick_size")
    instrument["metadata"] = {}
    instrument["display_name"] = instrument["base_asset"] + "/USDT Binance spot"
    live.append(
        dict(
            instrument=instrument,
            mappings=[
                dict(
                    provider="binance",
                    provider_symbol=instrument["base_asset"] + "USDT",
                    realtime_status="realtime",
                )
            ],
        )
    )
Path("config/instruments.binance.json").write_text(
    json.dumps(live, indent=2) + "\n", encoding="utf-8"
)
