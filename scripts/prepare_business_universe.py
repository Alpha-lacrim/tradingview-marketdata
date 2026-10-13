"""Freeze business candidates from captured TradingView directory/ranking responses.

Never selects on candle completeness. Never overwrites the accuracy-selected registry.
All employer-specific choices remain provisional; this command makes no network calls.
"""

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path

from marketdata.domain import RegistryEntry
from marketdata.registry import Registry

MAJORS = "EURUSD GBPUSD USDJPY USDCHF AUDUSD USDCAD NZDUSD".split()
CROSSES = (
    "EURGBP EURJPY EURCHF EURAUD EURCAD EURNZD GBPJPY GBPCHF GBPAUD GBPCAD GBPNZD "
    "AUDJPY CADJPY CHFJPY"
).split()
METALS = ["OANDA:XAUUSD", "OANDA:XAGUSD", "OANDA:XPTUSD", "OANDA:XPDUSD", "FX:XAUUSD", "FX:XAGUSD"]
ENERGY = [
    "TVC:USOIL",
    "TVC:UKOIL",
    "OANDA:NATGASUSD",
    "OANDA:WTICOUSD",
    "OANDA:BCOUSD",
    "FX:USOIL",
    "FX:UKOIL",
    "FX:NGAS",
]
FUTURES = [
    "COMEX:GC1!",
    "COMEX:SI1!",
    "NYMEX:PL1!",
    "NYMEX:PA1!",
    "NYMEX:CL1!",
    "NYMEX:BZ1!",
    "NYMEX:NG1!",
]


def freeze(ranking_path, directory_path, output):
    ranking = json.loads(ranking_path.read_text(encoding="utf-8"))
    directory = json.loads(directory_path.read_text(encoding="utf-8"))
    catalog = {r["s"]: r for r in directory["response"]["data"]}
    existing = {
        entry["mappings"][0]["provider_symbol"]: entry
        for entry in json.loads(Path("config/instruments.tradingview-256.json").read_text())
    }
    rows = ranking["response"]["data"]
    eligible = [
        r
        for r in rows
        if r["s"] == "BINANCE:" + r["d"][0]
        and r["d"][1:4] == ["BINANCE", "spot", "USDT"]
        and r["d"][4]
        and r["d"][5] is not None
        and Decimal(str(r["d"][5])) > 0
    ]
    eligible.sort(key=lambda r: (-Decimal(str(r["d"][5])), r["s"]))
    if len({r["s"] for r in eligible}) != len(eligible) or len(eligible) < 50:
        raise ValueError("insufficient or duplicate eligible TradingView Binance identities")
    proposal, candidates = [], []

    def add(source, category, main, rank=None, dollars=None, base=None):
        prefix, name = source.split(":", 1)
        broker = "FXCM" if prefix == "FX" else prefix
        old = existing.get(source)
        kind = (
            "spot"
            if category == "Binance"
            else "forex"
            if category in ("OANDA", "FXCM")
            else "commodity"
            if prefix != "TVC"
            else "cfd"
        )
        market = (
            "spot"
            if category == "Binance"
            else "otc"
            if category in ("OANDA", "FXCM", "Precious metals")
            else "reference"
            if prefix == "TVC"
            else "cfd"
        )
        currency = "USDT" if category == "Binance" else name[-3:] if kind == "forex" else "USD"
        canonical = (
            "CRYPTO:" + source
            if category == "Binance"
            else "FX:" + broker + ":" + name
            if kind == "forex"
            else "SPOT:" + broker + ":" + name
            if category == "Precious metals"
            else "REFERENCE:TVC:" + name
            if prefix == "TVC"
            else "CFD:" + broker + ":" + name
        )
        entry = old or dict(
            instrument=dict(
                canonical_symbol=canonical,
                display_name=f"{name} / {broker} via TradingView (provisional)",
                asset_class="crypto"
                if category == "Binance"
                else "forex"
                if kind == "forex"
                else "metals"
                if category == "Precious metals"
                else "energy",
                instrument_type=market,
                exchange=prefix,
                currency=currency,
                base_asset=base or (name[:3] if kind == "forex" else None),
                quote_asset=currency,
            ),
            mappings=[
                dict(
                    provider="tradingview_guest",
                    provider_symbol=source,
                    realtime_status="unknown",
                    metadata=dict(source_type=kind),
                )
            ],
        )
        proposal.append(entry)
        candidates.append(
            dict(
                asset_class=entry["instrument"]["asset_class"],
                category=category,
                tradingview_symbol=source,
                canonical_symbol=entry["instrument"]["canonical_symbol"],
                source_venue=broker,
                transport_exchange=prefix,
                market_type=entry["instrument"]["instrument_type"],
                business_priority="REQUIRED_CATEGORY_PROPOSED_MARKET",
                candidate_approval_status="PROVISIONAL",
                proposed_active=main,
                role="primary candidate"
                if main
                else "same-underlying source alternative; employer approval required",
                binance_rank=rank,
                ranking_volume_24h_usd=dollars,
                directory_identity_present=source in catalog or category == "Binance",
                native_1m_ohlc_support=None,
                latest_price_field_availability=None,
                volume_availability=None,
                volume_type=None,
                volume_units=None,
                data_status="unknown",
                expected_market_session=None,
                expected_test_minutes=None,
                immediate_candle_commits=None,
                recovered_candle_count=None,
                unresolved_expected_gaps=None,
                source_absent_observations=None,
                transport_failures=None,
                db_persistence_failures=None,
                immediate_completeness_percentage=None,
                eventual_completeness_percentage=None,
                close_to_commit_latency=None,
                qualification_status="NOT_TESTED",
            )
        )

    for rank, row in enumerate(eligible[:50], 1):
        add(row["s"], "Binance", True, rank, str(row["d"][5]), row["d"][4])
    for broker, category in [("OANDA", "OANDA"), ("FX", "FXCM")]:
        for name in MAJORS + CROSSES:
            source = broker + ":" + name
            if source not in catalog:
                raise ValueError("forex directory identity not confirmed: " + source)
            add(source, category, True)
    for source in METALS:
        add(source, "Precious metals", source.startswith("OANDA:"))
    for source in ENERGY:
        add(source, "Energy", source in ("TVC:USOIL", "TVC:UKOIL", "OANDA:NATGASUSD"))
    Registry.load(Path("config/instruments.tradingview-100.json"))  # Original baseline untouched.
    Registry([RegistryEntry.model_validate(e) for e in proposal])
    data = dict(
        status="PROVISIONAL_NOT_APPROVED_NOT_DEPLOYED",
        ranking_timestamp=ranking["captured_at"],
        ranking_source=ranking["url"],
        ranking_field="24h_vol|5",
        ranking_units="USD",
        ranking_definition=(
            "Top 50 listed Binance spot USDT pairs by TradingView rolling 24h USD volume, "
            "excluding null/nonpositive volume; raw USDT quote-volume ordering and exchange "
            "trading eligibility not independently established"
        ),
        ranking_definition_approval="PROVISIONAL_DEVIATION_FROM_RAW_USDT_QUOTE_VOLUME",
        ranking_input_sha256=hashlib.sha256(ranking_path.read_bytes()).hexdigest(),
        directory_input_sha256=hashlib.sha256(directory_path.read_bytes()).hexdigest(),
        majors=MAJORS,
        crosses=CROSSES,
        candidates=candidates,
        futures_alternatives=[
            dict(
                tradingview_symbol=s,
                asset_class="metals" if i < 4 else "energy",
                source_venue=s.split(":")[0],
                market_type="continuous futures alias",
                candidate_approval_status="PROVISIONAL_ALTERNATIVE_ONLY",
                proposed_active=False,
                qualification_status="NOT_TESTED",
                native_1m_ohlc_support=None,
                data_status="unknown",
                volume_availability=None,
                volume_type=None,
                volume_units=None,
                anticipated_volume_basis=(
                    "contract turnover if verified; not physical spot turnover"
                ),
                current_adapter_supported=False,
                blockers=[
                    "Continuous alias is not a concrete persistable futures identity",
                    "Current TradingView adapter rejects futures and aliases",
                    "Contract definition/expiry, rollover construction and authorized real-time "
                    "entitlements must be verified before use",
                ],
            )
            for i, s in enumerate(FUTURES)
        ],
    )
    output.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    Path("config/instruments.tradingview-business-candidates.json").write_text(
        json.dumps(proposal, indent=2) + "\n", encoding="utf-8"
    )
    active = {r["tradingview_symbol"] for r in candidates if r["proposed_active"]}
    Path("config/instruments.tradingview-business-proposal.json").write_text(
        json.dumps([e for e in proposal if e["mappings"][0]["provider_symbol"] in active], indent=2)
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            dict(
                candidates=len(candidates),
                proposed_active=len(active),
                binance=50,
                forex=42,
                futures_aliases_excluded=7,
            )
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ranking",
        type=Path,
        default=Path("docs/business-ranking-snapshot.json"),
    )
    parser.add_argument(
        "--directory", type=Path, default=Path("docs/business-directory-snapshot.json")
    )
    parser.add_argument("--output", type=Path, default=Path("docs/business-universe-proposal.json"))
    args = parser.parse_args()
    freeze(args.ranking, args.directory, args.output)
