"""Reproduce the frozen 100-source registry from retained prequalification evidence.

This offline command makes no source requests and never changes database activation.
It refuses to overwrite a different universe or select sources from a later test window.
"""

import argparse
import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path


def reproduce(evidence_path, output):
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    start, end = (datetime.fromisoformat(evidence[k]) for k in ("start", "end"))
    if start.utcoffset() != timedelta(0) or end - start != timedelta(minutes=60):
        raise ValueError("expected the frozen completed 60-minute UTC selection window")
    excluded = evidence["approved_inactive_source"]
    if (
        excluded["symbol"] != "CRYPTO:OKX:XAUTUSDT"
        or not excluded["history_and_identity_preserved"]
    ):
        raise ValueError("only the operator-approved XAUT inactive slot may be replaced")
    original = json.loads(Path("config/instruments.tradingview-pilot.json").read_text())
    candidates = json.loads(Path("config/instruments.tradingview-128.json").read_text())
    by_symbol = {entry["instrument"]["canonical_symbol"]: entry for entry in candidates}
    result = [
        entry for entry in original if entry["instrument"]["canonical_symbol"] != excluded["symbol"]
    ]
    selected = evidence["selected_additions"]
    if len(result) != 33 or len(selected) != 67 or any(row["native"] != 60 for row in selected):
        raise ValueError("require 33 preserved originals and 67 prequalified additions")
    for row in selected:
        entry = by_symbol[row["canonical_symbol"]]
        if entry["mappings"][0]["provider_symbol"] != row["provider_symbol"]:
            raise ValueError("source identity differs from frozen evidence")
        result.append(entry)
    if len({entry["instrument"]["canonical_symbol"] for entry in result}) != 100:
        raise ValueError("duplicate source in frozen universe")
    # Preserve the exact Windows-captured registry bytes, including CRLF, on any host.
    serialized = (json.dumps(result, indent=2) + "\n").replace("\n", "\r\n").encode()
    digest = hashlib.sha256(serialized).hexdigest()
    if digest != evidence["registry_sha256"]:
        raise ValueError("reproduced registry hash differs from frozen prequalification registry")
    if output.exists() and output.read_bytes() != serialized:
        raise ValueError("refusing to overwrite a different registry")
    output.write_bytes(serialized)
    print(json.dumps(dict(instruments=100, registry=str(output), sha256=digest)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, default=Path("docs/ordinary-100-selection.json"))
    parser.add_argument(
        "--output", type=Path, default=Path("config/instruments.tradingview-100.json")
    )
    args = parser.parse_args()
    reproduce(args.evidence, args.output)
