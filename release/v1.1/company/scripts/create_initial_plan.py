"""Write a new one-instrument administrator-reviewed initial ownership plan, never apply it."""

import argparse
import json
import os
from pathlib import Path
from uuid import UUID, uuid4

from marketdata.registry import Registry


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry", type=Path, required=True)
    p.add_argument("--source-inventory", type=Path, required=True)
    p.add_argument("--instrument", required=True)
    p.add_argument("--provider", required=True)
    p.add_argument("--producer-id", type=UUID, required=True)
    p.add_argument("--key-id", required=True)
    p.add_argument(
        "--statuses", required=True, help="explicit approved comma-separated data statuses"
    )
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    inventory = json.loads(args.source_inventory.read_text("utf-8"))
    if inventory["base"] != "0006" or inventory["source_extension"] != "sc0002":
        p.error("verified current source inventory required")
    epoch = UUID(inventory["source_epoch"])
    matches = [
        (i, m)
        for i, m in Registry.load(args.registry).subscriptions(args.provider)
        if i.canonical_symbol == args.instrument
    ]
    if len(matches) != 1:
        p.error("exact authorized instrument/provider mapping required")
    instrument, mapping = matches[0]
    value = dict(
        producer_id=str(args.producer_id),
        label="Reviewed initial collector installation",
        key_id=args.key_id,
        session_id=str(uuid4()),
        allowed_statuses=args.statuses.split(","),
        streams=[
            dict(
                instrument_id=str(instrument.id),
                provider=mapping.provider,
                stream=s,
                source_epoch=str(epoch),
                ownership_generation=1,
            )
            for s in ["candles", "quotes", "coverage", "status"]
        ],
    )
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        file.write(json.dumps(value, indent=2) + "\n")
    print(
        "Wrote initial plan. Review identity/status permissions before separate DBA provisioning."
    )


if __name__ == "__main__":
    main()
