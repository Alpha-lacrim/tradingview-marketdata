"""Validate and write a new worker configuration from an approved complete initial manifest."""

import argparse
import json
import os
from pathlib import Path

from marketdata.sync_config import SyncConfig


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--plan", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--endpoint", required=True)
    p.add_argument("--source-dsn-file", type=Path, required=True)
    p.add_argument("--token-file", type=Path, required=True)
    p.add_argument("--anchor-file", type=Path, required=True)
    p.add_argument("--ca-file", type=Path)
    p.add_argument("--enroll", action="store_true")
    args = p.parse_args()
    plan = json.loads(args.plan.read_text("utf-8"))
    config = SyncConfig(
        endpoint=args.endpoint,
        source_dsn_file=args.source_dsn_file.resolve(),
        token_file=args.token_file.resolve(),
        anchor_file=args.anchor_file.resolve(),
        ca_file=args.ca_file.resolve() if args.ca_file else None,
        producer_id=plan["producer_id"],
        session_id=plan["session_id"],
        session_generation=1,
        streams=plan["streams"],
        enroll=args.enroll,
    )
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        file.write(config.model_dump_json(indent=2) + "\n")
    print("Created worker config; startup checks epoch/ownership again.")


if __name__ == "__main__":
    main()
