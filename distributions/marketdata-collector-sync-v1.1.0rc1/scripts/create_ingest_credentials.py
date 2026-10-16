"""Create private producer bearer and server hash files without printing secrets."""

import argparse
import hashlib
import json
import os
import re
import secrets
from pathlib import Path
from uuid import UUID


def create(directory: Path, producer: UUID, key_id: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", key_id):
        raise ValueError("key ID must be 1..64 ASCII letters, digits, underscores or hyphens")
    # A fresh directory and exclusive files prevent silent credential replacement.
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    token = key_id + "." + secrets.token_urlsafe(32)
    values = {
        "ingest_token": token + "\n",
        "ingest_credentials.json": json.dumps(
            [
                dict(
                    producer_id=str(producer),
                    key_id=key_id,
                    token_sha256=hashlib.sha256(token.encode("ascii")).hexdigest(),
                )
            ],
            indent=2,
        )
        + "\n",
    }
    for name, value in values.items():
        descriptor = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            file.write(value)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--producer-id", type=UUID, required=True)
    parser.add_argument("--key-id", required=True)
    args = parser.parse_args()
    try:
        create(args.directory, args.producer_id, args.key_id)
    except (OSError, ValueError) as error:
        parser.exit(1, f"credential creation rejected ({type(error).__name__})\n")
    print(
        "Created private token and server fingerprint files. Restrict directory access before use."
    )


if __name__ == "__main__":
    main()
