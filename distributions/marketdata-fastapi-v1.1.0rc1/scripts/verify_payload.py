"""Verify an extracted payload before dependency installation; standard library only."""

import hashlib
import json
from pathlib import Path


def main():
    root = Path.cwd().resolve()
    count = 0
    for line in (root / "SHA256SUMS").read_text("utf-8").splitlines():
        digest, name = line.split("  ", 1)
        target = (root / name).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            raise ValueError("manifest path escapes payload or is missing")
        if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise ValueError("payload checksum mismatch: " + name)
        count += 1
    meta = json.loads((root / "RELEASE.json").read_text("utf-8"))
    if meta["component"] == "company":
        assert meta["destination_head"] == "0010"
        assert not (root / "source_migrations").exists()
        assert not (root / "src/marketdata/collector.py").exists()
    else:
        assert meta["source_base"] == "0006" and meta["source_extension"] == "sc0002"
        assert not list((root / "migrations/versions").glob("0007*"))
    print(
        json.dumps(dict(files_verified=count, version=meta["version"], component=meta["component"]))
    )


if __name__ == "__main__":
    main()
