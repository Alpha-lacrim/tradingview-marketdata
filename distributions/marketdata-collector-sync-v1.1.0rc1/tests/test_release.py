"""Portable worker/capture package, no live connection or database required."""

import hashlib
import importlib.util
import json
from pathlib import Path

from marketdata import __version__
from marketdata.sync_config import SyncConfig


def test_source_track_and_worker_are_present():
    meta = json.loads(Path("RELEASE.json").read_text())
    assert meta["version"] == __version__
    assert meta["source_base"] == "0006" and meta["source_extension"] == "sc0002"
    assert not list(Path("migrations/versions").glob("0007*"))
    assert importlib.util.find_spec("marketdata.sync_worker") is not None
    assert importlib.util.find_spec("marketdata.collector") is not None
    assert SyncConfig.model_fields["metadata_max_records"].default == 50


def test_complete_file_manifest():
    for line in Path("SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest, name
