"""Transport-free standalone package and public role-contract checks."""

import hashlib
import importlib.util
import json
from pathlib import Path

from marketdata import __version__
from marketdata.api import create_app as read_app
from marketdata.config import Settings
from marketdata.ingest_api import create_app as ingest_app


def test_no_collector_or_concrete_provider_shipped():
    assert importlib.util.find_spec("marketdata.collector") is None
    assert importlib.util.find_spec("marketdata.providers.tradingview") is None
    assert importlib.util.find_spec("marketdata.source_capture") is None
    assert __version__ == json.loads(Path("RELEASE.json").read_text())["version"]


def test_separate_http_contracts_without_database_or_transport():
    read = read_app(Settings(_env_file=None, application_role="api"))
    ingest = ingest_app(
        Settings(
            _env_file=None,
            application_role="ingest",
            ingest_credentials_file=Path("not-opened.json"),
        )
    )
    assert "/api/v1/candles" in read.openapi()["paths"]
    assert not any(p.startswith("/api/ingest/") for p in read.openapi()["paths"])
    assert "/api/ingest/v1/session/recover" in ingest.openapi()["paths"]
    assert not any(p.startswith("/api/v1/") for p in ingest.openapi()["paths"])
    assert read.openapi()["info"]["version"] == ingest.openapi()["info"]["version"] == __version__


def test_complete_file_manifest():
    for line in Path("SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == digest, name
