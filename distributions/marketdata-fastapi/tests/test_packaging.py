"""Portable snapshot/contract checks requiring no database or parent project."""

import hashlib
import importlib.util
import json
from pathlib import Path

import marketdata
from marketdata.api import create_app
from marketdata.config import Settings

ROOT = Path(__file__).resolve().parents[1]


def test_local_package_and_no_collector_or_concrete_transports():
    assert Path(marketdata.__file__).resolve().is_relative_to(ROOT)
    for module in (
        "marketdata.collector",
        "marketdata.providers.factory",
        "marketdata.providers.tradingview",
        "marketdata.providers.binance",
    ):
        assert importlib.util.find_spec(module) is None


def test_openapi_contract_equals_verified_snapshot():
    assert create_app(Settings(_env_file=None)).openapi() == json.loads(
        (ROOT / "docs/openapi.json").read_text(encoding="utf-8")
    )


def test_exported_source_hashes():
    manifest = json.loads((ROOT / "SOURCE_MANIFEST.json").read_text(encoding="utf-8"))
    for path, evidence in manifest["files"].items():
        content = (ROOT / path).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == evidence["sha256"], path
