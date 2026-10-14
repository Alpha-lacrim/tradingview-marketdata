"""Export/check the portable API snapshot without changing the parent implementation."""

import argparse
import ast
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "distributions" / "marketdata-fastapi"
MODULES = (
    "__init__ api api_models api_v1 candles config domain events fanout market_stream "
    "observability persistence price_selection production registry sample_data sessions ws_models "
    "ingestion candle_ingestion providers/__init__ providers/base providers/candles "
    "providers/candle_pool"
).split()
TESTS = (
    "conftest test_domain test_api_v1 test_market_stream test_market_permissions_migrations "
    "test_standalone_api test_browser_example test_company_bff_example"
).split()


def digest(content: bytes) -> str:
    return hashlib.sha256(content.replace(b"\r\n", b"\n")).hexdigest()


def extract(path: str, name: str) -> str:
    source = (ROOT / path).read_text(encoding="utf-8")
    for node in ast.parse(source).body:
        if getattr(node, "name", None) == name or (
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == name for target in node.targets)
        ):
            result = ast.get_source_segment(source, node)
            assert result is not None
            return result
    raise ValueError(f"missing exported helper: {path}:{name}")


def format_test(path: str, source: str) -> str:
    for arguments in (
        ["check", "--select", "I", "--fix", "--stdin-filename", path, "-"],
        ["format", "--stdin-filename", path, "-"],
    ):
        result = subprocess.run(
            [sys.executable, "-m", "ruff", *arguments],
            input=source,
            capture_output=True,
            text=True,
            check=True,
        )
        source = result.stdout
    return source


def export(check: bool) -> None:
    files: dict[str, tuple[list[str], bytes, str]] = {}

    def copy(source: str, target: str | None = None) -> None:
        files[target or source] = ([source], (ROOT / source).read_bytes(), "verbatim")

    for module in MODULES:
        copy(f"src/marketdata/{module}.py")
    for path in sorted((ROOT / "migrations").rglob("*.py")):
        copy(path.relative_to(ROOT).as_posix())
    for path in (
        "alembic.ini",
        "docs/ws-marketdata.schema.json",
        "examples/browser-marketdata.js",
        "examples/company_bff.py",
        "tests/browser-marketdata.test.mjs",
        "config/instruments.mock.json",
    ):
        copy(path)
    copy("Dockerfile.api", "Dockerfile")
    copy(".env.api.example", ".env.example")
    compose = (ROOT / "compose.api.yaml").read_text(encoding="utf-8")
    compose = compose.replace("Dockerfile.api", "Dockerfile").replace("55914", "55915")
    compose = compose.replace("8005", "8015")
    files["compose.yaml"] = (["compose.api.yaml"], compose.encode(), "portable filename/ports")

    for test in TESTS:
        path = f"tests/{test}.py"
        source = (ROOT / path).read_text(encoding="utf-8")
        source = source.replace(
            "from test_native_candles import make_candle", "from helpers import make_candle"
        )
        source = source.replace(
            "from test_integration_compatibility import snapshot", "from helpers import snapshot"
        )
        source = source.replace(
            "from test_process_separation import forbidden", "from helpers import forbidden"
        )
        if test == "test_api_v1":
            source = source.replace(
                "import asyncio\n", "import asyncio\nimport importlib.util\n", 1
            )
            source = source.replace(
                '    monkeypatch.setattr("marketdata.collector.build_providers", forbidden)',
                '    assert importlib.util.find_spec("marketdata.collector") is None',
            )
        if test == "test_standalone_api":
            source = source.replace(
                "import subprocess\n", "import importlib.util\nimport subprocess\n", 1
            )
            source = source.replace(
                "    import marketdata.collector",
                '    assert importlib.util.find_spec("marketdata.collector") is None',
            )
            source = source.replace(
                '    monkeypatch.setattr(marketdata.collector, "build_providers", forbidden)',
                '    monkeypatch.setattr("marketdata.registry.Registry.load", forbidden)',
            )
        files[path] = (
            [path],
            format_test(path, source).encode(),
            "local fixture imports; absent collector assertion",
        )

    helper_sources = [
        "tests/test_native_candles.py",
        "tests/test_integration_compatibility.py",
        "tests/test_process_separation.py",
    ]
    helper = (
        "from datetime import UTC, datetime, timedelta\n"
        "from marketdata.candles import MarketCandle\n\n"
    )
    for source, name in (
        (helper_sources[0], "NOW"),
        (helper_sources[0], "make_candle"),
        (helper_sources[1], "TABLES"),
        (helper_sources[1], "snapshot"),
        (helper_sources[2], "forbidden"),
    ):
        helper += extract(source, name) + "\n\n"
    files["tests/helpers.py"] = (
        helper_sources,
        format_test("tests/helpers.py", helper).encode(),
        "extract only transport-free fixture helpers",
    )
    broker_path = "config/instruments.tradingview-business-proposal.json"
    broker = json.loads((ROOT / broker_path).read_text(encoding="utf-8"))
    # Keep fixture evidence for source separation, never activate any provider.
    files[broker_path] = (
        [broker_path],
        json.dumps(broker, indent=2).encode() + b"\n",
        "test-only persisted broker registry",
    )

    api_path = "docs/api.md"
    api = (ROOT / api_path).read_text(encoding="utf-8")
    contract = (
        "# Market Data REST and WebSocket API v1\n\n"
        "Portable distribution: see [startup and tests](../README.md) and "
        "[producer integration](integration.md).\n\n"
    )
    contract += api[
        api.index("## Authentication and browser access") : api.index(
            "## Isolated local use and validation"
        )
    ]
    contract += api[
        api.index("## WebSocket market data v1") : api.index(
            "Use a separate project and only task-owned disposable databases:"
        )
    ]
    files[api_path] = (
        [api_path],
        contract.encode(),
        "contract sections; omit parent batch launch recipes",
    )
    frontend_path = "docs/frontend-integration.md"
    frontend = (ROOT / frontend_path).read_text(encoding="utf-8")
    frontend = frontend.replace("(developer-handoff.md)", "(../README.md)").replace("8002", "8015")
    files[frontend_path] = ([frontend_path], frontend.encode(), "portable startup link/port")

    from marketdata.api import create_app
    from marketdata.config import Settings

    schema = json.dumps(create_app(Settings(_env_file=None)).openapi(), indent=2, sort_keys=True)
    files["docs/openapi.json"] = (
        ["src/marketdata/api.py", "src/marketdata/api_v1.py", "src/marketdata/market_stream.py"],
        (schema + "\n").encode(),
        "OpenAPI from the verified parent factory; no database or transport",
    )

    manifest_path = DESTINATION / "SOURCE_MANIFEST.json"
    previous = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"files": {}}
    manifest = {
        "schema": 1,
        "hash_normalization": "CRLF to LF for text snapshots",
        "policy": (
            "API/schema snapshot; no collector or concrete transport. "
            "Distribution-owned files are not overwritten."
        ),
        "files": {},
    }
    for target, (sources, content, adaptation) in sorted(files.items()):
        content = content.replace(b"\r\n", b"\n")
        destination = DESTINATION / target
        if destination.exists() and destination.read_bytes().replace(b"\r\n", b"\n") != content:
            if check:
                raise ValueError(f"snapshot differs: {target}")
            old = previous["files"].get(target, {}).get("sha256")
            if old not in (
                digest(destination.read_bytes()),
                hashlib.sha256(destination.read_bytes()).hexdigest(),
            ):
                raise ValueError(f"preserve manually modified distribution file: {target}")
        elif check and not destination.exists():
            raise ValueError(f"missing snapshot file: {target}")
        if not check:
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(content)
        manifest["files"][target] = {
            "sha256": digest(content),
            "sources": {source: digest((ROOT / source).read_bytes()) for source in sources},
            "adaptation": adaptation,
        }
    serialized = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    if check:
        if manifest_path.read_text(encoding="utf-8") != serialized:
            raise ValueError("source manifest differs; refresh snapshot")
    else:
        manifest_path.write_text(serialized, encoding="utf-8")
    print(f"{'Verified' if check else 'Exported'} {len(files)} portable snapshot files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    export(parser.parse_args().check)
