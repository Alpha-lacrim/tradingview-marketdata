"""Allowlisted, reproducible candidate exports; never writes the frozen v1.0 tree or ZIP."""

import argparse
import ast
import hashlib
import json
import subprocess
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = "1.1.0rc1"
COMPANY_MODULES = (
    "__init__ api api_models api_v1 candles config domain events fanout market_stream "
    "observability persistence price_selection production registry sessions ws_models "
    "ingest_api ingest_auth replication replication_models quote_fields quote_snapshots "
    "reconciliation "
    "remote_status session_recovery release_admin release_runtime providers/__init__ providers/base"
).split()
DOCS = (
    "api frontend-integration fastapi-v1.1-protocol-v1 fastapi-v1.1-session-recovery "
    "fastapi-v1.1-runtime fastapi-v1.1-source-capture fastapi-v1.1-sync-worker "
    ""
).split()


def sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def build(component: str, python_image: str) -> Path:
    name = "marketdata-fastapi" if component == "company" else "marketdata-collector-sync"
    name += "-v" + VERSION
    output = ROOT / "distributions" / name
    if output.exists():
        prior = json.loads((output / "RELEASE.json").read_text("utf-8"))
        if prior["component"] != component or prior["version"] != VERSION:
            raise ValueError("refusing unrelated output directory")
        for line in (output / "SHA256SUMS").read_text().splitlines():
            checksum, path = line.split("  ", 1)
            if sha((output / path).read_bytes()) != checksum:
                raise ValueError("preserve manually changed candidate file: " + path)
        previous_files = {
            line.split("  ", 1)[1] for line in (output / "SHA256SUMS").read_text().splitlines()
        } | {"SHA256SUMS"}
    else:
        previous_files = set()
    output.mkdir(parents=True, exist_ok=True)
    files: dict[str, bytes] = {}
    sources: dict[str, str] = {}

    def copy(source: str, target: str | None = None) -> None:
        value = (ROOT / source).read_bytes().replace(b"\r\n", b"\n")
        files[target or source] = value
        sources[source] = sha(value)

    modules = (
        COMPANY_MODULES
        if component == "company"
        else [
            p.relative_to(ROOT / "src/marketdata").as_posix().removesuffix(".py")
            for p in (ROOT / "src/marketdata").rglob("*.py")
        ]
    )
    for module in sorted(modules):
        copy("src/marketdata/" + module + ".py")
    for p in (ROOT / "migrations").rglob("*"):
        if not p.is_file() or p.suffix not in {".py", ".mako"}:
            continue
        if component == "collector" and p.name[:4].isdigit() and int(p.name[:4]) > 6:
            continue
        copy(p.relative_to(ROOT).as_posix())
    copy("alembic.ini")
    if component == "collector":
        copy("alembic-source.ini")
        for p in (ROOT / "source_migrations").rglob("*.py"):
            copy(p.relative_to(ROOT).as_posix())
    roles = (
        ["v11_destination_roles"]
        if component == "company"
        else ["v11_source_sync_role", "v11_source_collector_role"]
    )
    for role in roles:
        copy("scripts/sql/" + role + ".sql")
    copy("scripts/create_ingest_credentials.py")
    for doc in DOCS:
        copy("docs/" + doc + ".md")
    copy("docs/ws-marketdata.schema.json")
    copy("docs/fastapi-v1.1-ingest-openapi.json")
    copy("examples/browser-marketdata.js")
    copy("config/instruments.mock.json")
    if component == "collector":
        copy("config/instruments.tradingview-business-proposal.json")
    for scope in ("common", component):
        directory = ROOT / "release/v1.1" / scope
        for p in directory.rglob("*"):
            if p.is_file():
                copy(p.relative_to(ROOT).as_posix(), p.relative_to(directory).as_posix())
    if component == "collector":
        files["Dockerfile"] = files["Dockerfile"].replace(
            b"COPY alembic.ini RELEASE.json ./",
            b"COPY alembic.ini RELEASE.json alembic-source.ini ./\n"
            b"COPY source_migrations ./source_migrations\nCOPY config ./config",
        )
        files["Dockerfile"] = files["Dockerfile"].replace(b'CMD ["read"]', b'CMD ["collector"]')
        files["Dockerfile"] = b"\n".join(
            b"HEALTHCHECK NONE" if line.startswith(b"HEALTHCHECK ") else line
            for line in files["Dockerfile"].split(b"\n")
        )
    from marketdata.api import create_app
    from marketdata.config import Settings

    files["docs/openapi.json"] = (
        json.dumps(create_app(Settings(**dict(_env_file=None))).openapi(), indent=2, sort_keys=True)
        + "\n"
    ).encode()

    project = tomllib.loads((ROOT / "pyproject.toml").read_text("utf-8"))
    locked = tomllib.loads((ROOT / "uv.lock").read_text("utf-8"))
    versions: dict[str, set[str]] = {}
    for package in locked["package"]:
        if "registry" in package.get("source", {}):
            versions.setdefault(package["name"], set()).add(package["version"])
    constraints = [n + "==" + next(iter(v)) for n, v in sorted(versions.items()) if len(v) == 1]
    dependencies = project["project"]["dependencies"][:]
    dependencies += project["project"]["optional-dependencies"]["migrations"]
    dependencies += ["httpx>=0.28,<1"]  # smoke tools, worker and verified HTTPS clients
    scripts = {"marketdata-admin": "marketdata.release_admin:main"}
    if component == "collector":
        scripts["marketdata-sync"] = "marketdata.sync_worker:main"
    pyproject = (
        '[build-system]\nrequires = ["hatchling==1.27.0"]\n'
        'build-backend = "hatchling.build"\n\n[project]\n'
        f"name = {json.dumps(name.removesuffix('-v' + VERSION))}\n"
        f'version = "{VERSION}"\nrequires-python = ">=3.12"\n'
        f"dependencies = {json.dumps(dependencies)}\n\n[project.scripts]\n"
        + "".join(json.dumps(k) + " = " + json.dumps(v) + "\n" for k, v in scripts.items())
        + '\n[dependency-groups]\ndev = ["pytest==8.4.2", "pytest-asyncio==1.4.0"]\n'
        + "\n[tool.uv]\nconstraint-dependencies = "
        + json.dumps(constraints)
        + '\n\n[tool.hatch.build.targets.wheel]\npackages = ["src/marketdata"]\n'
        + '\n[tool.pytest.ini_options]\nasyncio_mode = "auto"\n'
        + '\n[tool.ruff]\nline-length = 100\ntarget-version = "py312"\n'
        + '\n[tool.ruff.lint]\nselect = ["E", "F", "I", "UP", "B", "ASYNC"]\n'
    )
    files["pyproject.toml"] = pyproject.encode()
    for path in ("scripts/build_v11_release.py", "pyproject.toml", "uv.lock"):
        sources[path] = sha((ROOT / path).read_bytes().replace(b"\r\n", b"\n"))
    qualification_inputs = {
        path: sha((ROOT / path).read_bytes().replace(b"\r\n", b"\n"))
        for path in [
            "docs/fastapi-v1.1-milestone3b.md",
            "docs/fastapi-v1.1-milestone4.md",
            "docs/fastapi-v1.1-milestone4-verification.json",
        ]
    }
    migrations = {}
    for path, content in files.items():
        if "/versions/" not in path or not path.endswith(".py"):
            continue
        assignments = {
            target.id: ast.literal_eval(node.value)
            for node in ast.parse(content).body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name) and target.id in {"revision", "down_revision"}
        }
        if "revision" in assignments:
            migrations[path] = dict(**assignments, sha256=sha(content))
    files["MIGRATIONS.json"] = (json.dumps(migrations, indent=2, sort_keys=True) + "\n").encode()
    metadata = dict(
        version=VERSION,
        component=component,
        candidate=True,
        source_revision=subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        source_has_reviewed_uncommitted_changes=True,
        qualification_inputs=qualification_inputs,
        sources=sources,
        source_digest=sha(json.dumps(sources, sort_keys=True).encode()),
        destination_head="0010" if component == "company" else None,
        source_base="0006" if component == "collector" else None,
        source_extension="sc0002" if component == "collector" else None,
        python_image=python_image,
        hash_normalization="LF for text",
        uv_version="0.11.27",
    )
    files["RELEASE.json"] = (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode()
    for n, content in files.items():
        p = output / n
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(content)
    for command in (
        ["uv", "lock"],
        [
            "uv",
            "export",
            "--frozen",
            "--no-dev",
            "--no-emit-project",
            "--format",
            "requirements-txt",
            "--output-file",
            "requirements.lock",
        ],
        [
            "uv",
            "export",
            "--frozen",
            "--no-emit-project",
            "--format",
            "requirements-txt",
            "--output-file",
            "requirements-dev.lock",
        ],
    ):
        result = subprocess.run(command, cwd=output, capture_output=True, timeout=300)
        if result.returncode:
            raise RuntimeError(
                "release dependency locking failed: " + result.stderr.decode()[-2000:]
            )
    files["uv.lock"] = (output / "uv.lock").read_bytes().replace(b"\r\n", b"\n")
    files["requirements.lock"] = (output / "requirements.lock").read_bytes().replace(b"\r\n", b"\n")
    files["requirements-dev.lock"] = (
        (output / "requirements-dev.lock").read_bytes().replace(b"\r\n", b"\n")
    )
    files["SHA256SUMS"] = "".join(f"{sha(v)}  {n}\n" for n, v in sorted(files.items())).encode()
    for removed in previous_files - files.keys():
        target = (output / removed).resolve()
        if not target.is_relative_to(output.resolve()):
            raise ValueError("old managed path escapes candidate output")
        target.unlink()  # Only verified, task-owned stale export files; never original inputs.
    for n, value in files.items():
        (output / n).write_bytes(value)
    artifact = ROOT / "artifacts" / (name + ".zip")
    with zipfile.ZipFile(artifact, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for n, value in sorted(files.items()):
            entry = zipfile.ZipInfo(name + "/" + n, (2026, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = 0o100644 << 16
            archive.writestr(entry, value)
    print(
        json.dumps(
            dict(archive=artifact.as_posix(), sha256=sha(artifact.read_bytes()), files=len(files))
        )
    )
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python-image", required=True, help="reviewed python image name@sha256:digest"
    )
    args = parser.parse_args()
    if "@sha256:" not in args.python_image:
        parser.error("immutable base image digest required")
    for component in ("company", "collector"):
        build(component, args.python_image)
