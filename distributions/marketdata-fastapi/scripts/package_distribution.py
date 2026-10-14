"""Build and verify a clean, deterministic developer-handoff ZIP using only stdlib."""

import argparse
import hashlib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ROOT = Path(__file__).resolve().parents[1]
TOP_FILES = {
    ".dockerignore",
    ".env.example",
    ".gitattributes",
    ".gitignore",
    "Dockerfile",
    "README.md",
    "SOURCE_MANIFEST.json",
    "alembic.ini",
    "compose.yaml",
    "compose.timescale.yaml",
    "pyproject.toml",
    "uv.lock",
}
TOP_DIRS = {"src", "tests", "migrations", "config", "docs", "examples", "scripts"}
EXCLUDED = {
    ".git",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "dist",
    "build",
    "secrets",
    "logs",
    "data",
}
EXTENSIONS = {".py", ".md", ".json", ".js", ".mjs", ".yaml"}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def package(output: Path, extract_to: Path | None) -> None:
    output = output.resolve()
    if output.is_relative_to(ROOT):
        raise ValueError("ZIP output must be outside the project directory")
    contents: dict[str, bytes] = {}
    for source in sorted(ROOT.rglob("*")):
        relative = source.relative_to(ROOT)
        if any(part in EXCLUDED or part.endswith(".egg-info") for part in relative.parts):
            continue
        if source.name.startswith(".env") and relative.as_posix() != ".env.example":
            continue
        if source.suffix in {".pyc", ".pyo", ".log"}:
            continue
        if source.is_symlink():
            raise ValueError(f"symlink forbidden: {relative}")
        if not source.is_file():
            continue
        if relative.parts[0] not in TOP_DIRS and relative.as_posix() not in TOP_FILES:
            raise ValueError(f"unreviewed top-level file: {relative}")
        if len(relative.parts) > 1 and source.suffix not in EXTENSIONS:
            raise ValueError(f"unreviewed file type: {relative}")
        contents["marketdata-fastapi/" + relative.as_posix()] = source.read_bytes()
    output.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for name, content in contents.items():
            info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, content)
    with ZipFile(output) as archive:
        assert archive.testzip() is None
        assert set(archive.namelist()) == set(contents)
        for name, content in contents.items():
            assert archive.read(name) == content, name
        if extract_to is not None:
            extract_to = extract_to.resolve()
            if extract_to.exists():
                raise ValueError("extraction target must be new")
            extract_to.mkdir(parents=True)
            for name in contents:
                target = (extract_to / name).resolve()
                assert target.is_relative_to(extract_to)
            archive.extractall(extract_to)
            for name, content in contents.items():
                assert (extract_to / name).read_bytes() == content, name
    checksum = digest(output.read_bytes())
    output.with_suffix(".zip.sha256").write_text(f"{checksum}  {output.name}\n", encoding="utf-8")
    print(f"Verified {len(contents)} files: {output}")
    print(f"SHA-256: {checksum}")
    if extract_to is not None:
        print(f"Extracted and byte-verified: {extract_to}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--extract-to", type=Path)
    options = parser.parse_args()
    package(options.output, options.extract_to)
