# Candidate verification and reproducible build

Verify the separately supplied outer ZIP checksums, extract into NEW directories, and run
`python scripts/verify_payload.py`. RELEASE.json records version, base revision, uncommitted
source digest, Python base-image digest and component boundaries; MIGRATIONS.json lists every
revision/down_revision/file hash. SHA256SUMS covers all shipped files except itself. No private
secrets, backups, collected data, cache or installed environments are shipped.

Reference host tool: uv==0.11.27, Python3.12. The standard install is `uv sync --frozen --dev`
then `uv run pytest -q tests`. A network with restricted PyPI wheel-host access can use an
explicit COMPANY-APPROVED HTTPS mirror without changing requirements hashes:

```text
uv venv --python python3.12 .venv
uv --no-cache pip sync --python .venv/bin/python --require-hashes --index-url https://APPROVED_INDEX/simple/ requirements-dev.lock
uv --no-cache pip install --python .venv/bin/python --no-deps --index-url https://APPROVED_INDEX/simple/ --editable .
.venv/bin/python -m pytest -q tests
uv pip check --python .venv/bin/python
```

PowerShell uses `--python .venv/Scripts/python.exe`; choose an installed Python3.12 interpreter
in uv venv. The package source/manifest must already be verified before editable installation.
Normal service installation omits development dependencies; use requirements.lock instead.
Build backend is pinned hatchling1.27.0. Runtime/transitive wheels are hash-locked. Never use
--trusted-host or disable certificate/hash checks. Do not put authenticated index credentials
in Docker build arguments. Use approved secret-based build tooling if a private index needs auth.

Build the company image (or collector image from its own directory) with the exact base digest:

```text
docker build --no-cache --build-arg PYTHON_IMAGE=EXACT_RELEASE_JSON_IMAGE --build-arg PACKAGE_INDEX=https://APPROVED_INDEX/simple/ -t CHOSEN_COMPONENT:1.1.0rc1 .
```

PACKAGE_INDEX defaults to https://pypi.org/simple. The M5 sidecar distinguishes successful
fresh mirror builds from failed default-index attempts. Normal Docker base-image reuse is not
an application dependency cache. The tested clean builds run hashed pip installation with
--no-cache-dir, no wheelhouse mount and no installed host .venv. A failed network build is not
a passing build. Record the selected index and resulting image ID/requirements hash.

Collector Docker entrypoint defaults to the explicitly enabled collector. To run only the
worker, override entrypoint to `python` and command to `-m marketdata.sync_worker --config
/run/secrets/worker.json`, mount only source sync DSN/token/CA/config read-only and a dedicated
writable anchor directory. Never mount the company DSN or all administrator secrets. Adjust
configuration file paths to the container paths. Collector images have no misleading HTTP
healthcheck; use actual worker JSON/process and collector metrics/status monitoring.

Full isolated integration reproduction is in the repository's M5 report. It uses
compose.v11-m5-test.yaml (four new labelled databases/volumes), separate native source package
and READ/INGEST/worker image processes, verified private test CA, restricted principals and
injected observations. It does not contact TradingView or production. Run full pytest and
pipeline tests serially per cluster because test role names are cluster-wide. Full fixtures
truncate ONLY their guarded disposable _test databases; never set TEST_DATABASE_URL to a
company/source production database. PostgreSQL16.1 and Timescale2.25.1/PG17.7 are separate
profiles, never the same volume. Do not downgrade a production schema for testing.

Acceptance requires actual source/company candle/quote/revision/coverage comparisons, immutable
receipts, independent role/worker restarts and source lock fencing, not only a health check.
The M4 evidence is retained separately: genuine brief BTC dual acquisition and bounded injected99
workloads. M5 does not claim a new full genuine99, target-company deployment, physical sleep/reboot,
Linux systemd boot or licensing qualification. Obtain separate site/legacy-transition approval.

The source Docker image runs UID10001. Give that UID read permission only on its source DSN,
token/config/CA and write permission on its dedicated anchor directory (owner10001, mode0700).
Native systemd account UIDs may differ; do not reuse their files by making secrets world-readable.
The company image has no collector code or source schema; accepted M3B/M4 reports are referenced
by input hash, not embedded with collected genuine observation traces in the release archives.
