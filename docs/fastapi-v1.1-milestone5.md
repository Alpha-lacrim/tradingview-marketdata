# FastAPI v1.1 release candidate — Milestone 5

Date: 2026-10-16 (Asia/Tehran). Version: **1.1.0rc1**. Base source revision:
`4cedaedb172d89ca3d2566bcea477c8296643592`, plus the preserved, reviewed uncommitted
milestones and the changes listed below. The release manifests record the exact normalized
source-file hashes and component source digests; the commit alone cannot reproduce this dirty
workspace. No commit, push, company deployment, legacy collector migration or real producer transfer
is performed by this milestone.

This is the company's **first installation**. No v1.0 installation or production cutover is
required. The API and collector/worker are separate packages. They share the approved protocol
and persistence implementation; READ and INGEST are independent roles of one company image.
**Verdict: the release candidate is prepared and isolated installation is verified.**
Complete genuine99 and target-site production acceptance remain outstanding; no company deployment
or legacy collector transition is completed.

## Artifacts and migration boundaries

| Component | New output | Schema |
|---|---|---|
| Company API | `distributions/marketdata-fastapi-v1.1.0rc1/` and `artifacts/marketdata-fastapi-v1.1.0rc1.zip` | Destination **0010** |
| Collector and independent worker | `distributions/marketdata-collector-sync-v1.1.0rc1/` and `artifacts/marketdata-collector-sync-v1.1.0rc1.zip` | Base **0006**, independent capture **sc0002** (including sc0001) |
| Optional Python wheels | `artifacts/v11-m5-wheels/` | Application modules only; use the complete ZIP for migrations/configuration/runbooks |
| Verification sidecar | `docs/fastapi-v1.1-milestone5-verification.json` | Exact hashes, executed results and outstanding gates |

M3B introduced destination0010 and source sc0002. M5 adds **no migration** and changes no existing
migration. The legacy installation previously reported0004 remains untouched. Source installation
explicitly applies0006 then sc0002; company installation explicitly applies0010 and rejects source
capture. Generic destination `upgrade head` is not a source migration procedure. Runtime never
automatically migrates, provisions ownership or enables capture. Source templates are disabled.

Each ZIP has RELEASE.json, MIGRATIONS.json and SHA256SUMS. Exports normalize text to LF, sort paths
and use fixed ZIP metadata. Rebuilding verifies existing managed hashes before replacing candidate
files; it refuses manually modified payloads. Frozen v1.0 output is outside the exporter allowlist.
The company package includes no concrete TradingView transport, collector, worker or source schema.
Accepted M3B/M4 reports are referenced by hash, rather than copying genuine observation traces into
release ZIPs. No secrets, credential files, collected private observations, dumps or caches ship.

## Final executed verification

| Check | PostgreSQL16.1 | Timescale2.25.1 / PG17.7 |
|---|---|---|
| Final complete post-fix regression suite | **651 passed, 9 skipped**, 0 failures/errors,449.88s | **654 passed, 6 skipped**, 0 failures/errors,533.89s |
| Exported fresh migration/role/password/login/provision CLI | Passed | Passed |
| Exact-image HTTPS pipeline |31 passing assertions (including repeated parity checks) |31 passing assertions (including repeated parity checks) |
| Final actual candle / revision counts, source=destination |3 /1 |3 /1 |
| Latest quote field/time/order and coverage parity |1 /1 streams equal |1 /1 streams equal |
| Archived receipt fingerprint mismatches |0 |0 |
| Pending candle / quote financial state after restart |0 /0 |0 /0 |
| Committed WSS events observed |13 |13 |

Company package tests:3 passed. Collector/worker package tests:2 passed. Fresh Windows hashed
dependency installations, both pinned-backend wheel builds, both image dependency checks, all30
shared-module byte comparisons, file manifests/ZIP CRC and deterministic ZIP rebuilds pass.
Final fresh company image build312.99s; collector186.22s after a retained failed network attempt.
Both use --no-cache/verified HTTPS/hash checks. Static lint,431-file formatting,54-source-file
strict typing, Compose configuration and5 PowerShell example-block syntax checks pass. These
checks overlap the full suites; do not sum them as unique tests. Physical service boot is unrun.

Six common skipped cases are the optional Databento SDK module collection; one legacy backup
container test; two immutable v1.0/schema0006 process tests; one legacy database-outage container
test; and one old Batch4-image production-entrypoint test. PostgreSQL additionally skips three
Timescale aggregate variants, which pass on the TS profile. The new M5 image pipeline separately
executes actual two-worker READ/INGEST and durable database restarts; skipped legacy cases remain
skipped, never counted as passing. Full logical restore evidence is inherited from reviewed M4.

Outer ZIP SHA256:

```text
334a9969cafe726767d1dd0a4676f674412d61a7c9a0c50473bfa81e9ec61c05  marketdata-fastapi-v1.1.0rc1.zip
60c5d3614b6760cf81800d72ba2c02c224eee8a466e41ec7ecb1ee52979726e0  marketdata-collector-sync-v1.1.0rc1.zip
```

All artifact/wheel hashes are in artifacts/marketdata-v1.1.0rc1-SHA256SUMS and the verification
sidecar. The final local image IDs, Python3.12.15 version and exact source-file match proof are
recorded there too. Images were not pushed; an independent build may have different Docker layer
metadata/IDs. Source ZIP determinism is verified; bit-identical OCI image output is not claimed.

## Installation and operation guides

- Company first installation: [INSTALL-COMPANY.md](../distributions/marketdata-fastapi-v1.1.0rc1/INSTALL-COMPANY.md).
- Windows collector/worker: [INSTALL-WINDOWS.md](../distributions/marketdata-collector-sync-v1.1.0rc1/INSTALL-WINDOWS.md).
- Linux collector/worker: [INSTALL-LINUX.md](../distributions/marketdata-collector-sync-v1.1.0rc1/INSTALL-LINUX.md).
- Controlled0004 transition and later distinct-producer handover:
  [COLLECTOR-TRANSITION.md](../distributions/marketdata-collector-sync-v1.1.0rc1/COLLECTOR-TRANSITION.md).
- Backup/restore, receipts, reconciliation, rotation, rollback and monitoring:
  [OPERATIONS.md](../distributions/marketdata-fastapi-v1.1.0rc1/OPERATIONS.md).
- Fresh dependency/build and package verification:
  [VERIFY.md](../distributions/marketdata-fastapi-v1.1.0rc1/VERIFY.md).

The guides include copyable PowerShell/Linux commands, exact image pins, dedicated new volumes,
separate local collector/worker and company read/ingest logins, file-based secrets, TLS/CA setup,
gateway routing/bounds, Task Scheduler/systemd examples and bounded enrollment/repair. Company
PostgreSQL is internal-only; the worker receives only its **local** restricted DSN and producer
token. It needs no company DB password, live collector connection or browser credentials.

Administration creates new producer/ownership definitions only, with reviewed source epoch and
generation1. It cannot transfer an existing stream or rotate an existing role. Existing approved
handover/recovery protocols remain authoritative. An exact cloned DB/anchor/credentials is not
automatically distinguishable before remote progress; distinct installation identities and reviewed
handover remain required. No local identity/epoch reset tool or automatic disaster-recovery guarantee
has been invented. Invalid historical volume units/timestamps remain retained and block preflight.

## Changes and rationale

The exact M5-only file inventory is in the verification sidecar, compared with the360-file pre-M5
baseline under `artifacts/v11-m5-baseline/`, not with HEAD. Earlier uncommitted work is preserved.

| Files | Reason |
|---|---|
| `scripts/build_v11_release.py`; `release/v1.1/**`; two generated distributions | Allowlisted, deterministic standalone packages, locked dependencies, service/secret/config templates, install and recovery runbooks, package tests |
| `src/marketdata/release_runtime.py` | Component/role guards, file-based configuration, explicit capture opt-in, independent existing production entrypoints, optional direct TLS for isolated/native operation |
| `src/marketdata/release_admin.py` | Explicit named-DB/boundary checks, serialized migration tooling, first-install roles and reviewed new producer/stream provisioning; no implicit handover/rotation |
| `scripts/sql/v11_source_collector_role.sql` | Restricted local collector writer separate from restricted sync consumer; denies epoch/payload/checkpoint tampering, historical DELETE and DDL |
| `src/marketdata/quote_fields.py`; `quote_snapshots.py`; `source_capture.py` | Move unchanged shared field-clock helpers out of source capture, removing a discovered standalone company import dependency without shipping collector code |
| `persistence.py`; `tests/test_release_foundation.py` | Import source capture only when capture is enabled; destination-only ordered snapshots no longer import a source module absent from the company package. Real SQL regression blocks that import and verifies latest state plus committed event. |
| `source_capture.py`; `tests/test_source_capture.py` | Observed failed/busy connection cleanup: bounded graceful close cancels an outstanding query and releases the advisory lock; best-effort cleanup handles asyncpg client-state errors without suppressing delivery errors |
| `__init__.py`, `api.py`, `ingest_api.py`, root project/lock and ingestion OpenAPI | Explicit1.1.0rc1 version metadata; REST/OpenAPI paths and schemas unchanged |
| `tests/test_release_foundation.py`; `test_destination_ingestion.py` | Real DB migration/boundary/role/provision checks; frozen v1 read contract compares everything except the intentional info.version change |
| `scripts/verify_v11_release_install.py`; `verify_v11_release_pipeline.py`; `compose.v11-m5-test.yaml`; scoped cleanup | Reproducible task-owned installation and actual image/HTTPS/restart verification; no live source connection |
| README/CODEX/HANDOFF and M5 report/sidecar | Release entry points, evidence, preservation and remaining site prerequisites |

The advisory-lock cleanup change is narrow: a busy connection is closed with a two-second bound,
then terminated on cleanup failure. It performs no delivery mutations and grants no stale worker
authority. The regression holds a real PostgreSQL advisory lock during pg_sleep, exits the consumer
context, and proves a new backend acquires the lock. Existing in-flight lock-loss tests still apply.

## Build and test environment

Windows11/Python3.12.7/uv0.11.27 hosts fresh extracted-package environments. A separate disposable WSL
`marketdata-v11-m5-test` hosts Docker27.5.1, an exclusive root/socket and four labelled persistent
database volumes. Existing Docker Desktop remains stopped. PostgreSQL16.1 and Timescale2.25.1/PG17.7
are pinned independently in compose.v11-m5-test.yaml; no cross-major volume reuse occurs.

The actual test runtime uses host-networked containers bound to loopback, not the company gateway
topology. Production Compose files are configuration-validated; company gateway/public PKI, firewall
and actual site supervision remain installation prerequisites. READ/INGEST/worker test containers use
non-root users, read-only roots, dropped capabilities, no-new-privileges and narrowly mounted secrets.

Fresh Windows installs use `uv --no-cache pip sync --require-hashes`, then a clean pinned-backend
editable project install. Fresh images use `docker build --no-cache` and pip `--no-cache-dir
--require-hashes`. Default PyPI access failed on DNS/download timeouts in this environment. Explicit
verified HTTPS mirror `https://mirrors.aliyun.com/pypi/simple/` succeeded with identical locked wheel
hashes. TLS/hash checks are never disabled and no host wheelhouse/venv is copied into an image.
Company approval of its reachable package index is still required. Earlier cache-built images are
not cited as clean-release build evidence.

Both packages run their own tests, dependency checks and complete file-manifest verification. The
fresh-install script executes the **exported** admin module from the newly installed environments
against brand-new UUID-named databases on both engines, verifies actual heads and least-privilege
role flags and rejects cross-track schema installation. Integration suites also exercise historical
upgrades0004->0005->0006/sc0001/sc0002 and destination0006->0010 on disposable databases.

The image pipeline uses an injected native-candle/independent-quote collector driver from the fresh
collector environment, actual company READ/INGEST images and the actual standalone worker image.
Private test certificates are trusted explicitly. Assertions compare actual financial state,
revisions, field clocks/order, coverage and events, not merely receipts. It exercises immutable
delivery, retry/correction, WSS/ChartData, the shipped smoke command, worker restart with queued
quotes, independent role restart and durable PostgreSQL restarts with collection stopped.

Final counts, failures from earlier attempts, skip reasons, durations and output hashes are recorded
in the sidecar. Skips are never counted as passes. Earlier failed attempts are retained; timeout
reruns are distinguished from product corrections. A Timescale pipeline observer reused a
keep-alive socket after the deliberately restarted READ service and received RemoteProtocolError.
The test observer now opens a new verified connection after restart; production worker transport
retry behavior is unchanged. The failed observer run is retained separately. No package-only smoke is presented as genuine
acquisition acceptance.

## Reproduction (isolated only)

Start only new labelled databases/volumes from compose.v11-m5-test.yaml on a task Docker engine.
The checked-in profile publishes the four DB ports on loopback. The M5 WSL run used equivalent
host-network/loopback commands; its exact image/config inventory is retained under artifacts.
Never point TEST_DATABASE_URL or these scripts at a legacy/company database. Run suites and
pipeline/install scripts serially **per cluster**, since integration role names are cluster-wide.

```powershell
uv sync --frozen --dev
uv run python scripts/build_v11_release.py --python-image python:3.12-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258
docker compose -f compose.v11-m5-test.yaml up -d
$env:TEST_DATABASE_URL='postgresql://m5_test:disposable_test@127.0.0.1:55995/marketdata_v11_m5_test'
# On this NEW database only, migrate destination explicitly before running the suite:
$env:DATABASE_URL=$env:TEST_DATABASE_URL
uv run alembic upgrade 0010
uv run pytest -q tests --junitxml=artifacts/CHOSEN_NEW_PG_RESULTS.xml
# Repeat with55996 and -x timescale=true before upgrade0010 for the TS profile.
```

Prepare new private JSON for test scripts with `source`, `destination` fixture DSNs and a
`docker_prefix` argument list (`["docker"]` on an ordinary task engine), plus `source_container` and
`destination_container` names matching the four test containers. Never publish real credentials.
Extract the two ZIPs into a NEW install root, create company-env/collector-env there, follow VERIFY.md
and build the two images first. Then execute:

```text
python scripts/verify_v11_release_install.py --database-file PRIVATE_PG_TEST_CONFIG --install-root NEW_INSTALL_ROOT --output NEW_INSTALL_EVIDENCE
python scripts/verify_v11_release_pipeline.py --database-file PRIVATE_PG_TEST_CONFIG --output NEW_PIPELINE_EVIDENCE --company-image REVIEWED_COMPANY_IMAGE --collector-image REVIEWED_COLLECTOR_IMAGE --company-package EXTRACTED_COMPANY_ROOT --company-python NEW_COMPANY_ENV_PYTHON --collector-package EXTRACTED_COLLECTOR_ROOT --collector-python NEW_COLLECTOR_ENV_PYTHON
```

Repeat with the Timescale config and --timescale. These scripts refuse unexpected fixture DSNs.
Test credentials `disposable_test` belong only to isolated fixtures and are not production templates.
Cleanup must verify task labels, resolved WSL root and volume mounts; the included M5 cleanup script
is specific to this session's private WSL runtime, not a general production uninstall command.

## Evidence and readiness boundaries

- M5 builds/installs are separate from M4 evidence. Reviewed M4 includes a genuine brief BTC dual
  capture (six completed native candles plus independent quotes) and bounded injected99 workloads
  on both engines:900s at1/feed/s,120s at5/feed/s. It does not qualify genuine99 sustained behavior.
- No new genuine TradingView connection is made in M5. Full genuine99 source publication,
  provider entitlements/distribution rights, employer universe approval, sustained freshness and
  source-specific omissions remain open. OANDA/FXCM identities and scoped aliases remain distinct.
- Actual company DNS/TLS/gateway/firewall/secret provisioning, off-host backup schedules/RPO/RTO,
  target Windows reboot/sleep and Linux systemd supervision need site rehearsal. No company
  deployment or legacy collector transition is completed. Those require separate authorization.
- Coalescible quote delivery guarantees latest genuine field state, not unlimited tick history.
  API/session health cannot make old prices fresh. Proposed thresholds in OPERATIONS are not SLAs.
- Restoration with uncertain source identity/security policy is a stop-and-review operation.
  Old receipts/export markers do not prove actual destination completeness. Preserve anchors,
  immutable bytes, original observations and revoked-owner policy outside rollback copies.

The release-candidate verdict and exact executed results are finalized in the verification sidecar.
Complete source packages and isolated technical qualification must not be described as completed
company production deployment or full genuine business-universe acceptance.
