# Current state — 2026-10-14

## Repository and authorization

Consolidation PR https://github.com/Alpha-lacrim/tradingview-marketdata/pull/2 is merged.
This workspace started clean on detached HEAD fdf7759d31d2a0a1738410ffb7825c1b951ee1c6
(`Production access approval`), following main merge a1cf992a. Preserve that user commit.
The operator requested the earlier commercial preparation committed locally on main,
with no push or deployment. The earlier developer audit left uncommitted edits to
HANDOFF/README/frontend docs/browser example/smoke and new developer-handoff/browser tests.
The standalone API task preserved and completed those frontend handoff changes. The operator authorized a local commit of the completed extraction and Go ChartData compatibility work on main on 2026-10-14. No push or deployment is authorized.
The temporary codex/commercial-tradingview-preparation branch was removed. To free main,
the older home-rooted worktree was detached at its unchanged 324b1d83 commit; its tracked
edits were verified unchanged. Other worktrees and paused database-efficiency work remain
untouched. This workspace now owns the local main checkout.

The operator confirmed full TradingView support permission and an appropriate company
access arrangement for this use case, then confirmed the existing public-site connection is
approved. No dedicated endpoint/authentication was supplied. The earlier access permission
gate is resolved for this installation. Upstream entitlements, realtime classification,
experimental protocol and persisted tradingview_guest identity are preserved.

## Developer handoff audit

Verdict: **READY for independent developer handover, with production limitations**.
Deliver artifacts/marketdata-fastapi-v1.0.zip as the verified self-contained project bundle;
its editable source remains distributions/marketdata-fastapi/.
The project README contains copy-anywhere Docker/wheel setup, explicit migrations, SELECT-only
credentials, simulated native data, examples, tests and producer integration. The original
docs/standalone-api.md covers root packaging; docs/developer-handoff.md retains the earlier
full mock-platform/browser walkthrough. The employer explicitly requested a directory able
to become its own repository later; no repository was created. Keep one authoritative source
owner until a split is approved, then coordinate shared schema/writer/API release versions.

scripts/export_fastapi_distribution.py reproduces 53 managed snapshots and --check verifies
parity. API adds only the opt-in ChartData route; domain/persistence/migrations remain unchanged.
Snapshots use LF normalization; manifest hashes
normalize CRLF/LF. Concrete providers and collector entrypoint are absent. Generic type/compatibility
definitions remain dormant so shared API source does not need rewriting. Only create_app is
supported in the distribution. Test imports use extracted local fixture helpers; full REST/WS,
permissions/replay/migration, browser and BFF regressions are included. Its own pyproject/uv.lock,
Dockerfile/Compose/.env.example/docs/examples/test_packaging require no parent source/install,
symlink or submodule. The exporter refuses to overwrite manual distribution edits.

FastAPI was already process-independent. Minimal changes: move IngestionService to a typing-only
import; move httpx to collector extra and Alembic/SQLAlchemy/psycopg to migrations extra (dev
still includes these). Full Dockerfile explicitly retains all earlier runtime extras.
Dockerfile.api defaults to the minimal runtime, with a separate tools target. compose.api.yaml
uses tmpfs PostgreSQL, explicit migrate/seed commands and its own SELECT-only API principal.
marketdata.sample_data is a bounded standalone Store writer, rejects non-_test/production targets,
ignores .env, and marks every observation simulated. It generates native completed 1m bars,
coverage, quotes and transactional events without provider transport. No new schema/migration,
framework, infrastructure, contract change or duplicated persistence implementation.

Fixed only example/setup blockers: browser bootstrap no longer drains an older subscription
price snapshot after REST refresh; receipt comparisons handle omitted fractional seconds
and preserve microseconds; terminal HTTP configuration/auth errors stop reconnecting. Added
Node regressions run through pytest when Node is available. scripts/smoke.py now expects
API-only database streaming mode and uses optional API_KEY for REST/legacy WS authentication.
README and frontend guide link the new handoff. Default mock mode produces simulated quotes;
v1 native history is empty, while DB/TCP fixtures independently exercise native candles.

TradingView candles/ticks remain alternative collector modes. Simultaneous collection is a
separate milestone, required before promising continuous TradingView quotes alongside native
candles, not before API contract integration. The company session authorizer and final browser
architecture remain external dependencies; the BFF is provisional, not a supplied login system.

## Commercial production/test preparation

Read docs/commercial-tradingview-test.md. New compose.production.tradingview.yaml extends
file-secret production deployment with the preserved accuracy-selected 100-source registry,
native 1m candles, four connections, two requests per worker/eight global, two-second pacing
and five-second grace. Futures remain disabled. No business-proposal promotion or expansion.
The user's approval default remains true; explicit TRADINGVIEW_PRODUCTION_ACCESS_APPROVED=false
is honored in Settings and Compose. Example env, provider/deployment docs and tests now agree.

scripts/smoke_production_tradingview.py observes authenticated versioned REST/WS with an API
origin, private token file and configured HTTPS WS Origin. It needs no SQL credentials and
opens no extra provider connection. It reconciles committed native candles with exact
Decimal/source/status REST history and checks latest/coverage/health. Missing symbol/minute
pairs fail, including closed/sparse markets; sanitized checkpoints retain events and failure
categories. Corrections racing REST comparison also fail visibly. New integration tests cover
real committed TCP/WS reconciliation, wrong prices/source, simulated status and forming bars.
The approved production factory test proves four 25-source workers without upstream I/O;
explicit revocation remains tested.

Schema head is 0006; no migration added. API factories never start ingestion. Replay journal,
independent broker histories, UTC timestamps, Decimal values, unknown/non-additive source
volume and candle-close versus live quote semantics are unchanged. Only supported Store
writes append journal events; old binaries/direct SQL do not.

## Verification

Current compatibility handoff adds GET /api/v1/dashboard/candles, preserving all existing
REST/WS payloads. a=Unix seconds, b=Open, c=Close, d=High, e=Low, f=Count, g=Volume,
h=Value; no i. Arrays align and empty history returns eight empty arrays. The schema has
no defined Count/Value mapping, so approved zeros are placeholders; genuine typed stored
volume is reused, otherwise zero. Never substitute observations/trade Count or calculate
Value. Float64 conversion is explicit only at the compatibility boundary, with finite-range
validation; original endpoints retain decimal strings. Native/derived query, status partitions,
gaps, pagination, source selection, auth and read-only permissions are unchanged. The opaque
next cursor lives in X-Marketdata-Next-Cursor, exposed to permitted CORS Origins. No collector,
persistence, event-journal or schema change. Plain PostgreSQL demo is now 16.1; Timescale
remains a separate optional override.

Copied-project full suites: PostgreSQL 16.1 (no Timescale extension) 143 passed / 28.10s;
PostgreSQL 17.11 143 passed / 29.63s; TimescaleDB 2.25.1 on PostgreSQL 17.7 143 passed /
35.63s. Exact versions were queried, not inferred from tags. An initial PG17 fixture connect
timeout had 132 passes / one setup error; full rerun passed without weakening checks.
ZIP-candidate extraction in a fresh environment: PG16.1 143 passed / 31.83s, Timescale
143 passed / 35.69s. Ruff/format 49 files, mypy 24 source / 25 with BFF, wheel/sdist and
fresh minimal runtime image passed. Source manifest parity passed (53 managed files).
The old 16 OpenAPI paths and pre-existing schemas compare exactly unchanged. Twenty
existing dependency deprecation warnings remain. Prior independent 116-test-per-engine
extraction evidence is retained separately in distribution docs/verification.md.

Two-worker task-owned runtime image on exact PostgreSQL 16.1 with SELECT-only sample_api:
both simulated sources/all five candle intervals, ChartData vs exact-decimal REST, auth,
coverage/latest, health/docs/metrics, committed quote WS, reconnect/snapshot/replay,
legacy 1013 and post-writer API restart passed. Harness quoting and assertion errors were
fixed in the harness only. Task-owned tmpfs containers use label marketdata.task=chartdata-handoff,
DB ports 55925/55926/55927 and API 8017. No deployed resources or existing volumes changed.

Final ZIP: artifacts/marketdata-fastapi-v1.0.zip (66 files), with .zip.sha256 companion.
SHA-256: 3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a.
Packaging helper scripts/package_distribution.py is stdlib-only, allowlists source/docs/tests,
excludes actual .env/data/dumps/environments/caches and verifies CRC, entries and extracted
bytes. Public disposable demo fixtures/.env.example are deliberate. Final extraction lives
outside the repository; ignored artifacts/chartdata-copy-path.txt records the task temp root.
Only documentation changed after candidate full suites; executable/config/test contents are
identical. Final extraction passed 70 DB-independent tests (73 integration deselected),
Ruff/format (49 files) and mypy (25 including BFF), including local-package/manifest/OpenAPI
checks. All four task-labeled test containers were removed without volume operations;
the original TradingView app/database and backend database remained running unchanged.

Verdict: READY FOR DEVELOPER HANDOFF. Remaining company work is server/network/TLS/secrets,
SELECT-role provisioning and genuine company-session authorization for the provisional BFF.
Zeros cannot distinguish unavailable from actual zero; float64 loses decimal precision.
Count/Value need a separately agreed source/business contract before typed integration.
No target-host deployment, actual database migration, collector changes, live feed or new
capacity claims. The completed handoff is recorded in a local main commit at the user request. No push, deployment or new GitHub repository was performed.

Standalone task full suites on fresh task-owned Batch 4 tmpfs databases: PostgreSQL
**444 passed, 4 skipped**, 114.85s; TimescaleDB **447 passed, 1 skipped**, 119.83s. PostgreSQL
skips three Timescale-only aggregate tests; both skip the optional prior production-image
fixture because BATCH4_TEST_IMAGE was not selected. Fresh migrations to 0006 and existing
rollback/re-upgrade, permissions, outage/recovery, replay, worker restarts, backup/restore,
collector compatibility and browser Node regressions passed. Four new standalone tests cover
forbidden optional/collector imports, sample target safeguards, native/derived REST history,
fresh/stale simulation semantics, SQL denial, actual WS publication, reconnect and replay.

Built fresh Dockerfile.api runtime/tools images under marketdata-standalone-check. Explicit
migrate + seed + two-worker API passed on its own tmpfs DB (55914/API 8005), with no collector.
Real container REST smoke: docs/OpenAPI, all five native/derived candle intervals, latest,
coverage, auth, health, metrics, WS committed quote, reconnect snapshot and replay passed.
Inside that minimal Linux image, optional dependencies are absent and an import blocker
proved full lifespan/DB reads without ingestion/concrete provider imports; production-mode
startup also accepted its actual SELECT-only principal. API restart stays healthy without
migrations or writer. Ignored helpers: artifacts/standalone_smoke.py and api_image_boundary.py.

Ruff check/format (150 Python files), mypy source (38 files) and source+BFF (39 files) passed.
uv lock refreshed offline without changing resolved package versions. uv build produced the
wheel/sdist. Clean wheel installation and DB startup passed, including production-mode SELECT
role validation and absence of optional dependencies (its resolver selected newer compatible
FastAPI/Pydantic versions; full suites used the lockfile). The initial
offline install lacked cached Windows dependency wheels and was retried with registry access.
Existing dependency deprecation warnings remain. No new live-provider smoke, load benchmark,
target-host HTTPS qualification or production DR test. Only task-owned disposable resources
were used; pre-existing containers/databases/volumes, source selection and other worktrees
were untouched. Task-owned standalone and Batch 4 Compose services were removed after
validation without volume operations. Do not infer current provider health from synthetic tests.

## Deployment and next actions

Existing tradingview-app-1 was unhealthy and tradingview-db-1 healthy on inspection.
Neither was switched, recreated or stopped. Live commercial smoke has NOT RUN. No current
live feed correctness claim. Legacy schema0004 combined collector lacks the ownership lock:
verify image/database/volume, back up, stop legacy ownership, migrate using the reviewed
procedure and provision separate principals/secrets before replacement. Follow
docs/collector-fastapi-integration.md and docs/production-deployment.md. Do not run legacy
switch helpers or a second ordinary live scheduler here.

Next: hand developers artifacts/marketdata-fastapi-v1.0.zip and the standalone README. Keep the source
manifest/exporter workflow until a separate repository and implementation owner are approved.
For production,
provision target reviewed images/digests, mounted DSNs/token, HTTPS origins/proxy,
backup/monitoring and company session adapter; perform the reviewed replacement, then run
the documented three-minute smoke during overlapping source sessions. Follow with sustained
target-host/source-coverage soak and company HTTPS/browser testing. Fixtures/synthetic tests
do not establish WAN latency, sustained capacity or source completeness.

Earlier measured API qualification remains in docs/api-batch4-verification.md,
docs/api-batch4-performance.md and evidence JSON/source manifest. Consolidation details:
docs/main-consolidation.md. Source experiments/ordinary-100 qualification and unapproved
business proposal remain in their reports. No new load benchmark or production DR test.
