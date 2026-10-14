# Current state — 2026-10-14

## Repository and authorization

Consolidation PR https://github.com/Alpha-lacrim/tradingview-marketdata/pull/2 is merged.
This workspace started clean on detached HEAD fdf7759d31d2a0a1738410ffb7825c1b951ee1c6
(`Production access approval`), following main merge a1cf992a. Preserve that user commit.
The operator requested this preparation committed locally on main, with no push or deployment.
The temporary codex/commercial-tradingview-preparation branch was removed. To free main,
the older home-rooted worktree was detached at its unchanged 324b1d83 commit; its tracked
edits were verified unchanged. Other worktrees and paused database-efficiency work remain
untouched. This workspace now owns the local main checkout.

The operator confirmed full TradingView support permission and an appropriate company
access arrangement for this use case, then confirmed the existing public-site connection is
approved. No dedicated endpoint/authentication was supplied. The earlier access permission
gate is resolved for this installation. Upstream entitlements, realtime classification,
experimental protocol and persisted tradingview_guest identity are preserved.

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

Offline: 312 passed, 130 integration tests deselected. Full disposable PostgreSQL suite:
439 passed, three expected aggregate skips, 111.17s. Includes migrations/rollback, permissions,
outage/recovery, replay, two-worker TCP/WS, backup/restore and actual Linux production image
startup/restart with mounted SELECT-only credentials. Full TimescaleDB: 442 passed, 122.88s.
Ruff check/format (145 files), strict mypy source (37 files) plus source/new smoke (38 files),
CLI help, merged Compose profile and explicit false revocation, Alembic head 0006 and whitespace
checks passed. Existing dependency deprecation warnings remain.
After the final checkpoint sanitization test/type fixes, the targeted production and smoke
regressions passed (9 tests) on TimescaleDB. Only task-owned test containers/network were
removed after validation; the pre-existing containers remain running.

Task-owned image marketdata-batch4:qualification built locally, not pushed/deployed:
sha256:5c0ffa50c3f55641cc6af70eb91f579d6cc19850f5c5f46a5916276d1b70898d.
Ignored JUnit: artifacts/commercial-postgres.xml and commercial-timescale.xml. Only
marketdata-batch4-tests databases/network are used; no production SQL or volume changes.

## Deployment and next actions

Existing tradingview-app-1 was unhealthy and tradingview-db-1 healthy on inspection.
Neither was switched, recreated or stopped. Live commercial smoke has NOT RUN. No current
live feed correctness claim. Legacy schema0004 combined collector lacks the ownership lock:
verify image/database/volume, back up, stop legacy ownership, migrate using the reviewed
procedure and provision separate principals/secrets before replacement. Follow
docs/collector-fastapi-integration.md and docs/production-deployment.md. Do not run legacy
switch helpers or a second ordinary live scheduler here.

Next: provision target reviewed images/digests, mounted DSNs/token, HTTPS origins/proxy,
backup/monitoring and company session adapter; perform the reviewed replacement, then run
the documented three-minute smoke during overlapping source sessions. Follow with sustained
target-host/source-coverage soak and company HTTPS/browser testing. Fixtures/synthetic tests
do not establish WAN latency, sustained capacity or source completeness.

Earlier measured API qualification remains in docs/api-batch4-verification.md,
docs/api-batch4-performance.md and evidence JSON/source manifest. Consolidation details:
docs/main-consolidation.md. Source experiments/ordinary-100 qualification and unapproved
business proposal remain in their reports. No new load benchmark or production DR test.
