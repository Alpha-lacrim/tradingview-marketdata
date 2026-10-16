# FastAPI v1.1 — milestone 2 review

Milestone 2 implements the authenticated destination ingestion runtime on the approved milestone-1
transaction foundation. Work stops for review. No source collector, outbox/worker or production
installation was changed. The company still has no v1.0 installation. The immutable v1.0
standalone distribution/ZIP remains schema 0006; root candidate code now requires 0008.

Read the [final protocol](fastapi-v1.1-protocol-v1.md), [runtime and reproduction guide](fastapi-v1.1-runtime.md),
and [machine-readable evidence](fastapi-v1.1-milestone2-verification.json).

## Files and rationale

| Files | Change |
|---|---|
| src/marketdata/ingest_api.py | Separate ingestion-only factory, four batch routes, scoped receipt lookup, session admission/renewal, readiness, HTTPS/authentication guard, streaming/body/record limits, deadlines, bounded in-flight work and sanitized errors. Pure ASGI middleware keeps cancellation and database transaction in one task. |
| src/marketdata/ingest_auth.py | Private hash-file verifier, authenticated producer principal, strict session requests, policy-locked admission and same-session renewal. |
| src/marketdata/config.py; production.py | Explicit ingest role and bounded settings; file-mounted DSN/credential configuration; same-image production entrypoint; read factory remains api-only. |
| migrations/versions/0008_ingestion_runtime.py | Additive request budget, narrow SQL admission/renewal functions, captured-status read view. Approved 0007 was not rewritten. |
| scripts/sql/v11_destination_roles.sql | Fresh reader SELECT on status view; ingestion EXECUTE on narrowly scoped functions without policy UPDATE or budget-table DML. |
| src/marketdata/replication.py | Optional authenticated key ID checked inside the upload transaction under producer lock; expanded startup permission checks. Existing trusted callers retain their default signature behavior. |
| src/marketdata/replication_models.py | Wire timestamps require RFC3339 strings and normalize to UTC, including health fields; reject protocol-version and status-boolean coercion. Decimal/ns/identity/ordering semantics remain shared. |
| src/marketdata/persistence.py; remote_status.py; market_stream.py | Head check 0008, stronger reader checks, source-captured status projection, independent per-source WS health, conservative aggregate health and preserved TTL/fences. No public read schema change. |
| src/marketdata/sample_data.py; scripts/qualify_api.py; tests/test_api_v1.py | Existing SELECT-role helpers now grant the required read-status view. The initial full run exposed this real compatibility requirement. |
| scripts/create_ingest_credentials.py | Generates random private collector token and server fingerprint files, refuses overwrites and prints no token. Windows ACL remains an explicit operator step. |
| tests/test_ingest_http.py; test_ingest_migrations.py; test_destination_migrations.py | HTTP/TLS, identity/authorization/session/limits, failure and compatibility regressions; explicit populated 0007 upgrade/roundtrip; current-head fresh installation expectation. |
| compose.v11-m2-test.yaml | New disposable tmpfs databases with separate project, labels and loopback ports. No existing deployment file was applied. |
| README; CODEX; HANDOFF; docs/api.md; production-deployment.md; new milestone/protocol/runtime documents | Current schema/runtime boundaries, additional SELECT grant, private provisioning, HTTPS/gateway requirements, reproduction and remaining source-side work. |

Earlier Phase-A/M1 changes remain uncommitted alongside these edits; they were not discarded or
silently committed. Unrelated TradingViewConvHistory/ and all other worktrees remain preserved.
HEAD remains 4cedaedb172d89ca3d2566bcea477c8296643592. No push or commit was performed.

## Migration and transaction design

0008 descends from 0007. It adds one ordinary PostgreSQL table, ingest_request_budgets, keyed by
producer, with current fixed-minute window and bounded request count. This is operational quota
metadata, not an upload receipt or source outbox. Shared database admission counts requests across
application processes and process recreation. Quota consumption commits before batch processing;
a rejected/rolled-back batch may still consume quota. Per-process concurrency bounds and the
required gateway controls supplement this authenticated quota, with no broker/Redis dependency.

Two SECURITY DEFINER functions use a fixed search_path and fully qualified policy tables, with
PUBLIC execution revoked. ingest_admit_request locks producer policy for enabled/key validation
and updates only the request budget. ingest_renew_session locks the producer row and extends only
its matching, enabled, still-live session. Neither grants an expired client a new identity,
changes ownership or bypasses the 0007 fencing trigger. Expiry requires explicit administrator
re-admission using a new session UUID and higher generation. There is no public takeover route.

The read-only ingest_read_status view joins current status ownership and live producer/session
fences to captured source status. Reader receives SELECT only on the view, without credential or
receipt-table access. Store uses it when remote status ownership is configured and retains the
local singleton path otherwise. It never stamps upload time onto source status. Global summaries
are conservative; a healthy source's WS state is evaluated separately from other missing sources.
Queue depth is not multiplied for repeated snapshots of the same run. No health timestamp is
invented for sources with no captured status.

The market write transaction remains DestinationWriter + existing Store helpers/event journal.
No new financial writer or event model was added. A key rotation between request authentication
and storage is caught by the same producer lock used for policy fencing. Market changes, candle
revisions, coverage, latest state, event entries, observation identities and committed receipts
remain atomic. HTTP returns the saved receipt only after transaction exit. Same bytes replay the
same result; modified bytes conflict; duplicate observations across batches remain harmless.
Source times and collector times stay separate from server ingestion metadata. Existing Store
write wrappers, read endpoint/WS schemas and ChartData a–h arrays remain compatible.

0008 downgrade removes quota/functions/status projection, preserving financial rows and 0007
receipts; it is a test roundtrip, not a recommended operational rollback. All migrations and role
operations in this milestone ran only inside task-owned disposable database clusters.

## Actual verification

| Final root suite | Passed | Skipped | JUnit seconds |
|---|---:|---:|---:|
| PostgreSQL 16.1 / 0008 | 523 | 8 | 148.354 |
| TimescaleDB 2.25.1 / PostgreSQL 17.7 / 0008 | 526 | 5 | 169.715 |

Each engine executed all **13 new milestone cases: 2 unit/tool cases and 11 real database cases**,
with zero new skips/failures. They verify actual HTTPS certificate/hostname verification and
untrusted-certificate rejection, authenticated native candles and independent quote-shaped
observations, stale quote/correction ordering, REST exact decimals, ChartData, committed WS
publication, duplicate/conflicting uploads, concurrent retries, scoped receipt retrieval,
post-commit lost-response replay through a recreated application/new pool, session expiry/
revocation/rotation, shared cross-instance quotas, chunked/body/record limits, backpressure,
rollback after pending data/events/receipt insertion on request timeout, and captured status expiry.
Fixtures remain simulated; they are not genuine TradingView quote-acquisition evidence.

Fresh head, populated 0006->head, populated 0007->0008 and downgrade/re-upgrade preservation ran
on both engines. Actual restricted writer and SELECT-only reader SQL permissions were exercised,
including ordinary read contracts and rejection of unsafe runtime startup. Root read OpenAPI
still matches the frozen v1.0 contract. Existing replay, bounded retention and frontend/browser
regressions ran in the full suites. Tests do not promise exactly-once WS delivery.

The skipped cases are not counted as passes: both engines skip two immutable schema-0006 binary
checks and three optional older backup/container/DB-pause fixtures; PostgreSQL additionally skips
three Timescale-only aggregate cases. Exact names/reasons and JUnit/code hashes are in the evidence
JSON. The unchanged frozen suite was not rerun in milestone 2; milestone 1's separate 143-pass
runs per engine remain prior evidence only. Current frozen ZIP SHA-256 and all 53 managed source
hashes were reverified, with no distribution edits.

Ruff check/format passed (228 files); strict mypy passed (44 source/helper files); Alembic reports
single head 0008; disposable Compose validation and git diff --check passed. Existing Starlette/
HTTPX and legacy WebSocket deprecation warnings remain. No new production dependencies were added.

Two standard Dockerfile.api builds failed fetching build-system package metadata because Docker's
package network timed out. An initial offline variant invalidated a cached pip-install layer and
also failed DNS resolution. A corrected task-only recipe preserved that cached dependency layer
and applied --offline solely to the final uv sync, successfully building the minimal Linux image.
The image passed both production factory lifespans against real 0008 PostgreSQL with the actual
restricted principals and collector/concrete-provider/optional HTTPX imports blocked. This is a
**cache-built Linux runtime smoke**, not a passing standard fresh online build or a released image.
The evidence records the image ID and local recipe hashes. A first smoke harness accidentally
asked its own import blocker to find HTTPX; package metadata inspection corrected that assertion.

Development test failures were resolved: initial full PostgreSQL run had two missing-view-grant
failures (both fixed, then full suites passed); TLS certificate generation needed an explicit
OpenSSL config, and this workstation's injected pip truststore client wrapper needed replacement
with the original verifying stdlib SSLContext in the test harness. Certificate verification was
never disabled. Harness assertions initially used the wrong event column/source-status key and
were corrected to the inspected existing schema. Final reported suites have zero failures/errors.

## Preservation and remaining risks

The task-only tmpfs databases were removed after exact name/project/task-label/storage checks,
without volume deletion. The transient Linux smoke container removed itself. Legacy TradingView
app/database stayed stopped with identical image IDs and start/finish timestamps. No source SQL,
mount, provider connection, second collector, lifecycle change or data checksum was taken. This
is preservation evidence through absence of source actions and stable lifecycle, not a claim
that stopped source database bytes were independently hashed. Frozen v1.0 ZIP remains
3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a.

Standard fresh image build/final installation, v1.1 export/ZIP, target gateway/HTTPS/CA validation,
real multi-process ingestion restart/recovery, sustained throughput/load/soak, genuine current
provider quotes and real WAN failure recovery remain unqualified. The HTTP lost-response test
injects a post-commit transport-style failure and recreates the application/pool; it does not
kill a remote production process. Gateway unauthenticated abuse/header/connection controls are
required external configuration. Fixed-minute quotas allow boundary bursts; use consistent
limits across processes. Session expiry deliberately requires administrator re-admission.
Receipt/observation retention/archive policy must preserve retry safety. Coverage snapshot
chunking beyond the explicit range ceiling remains future design work.

The destination cannot recover source quotes lost before durable capture. Source follow-up must
capture candles/corrections/individual quotes before coalescing with commit-ordered positions,
immutable identities and a durable epoch, bounded retention of unacknowledged records, explicit
backpressure, immutable staged batches, separate candle/correction/quote checkpoints and recovery/
reconciliation after lost acknowledgments and outages. Latest-only source mode cannot reproduce
intermediate quotes. The installed legacy collector remains unmodified/candle-only; genuine
independent-price installation and live qualification still block complete production acceptance.

Recommended next milestone, after this review: design and implement opt-in source durability and
the standalone synchronization worker on isolated source-copy databases, with explicit session
re-admission, retention/backpressure and reconciliation tests. Any live collector switch remains
separately reviewed. No deployment, frozen ZIP replacement or next-milestone work was performed.
