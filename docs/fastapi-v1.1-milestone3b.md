# FastAPI v1.1 — Milestone 3B review

Implementation is limited to destination coalesced quote ordering and an independent durable
source HTTPS worker. No live collector/provider connection, legacy/company database migration, deployment,
commit, push, ownership transfer or frozen-package replacement is authorized/performed.

Read the [exact worker/protocol/operations guide](fastapi-v1.1-sync-worker.md),
[ingestion OpenAPI](fastapi-v1.1-ingest-openapi.json) and
[verification JSON](fastapi-v1.1-milestone3b-verification.json).
The company still has no v1.0 installation. This milestone is not a complete production release.

## Findings and implementation

The approved source capture provides committed, versioned full quote snapshots; destination
latest state previously used the primary timestamp/receipt/UUID guard. Those are different
orders. The narrow extension adds a fenced destination position barrier, validates field
provenance and uses the existing Store writer/latest-state/event-journal transaction to materialize
accepted snapshots. Equal-time snapshots and a new primary field with an older genuine clock
are valid. Old snapshots cannot overwrite newer positions. Epochs/ownership generations remain
authoritative, and stale producers cannot regain access. Legacy read APIs are unchanged.

A committed receipt now carries snapshot-state evidence. The worker also verifies **current
actual financial state** through bounded authenticated reconciliation before accepting a first
local receipt. It does not equate HTTP 200 with quote parity. Safe supersession requires a newer
accepted version in the same context and an actual payload matching a known local/staged snapshot.
Financial candle hashes preserve arbitrary decimal precision, typed volume and additive metadata.
Recurring quote reconciliation compares full typed fields/provenance and ownership/epoch/version,
including a damaged restored payload that retains the original derived identity.

The worker consumes the existing SourceCapture interface, retaining its commit-serialized clock,
UUIDs, export markers, paging, quote versions and acknowledgment semantics. Narrow permission
paths and explicit resnapshot operations extend those helpers; no competing outbox/quote allocator
or market-data writer was introduced. Source counters, source financial data and pending versions
remain independent of company ingestion availability.

## Changed files and rationale

The verification JSON lists exact paths and hashes relative to the saved pre-M3B workspace;
the following is the complete functional grouping.

| Files | Purpose |
|---|---|
| migrations/versions/0010_quote_snapshots.py | Add ingest_quote_heads, durable destination identity and producer/epoch high-water index. |
| source_migrations/versions/sc0002_sync_worker.py | Add fenced worker owner/state, immutable batches, summarized receipt archive and commit guard; narrow state/capture/storage functions. |
| src/marketdata/quote_snapshots.py | Validate/merge field provenance without inventing times, prices or clears. |
| src/marketdata/reconciliation.py | Strict bounded owner/session-authorized financial/head inspection, exact candle hashes. |
| src/marketdata/replication.py | Position/ownership quote barrier, transactional quote-state receipt evidence, narrow missing-candle restoration through shared persistence. |
| src/marketdata/persistence.py | Destination head 0010, source sc0001/sc0002 compatibility, reader privilege checks, caller-owned ordered latest snapshots and approved missing-row restoration options. Existing defaults retained. |
| src/marketdata/ingest_api.py | Add bounded authenticated reconcile operation and typed OpenAPI; existing admission/recovery/upload middleware retained. |
| src/marketdata/source_capture.py | Restricted function paths for existing acknowledgments/cleanup/seeds; bounded explicit resnapshot using the same clock/UUID allocator; safe terminated-lock cleanup. |
| src/marketdata/sync_config.py | Bounded private configuration, HTTPS origin and complete manifest validation. |
| src/marketdata/sync_http.py | Certificate/hostname verification, no redirects/environment proxies, total deadline, bounded responses and sanitized classifications. |
| src/marketdata/sync_state.py | Fenced durable transactions, immutable bytes/hash, receipt/state/ack phases and separate continuity anchor. |
| src/marketdata/sync_worker.py | Independent scheduling, durable recovery, actual-state receipt verification, enrollment/reconciliation, retry/quarantine and metrics/CLI. |
| scripts/sql/v11_source_sync_role.sql; v11_destination_roles.sql | Explicit distinct source worker/company writer/reader privileges; no financial source DML from worker. |
| tests/test_sync_worker.py; process_sync_worker.py; process_sync_destination.py | Unit, real PostgreSQL/TLS, concurrent ownership, process-kill, restart, restore and backlog cases. |
| tests/test_destination_migrations.py; test_ingest_migrations.py; test_session_recovery.py; test_source_capture.py | Current-head expectations updated; approved behavioral checks retained. |
| pyproject.toml; .gitignore; compose.v11-m3b-test.yaml | Console entry point, publishable source-role script, isolated task-labeled tmpfs test configuration. |
| OpenAPI/protocol/runtime guides; new worker guide/report/evidence; README/CODEX/HANDOFF/deployment banner | Actual boundaries, operations, verification and next handoff. |

Existing changes from previous milestones remain uncommitted. They are not relabeled as M3B
changes. Base main revision remains `4cedaedb172d89ca3d2566bcea477c8296643592`.
The [milestone-only review patch](../artifacts/v11-m3b-task.patch) compares these 34 source/doc
paths with the saved pre-M3B workspace, including its already approved uncommitted files.

## Migration, permissions and concurrency

Destination `0010` descends from `0009`; source `sc0002` descends from `sc0001` on base `0006`.
Approved old migration bytes remain untouched. Fresh destination installs, populated upgrades,
representative old source upgrades and explicit sc0001→sc0002 preservation are tested separately.
No destination migration is installed on a source. No runtime automatically migrates anything.

The local advisory-lock backend, SQL owner token/generation/PID/backend-start tuple, row locks
and deferred commit checks fence old tasks. Claim and state changes serialize in PostgreSQL;
the watchdog cancels asynchronous work after loss. A response from a remotely committed upload
cannot grant a stale local task checkpoint access. Role tests exercise actual denied SQL writes.

Delivery is four separate recoverable steps: immutable staging commit; verifying HTTPS; validated
receipt commit; source acknowledgment commit. Cleanup follows acknowledgment. Candle ranges do
not skip unacknowledged events, and quote N acknowledgment cannot clear N+1. Durable delivery
counts distinguish completed candles, corrections, history seeds and quote/status/coverage records.
They count intended observations acknowledged, not newly created financial revisions.
Coverage/status round-robin progress also advances on absent, expired or unchanged source state;
multiple-stream tests verify that idle instruments cannot starve later grants.

Session state preserves exact unresolved M2.1 recovery bytes/ID and complete manifest. Lost results
replay that operation. New envelopes receive new batch IDs while retaining original observation
records; old bodies remain immutable. No unknown replacement-session discovery, automatic epoch
reset, ownership edit or new producer creation exists.

## Verification interpretation

Tests inject market observations and use real local PostgreSQL/TLS; none proves genuine live
provider acquisition. Suites run sequentially against unchanged source (137 hashed implementation,
test, configuration and role-script files). Final acceptance results:

| Suite | Passed | Skipped | Failures / errors |
|---|---:|---:|---:|
| PostgreSQL 16.1, full 636 cases | 628 | 8 | 0 / 0 |
| TimescaleDB 2.25.1 / PostgreSQL 17.7, full 636 cases | 631 | 5 | 0 / 0 |

All 56 new worker cases passed on each engine, including 50 real-database integration cases; none
was skipped. Root lint and formatting pass (256 files); strict mypy passes (51 source files).
Destination/source migration heads, Compose configuration and git diff checks pass. The rebuilt
wheel installs and its console help runs in a fresh task-owned environment; this is CLI/import
smoke, not complete v1.1 ZIP/image installation qualification. Root package metadata remains 0.1.0;
the API contract remains v1.1 and the frozen v1.0 package is not replaced.

Skipped cases are listed individually in the verification JSON and JUnit. Both engines omit the
two frozen v1.0 binary scenarios requiring their immutable 0006 schema and three optional historical
Batch3/4 container/backup/production-image scenarios. PostgreSQL additionally skips three
Timescale aggregate variants. Current-head REST, WebSocket, replay, ChartData and restricted-role
suites execute; no skipped scenario is relabeled as passing.

Development failures are retained in ignored artifacts and are separate from acceptance. New
sc0002 CASE syntax and stale head/OpenAPI assertions were fixed without changing approved
migrations. Mixed parent/child source versions invalidated earlier process runs; final suites
freeze the source throughout. The task-owned Windows TLS harness now bounds shutdown, and
restart tests discard dead-process TLS pools/monotonic timers. A turn interruption stopped the
foreground disposable daemon; the 9 connection errors from that unavailable service are not
counted as passes. Only its labelled task services were restarted. Full payload parity and idle
metadata fairness issues found during review now have direct regression checks.
An intermediate Timescale run passed 630 cases/skipped 5 but failed a single-attempt recovery
test. Eight isolated diagnostic reruns passed; the failure itself did not preserve enough state
to classify its transient cause. The test now injects a transport interruption and verifies
unchanged staged bytes, retained source version, actual persisted backoff and eventual parity.
The task TLS server's idle expiry exceeds the client pool's expiry; explicit connection-loss
tests remain. No production transport, ordering or ownership rule was loosened for this change.

Coverage of the requested acceptance groups:

| Requested cases | Evidence in suites |
|---|---|
| 1–10 quote ordering | Forward/reverse/equal-time/concurrent snapshots, older primary, independent clocks, partial fields, same-price observations, field parity, recovered sessions and new epoch positions. |
| 11–20 source consumption | Quote-position gaps, actual byte splits and partial acknowledgment, old quote ack, bounded seed reuse, competing worker processes, lock loss during TLS and commit, denied local writes and cleanup gates. Existing 3A transaction/cursor races retained. |
| 21–32 transport/recovery | Verified TLS and untrusted CA rejection; duplicate immutable receipts; lost upload/recovery responses; 429/503; expiry; server process kills before/after upload and recovery commit; revocation, transfer, unknown session and token rotation; Python and actual worker process crash boundaries. |
| 33–42 reconciliation/operations | Genuine simulated historical correction, 125-event bounded backlog with continuing quote delivery, cumulative coverage/captured status, missing candle/quote detection and explicit resnapshot, restore anchor/high-water failures, source capacity rollback, and unchanged REST/WS/ChartData in existing full suites. |

The 125-event case verifies bounded scheduling/retained progress, not sustained production capacity
or the 1,000,000-event retention limit. Existing 3A includes a simulated 100-stream capture test.
There is no complete individual tick-history guarantee or exactly-once WebSocket promise.

## Preservation and remaining qualification

Frozen v1.0 ZIP SHA-256 remains
`3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a`.
Verification compares all 66 ZIP entries with the frozen tree and the pre-M3B file baseline.
The original collector/database/Docker volumes were not read/mutated/restarted and no provider
connection was established. Their prior stopped/schema0004 state is historical evidence, not
a new content checksum or lifecycle query. Unrelated worktrees and TradingViewConvHistory remain
outside this task. No source/company installation was performed.

Tests used a newly created WSL distribution and independent Docker 27.5.1 daemon with its own
socket/data root, vfs storage and task-labelled tmpfs containers. Ports 55985/55986 were limited
to this disposable setup. Engine versions, image digests, container IDs and exact cleanup are
recorded in the verification JSON. After both suites completed, only those verified containers
and that task distribution/virtual disk were removed. Logs, JUnit, baseline copies and the
worker wheel remain under `artifacts/v11-m3b-*` for review. No Docker Desktop repair was attempted.

Operational limits remain explicit:

- Full company gateway/CA deployment, Windows sleep/reboot/Task Scheduler and Linux systemd
  installation, real WAN faults and sustained realistic universe/backfill load remain unqualified.
- Independent genuine TradingView quote acquisition in the actual old installation remains
  unverified. Code/tests with injected observations do not close that production acceptance gate.
- Arbitrary partial financial corruption with intact newer barriers may need administrator repair;
  the worker detects/stops rather than rewriting clocks or bypassing accepted corrections.
- Correction-manifest comparisons are bounded to 128 rows per candle key; larger histories are
  flagged incomplete. Pre-enrollment revision audit history is compared/reported but cannot be
  reconstructed through protocol v1 without inventing absent original observation timestamps.
- Source database plus private token/anchor cloning cannot distinguish physical installations;
  use distinct producer credentials and explicit reviewed transfer. Keep clocks synchronized.
- Source logical budgets exclude physical heap/index/WAL/disk. Replaced/quarantined batches and
  summarized receipt/export/destination-ledger audit growth require monitored retention planning.
- Fragmented coverage beyond current protocol bounds needs reviewed protocol work; no truncation.
  Status transports an authentic matching captured health object and does not assert independent
  quote freshness from candle-collector health.

Recommended Milestone 4: freeze/review this design and permission SQL, qualify fresh v1.1 packaging
and split company roles behind the real gateway/CA on isolated infrastructure, stress agreed
instrument/backfill/outage budgets, exercise Windows sleep/reboot and Linux service operations,
rehearse consistent backups/restore/explicit producer handover, and separately qualify genuine
dual TradingView acquisition and licensing/redistribution rights. Review the actual legacy upgrade
and rollback plan before any source rollout. Stop after M3B review; no deployment is implied.
