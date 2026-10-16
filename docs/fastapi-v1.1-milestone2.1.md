# FastAPI v1.1 — milestone 2.1 review

Implemented secure automatic recovery of an expired session for the same still-authorized
producer. The approved milestone-2 runtime was extended; it was not rebuilt or replaced.
Stop for review. No synchronization worker, collector change, company deployment, commit or
push was performed. The company still has no v1.0 installation; the frozen release stays intact.

Read the [exact recovery protocol and Windows/Linux examples](fastapi-v1.1-session-recovery.md),
[updated ingestion OpenAPI](fastapi-v1.1-ingest-openapi.json), [runtime instructions](fastapi-v1.1-runtime.md)
and [verification evidence](fastapi-v1.1-milestone2.1-verification.json).

## Files and rationale

| File | Milestone 2.1 delta |
|---|---|
| migrations/versions/0009_session_recovery.py | Additive recovery receipt table and tightly scoped SECURITY DEFINER recovery function; 0008 remains byte-for-byte unchanged. |
| src/marketdata/session_recovery.py | Strict bounded request/receipt models and caller-owned outer transaction; return only after durable commit. |
| src/marketdata/ingest_api.py | Adds POST /api/ingest/v1/session/recover through existing HTTPS/auth/quota/deadline middleware, sanitized errors, typed recovery OpenAPI and startup checks. Existing route handlers/renewal semantics remain intact. |
| src/marketdata/persistence.py | Exact-head check 0009; reader rejects recovery-table writes or recovery-function EXECUTE. Financial writer and query behavior unchanged from approved M2. |
| scripts/sql/v11_destination_roles.sql | Ingestion receives only EXECUTE on the new function; no producer/ownership UPDATE or recovery-table SELECT/DML. Reader receives no recovery privileges. |
| tests/test_session_recovery.py | 27 new contract/security/database/process cases using the approved runtime fixture. |
| tests/process_recovery_worker.py | Test-only TLS child server paused at transaction boundaries and killed by its owning test. It is not a collector or synchronization worker. |
| tests/test_ingest_migrations.py; test_destination_migrations.py | Current-head expectations advance to 0009; prior upgrade/fresh-install assertions remain. |
| compose.v11-m21-test.yaml | Separate task-labeled tmpfs PostgreSQL/Timescale services, loopback 55965/55966, database marketdata_v11_m21_test. |
| New recovery guide/OpenAPI/report/evidence; protocol/runtime guides; README/CODEX/HANDOFF/deployment banner | Exact client transitions, ownership/retry rules, provisioning/grants, clock expectations, reproducible checks and next scope. |

Against the approved milestone-2 source hashes, only five existing executable/config/test files
changed: ingestion API, Store head/reader permission checks, grants script and two migration-test
head expectations. Fourteen recorded M2 files remain byte-identical, including migration 0008,
producer authentication, financial transaction coordinator, observation models, status projection,
production command, sample/benchmark setup and all M2 HTTP tests. These checks do not imply every
untracked file was new to this milestone: prior Phase-A/M1/M2 changes remain uncommitted and were
preserved. HEAD remains 4cedaedb172d89ca3d2566bcea477c8296643592. TradingViewConvHistory/ and other
worktrees were untouched.

## Database and recovery semantics

0009 descends from 0008 and adds one ordinary PostgreSQL table, ingest_session_recoveries.
Its primary key is producer/recovery ID; a unique producer/previous-session/generation constraint
prevents multiple transitions from the same previous fence. It retains the canonical request,
SHA-256 fingerprint, original credential key ID for audit, previous/new identities/generations,
server recovery time and immutable committed receipt. No automatic deletion/retention purge is
introduced. Neither runtime gets direct table SELECT or mutation privileges.

The function validates the current enabled producer and key under FOR UPDATE, then locks
requested stream rows FOR SHARE in stable order and verifies the complete manifest, current
producer owner, exact dataset epochs and ownership generations. It requires the expected
previous session/generation to match and be expired. A new random session UUID, generation+1,
120-second lease and receipt commit atomically. The existing SQL fence trigger remains active.
No source-owner row, producer definition, market row, event journal, observation ID or batch
receipt is reassigned, fabricated or discarded. Existing Store market transactions and frontend
contracts are reused unchanged.

Identical logical requests under one recovery ID return the original result while its resulting
session is still current and ownership still matches. Replay never renews the lease. Competing
IDs expecting the same old session have one winner. Old requests cannot reveal/follow a later
session: subsequent recovery/admin rotation makes them obsolete, and ownership changes reject
both new recovery and saved-response replay. Revocation is rechecked inside the transaction,
even after HTTP authentication. Valid newly rotated credentials may retrieve their known
current operation; old retained keys cannot. Intentional installation transfer uses a distinct
producer/credential and administrator-updated ownership generations.

Normal process restarts and temporary outages retain durable local state. An expired but
unchanged authorization can recover automatically. Unknown/replaced sessions, changed grants,
missing durable state, revocation or deliberate ownership transfer still fail closed. A clone
with the exact same credential and winning recovery state is the same principal; physical
installations must not share identities. The future worker needs a local ownership lock.

The additive upgrade preserves existing market/event/receipt data and producer/owner rows.
Fresh installation and populated 0006/0007/0008 upgrades ran on isolated databases. Downgrading
0009 drops only its function/receipt table and loses recovery retry history; it is not an
operational rollback procedure or permission to migrate the stopped collector. The updated
runtime and head must be installed together; old exact-head binaries remain version-bound.

## Actual passing and skipped verification

| Final root suite / head 0009 | Passed | Skipped | JUnit seconds |
|---|---:|---:|---:|
| PostgreSQL 16.1 | 550 | 8 | 165.714 |
| TimescaleDB 2.25.1 / PostgreSQL 17.7 | 553 | 5 | 186.223 |

Each engine ran all **27 new cases: 6 unit/contract cases and 21 real database cases**, with no
new skips/failures. Coverage includes ordinary expiry, live-session rejection, stable epochs,
old-session upload fences, delayed pending batches and old receipt access, independent/deduplicated
observations after a new envelope, identical and competing recovery races, payload conflicts,
conditional replay after lease expiry, later obsolete receipts, source-owner transfer and lock
races, disabled/revoked/unauthorized producers, rotation and revocation after authentication,
transaction rollback and actual terminated database connections.

Two tests per engine start real TLS ingestion processes, verify the certificate, kill only those
child processes before commit or after commit but before response, restart the TLS process and
retry the same recovery request. They prove rollback/retry and committed-response replay across
actual process termination, with one generation advance and one durable acknowledgment. OpenSSL
was present, so these tests ran; no TLS verification was disabled. Existing M2 HTTPS/read-only
REST/ChartData/WS, replay, concurrency, ordering and bounded-retention tests also ran in the full
suites. Existing read OpenAPI still matches frozen v1.0. No exactly-once WS guarantee is added.

Additional automated permission probes on both engines disabled the reader's session read_only
setting and confirmed SQL denial of new-function execution and receipt-table SELECT. An
accidental EXECUTE grant was then detected/rejected at production reader startup. These probes
are supplementary checks, not extra pytest passes.

The existing skips remain explicit: both engines skip two immutable schema-0006 binary checks
and three optional older container/backup/DB-pause fixtures; PostgreSQL also skips three
Timescale-only aggregate cases. Exact names/reasons are in the evidence JSON. Skips are not
passes. Frozen v1.0 tests were not rerun; its prior M1 evidence remains historical. All 66 ZIP
entries still match the frozen tree, ZIP hash is unchanged, and 0008 matches its approved hash.

Ruff check/format passed (235 files), strict mypy passed (45 source/helper files), Alembic has
one head 0009, Compose config and git diff --check passed. Existing Starlette/HTTPX/legacy WS
deprecation warnings remain. No new production dependency or coordination service was added.
No M2/image rebuild was attempted. The prior standard-image package-network limitation remains
for release packaging; this milestone does not relabel the earlier cache build as a fresh build.

Development harness issues were corrected before final full runs: a duplicated suffix in the
new disposable DB name, an imported fixture registration, a candle helper argument, a cached
pg_stat_activity snapshot in the lock observer, and a termination-listener race in a connection
failure assertion. The task-only empty DB containers were recreated once after validating their
labels/tmpfs. Final full suites have zero failures/errors; no application tests were weakened.

## Preservation and remaining operational risks

The live TradingView app/database remained stopped with identical image/start/finish metadata.
No source SQL, data mount, provider connection, second collector, production volume change,
company deployment, commit or push occurred. A source-data checksum was not taken; preservation
evidence is unchanged lifecycle/images and absence of source actions. The frozen v1.0 ZIP remains
3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a. Task DB containers were removed
only after exact name/project/task-label/tmpfs verification, with no volume operations. Test TLS
child processes were all reaped. Session receipt IDs/credentials require private local storage.

Recovery alone cannot make source observations durable. The local outbox/worker is not implemented
and real laptop/WAN, sleep/reboot, company gateway/CA and source quote-history recovery are not
qualified. Dataset/ownership manifest changes during a prolonged outage intentionally require
reviewed configuration. The 1000-grant/256-KiB ceiling is explicit; larger producer scopes require
reviewed protocol/provisioning changes rather than omitted grants. DB clock steps affect lease
length operationally; keep clocks synchronized. Existing five-second source/collector future-time
rejection is unchanged; retain/quarantine/retry original observations without rewriting timestamps.
A recovered session and healthy API do not imply fresh quotes or fresh captured collector status.

## Recommended milestone 3 design

Begin with a separate opt-in source-durability design on disposable source-copy databases,
verifying installed schema 0004 versus current code and exact source mappings. Do not migrate
or start the live collector. Prefer an atomic local outbox committed with every candle change,
legitimate correction and individual quote before coalescing; the bounded destination/frontend
journal and latest-only quotes cannot supply this guarantee. Use a durable dataset epoch,
commit-ordered source positions, stable observation IDs and full original timestamps/decimals.
Define bounded retention, disk-budget alarms and explicit backpressure; never delete unacknowledged
records or silently claim completeness. Source schema/runtime rollout remains separately reviewed.

Then design a standalone worker with one local ownership lock, bounded immutable batches, durable
session/manifest/recovery-operation state and transactional delivery checkpoints. Resolve uncertain
recovery first, check old batch receipts, use new batch IDs for new session envelopes, and preserve
observation IDs/positions/epochs. Track candles/backfill, corrections and quotes independently;
include overlap/reconciliation for late corrections rather than bucket_start-only polling. Test
outage/sleep/process crash/source restore, lost acknowledgments and quota/disk exhaustion using
isolated infrastructure, then review genuine independent quote qualification and the approved
installation switch separately. No milestone-3 code or live rollout was started here.
