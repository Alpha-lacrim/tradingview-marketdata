# FastAPI v1.1 — milestone 1 destination foundation

Date: 2026-10-15. Base: `4cedaedb172d89ca3d2566bcea477c8296643592`.
Status: destination persistence implementation; no HTTPS ingestion service or sync worker.
Company's first installation remains the future complete v1.1 release.

## Scope and decisions

The reviewed Phase A architecture is retained. This milestone narrows migration `0007` to
destination durability. It creates **no local outbox**, admission-budget service, collector
hook, provider connection or deployed process. Destination receipts do not recover observations
that were never durably captured on the collector.

The existing market tables, writer SQL and `events.append_events` remain authoritative.
The read API, WebSocket models and Go ChartData implementation are unchanged. Source head
is now `0007`; the immutable v1.0 source/ZIP still requires `0006`. The development source
has not been packaged or advertised as a completed v1.1 release.

## Changed files and rationale

| File | Change |
|---|---|
| `migrations/versions/0007_destination_ingestion.py` | Add producer sessions, exclusive source ownership, receipts, observation identities, source-version barriers, captured status and candle receipt high-water state. |
| `src/marketdata/persistence.py` | Caller-owned transaction helpers around existing writes; cross-process source locks; reusable staging tables cleared between helper calls; no-op receipt ordering and semantic volume-unit corrections; exact `0007` startup check and expanded read-role safety check. |
| `src/marketdata/replication_models.py` | Strict transport-neutral batch/record models, raw JSON ambiguity/size checks and canonical observation hashing. No HTTP endpoint. |
| `src/marketdata/replication.py` | One-transaction destination coordinator, batch/observation replay protection, source authorization/fences, captured-status expiry, receipt lookup and restricted-writer privilege validation. |
| `scripts/sql/v11_destination_roles.sql` | Explicit non-owner read/write roles, narrow table/sequence/function permissions and known optional Timescale views. No passwords supplied. |
| `compose.v11-m1-test.yaml` | Task-owned PostgreSQL 16.1 and Timescale 2.25.1/PG17 test containers, loopback-only ports and tmpfs; no deployed volumes. |
| `tests/test_destination_ingestion.py`, `tests/test_destination_migrations.py` | Real database durability, ordering, concurrency, permissions, migration and existing read-interface regressions, plus transport-model checks. |
| `tests/conftest.py` | Clear destination policy/receipt fixtures only inside the existing disposable `_test` fixture. |
| `tests/test_dual_ingestion.py` | Explicitly skip the two immutable-v1.0-binary tests on a `0007` DB. The frozen full suite is qualified separately on `0006`; current `0007` REST/WS tests remain active. |
| `CODEX.md`, `README.md`, `HANDOFF.md`, this report | Record the new source head, scope, verification and remaining work. Phase A report remains the reviewed historical proposal. |
| `.gitignore` | Allowlist only the reviewed role setup SQL; SQL dumps remain ignored. |

## Actual migration

`0007` is additive over `0006`. Existing financial rows, coverage, revisions, latest state,
journal events and journal epoch/head/floor are preserved. Eight new ordinary PostgreSQL
tables are used with either plain PostgreSQL or Timescale market tables:

| Table | Identity / purpose |
|---|---|
| `ingest_producers` | Stable UUID, non-secret credential key ID, enabled flag, active session UUID, increasing session generation and server-side session expiry. Actual credential authentication is milestone 2. |
| `ingest_source_owners` | Primary key `(instrument_id,provider,stream)` allows one authoritative producer per stream. Stores dataset epoch, increasing ownership generation and permitted observation statuses. |
| `ingest_batches` | Primary key `(producer_id,batch_id)`; exact raw payload SHA-256, stream/epoch, immutable committed acknowledgment and separate server ingestion time. |
| `ingest_observations` | Primary key `(producer_id,source_epoch,observation_id)`; source position, canonical content hash, full original normalized observation, outcome, receipt reference and server ingestion time. Receipt FK is deferred until commit. |
| `ingest_candle_heads` | Natural native-1m candle key; highest accepted ownership generation/source position and content hash, including no-op barriers. This is a delivery ordering barrier, not an independent financial serving table. |
| `ingest_coverage_heads` | Latest accepted authoritative coverage-snapshot version per instrument/provider. |
| `ingest_source_status` | Versioned per-source captured health, original capture/expiry, sending session, source ownership and server ingestion time. |
| `candle_observation_heads` | Highest actual collector receipt per native candle key, seeded from historical/latest candle receipts. Prevents an older correction from exploiting a newer no-op observation. |

Primary/foreign keys and CHECK constraints enforce identity, supported streams, valid statuses,
positive versions, fingerprint lengths, non-null receipts and status lifetime ordering.
Triggers reject decreasing session/ownership fences, changed session IDs or producer/epoch
ownership without a higher generation, and revival of an expired lease without increasing
the session generation.

Two narrow `SECURITY DEFINER` functions acquire shared row locks on producer/source policy.
They have a fixed search path, explicitly qualified policy tables and revoked PUBLIC execute.
The writer receives EXECUTE permission rather than UPDATE permission over authorization policy.
That allows admin revocation/transfer to serialize with in-flight accepted writes.

Downgrade drops only new destination metadata/functions. It preserves existing market/event
tables but loses new receipt/fencing history, so it is **not** an automatic production rollback.
Only task-owned test databases were migrated or downgraded in this session.

## Transaction and replay behavior

`DestinationWriter.commit(authenticated_producer, payload_bytes)` is an internal server boundary.
The UUID argument must come from trusted authentication; it is not proof of authentication by
itself. There is no exposed route that accepts this argument from the public.

1. Bound and decode exact UTF-8 JSON bytes; reject duplicate JSON keys and invalid values.
   Match the envelope producer to the trusted authenticated identity and hash the raw bytes.
2. Acquire one Store connection and one transaction with `synchronous_commit=on`. Lock producer
   policy and validate enabled/session/generation/expiry. Lock the batch identity and return
   its prior receipt when the fingerprint matches; reject a changed payload under the same ID.
3. Prelock quote UUIDs and producer/epoch observation IDs, lock the source policy rows, then
   acquire all market-source advisory locks in a fixed order. Validate exact registry identity,
   stream ownership, dataset epoch and allowed source status. These are transaction-scoped
   database locks, not process-local mutexes.
4. Check the observation ledger. An identical observation in another batch is `duplicate`;
   changed content or source position under that ID conflicts. For candle/coverage/status
   keys, compare `(ownership_generation,source_position)`; an older version is `superseded`,
   and conflicting content at an equal version is rejected.
5. Use the shared Store helpers on that same connection. Candles retain original receipt
   ordering, revision audit and coverage behavior. Quotes persist history while latest state
   uses the existing source-nanosecond/receipt/UUID guard. A delayed quote without source time
   is retained in history without becoming a latest candidate. Existing Store callers keep
   their prior latest-selection behavior through default helper arguments.
6. Write full import provenance/outcomes, update ordering barriers, and insert the final
   receipt. The existing event journal is written in the same transaction, using its existing
   locked commit-order clock and bounded retention. Recheck lease expiry before the receipt.
7. Exit the transaction successfully, then return the acknowledgment. Failed/deferred-constraint
   commits return no successful result. Retry exact bytes after an uncertain acknowledgment.

No-op financial observations produce no revision/event. Genuine `volume_type` or
`volume_additive` changes are semantic corrections and do update audit/history/latest state.
Operational metadata changes alone remain no-ops; their original imported provenance remains
in the observation ledger. Original domain source/collector timestamps are never replaced by
server ingestion time. Server ingestion time is a transaction observation, not an exact physical
commit timestamp.

Source-position barriers complement the shared candle receipt guard. A higher transport
position with an older collector receipt cannot force a financial regression; a recorded
observation and its ordering barrier are not proof that it changed materialized financial
state. Future reconciliation must compare the ledger/outcome and actual materialized rows.

Receipt replay requires a current valid session. An enabled, authenticated producer can use
the separate receipt lookup after session rotation, without writing data. Revoked producers
cannot retrieve receipts through this boundary. Transfer ownership by updating the existing
source-owner row and increasing its generation; do not delete/recreate ownership to reset fences.
Queued writes from the old owner are then rejected, while its previously committed data remains.

## Compatibility of existing callers

`Store.write(ticks)`, `write_quotes(ticks)` and `write_candles(candles,coverage)` retain their
signatures, transaction ownership and return types. Their existing callers were not modified.
`write_candles` still publishes its compatibility `last_candle_write` result after successful
commit; the destination path instead receives a request-local result dictionary.

New helpers `write_observations_in_transaction`, `write_candles_in_transaction` and
`apply_coverage_in_transaction` require an already active transaction and never acquire a
second connection or commit it. Multi-helper callers must prelock the union of sources through
`Store.lock_sources` before any journal append. Temporary staging is explicitly cleared on each
helper invocation, including multiple calls in the same outer transaction.

The only intended shared candle behavior corrections are the no-op receipt watermark, protection
of absent keys under concurrency, and semantic volume metadata corrections. The public read
schemas, API errors/authentication, WS schemas/replay, and ChartData arrays are unchanged.
Count/Value remain the existing compatibility zeros, and typed volume retains its existing rules.
No exactly-once WebSocket guarantee is added.

Coverage snapshots preserve declared unknown/not-expected partitions, subtract actually stored
native bars, and derive missing expected destination minutes. Earlier and disjoint windows can
extend coverage; an untracked bridge remains unknown. Source missing ranges remain provenance
in the immutable observation rather than being copied as proof of destination completeness.
On the wire these are cumulative whole-source snapshots: within one ownership generation a
newer accepted window must contain the previous accepted window. Non-cumulative updates fail
with `coverage_window_conflict`. This prevents a newer disjoint window from silently superseding
older historical coverage. The shared internal merge helper can still merge bounded windows;
the future publisher must assemble cumulative snapshots with honest unknown bridge partitions.
The 500-range ceiling is explicit; larger snapshots need a separately reviewed manifest/chunking
protocol rather than silent truncation or acceptance of incomplete coverage.

Captured status retains source capture and expiry (maximum 120-second lifetime), is invalidated
by lease/session/ownership changes, and cannot be refreshed by retry/upload time. Its internal
query does not yet replace the existing local `collector_status` singleton or provide the
remote REST/WS status projection; that adapter belongs to milestone 2. Status freshness is
separate from quote freshness and collector/API liveness.

## Wire decisions for milestone 2

The implemented models define ingestion protocol v1. These are transport-neutral foundations;
no HTTPS/authentication/rate-limit implementation is claimed.

- Batch: `protocol_version=1`, `producer_id`, `session_id`, positive BIGINT JSON integer
  `session_generation`, `source_epoch`, `batch_id`, `stream`, and nonempty `records`.
- Streams are `candles`, `quotes`, `coverage`, `status`. Corrections/backfill use candle records
  on the same `candles` stream and ordering domain; they are not independent competing writers.
  The later worker must track their delivery progress separately locally.
- Record: stable UUID `observation_id`, canonical positive decimal-string `source_position`,
  discriminated `kind` (`candle`, `quote`, `coverage`, `status`), and typed `observation`.
  Quote observation IDs equal original `tick_id`; identities/positions are immutable within
  producer/dataset epoch. Positions describe local change capture, never provider sequence.
- Monetary inputs are finite plain decimal strings, maximum 128 characters. Native volume
  requires a supported verified unit; unavailable volume stays null. Source nanoseconds use
  decimal strings on this wire and remain integers/NUMERIC-safe in persistence. Do not truncate
  nanoseconds or create missing bid/ask/last/source timestamps.
- Time inputs must be timezone-aware; models normalize them to UTC. Canonical client JSON
  should use `Z`. Non-UTC aware RFC3339 offsets are normalized; naive inputs fail. Genuine
  source and collector times remain distinct. Implausibly future source/collector times
  (over five seconds ahead of server admission time) fail rather than being rewritten.
- Hard safety ceilings: 2 MiB body, 500 candle/quote records, 50 coverage/status records,
  500 coverage partition ranges and 16 KiB observation metadata. HTTP settings may lower
  these ceilings. No compressed-body support is introduced here.
- Coverage includes exact source identity, minute-aligned start/end, disjoint bounded
  `unknown`, `not_expected`, `source_missing` ranges and captured session information.
- Status includes exact source identity, actual run ID, captured/expiry timestamps, running/
  database/queue state and a captured health object restricted to that source. Health's
  `updated_at` is required; retry parsing never generates a new source timestamp.
- Batch fingerprint is SHA-256 over exact raw bytes, including envelope. Whitespace changes
  under a reused batch ID conflict. Observation fingerprint covers canonical typed kind/content,
  excluding the delivery envelope; source position is independently checked for identity reuse.
- Persisted acknowledgment: protocol version, producer/epoch/batch, stream, payload hash,
  `state=committed`, original server ingestion time, per-observation outcomes and outcome counts.
  Same-batch replay returns the original result, not a newly constructed acknowledgment.
- Stable internal conflicts include `batch_payload_conflict`, `observation_payload_conflict`,
  `source_version_conflict`, `producer_session_fenced`, `source_not_authorized` and
  `source_identity_conflict`. HTTP mapping/body schema is still milestone 2 work.

Proposed later HTTPS routes remain `/api/ingest/v1/{candles,quotes,coverage,status}`, receipt
lookup and reconciliation. Authentication must resolve producer credentials independently of
the body, verify TLS and `X-Payload-SHA256`, enforce gateway/application limits, sanitize errors
and reject cross-role credentials. Credential key ID here is an identifier, not a bearer token.
Runtime policy/session renewal endpoints or administrative procedures must not permit an
expired session to resume without a new generation. All versions/epochs must survive restarts.

## Reproduction on disposable infrastructure

From the repository root, these commands target only the new task project:

```powershell
docker compose -p marketdata-v11-m1-tests -f compose.v11-m1-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://m1_test:disposable_test@127.0.0.1:55945/marketdata_v11_m1_test'
uv run --no-sync alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m1-full-pg16.xml
$env:DATABASE_URL='postgresql://m1_test:disposable_test@127.0.0.1:55946/marketdata_v11_m1_test'
uv run --no-sync alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m1-full-timescale.xml
```

Linux uses the same Compose file and Alembic/pytest commands, with `export DATABASE_URL=...`
and `export TEST_DATABASE_URL="$DATABASE_URL"`. Credentials above are deliberate disposable test
credentials, not production examples. Never substitute the collector DSN. Fixtures truncate
their `_test` tables; migration tests also create/drop uniquely named fresh `_test` databases
inside the task-owned cluster. Do not run suites concurrently against the same database.

For grants, review `scripts/sql/v11_destination_roles.sql` after migrating the dedicated new
database. It creates fresh role names and deliberately fails rather than reuse an existing role.
Provision private login credentials separately. Never grant the reader membership in the writer
or owner roles. `DestinationWriter.validate_permissions()` is mandatory at future ingest startup;
it is separate from Store's retained collector semantics.

## Verification results

Final measured results and JUnit/SHA-256 evidence are recorded in
`docs/fastapi-v1.1-milestone1-verification.json`. Tests are explicitly fixture/simulated
software tests, not proof of genuine current TradingView data.

| Suite / database | Passed | Skipped | JUnit seconds |
|---|---:|---:|---:|
| Root / PostgreSQL 16.1 / schema 0007 | 510 | 8 | 150.25 |
| Root / TimescaleDB 2.25.1 on PostgreSQL 17.7 / schema 0007 | 513 | 5 | 166.05 |
| Frozen standalone v1.0 / PostgreSQL 16.1 / schema 0006 | 143 | 0 | 36.64 |
| Frozen standalone v1.0 / TimescaleDB 2.25.1 on PostgreSQL 17.7 / schema 0006 | 143 | 0 | 40.46 |

Each root run includes **32 new milestone cases: 11 model/contract cases and 21 real database
cases**, all passing with zero skips. They cover fresh and populated migration, receipt/data/
event atomicity, lost-response replay through a new pool, payload/identity conflicts, concurrent
same-key writes, cross-producer UUID collisions, no-op/correction/volume-unit ordering,
nanosecond/delayed/receipt-only quotes, cumulative coverage, status expiry/session rotation,
ownership transfer blocked by an in-flight transaction, and actual restricted writer/read-only
roles through REST, ChartData and WebSocket publication.

Root PostgreSQL skips three Timescale-only aggregate cases; both root engines skip two legacy
dual-collector-to-exact-v1.0-binary cases and three optional older Batch 4 container/backup/
database-pause checks requiring their specific fixtures/images. These skipped checks are
**not** counted as passes. The standalone suite on 0006 does not pretend to rerun those two
legacy dual-collector tests or make the immutable binary compatible with 0007.

Ruff check passed; Ruff format check passed for 219 files; strict mypy passed for 40 source
files; Alembic source head is 0007; test Compose configuration and `git diff --check` passed.
Existing Starlette/httpx and legacy WebSocket dependency deprecation warnings remain.

During development, an obsolete draft-0007 disposable schema failed refresh after the draft
DDL changed; only task-owned tmpfs databases were rebuilt, followed by successful final fresh
and populated upgrade/roundtrip tests. A pytest-generated 2 MiB parameter ID exceeded Windows'
environment-variable limit; explicit short test IDs fixed that harness issue. Final suites
have zero failures/errors; lint/type issues found during implementation were corrected.

The frozen v1.0 suite runs with an explicit local-source PYTHONPATH and the existing interpreter
on separate `m1_v10_compat_test` databases migrated by its own unchanged migration sources to
`0006`. This verifies its existing package/API against its supported schema; it is not a fresh
dependency installation and does not claim the frozen binary supports `0007`.

No live collector/data SQL was executed. Before/after Docker inspection retains the same stopped
collector/database image IDs and start/finish timestamps. The v1.0 ZIP SHA-256 remains
`3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a`.
Both task-owned tmpfs database containers were removed after verifying their exact project,
task labels and storage. No volume deletion command was used. No stopped source database
content hash was taken: preservation evidence is unchanged lifecycle/image identity and the
absence of source SQL, mounts or data/volume actions.

## Remaining risks and next scope

No HTTPS ingestion, producer credential verifier, WAN failure handling, reverse-proxy limits,
sync worker, reconciliation service, source outbox, live quote qualification, sustained workload
benchmark, v1.1 ZIP or production installation has been implemented or verified. The current
coordinator favors bounded transaction correctness and processes versions sequentially; quote
batch throughput has not been qualified. PostgreSQL connection failures/deadlocks/uncertain
commits must receive retryable sanitized HTTP handling in milestone 2. Receipts and imported
observations are not automatically expired; size/archive policy must preserve replay safety.

The source-side follow-up is separate: durable capture before quote coalescing, complete candle/
correction change capture, an actual commit-order source position and dataset epoch, bounded
retention without deleting unacknowledged observations, backpressure, immutable staged HTTP
batches, durable checkpoints and source/destination reconciliation. Latest-only quotes cannot
reconstruct intermediate history. None of these needs is solved by the destination ledger.

The installed legacy collector remains unmodified and candle-only. Full independent-price
acceptance remains blocked until the separately reviewed quote-capable source runtime is
installed and genuinely qualified. Existing access approval remains recorded; company/provider
distribution scope and entitlements still belong in deployment governance documentation.

Recommended milestone 2: implement the separate ingestion FastAPI factory/role on this Store,
producer authentication and session admission/renewal, strict HTTPS batch/receipt adapters,
error mapping, cross-role authorization, limits and captured-status projection, with isolated
HTTP/TLS tests. Keep reconciliation transport, the local worker/outbox and all live rollout
outside that scope until separately reviewed. Stop milestone 1 for review before that work.
