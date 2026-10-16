# Milestone 3A review: durable local capture

Scope: source durability only, on isolated injected observations. No HTTPS synchronization
worker, live collector change, company deployment, commit, push or new release ZIP.
The destination milestones remain the approved foundation for the company's future first v1.1
installation. Read the [verified audit, compatibility matrix, migration/restore policy and exact
milestone 3B consumer contract](fastapi-v1.1-source-capture.md).

## Implementation and changed files

This list describes this milestone's changes relative to the initial dirty workspace, not all
earlier uncommitted work shown by Git. Five pre-existing executable source files changed;
all other pre-existing application/migration files match the captured baseline.

| Files | Rationale |
|---|---|
| `alembic-source.ini`; `source_migrations/env.py`; `source_migrations/versions/sc0001_local_capture.py` | Independent, additive source extension on base 0006, separate version table and refusal of destination 0009. Persistent epoch/clock, unacknowledged candle outbox, export markers, latest quote state and shared receipt guard. Existing0004 source upgrade goes only through existing 0005/0006 first. |
| `src/marketdata/source_capture.py` | Local transaction hooks, stable event identities, serialized capture positions, field-aware latest snapshots, bounded durable consumer reads, acknowledgment CAS, safe cleanup, single-consumer session lock and resumable enrollment. No HTTP or provider transport. |
| `src/marketdata/persistence.py` | Explicit source compatibility/activation guard; hook shared candle and quote transactions. Source-only full snapshots use field guards; raw quote history remains original deltas. Existing Store callers/signatures/counts and default destination behavior remain. |
| `src/marketdata/config.py`; `.env.example` | Default-disabled independent-collector-only flag; retained candle row/encoded-byte budgets. API/ingest/combined roles cannot accidentally activate source capture. |
| `src/marketdata/providers/tradingview.py` | Generic timestamped_fields metadata identifies last as the only price field with lp_time. Existing quote transport, identity checks, delayed rejection and scoped aliases stay unchanged. |
| `src/marketdata/price_selection.py`; `src/marketdata/events.py` | Preserve selected-field clocks when quoting a partial-field snapshot. New bid/ask observations or successful delivery cannot refresh an old last. Journal keeps only validated timestamp strings; existing arbitrary-metadata filtering and all public models stay intact. |
| `tests/test_source_capture.py` | 22 cases: one unit case, twenty-one real-DB cases including parameterized upgrades/clock races and caught-error savepoint rollback; injected observations and a task-only abrupt child-process exit. A 100-stream candle/quote batch verifies bounded capture without raw tick history. |
| `compose.v11-m3a-test.yaml` | New task-owned PostgreSQL 16.1 and Timescale2.25.1/PG17 services, loopback ports 55975/55976, labels and disposable tmpfs, no collector/volume. |
| `docs/fastapi-v1.1-source-capture.md`; this report; `docs/fastapi-v1.1-milestone3a-verification.json` | Audit/mappings, exact consumer/wire boundary, operations/restore policy, reproducible evidence and remaining acceptance limits. |
| `README.md`; `CODEX.md`; `HANDOFF.md` | Current source/destination boundary, opt-in procedure, lock order, verification and next reviewed milestone. Historical approved reports/protocol/OpenAPI remain unchanged. |

## Transaction, ordering and delivery guarantees

Native candle changes reuse the existing Store financial SQL, latest state, revision audit,
tri-state coverage and frontend committed journal. The source outbox, export marker and their
clock/counters commit in the **same transaction**. Identical bars emit no false correction;
old corrections are rejected by the receipt barrier, including a newer identical observation.
Changed old buckets and late new buckets receive new capture positions regardless of time.
Source-only savepoints also prevent partial helper commits when an outer caller catches a
Python validation/backpressure exception and continues its transaction.
Durable change_kind identifies completed/corrected/seeded events for separate worker progress;
the wire record remains the existing protocol-v1 CandleRecord.

Positions use a locked transactional clock row, not a sequence. A later writer cannot publish
a higher position while an earlier allocating transaction remains invisible. Rollback rolls
back both allocation and observations. Tests explicitly hold the first transaction open on a
different instrument, observe the empty committed page, then release it through commit and
rollback. Source locks -> capture clock -> existing event clock is the common lock order.

Quotes are partial inputs and coalescible durable full state. Missing/null is no update; fields
retain their exact original value, timestamp/ns, receipt, ID, status, sequence and metadata.
Source-time/receipt/UUID guards run while PostgreSQL locks serialize all supported writers.
The new snapshot/position and local latest state commit atomically with the frontend event.
An old acknowledgment advances only acked_version N and leaves N+1 pending. No mandatory raw
tick-history retention was introduced; optional existing history still stores original deltas.

Source capture is not enabled by schema installation or application startup. It requires the
explicit collector flag and base 0006 + sc0001. A source accidentally started without the flag
fails with an instruction **not** to run destination upgrade head. A destination refuses the
source migration. No root0007–0009 migration was edited, reused as a source migration or applied
to a source database in this implementation.

## Actual passing, skipped and not-run results

| Final complete suite | Passed | Failed/errors | Skipped | Duration |
|---|---:|---:|---:|---:|
| PostgreSQL 16.1 | 572 | 0 | 8 | 182.47s |
| TimescaleDB 2.25.1 on PostgreSQL 17.7 | 575 | 0 | 5 | 202.19s |

JUnit: `artifacts/v11-m3a-releasecheck-pg16.xml` and
`artifacts/v11-m3a-releasecheck-timescale.xml`. Both contain all 22 source cases, with **zero
source skips**: one unit and 21 real-database cases. These counts are included in the full suites,
not additional passes. The caught-error savepoint test separately passed on Timescale before
these final full runs. Machine-readable results, hashes and exact skipped names are in
[the verification JSON](fastapi-v1.1-milestone3a-verification.json).

Both engines skip two frozen-0006 binary compatibility cases and three older optional fixtures
(Batch4 backup/restore, Batch3 DB pause, Batch4 Linux production-image smoke). PostgreSQL also
skips three Timescale aggregate cases. They are **not passing tests**. The immutable v1.0
runtime suite was not rerun in this milestone; its source/ZIP/manifest integrity was verified.
Current destination 0009 REST/ChartData/WebSocket, journal retention/replay, authentication,
restricted roles, producer/session fencing and ingestion receipt tests actually passed.

Ruff check and formatting passed (242 files), strict mypy passed (45 source files), Compose
configuration, destination/source Alembic heads and `git diff --check` passed. The 100-stream
case is a functional bounded-workload test, not a production throughput benchmark. Genuine
TradingView, full remote-worker/WAN recovery, physical laptop restart/sleep, source restore/clone
automation, v1.1 packaging and company installation were **not run**. The one initial parallel
Timescale 50 ms startup-timeout failure is retained and explicitly distinguished from these
successful sequential results; the existing timeout test was not weakened or modified.

## Verification method and preservation

All database tests use task-owned `_test` databases. Source fixtures create uniquely named
databases, install base 0006 and sc0001, and drop only those databases. Upgrade fixtures populate
representative0004/0006 market/latest rows, including a newer no-op latest receipt, and verify
preservation after upgrade. These are synthetic representative schemas, not copies of the
stopped production-like collector database.

Docker Desktop's Linux daemon was unavailable. No Desktop startup or existing collector
container lifecycle operation was performed. Tests instead used a separately imported disposable
WSL distribution named `marketdata-v11-m3a-test`, a downloaded official static Docker 27.5.1
daemon with its own `/tmp/marketdata-v11-m3a` data/exec roots, pidfile and Unix socket. It uses
no bridge/iptables changes, task-only containers, host networking with PostgreSQL explicitly
listening on 127.0.0.1, and tmpfs PGDATA. This follows Docker's documented
[separate daemon configuration](https://docs.docker.com/reference/cli/dockerd/).
It never mounts/reuses Docker Desktop's data, collector database or existing volumes.
Container/image/version evidence and final results are in the verification JSON.
After verification, the exact labelled tmpfs containers were stopped/removed, the verified
task daemon shut down cleanly, and the task WSL distribution was unregistered only after its
absolute path was checked inside this workspace. Both task database ports are closed.

Baseline evidence is `artifacts/v11-m3a-baseline.json`; JUnit artifacts retain every recorded run,
including an initial parallel-run Timescale failure in an existing 50 ms startup-timeout test.
That test passed alone without code/test changes. Full sequential reruns are the acceptance
results; do not treat the failed run as a pass or add repeated runs to unique test counts.
Existing dependency deprecation warnings remain; dependencies were not changed.

The frozen ZIP SHA256 remains
`3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a`.
Frozen source/manifest bytes and destination migrations 0007/0008/0009 are compared with the
initial workspace hashes. No existing collector SQL, data mount, provider connection, second
collector, old container start/stop, production migration, company installation, commit or push
occurred. The original Docker daemon was unavailable, so its present lifecycle metadata and
database contents were **not** re-queried/checksummed; preservation evidence is absence of
actions against it, not a newly asserted database-content hash.

## Reproduce the full suites

From a clean test shell, use only this task project and these disposable DSNs. Docker Desktop
must already be available for the normal Compose path; do not start it if doing so would violate
the operator's collector-preservation constraints. Do not run suites concurrently on one DB.

PowerShell:

```powershell
docker compose -p marketdata-v11-m3a-tests -f compose.v11-m3a-test.yaml up -d --wait
$env:SOURCE_CAPTURE_ENABLED='false'
$env:APPLICATION_ROLE='api'
$env:DATABASE_URL='postgresql://m3a_test:disposable_test@127.0.0.1:55975/marketdata_v11_m3a_test'
uv run --no-sync alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m3a-releasecheck-pg16.xml
$env:DATABASE_URL='postgresql://m3a_test:disposable_test@127.0.0.1:55976/marketdata_v11_m3a_test'
uv run --no-sync alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m3a-releasecheck-timescale.xml
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy
git diff --check
```

Linux:

```bash
docker compose -p marketdata-v11-m3a-tests -f compose.v11-m3a-test.yaml up -d --wait
export SOURCE_CAPTURE_ENABLED=false APPLICATION_ROLE=api
export DATABASE_URL='postgresql://m3a_test:disposable_test@127.0.0.1:55975/marketdata_v11_m3a_test'
uv run --no-sync alembic upgrade head
export TEST_DATABASE_URL="$DATABASE_URL"
uv run --no-sync pytest -q --junitxml=artifacts/v11-m3a-releasecheck-pg16.xml
export DATABASE_URL='postgresql://m3a_test:disposable_test@127.0.0.1:55976/marketdata_v11_m3a_test'
uv run --no-sync alembic -x timescale=true upgrade head
export TEST_DATABASE_URL="$DATABASE_URL"
uv run --no-sync pytest -q --junitxml=artifacts/v11-m3a-releasecheck-timescale.xml
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy
git diff --check
```

Source-specific fixtures perform their own fresh/representative-source migrations; the main
regression database deliberately stays destination 0009. See the source guide for separate,
copyable source schema commands. Real TLS tests require OpenSSL and their normal verifying
certificate context; no TLS verification bypass is used. Missing optional infrastructure and
Timescale-inapplicable cases are skips, never passes.

## Remaining risks and recommended milestone 3B

* Build one independently deployed worker with durable immutable batch staging and saved
  receipts, local ownership lock/fence, per-stream progress, fair bounded scheduling, HTTPS
  verification/authentication, retry/backoff and the existing secure session recovery.
* Test/refine destination handling of coalesced quote source versions, equal-time snapshots and
  primary-field switches. Current destination time/receipt/UUID ordering can supersede a newer
  local snapshot. Reuse the existing ownership/ledger/Store/event transaction model and protocol;
  do not alter timestamps or weaken fencing to force acceptance. This is an integration gate.
* Add bounded durable coverage/status snapshot staging and financial/receipt reconciliation,
  including re-snapshot repair when destination data is restored/missing. Export markers are
  enrollment bookkeeping, not proof of permanent destination completeness.
* Budget disk/WAL and outbox capacity for outages/backfill. Backpressure prevents partial local
  commits; finite in-memory and upstream retention cannot recover every never-committed sample.
  Full individual quote-history replication is not required or claimed.
* Restores/clones need explicit external high-water comparison and administrator-reviewed
  epoch/ownership reconciliation. No automatic cursor/epoch reset or restore acceptance claim.
* Historical payloads must pass current protocol validators before enrollment. For example,
  non-null volume without a verified unit needs explicit review; migration preservation does
  not prove every older payload can be uploaded. Do not guess units, round values or drop rows.
* Current dual quote acquisition exists and is fixture-tested, but is not verified as deployed
  in the older candle-only installation or newly qualified against genuine live TradingView.
  Full genuine-price production acceptance remains a later approved rollout/qualification.
* Finish immutable-batch/session/outage end-to-end testing, then release packaging, fresh v1.1
  installation and company qualification. Startup success alone does not establish readiness.

Stop after milestone 3A for review. No full remote worker or live rollout is implemented here.
