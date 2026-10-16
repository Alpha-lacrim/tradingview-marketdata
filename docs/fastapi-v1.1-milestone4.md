# FastAPI v1.1 — Milestone 4 verification

Date: 2026-10-16, Asia/Tehran. Recommendation: **ready for Milestone 5 packaging and fresh-install
verification, subject to the release gates below; not yet qualified for company production**.
This milestone stops here. No deployment, commit, push, legacy collector migration, existing
container/volume operation or frozen v1.0 replacement was performed.

The reviewed architecture works across separate native collector, worker, HTTPS INGEST and
HTTPS READ processes with independent source/company databases and restricted runtime roles.
Both full regression suites pass. Financial comparisons inspect actual company tables, quote
ordering heads, revision manifests and events, rather than trusting upload receipts alone.
The only product change is bounded metadata batching with its required transaction isolation.

## Evidence levels and environment

| Evidence | Result | Boundary |
|---|---|---|
| Deterministic injected correctness | Passed on PostgreSQL and TimescaleDB | Synthetic observations are labelled simulated, never real market data. |
| Isolated complete pipeline | Passed over verified HTTPS with separate OS processes | Same Windows workstation/user; not cross-host company network or secret isolation. |
| Genuine independent TradingView acquisition | Passed for isolated BINANCE:BTCUSDT, 60-second steady observation plus startup/backfill | Actual dual acquisition, six native completed candles and independent current quotes; not all 99 feeds or sustained upstream qualification. |
| Representative business-universe performance | Bounded injected 99-feed runs passed on both engines | Fifteen minutes at 1 observation/feed/s, two minutes at 5/s; these rates are provisional test inputs, not measured TradingView publication rates. |

Audit started at main `4cedaedb172d89ca3d2566bcea477c8296643592`, already containing extensive
approved uncommitted milestone changes. All 137 M3B implementation hashes matched before edits.
The 347-file pre-M4 baseline is retained under `artifacts/v11-m4-baseline/`. Instructions,
CODEX/HANDOFF, prior reports, protocol/recovery/OpenAPI, writers, SourceCapture, worker,
credentials/role SQL and read/ChartData/WebSocket tests were inspected. No missing predecessor
implementation was assumed or rebuilt.

Actual destination head is **0010**, including M3B quote snapshot ordering. Actual source
boundary is **0006 + sc0002**, including worker state over sc0001. No migration changed in M4.
The historically observed old collector `0004` remains a different, untouched installation.
Destination `upgrade head` must never be used to align a source database.

Windows 11 / Python 3.12.7 ran native child processes. A separate disposable WSL distribution
`marketdata-v11-m4-test` hosted Docker 27.5.1 using its own vfs root/socket. Existing Docker
Desktop stayed stopped. PostgreSQL 16.1 and TimescaleDB 2.25.1 / PostgreSQL 17.7 used pinned
images recorded in `compose.v11-m4-test.yaml` and the verification JSON. Timescale tests enabled
two hypertables and twelve continuous aggregates. Real restart tests used four separately
labelled task volumes; ordinary tests used tmpfs. On the recovery databases, fsync,
synchronous_commit and full_page_writes were all on, and PostgreSQL listened on 127.0.0.1.
No company database or preserved collector database was connected.

Compose configuration was validated. Actual services were created with equivalent explicit
Docker commands on the separate WSL daemon, not by executing Compose or the production image.
The [reproduction guide](fastapi-v1.1-qualification.md) supplies PowerShell/Linux commands and
flexible bounded load profiles. Dependency versions, sanitized container inventory and cleanup
evidence are retained under `artifacts/v11-m4-runtime/`.

## Verified source-to-destination compatibility

| Actual local field/state | Transfer and destination | Verified semantics |
|---|---|---|
| `market_candles.payload`, instrument/provider/bucket key | `source_candle_outbox.record` → protocol-v1 CandleRecord → existing Store/`market_candles` | Decimal strings, native 1m OHLC, genuine available volume units, UTC buckets/collector receipt, identity and provenance preserved. No fabricated bars. |
| Candle change, revision, coverage and source clock | One source transaction; destination existing writer/event/receipt transaction | Identical observations do not create corrections; late buckets and historical revisions retain source order. No bucket-time watermark. |
| `source_capture_clock.source_epoch/head` | Batch epoch, positive decimal-string positions | Commit-serialized allocation; sequence IDs alone are not used as commit cursors. Rollback/concurrent cursor tests pass. |
| `source_quote_state.record/version/acked_version` | Full coalesced QuoteRecord → `ingest_quote_heads` + `latest_prices` | Same epoch/owner context uses source position; all field values, clocks, original field IDs/status/metadata and derived snapshot identity match. |
| Quote partial fields | Field-aware local merge and ordered destination snapshot | Omitted fields remain available; old last keeps its clock through newer bid/ask. No unlimited individual tick-history promise. |
| `source_sync_batches` → verified local receipt → source acknowledgment | Existing immutable batches/committed receipts | Four recoverable stages; new version survives acknowledgment of old version. |
| `candle_coverage` window, missing/unknown/not_expected/session_info | Separately versioned `/coverage`, actual destination bars | All 99 final snapshots equal in the frozen-source backfill and both soaks. Unknown minutes are not counted as known upstream omissions. |
| `collector_status` captured run/health/updated_at/expiry | Separately staged `/status` snapshot | Original observation/expiry retained; worker/API/session health does not refresh market data. |
| Provider mapping and authorized manifest | Producer/epoch/ownership generation and existing authorization | OANDA and FXCM remain separate instruments/streams; scoped aliases remain provider mappings, not identity mergers. |
| Existing event journal + materialized history | Existing READ REST, ChartData, WebSocket/replay | No public contract or journal model changed. At-least-once/replay handling, not exactly-once WebSocket delivery. |

The current checkout contains independent quote acquisition and opt-in dual candle/quote
operation. A genuine isolated run exercised that code. This says nothing about the unchanged
older installed collector's operating mode. Historical enrollment still uses bounded
`seed_candles()`/`seed_quotes()` and preflight. Legacy unknown volume units or invalid times
remain rejected and locally retained for review; they are not rounded, fabricated or discarded.

## Actual passing tests and separate skips

| Final full suite | Passed | Skipped | Failed / errors | Total | JUnit seconds |
|---|---:|---:|---:|---:|---:|
| PostgreSQL 16.1 | 643 | 8 | 0 / 0 | 651 | 445.461 |
| TimescaleDB 2.25.1 / PG17.7 | 646 | 5 | 0 / 0 | 651 | 605.515 |

JUnit: `artifacts/v11-m4-pg-full.xml`, `artifacts/v11-m4-ts-full.xml`; adjacent logs retain
warnings and complete results. Skips are **not passes**. Five common skips: two immutable
v1.0-binary scenarios requiring exact head 0006, older optional Batch4 backup/container tests
and Batch3 PostgreSQL-outage test whose specific legacy test infrastructure was not enabled.
PostgreSQL adds three Timescale-aggregate-only skips. The new M4 logical-restore and real
database-restart exercises passed independently; the older skipped tests remain skipped.

The full suites include fresh/upgrade destination migrations; representative source upgrades
through 0005, 0006, sc0001 and sc0002; restricted reader/writer permissions; atomic financial,
journal and receipt commits/rollback; duplicate/conflicting batches; simultaneous writes;
correction ordering; reversed/equal-timestamp quote snapshots; partial updates/primary switches;
ownership transfer/revocation; seed/preflight limits; source rollback/clock races; TLS/auth/payload
bounds; existing REST/ChartData/WebSocket contracts. Existing M3B tests were retained.

| Separate process-level run | Passing assertions | Scope |
|---|---:|---|
| `v11-m4-pg-faults06` | 45 | Four crash boundaries, immutable replay, local contenders, lock loss, lost upload/renew/recovery responses, transport outage, correction and final parity. |
| `v11-m4-pg-restore-final` | 33 | Actual pg_dump/restore clones, missing financial rows, source/destination rollback detection and actual durable-volume PG restarts. |
| `v11-m4-ts-restore-final` | 33 | Same restore/restart scenarios with actual Timescale schema. |
| `v11-m4-pg-edges02` | 30 | Stale primary/bid-ask clocks, unchanged numeric price/new observation, real 429/backoff, expired replay/reconnect, revocation, future-time quarantine, administrator handover. |
| `v11-m4-pg-backfill-frozen2` | 20 | 99 feeds, 990 bounded historical bars, quotes during backlog, correction, independent financial and coverage drain. |
| `v11-m4-pg-99-soak` | 18 | 99-feed 900-second steady +120-second burst; exact financial parity. |
| `v11-m4-ts-99-soak` | 18 | Same injected load with Timescale enabled and proposed status TTL 60s. |
| `v11-m4-pg-genuine-final` | 14 | Actual native candle acquisition, independent quote acquisition, HTTPS, READ/ChartData/WebSocket and exact parity. |

Each named directory is under `artifacts/` and contains `report.json`; major complete runs
also contain `database-comparison.json`, `measurements.json`, `per-instrument.csv`, complete
quote snapshots and trace JSON. Assertions overlap and are not added to the full pytest count.
Edge qualification deliberately ends with pending invalid/fenced data; no final-drain claim
is made for that state. Latest static checks: Ruff lint and format, strict mypy (51 source
files), Alembic heads, Compose config, PowerShell parse and git diff checks pass. Twelve new
qualification safety/profile unit cases also passed separately.

## Financial results and trace examples

| Run | Source/company candles | Revisions | Latest snapshot parity | Coverage parity | Pending candle / quote streams |
|---|---:|---:|---:|---:|---:|
| PG 99-feed soak | 1782 / 1782 | 1 / 1 | 99 / 99 | 99 / 99 | 0 / 0 |
| TS 99-feed soak | 1782 / 1782 | 1 / 1 | 99 / 99 | 99 / 99 | 0 / 0 |
| Frozen-source PG backfill | 990 / 990 | 1 / 1 | 99 / 99 | 99 / 99 | 0 / 0 |
| Genuine BTCUSDT | 6 / 6 | 0 / 0 | 1 / 1 | 1 / 1 | 0 / 0 |

Deterministic candle fingerprints include actual financial values and business identity, not
HTTP status. PG soak checksum is
`fa9bb308c87d167da4971c0a0b04619088c45e755c7a72da3c7f52ffee5536b1`;
TS soak `7cd5c6926bcceccb700d4d0ae8e04c0d93c0f867f97fe25a593e2c191b0f496f`;
frozen backfill `35a10f7ef776d06718b9d21a994ff68687e00f0db000e51f87c834fa8d4eac52`.
Each source/company pair matches; separate runs have different UTC buckets. Revision content
manifests also match. Values are compared as persisted decimals, not ChartData float renderings.

PG soak committed 327 batches: 1783 candle records including the correction, 13266 coalesced
quote snapshots, 1832 coverage and 6850 status records. Of the source's batches, 326 were already
archived and one remained acknowledged pending normal pruning; archived payload hashes had zero
destination receipt mismatches. TS committed 282 batches. Neither claims to replicate all
148500 injected quote updates: superseded intermediate observations may coalesce.

The genuine candle trace follows observation `ebfe0aed-05ac-552f-b786-0dded8f6d399`, source
position 4, batch `c247dfa3-603a-4de1-a39c-32d91c44b91d`. Its actual 10:28–10:29 UTC native
bar is O=82008.01, H=82024.26, L=82007.99, C=82019.98, volume=6.42534 (reported base volume).
Collector receipt is 10:33:21.822344 UTC because this is requested historical backfill;
local commit observer ends 10:33:23.885802; destination ingestion metadata is
10:33:28.814442. The committed candle event is observed over WSS at 10:33:28.891728.
REST returns exact decimal OHLC/identity; ChartData uses Unix-second `a`, O/C/H/L `b/c/d/e`,
Count `f`, Volume `g`, Value `h`, chronological equal arrays, zeros only where unavailable,
and eight empty arrays for an empty result. A separate actual upstream correction was not
manufactured: correction acquisition is injected evidence.

The genuine quote trace is independent observation `d87c33d8-fe35-5b70-be26-b2c971f775d0`,
position 9, batch `010296cd-585c-42f5-94ad-1a06bc64240a`. It carries last=82124.0,
bid=82124.0, ask=82124.01, not the completed candle close. Last's upstream time is
10:33:21 UTC; bid/ask source times are unavailable and remain null. Their genuine collector
receipt is 10:33:25.239510. HTTP ingestion metadata is 10:33:26.847842 and WSS observes
the committed price event at 10:33:26.901536. Field IDs, provenance, realtime classification,
UTC clocks and original receipt times persist through source snapshots, destination latest
state and receipt evidence. Quote volume's upstream window is unverified and is not treated
as native per-minute candle volume. Complete trace payloads retain these qualifications.

## Failure recovery, ownership and reconciliation

Worker termination was injected after durable staging, HTTP completion before local receipt,
local verified receipt commit and source acknowledgment. Restart recovered each phase without
changing staged bytes. A newer quote arriving while an older receipt was pending survived
the old acknowledgment. A second worker failed the existing advisory-lock acquisition and
did not advance the SQL owner generation. Killing the owner backend during an in-flight
HTTPS operation prevented that stale process from recording a receipt or acknowledging source
data. Supervision must restart a worker that loses its database ownership connection.

The test server was killed after destination commit before its response: retry found exactly
one durable receipt. Lost renew response retained the same generation. Expiry recovery and
lost recovery response retained the same durable recovery operation and advanced only once.
Source epoch/positions/observation IDs remained stable. Credentials never grant stream transfer.
Explicit administrator handover advanced ownership to a distinct producer; the old retained
credential/session could not resume or recover those streams. Producer disable/revocation
blocked writes while leaving local pending data intact. No ownership generation was rewound.

Network/API unavailability left committed candles and latest pending quotes recoverable. Actual
source/company PostgreSQL containers were restarted using owned durable volumes and preserved
history. Process restarts simulate relevant crash/sleep recovery boundaries, but do not qualify
physical Windows sleep, reboot, Task Scheduler or Linux service supervision.

Logical restore used real pg_dump/pg_restore into new task databases. Timescale additionally
used its [documented pre_restore/post_restore sequence](https://www.tigerdata.com/docs/deploy/self-hosted/backup-and-restore/logical-backup).
Database-level ACLs were reapplied because they are not carried by the table dump. Restored
source versus a newer external anchor stopped with `source_restore_or_identity_mismatch`;
a clone without anchor stopped `external_anchor_missing`; old source plus old anchor against
a newer destination stopped `source_restore_high_water_mismatch`. Financial upload was rejected
before any resumption. Destination rollback was detected despite retained source export markers.
Uncertain continuity needs administrator review; the system never silently resets an epoch.

Deleting company financial rows while keeping old receipts demonstrated why export markers
are insufficient. Bounded reconciliation inspected actual candles/quote materialization,
reported discrepancies, explicitly staged safe resnapshots and restored exact financial state
and revision evidence. SourceCapture's delivery snapshot is authoritative for quote identity;
a derived repair snapshot can have a different UUID from local reader materialization while
preserving its original fields/clocks. Arbitrary audit corruption or unobserved commits after
backup rollback is not claimed automatically repairable.

Future-dated source/collector timestamps are rejected, not rewritten. The edge run's local
quarantine diagnostic is `collector_clock_ahead`; later handover is `manifest_mismatch`.
Keep clocks synchronized, retain original pending records and investigate; never mutate bytes
under an existing batch ID. Current source data age remains separate from successful transport.
An old last with a new bid/ask remained stale in REST and WSS while health endpoints were good;
the same numeric price with a genuinely new observation refreshed only its own field clock.

Historical coverage, source acquisition status and transport backlog remain separate. Status
uses captured source expiry; coverage hashes/versions and durable metadata batches are distinct
from candle events. Intentional fixture omissions remain missing ranges. Genuine coverage
unknowns are preserved, not called expected-minute completeness. The 99-feed CSV lists every
identity's sampled age/staleness; no difficult feed was removed to improve an aggregate.

## Measured workload and proposed service targets

Each soak generated **89100 steady +59400 burst =148500** injected observations. The registry
is exactly the provisional employer 50 Binance spot, 21 OANDA forex, 21 FXCM forex, four metal
and three energy feeds; it is not the earlier accuracy-selected 100-feed universe. Prices and
available volume are visibly simulated. An injected 24x7 schedule tests known coverage gaps
without asserting that forex/CFD upstream trading calendars are 24x7.

| Sampled metric | PG16.1 | TS2.25.1 / PG17.7 |
|---|---:|---:|
| Steady latest-price age P50 / P95 | 6.595 / 11.648 s | 7.511 / 13.211 s |
| Steady maximum age, including startup transition | 44.686 s | 44.870 s |
| Burst latest-price age P50 / P95 / maximum | 10.003 / 20.625 / 21.629 s | 11.728 / 26.948 / 26.952 s |
| Stale steady samples / feed-observations | 198 / 17820 | 198 / 17820 |
| Stale burst samples / feed-observations | 0 / 2376 | 0 / 2376 |
| Staging → destination commit observer, quote P95 | 2.045 s | 2.257 s |
| Local quote Store transaction observer P95 | 1.333 s | 1.294 s |
| Journal append → WSS observer P95 | 0.873 s | 1.008 s |
| Sampled peak pending candles / quote streams | 99 / 99 | 99 / 99 |
| Source / destination sampled connection peak | 9 / 5 | 9 / 7 |
| Source DB first → final sample bytes | 13193699 → 114450959 | 14688783 → 107995663 |
| Destination DB first → final sample bytes | 14348771 → 104985059 | 17154575 → 99191955 |

The 198 stale steady observations in each run are two early samples across all 99 feeds: the
acceptance/correction phase had deliberately paused quote generation before load began. They
are retained in the statistics. The separate acceptance sample was also stale. No burst sample
was stale. Final drained quote age was 10.626s PG and 5.176s TS. Status TTL was 20s PG and
60s TS; all 99 were unexpired for 148/180 PG steady samples and 19/24 burst samples, versus
180/180 and 24/24 TS. These different profiles and shared-host contention prevent treating
this as a controlled engine-speed comparison. The 60s health TTL does **not** change the
30s quote-staleness threshold or approximately 120s session lease.

Peak process RSS (collector/worker/ingest/read) was approximately 114/81/86/87 MiB PG and
114/80/86/88 MiB TS. Their observed CPU-seconds were respectively 337.83/297.02/99.25/27.67
over 1030.53s PG and 364.05/266.06/90.38/29.09 over 1025.05s TS. These are per-process
Windows counters, not host/database CPU attribution. Database sizes include retained journal,
receipts, tables/indexes and bloat; logical pending-outbox size alone is not disk capacity.

The final frozen-source 990-candle backfill drained in 104.844s while one-Hz current prices
continued. Backfill quote-age P95 was 12.429s, maximum 13.380s once available; the initial
unavailable quote sample is counted separately as 99 non-fresh observations. Brief acceptance
and phase transitions remained visible as stale samples. Final correction/quote/coverage
parity was complete. This bounds starvation for the tested 10-minute backlog, not unlimited
multi-year throughput. Genuine BTC's twelve steady age samples had P50 5.479s, P95 7.469s,
maximum 8.364s and no stale samples; it is a brief acquisition check.

HTTP duration includes client quota waiting; expected receipt-not-found lookups for new batches
are logged separately (327 PG, 282 TS) and are not rejected financial uploads. No unhandled
financial HTTP failure occurred in the final soaks. Actual 429/retry behavior is exercised in
the edge run. Store/destination timing surrounds transaction completion, not physical WAL
flush instrumentation. Journal-to-WSS timing includes reader polling and transaction visibility.
REST visibility is sampled at approximately five-second intervals. True upstream publication
time, exact outbox-visible versus worker-queue delay decomposition, host-wide/database CPU,
company WAN/gateway latency and sustained real publication rates remain unmeasured; the report
does not invent those results. The tracing files retain source/receipt/stage/commit timestamps
where available. Other isolated tests overlapped parts of the soaks on this workstation.

Proposed, **not employer-approved**, service targets for a warmed-up reviewed deployment:
one-Hz current-price age P95 ≤15s; bounded five-Hz burst P95 ≤30s; alert any genuine primary
field age >30s independently of worker/API health; alert oldest pending candle >60s during
normal collection; bounded 990-bar/99-feed catch-up ≤120s on comparable capacity; ≤1s P95
journal-to-WSS as an initial tuning target (TS measured 1.008s, slightly above); status capture
every 5s with a reviewed 60s expiry; no unresolved delivery/revision mismatch. Coverage targets
must use provider calendars and genuine availability; these tests do not establish a native
99.9% completeness SLA. Startup grace and backlog alerts need explicit operational approval.

## Narrow corrective change and changed files

The pre-fix 99-feed 60s baseline delivered financial state but only 13 status streams had ever
been uploaded and two remained unexpired. One status/coverage record per scheduler turn was
the measured bottleneck. M4 adds configurable bounded metadata batches (default/max 50,
also bounded by request records/bytes); financial protocol and fairness slots are unchanged.
Sorted stream business locks precede the shared capture clock. READ COMMITTED sees the current
clock after lock waits; a forced-interleaving regression proved that the previous REPEATABLE
READ snapshot could cause avoidable serialization retries under unrelated quote commits.
One captured status row is reused without changing its timestamp or expiry. Staged bytes,
dedup hashes and traversal cursor commit atomically. Byte-split records are revisited rather
than silently marked exported. Shared-clock gaps from metadata remain legitimate candle gaps.

| Files changed this milestone | Rationale |
|---|---|
| `src/marketdata/sync_config.py`, `src/marketdata/sync_worker.py` | Configurable bounded metadata batches and lock/isolation-safe staging; shared persistence/event model reused. |
| `tests/test_sync_worker.py` | Batch coverage/unchanged expiry, atomic rollback and forced concurrent-clock regressions; existing single-record traversal test explicitly sets limit 1. |
| `tests/test_destination_migrations.py` | Force-drop generated disposable test database despite reconnecting Timescale background workers. |
| `tests/test_process_separation.py` | Ownership-loss test sets 0.1s polling rather than racing its 5s deadline against the default 5s poll. No production ownership behavior changes. |
| `tests/test_v11_qualification.py` | Twelve unsafe-DSN, exact business-universe and bounded genuine/load-profile cases. |
| `compose.v11-m4-test.yaml` | Pinned disposable ordinary and durable-recovery engine pairs, loopback ports and task labels. |
| `scripts/qualify_v11_pipeline.py` | Separate-process TLS integration, immutable/fault checks, configurable injected/genuine load and final independent coverage drain. |
| `scripts/v11_qualification_process.py`, `scripts/v11_qualification_source.py`, `scripts/v11_qualification_resources.py` | Test-only child entrypoints/doubles, sanitized trace/fault boundaries, genuine factory reuse and OS resource counters. |
| `scripts/qualify_v11_recovery.py`, `scripts/qualify_v11_edges.py` | Actual restore/restart, missing-state repair, freshness/security/rate/replay/handover exercises. |
| `scripts/report_v11_qualification.py` | Read-only exact financial/order/revision/coverage comparisons, traces, per-instrument and phase/resource evidence. |
| `scripts/cleanup_v11_m4_runtime.ps1` | Session-only path/label/mount-verified removal of owned infrastructure. |
| `docs/fastapi-v1.1-sync-worker.md`, this report, qualification guide, verification JSON, `CODEX.md`, `HANDOFF.md`, `README.md` | Current behavior, reproducible commands, measured limits, preservation and next review state. |

No destination/source migration, ingestion OpenAPI, public API contract, authentication,
ownership SQL, financial Store SQL or frozen distribution changed. Metadata records and HTTP
endpoints retain protocol v1; clients can set metadata_max_records=1 if necessary. No new broker.

Development failures are retained, not counted as final passes: missing OpenSSL config;
historical enrollment initially omitted because normal collector activation starts at the
current minute; fault-harness missing import/readiness/contender timing; two initial TS suite
failures from background DB connections and test polling deadline; restore missing Timescale
hooks; comparing a repair snapshot to the wrong local identity; tmpfs restart erasing only
disposable test data; a backfill test injector sharing an asyncpg observer connection;
premature WebSocket assertions; two lint line-length errors; and a later test-runtime restart
needed before the final bounded backfill rerun. These were test/environment fixes. The first
backfill report's financial parity was correct but coverage was only 50/99 at shutdown: final
acquisition was still advancing coverage. The harness now freezes acquisition and separately
waits for coverage. `pg-backfill-frozen2` proves 99/99; the earlier run is not coverage acceptance.

## Preservation, exit gate and remaining release work

All 66 archived v1.0 files still match `distributions/marketdata-fastapi`, and ZIP SHA256 is
`3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a`.
Existing ignored tool caches are not release contents. The pre-M4 baseline confirms only the
listed cohesive changes; approved earlier uncommitted implementation and unrelated
`TradingViewConvHistory/` were preserved. Source/destination migrations and public/security
modules remain byte-identical. No second collector was connected to the existing installation;
the sole genuine test used its own fresh database and scoped registry. Task database dumps
and credentials remain private local evidence, not release artifacts.

Automatic approval review blocked optional deletion of newly generated ignored Ruff cache files
inside the frozen distribution tree. They were left in place; all 66 archived release files
and the ZIP remain unchanged. No permission escalation or alternative deletion was attempted.

After evidence extraction, all eight verified task containers and four labelled task volumes
were removed, then the verified task-only WSL distribution/virtual disk was unregistered.
The cleanup record contains the resolved absolute path and sanitized inventory. Existing
Docker Desktop remains stopped; no legacy container or volume was started, stopped, migrated,
truncated, restored or deleted. There was no commit or push.

M4's financial durability, quote field/order parity, acknowledgment/recovery, worker fencing,
actual-state reconciliation and read compatibility gates pass. Packaging may proceed after
this review, with these explicit outstanding production gates:

- Full genuine 99-feed availability, scoped aliases, entitlements, native publication omissions,
  sustained independent-price rates and per-provider freshness. The provisional registry still
  needs business approval; genuine upstream correction acquisition was not observed.
- Market-data licensing and redistribution approval; technical access and this test do not
  establish distribution rights or bypass provider entitlements.
- Fresh v1.1 source/ZIP/image installation, appropriate package version, read/ingest Compose,
  secrets/backup/restore runbook and explicit source rollout plan. Frozen v1.0 remains frozen.
- Company gateway/public-CA/WAN, firewall/database network isolation, target-host permissions,
  Linux production container/service supervision and physical Windows sleep/reboot qualification.
  Local process separation under one OS user is not a secret security boundary.
- Longer dedicated-host soak and larger backfill; approved freshness/backlog/coverage targets,
  status expiry and real capacity/resource budgets. Instrumentation does not yet isolate every
  acquisition/queue/outbox/WAL phase requested by a production observability SLA.
- Disk/WAL/journal/receipt retention and alerts beyond pending-outbox limits. Never evict
  unacknowledged candles; superseded quotes may coalesce; arbitrary missing audit history may
  require reviewed repair. Cloned private identity plus rolled-back external state cannot prove
  physical installation continuity without administrator handover/distinct credentials.

Recommended M5 scope is release packaging and fresh isolated installation verification of
these exact reviewed components, service supervision/secret mounts, role/network checks,
backup/restore instructions and operator qualification gates. Do not broaden M4 into deployment
or activate all feeds. Complete production acceptance remains conditional on the outstanding
genuine-source and target-environment evidence.
