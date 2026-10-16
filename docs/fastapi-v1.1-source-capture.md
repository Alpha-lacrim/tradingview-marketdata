# Milestone 3A: local source capture and milestone 3B contract

This is an opt-in source foundation, not an HTTPS worker or a deployed collector.
Destination migrations 0007, 0008 and 0009 and the approved ingestion protocol remain intact.
The company still requires a fresh v1.1 installation later; the frozen v1.0 package is unchanged.

## Verified source audit and compatibility matrix

Checkout: main, `4cedaedb172d89ca3d2566bcea477c8296643592`, plus the existing uncommitted
approved milestones. Findings concern this checkout. No remote fetch, source SQL or live
TradingView session was used. The earlier installed schema-0004, candle-only deployment
is documented in the Phase A audit; its stopped database was not queried again.

| Source evidence / fields | Destination protocol/storage | Reuse / limits |
|---|---|---|
| `domain.py`, `registry.py`: deterministic instrument UUID, canonical symbol; `provider_instruments` `(instrument_id,provider)`, exact provider_symbol/venue | Existing registered mapping and exclusive stream owner | Capture validates exact mapping; registration still rejects identity reassignment. OANDA:EURUSD and verified scoped FX:EURUSD/FXCM remain different instruments. No registry promotion. |
| `candle_ingestion.py`: bounded native-history recovery, correction overlap, `is_backfill`; `tradingview_candles.py` validates completed vectors | `candles` stream, `kind=candle` for both new bars and corrections | Same Store SQL and native `MarketCandle`; no tick-derived or fabricated bars. Historic bucket time never acts as a delivery cursor. Source lookback remains finite; corrections never supplied by the provider cannot be captured. |
| `market_candles`: instrument_id/provider/provider_symbol, interval, bucket_start/end, open/high/low/close, volume, price_basis, realtime_status/source_status, received_timestamp, metadata/payload | `CandleObservation` has the same domain fields | NUMERIC/Decimal -> finite plain decimal strings; no rounding. Bucket boundaries are native source bar times, not a claimed publication timestamp. Raw unknown volume remains metadata; typed volume must be base/tick. |
| `Store.write_candles_in_transaction`: advisory source locks, receipt guards, semantic comparisons, revision audit, latest state | Existing destination candle heads and shared writer | A genuine accepted financial/status/volume-unit change emits one outbox record. Identical bars advance no-op receipt guards without false revisions/events. Backfilled provenance remains historical. Equal/older receipts do not regress state. |
| `candle_coverage`: coverage_start/end, missing, unknown, not_expected, session_info; actual bars remove uncertainty/gaps | Existing `coverage` protocol; destination recomputes missing from its stored bars | Current tri-state/session behavior reused unchanged. 3A does not enqueue coverage/status as candle events. 3B must capture cumulative bounded snapshots in a repeatable read, assign durable versions and stage them; a complete source claim never proves destination completeness. |
| `TradingViewGuestProvider.parse_quote`: independent partial lp/bid/ask deltas; optional lp_time, receipt time; identity and realtime gates | `quotes` stream, `QuoteObservation` | Implemented and fixture-tested in current checkout. lp_time applies only to last; a generic timestamped_fields marker now preserves that distinction. No new source capability, delayed access, bid/ask timestamp, execution or quote volume window is invented. |
| `providers/factory.py`, `tradingview_pipeline.py`: opt-in dual mode, latest-only default, optional quote history | Independent candle and quote delivery | Current code can collect both. Installed older candle-only binary cannot. Genuine live dual availability/deployment remains unverified in this milestone. No live source connection was opened. |
| `IngestionService`: bounded queue, retained failed batch, retry; prior latest-only Store selected newest whole delta per stream in each batch | Latest-price delivery guarantee | Capture now merges all batch deltas per field before coalescing durable state. Once committed, the newest snapshot/field observations persist. No complete tick-history guarantee. Queue overflow/shutdown loss before commit remains logged/counted. |
| `latest_prices`: instrument/provider, order_ns, received_timestamp, tick_id, payload | Existing quote latest materialization and history | Source capture adds field-aware merging and a coalescible pending snapshot; optional market_ticks history still stores the original deltas. The destination's existing financial/event SQL is not duplicated. |
| `marketdata_event_clock/events` (base 0006), bounded retention, stripped metadata | REST replay and WebSocket committed events | Existing journal stays bounded. Source candle outbox is separate and never trims unacknowledged events by age. New coalesced quotes retain only selected-field clocks in the frontend journal so a bid update cannot refresh an old last. Existing public response schemas and ChartData arrays stay unchanged. |
| `collector_status` (base0005), provider/symbol health, real capture/expiry clocks | Existing `status` protocol and expiring destination status | Session/transport health remains independent of source freshness and delivery success. 3A does not renew or manufacture remote status. |

Actual writer boundaries: `write`, `write_quotes`, `write_candles` still own their transactions
and return counts. Their caller-owned helpers retain their signatures. Source capture hooks
execute on that same connection before commit; no independent writer SQL for candles,
coverage, revisions or frontend events was added. The default-disabled path remains the
approved destination-0009 path. Capture requires the independent collector role; api, ingest
and development combined roles are rejected when capture is enabled.

## Separate source migration and activation

Source schema = **base 0006 + independent sc0001** in `source_capture_version`.
Destination schema = **0009** in `alembic_version`, without sc0001.

For an explicitly reviewed later rollout, a source at 0004 first receives only existing
additive 0005 (collector health) and 0006 (frontend journal). The separate `alembic-source.ini`
then installs sc0001; it refuses a destination or incompatible base. No destination policy,
receipt, authentication or session-recovery migration is applied to the collector. Startup
does not create, migrate or seed schema. Do not run generic `alembic upgrade head` on a source.
The older exact-0004 binary must not run against the upgraded source. A new source installation
uses the same two explicit commands. There is no destructive automatic downgrade.
Preflight legacy payloads separately before enrollment: non-null volume without a verified
unit, unsupported precision/status or invalid original clocks must remain intact for review.
Schema preservation is not a claim that every old payload passes today's wire validators.

sc0001 adds:

* `candle_observation_heads`: shared Store compatibility guard, seeded from maximum historical
  and latest candle receipts without changing market records.
* `source_capture_clock`: one persistent random dataset epoch, transactional BIGINT head,
  candle acknowledgment, retained row/encoded-byte counters and activation time.
* `source_candle_outbox`: primary-key position, unique observation UUID, source identity,
  immutable complete protocol record, encoded byte count, durable change_kind
  (candle_completed/candle_corrected/history_seed), and separate local capture time.
* `source_candle_exports`: one enrollment/latest-export marker per native candle business key.
  It prevents duplicate history seeding after outbox cleanup, and survives process restarts.
* `source_quote_state`: one full snapshot, exact field provenance, current version and
  acknowledged version per source. A partial pending-version index supports bounded selection.

All identity references use the existing registry. Positive position/version and acknowledgment
bounds are database constraints. Epochs and observation IDs are stored durably, not regenerated
on startup. Observation UUIDs are UUIDv5(dataset epoch, candle/quote + allocated position).
`source_position` describes local capture order, never a fabricated provider sequence.
The source dataset epoch is independent of the bounded frontend journal's replay epoch.

Copyable **isolated test** PowerShell setup (not approval to migrate a deployed source):

```powershell
docker compose -p marketdata-v11-m3a-tests -f compose.v11-m3a-test.yaml up -d --wait
docker compose -p marketdata-v11-m3a-tests -f compose.v11-m3a-test.yaml exec -T postgres createdb -U m3a_test marketdata_v11_m3a_source_test
$env:DATABASE_URL='postgresql://m3a_test:disposable_test@127.0.0.1:55975/marketdata_v11_m3a_source_test'
uv run --no-sync alembic upgrade 0006
uv run --no-sync alembic -c alembic-source.ini upgrade head
$env:APPLICATION_ROLE='collector'
$env:SOURCE_CAPTURE_ENABLED='true'
# Tests inject observations. Do not start a collector or provider connection for verification.
```

Linux uses the same two migration commands with:

```bash
docker compose -p marketdata-v11-m3a-tests -f compose.v11-m3a-test.yaml up -d --wait
docker compose -p marketdata-v11-m3a-tests -f compose.v11-m3a-test.yaml exec -T postgres createdb -U m3a_test marketdata_v11_m3a_source_test
export DATABASE_URL='postgresql://m3a_test:disposable_test@127.0.0.1:55975/marketdata_v11_m3a_source_test'
uv run --no-sync alembic upgrade 0006
uv run --no-sync alembic -c alembic-source.ini upgrade head
export APPLICATION_ROLE=collector SOURCE_CAPTURE_ENABLED=true
```

For Timescale use `alembic -x timescale=true upgrade 0006` in the first command; the extension
tables stay ordinary PostgreSQL tables. Use private DSN files/ACLs for a future real installation,
not these disposable credentials. Source and destination DSNs never cross the company boundary.
Installing the schema alone does not activate capture; the collector flag and compatible binary
are explicit. Disabling capture on an enrolled source is not a continuity-preserving rollback:
startup fails without the explicit flag. Never mix a direct-SQL/older writer with active capture.

## Candle atomicity and commit ordering

The shared Store first locks its full sorted instrument/provider set. Accepted candle changes,
latest candles, coverage, no-op heads, revisions, outbox, export markers and frontend journal
are inside the caller's transaction. Capture forces synchronous_commit=on for these commits.
Any constraint, capacity or injected failure rolls back all of them, including allocated positions.
Current outer Store callers receive a successful return only after transaction exit.
Source-enabled caller-owned helpers also use savepoints: catching a Python capture/validation
error inside an outer transaction cannot commit a half-completed helper. Released savepoints
do not release the successful writer's capture clock before the outer commit.

The clock row is locked with FOR UPDATE. Positions are allocated by updating that row in the
same transaction; the lock is retained until commit/rollback. Writer B on a different instrument
can prepare market changes, but cannot allocate/publish position N+1 while A holds uncommitted
position N. An MVCC consumer sees the old head until A commits. On rollback A's allocations and
events disappear and B may reuse the invisible number. There is no sequence reservation gap
that a consumer can permanently skip. Cross-stream quote allocations may leave *visible* gaps
between candle positions; these are intentional and safe.

Lock order is sorted source locks -> source capture clock -> existing frontend clock. A caller
using several helpers in one transaction must prelock the union of all its sources, as before.
Consumer acknowledgment/cleanup locks only the capture clock. Source history seeding acquires
source locks before the clock. Readers use one repeatable-read snapshot without writer locks.
Concurrency tests hold an early transaction open, attempt a different-source later commit, read
the cursor before release, then exercise both commit and rollback. Overlapping correction and
quote writers additionally test deterministic final state.

## Latest prices: partial inputs, durable full snapshots

Each non-null field is a genuine observation; omitted/null fields mean **no update**, not clear.
There is no implicit clearing API. Price/size/volume fields retain value, original observation ID,
source time/ns when applicable, collector receipt, data status, sequence and original metadata.
Fields are compared by source time/ns, receipt and original UUID; without source time the only
available ordering is receipt time. Receipt ordering cannot prove exchange ordering. A field
without a source clock is not assigned another field's source timestamp.

All deltas in a Store call are merged under the source lock and clock; one version/snapshot per
changed stream is committed. Source capture uses its field comparison to update local latest
state; raw history, when explicitly enabled, remains original deltas with original UUIDs. There
is no new mandatory quote-history table. Replaying the same or an older delta does not advance
state. A new same-price observation can advance clocks without claiming a candle correction.

Snapshot ID is a new durable **coalesced state observation** identity, not an execution ID or a
claim to reproduce each upstream delta. The record's Tick ID equals that snapshot observation
ID as required by protocol v1; original delta IDs remain per-field provenance. The last field
is the primary price when present, otherwise bid, then ask. Source time/ns and status come from
that primary field; collector receipt is the newest actual accepted field receipt. Individual
clocks remain in metadata. `source_capture_primary` keeps freshness conservative in REST and
the bounded frontend journal, including a last without source time. Upload/acknowledgment never
changes any of these times or marks a stale quote fresh.

At most seven observed fields plus one pending snapshot persist per registered stream. Updating
version N+1 replaces the pending local N payload, so an in-flight worker **must already have
durably staged N's immutable bytes**. Acknowledging N sets acked_version at most N; it cannot
clear N+1. Acknowledging the current version returns delivered=true. Raw individual quote-history
delivery is explicitly outside this guarantee, even if optional local raw history is enabled.

## Exact milestone 3B local consumer interface

`SourceCapture(Store)` exposes bounded operations; it performs no remote HTTP:

| Operation | Returned/committed contract |
|---|---|
| `consumer_lock()` async context | Holds one dedicated PostgreSQL session advisory lock. A competing local worker fails. Supervise that connection and stop all staging/recovery/ack work when it is lost; fence future staging-state writes as well. Never clone installation credentials/state. |
| `page(limit=100)` | Repeatable-read `{source_epoch, after, through, records, event_kinds}` from the durable candle ack; position-ordered complete CandleRecords, up to 500. through is last selected position, or the committed head for an empty page. Corrections use the same candle stream and retain different observation IDs/positions. event_kinds maps observation IDs to candle_completed/candle_corrected/history_seed for local progress tracking; never add it to protocol records. |
| `pending_quotes(limit=100)` | Repeatable-read `{source_epoch, records}`, version-ordered pending full QuoteRecords. Version = source_position; each has instrument/provider identity and per-field provenance. Limit 1..500. |
| `acknowledge_candles(epoch, after, through)` | One synchronous local transaction; checks epoch and bounds, then compares/advances durable ack. Exact replay is safe; concurrent incompatible advancement fails. Only call after verifying committed destination receipts for **all** staged records through that cursor. |
| `acknowledge_quote(epoch, instrument, provider, version)` | One synchronous local transaction; monotonically advances acked_version, refuses unknown/future versions, returns whether current state is delivered. Older ack leaves any newer version pending. |
| `cleanup(limit=500)` | One transaction deletes at most the bounded count at/below the candle ack and releases counters. Unacknowledged rows are never deleted. Quote state/field provenance stays after ack. |
| `seed_candles(instrument,provider,limit=100)` | Bounded anti-join of stored native candles against durable export markers, under the same source lock. Emits original stored payload once without changing financial history/events. Repeat until zero for initial enrollment. No bucket high-water cursor; later corrections atomically replace the export marker and enqueue a new version. |
| `seed_quotes(limit=100)` | Enroll stored genuine latest_prices that have no durable quote state. Never reads candle-close fallback. No reconstruction of intermediate historical quotes. |

Acknowledgment calls are trusted local persistence operations, not proof of remote delivery.
3B owns durable immutable staging, authenticated receipt validation and the policy for invoking
them. Row-count limits alone do not enforce HTTP's 2 MiB bound: split **unstaged** pages using
actual encoded bytes (and destination limits), preserving page progress for each sub-batch.
Never acknowledge the whole source head just because a bounded nonempty page was uploaded.
Consumer lock does not grant destination ownership, session generation or admission rights.

Use the existing `/api/ingest/v1/candles` and `/quotes` protocol, not a new endpoint/envelope.
Bind the persistent source epoch to pre-provisioned stream ownership. Retain observation IDs,
positions, field provenance and original clocks in staged records. Persist producer/session,
ownership manifest, immutable batch bytes/hash, selected cursors/versions, and the eventual
verified receipt atomically in worker-owned durable tables. The worker must retain bytes for
in-flight quotes even when local latest state advances. Candle/correction/backfill progress
uses event_kinds and original backfilled metadata while consuming this one ordered candle log.
Use four recoverable steps: commit immutable staging, perform HTTPS, commit the verified
receipt in worker state, then acknowledge the source in a separate transaction. A crash before
the final acknowledgment can replay the saved receipt/ack; a crash before receipt storage
rechecks the original remote batch receipt. Never acknowledge from an unpersisted HTTP result.
The local worker role should have no market/policy mutation privileges: 3B must introduce narrow
acknowledgment/cleanup grants or functions without allowing epoch/head/payload modification.

3B must test destination integration for coalesced snapshots before claiming delivery of all
fields: existing destination latest ordering uses source ns/receipt/UUID, not source_position
for quotes. A primary-field switch or equal-time distinct snapshots can otherwise be retained
as superseded. A narrow source-version/field-aware destination refinement may be needed, with
the same protocol and existing transaction/event model. Do not rewrite clocks, IDs or prices to
force acceptance. This milestone establishes local correctness; it does not claim that untested
remote corner cases pass. Bid/ask source times absent upstream remain absent. Destination
metadata/record bounds and licensing/redistribution entitlements still apply.
An HTTP 200 committed receipt proves storage, not latest-price parity or freshness. Before
counting a superseded quote as delivered, 3B must establish that a legitimately newer accepted
source version already covers it; the snapshot-ordering integration gate must be resolved.

Use approved admission/renewal/recovery unchanged. After expiry retain all pending observations
and bytes; recover the known producer with its expected previous session and exact manifest.
Check old receipts. New envelopes after session recovery get new batch IDs while retaining
observation identity/epoch/position. Never mutate bytes under an existing batch ID. A replaced,
revoked or disabled producer stops, retains its queue and cannot acquire another stream.
Five-second future-clock rejections retain original pending data; diagnose synchronization,
wait or explicitly quarantine. Do not clamp timestamps, refresh status TTL or silently discard.

## Storage, failure and restore policy

Default limits: 1,000,000 retained candle events and 512 MiB of canonical encoded record bytes,
configurable with SOURCE_CAPTURE_MAX_ROWS/MAX_BYTES. A record over 64 KiB or quote snapshot
over 16 KiB fails its market transaction. Counters count retained acknowledged rows until cleanup.
These are payload budgets, not an estimate of heap/index/WAL/disk bytes. Monitor filesystem/WAL,
pending rows/bytes, oldest queued time, quote version lag, collector retry metrics and upstream
health independently. At 100 candles/minute the row bound is roughly 6.9 days before corrections;
the byte bound can bind earlier. Size for expected offline duration and history enrollment.

On exhaustion the owning market transaction rolls back. Existing candle/batch writers retain
the bounded pending batch and retry with logged/counted database failure; there is no silent
unacknowledged eviction. During a prolonged disk/outbox outage, future provider observations
may be unavailable beyond the bounded in-memory/upstream recovery horizons. The guarantee is
for intended **locally committed** candles, not observations never acquired/committed. Free space
by verified acknowledgment/cleanup, increasing a reviewed budget or explicit recovery, never
deleting pending rows. Export markers/receipt guards grow with stored candle keys and are not
time-trimmed; a future market-history retention decision must account for reconciliation.

Ordinary process/server restart preserves epoch, positions, pending snapshots and checkpoints.
Restore must restore market data, all source-capture tables and worker state consistently.
Never reset head or epoch on restart. A database rollback/clone/replacement can reuse positions
and UUIDs under an old epoch and is **not automatic safe continuation**. Stop capture/worker,
compare backup and destination receipts/high-water state, reconcile retained observations, then
use an administrator-approved new dataset epoch/ownership generation when necessary. Preserve
old staged batches/receipts as evidence; do not bulk rewrite their bytes or identities. An
external/remote high-water anchor is needed to detect arbitrary rollback of all local state;
3A alone cannot detect such a restore. No restore tool or live rollout is authorized here.

The next milestone should implement durable staging/checkpoints and one locked worker, receipt
recovery/session transitions, byte-bounded fair candle/quote/backfill scheduling, captured
coverage/status snapshots, reconciliation and verified HTTPS retry/backoff. Qualify injected
end-to-end candle/correction/quote delivery, outages, sleep/restart, lost acknowledgments and
freshness before a separately authorized genuine-source rollout or release package.
