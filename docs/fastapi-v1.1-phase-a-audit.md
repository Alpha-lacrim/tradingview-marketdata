# FastAPI v1.1 integration — Phase A audit and design

Date: 2026-10-15 (Asia/Tehran). Audited checkout: `main`,
`4cedaedb172d89ca3d2566bcea477c8296643592`.

**Verdict:** the existing persistence and read API are a suitable foundation. The requested
HTTPS ingestion and durable synchronization system does not exist yet. Full acceptance is
blocked by the installed candle-only collector's lack of independent quote capture and a
durable replication feed. Phase B must follow review of this proposal.

This session changed only this report and `HANDOFF.md`. No application, migration, package,
configuration, database, container lifecycle, volume, branch or deployment changes were made.
No provider connection was opened. Existing untracked `TradingViewConvHistory/` was preserved.
No Git fetch was performed; findings concern this checkout, not an assertion about remote HEAD.

## 1. Verified version and deployment boundaries

| Boundary | Evidence inspected | Finding |
|---|---|---|
| Checked-out platform | `src/marketdata/`, migrations, tests, Git | Source migration head `0006`; independent `api`/`collector` roles and development `combined` role. Opt-in dual collection is present. |
| Standalone v1.0 source | `distributions/marketdata-fastapi/` | Present, self-contained API snapshot. Concrete collector/provider implementations are excluded. Its supported runtime is `marketdata.api:create_app`. |
| Delivered v1.0 ZIP | `artifacts/marketdata-fastapi-v1.0.zip` | Present, 66 entries; CRC and all 53 managed-file hashes pass. All archive entries match the directory after CRLF/LF normalization. Preserve this release. |
| Source/snapshot drift | Manifest hashes and file diffs | Four parent files have changed since export: `config.py`, `ingestion.py`, `persistence.py`, `providers/base.py`. These are documented dual-collector additions, not missing v1.0 files. |
| Public API version | `api.py:272`, both pyprojects | FastAPI advertises `1.0.0`; package metadata still says `0.1.0`. v1.1 needs explicit, consistent release metadata. Existing `/api/v1` read routes should retain their paths. |
| Installed collector | Docker inventory, filtered configuration, source read from stopped container using `docker cp ... -` into memory | `tradingview-app:ordinary-100`, image ID `sha256:ca498afb02f6232440e3386936a4097e865cf9fefdbd0481adc0cc7ce623023f`. Its older `create_app` starts ingestion. Storage mode is `candles`; registry path is `config/instruments.tradingview-100.json`; futures disabled. |
| Installed collector schema expectation | Installed `persistence.py:87`, installed migration files | Binary requires exactly `0004`; installed migrations stop at `0004`. Those four migration files match the checkout after LF normalization. |
| Actual collector DB revision | Earlier evidence versus current inspection | `docs/dual-ingestion-verification.json` records a prior SQL observation of `0004`. It was **not re-queried now** because the DB is stopped. Binary expectation is not a substitute for querying database state. |
| Current runtime | `docker ps`, `docker ps -a`, `docker context show` | No running containers in `desktop-linux`. TradingView app and DB are stopped. No restart was attempted. Earlier handoff statements about a currently running unhealthy app are historical. |
| Company installation | User-provided deployment boundary | Company has not installed v1.0. No company host/database was accessed. Plan a fresh v1.1 installation, with optional isolated `0006 → 0007` compatibility testing. |

ZIP SHA-256:
`3a3ca216098a0f2a6632a8dcdb1dfb96af116a4c079c825876f253fbd8cea67a`.

Additional installed-source SHA-256 evidence (read only, no extraction into the source tree):

| Installed file | SHA-256 |
|---|---|
| `config.py` | `a47d56df29bab90cd8cf0f2d851404bb33203ff1450dfbd57f5d6b419676373b` |
| `api.py` | `fe13b1e2062f88c59402859db7601c4d9a8a478abddb5faff53327d772797a0b` |
| `persistence.py` | `71568c7fbe479a37eae87eb2026f66a84231e1328daa4d14fbd775f1eb8c993f` |
| `ingestion.py` | `cf4a4bc81076063c4b55d76045693c0bb77168c343f6905e524bfaf0daee6b78` |
| `providers/factory.py` | `dab23a5ba9f2a1a369fcd269777f4368baf627a44c06ef9e22dd0cde3a5689ca` |

## 2. Source-to-destination compatibility matrix

All six checked-out migration files match the standalone snapshot after LF normalization.
`0002` is optional Timescale configuration; ordinary PostgreSQL remains supported.

| Data/function | Source identity/storage | Existing company-facing behavior | Reuse and required adaptation |
|---|---|---|---|
| Registry | `instruments`, `provider_instruments`; UUIDv5 from canonical symbol (`domain.py:95`) | Same schema/models | Reuse registration rules through administrative provisioning. Ingestion must resolve an existing authorized mapping, validate the canonical UUID and full identity, and reject reassignment. Never register arbitrary uploaded symbols. |
| Native completed 1m candles | `market_candles`, PK `(instrument_id,provider,interval,bucket_start,price_basis)` | Native history and derived larger intervals | Reuse `MarketCandle` and `Store.write_candles`, with a shared-connection transaction entrypoint. Do not derive these candles from quotes. |
| Corrections | Receipt-ordered UPSERT plus `candle_revisions` | Corrected REST history and `candle_corrected` events | Reuse audit/UPSERT/event code; add persistent source version ordering and concurrency protection. Legacy revision rows cannot reconstruct every original full observation. |
| Quotes | `market_ticks` + `latest_prices`, or root-only `write_quotes` latest-only path | Decimal-string quote fields, separate candle-close fallback | Reuse historical `Store.write` on the company server so accepted quotes persist even when too old to update latest. Add import identity ledger. |
| Quote latest ordering | `(order_ns,received_timestamp,tick_id)`; ns uses source time when present | Older source times cannot replace newer ones | Preserve the guard and test it over HTTPS. Receipt-only observations have no provable exchange ordering; retain that limitation and never create a source timestamp. |
| Source/receipt times | Candle bucket times and collector receipt; quote source time/ns can be null | Explicit source/receipt and freshness semantics | Preserve verbatim meanings. Add server ingestion time in import metadata, not by rewriting domain timestamps. A bucket boundary is not an invented exchange publication time. |
| Coverage | `candle_coverage` tri-state ranges and `session_info` | Expected gaps, unknown and not-expected remain distinct | Carry versioned source coverage snapshots. Recompute destination gaps against actually stored candles; do not copy a source's completeness claim before its records arrive. |
| Commit journal | Absent in installed schema `0004`; schema `0006` clock/events in newer code | Committed replay and WS events | Reuse on the destination. It is bounded and strips most metadata; it is not a durable source outbox. Old binaries/direct SQL do not append its events. |
| Collector health | No `collector_status` in `0004`; singleton snapshot from `0005` | Expiring health/source-status projections | Add producer-scoped receipt/status storage. Preserve capture/expiry time when projecting into the existing singleton interface. Upload time must not renew source health. |
| Batch/observation receipts | None | None | New additive tables and authenticated endpoints required. Market PKs/latest ordering alone do not detect changed payloads under reused IDs. |
| Go ChartData | `api_models.py:173`, `api_v1.py:180` | `a` time, `b` open, `c` close, `d` high, `e` low, `f` count, `g` volume, `h` value | Preserve arrays, alignment, pagination header, float64 boundary and empty shape. Count/Value remain documented compatibility zeros; null typed volume becomes compatibility zero. No invented trade count/value. |
| Frontend REST/WS | Same root/snapshot `api.py`, `api_v1.py`, models, events, price selection | Existing contracts and bounded replay | Keep schemas/auth/read routes compatible. Add ingestion routes in the ingestion role only. No exactly-once WS claim. |

OANDA and FXCM must stay separate canonical instruments. The proposed business registry has
`FX:OANDA:EURUSD → OANDA:EURUSD` and `FX:FXCM:EURUSD → FX:EURUSD`, both under persisted provider
`tradingview_guest`. The adapter explicitly validates the FX/FXCM venue relationship
(`providers/tradingview.py:285`). The configured 100-source profile contains OANDA EURUSD,
but not FXCM EURUSD; do not imply that the broader proposed business registry is deployed.

## 3. Writer and ordering findings

The reusable writer is already substantial:

- `Store.write` / `_write_observations` (`persistence.py:329–389`) owns a connection and
  transaction, stages exact ticks, persists history, conditionally updates latest, and appends
  events in that transaction. Tick history deduplicates `(storage_timestamp,tick_id)`.
- Root `write_quotes` coalesces each batch to newest latest state, stores no tick history,
  and journals only latest updates. It cannot retain every intermediate quote.
- `write_candles` (`persistence.py:646–893`) owns another connection/transaction and combines
  coverage, revision audit, candle UPSERT, latest state and journal writes.
- `publish_collector_status` (`persistence.py:222`) owns its connection and refreshes TTL using
  the local database clock. Calling it unchanged for remote backlog would misstate health.
- `append_events` (`events.py:36–89`) locks/updates the singleton clock in the data transaction.
  Its cursor is commit-ordered; a bare sequence or revision ID is not an equivalent checkpoint.
  Retention deletes journal entries, not market history. Arbitrary metadata is deliberately
  excluded from journal payloads.

An outer HTTP transaction calling today's Store methods would **not** make receipts atomic:
the methods acquire separate connections. Extract connection-taking internal helpers, keeping
existing public methods as transaction-owning wrappers. Return write results explicitly;
avoid using shared mutable `last_candle_write` to report concurrent request results. Preserve
collector callers in `ingestion.py`, `candle_ingestion.py`, sample writers and qualification tools.

Specific risks inferred from SQL inspection, **not newly reproduced against a database**:

1. Candle changes require strictly newer collector receipt time **and** a changed OHLCV/status
   tuple. A newer identical candle does not advance the historical row's receipt watermark,
   while `latest_candles` can advance. Example ordering: A at receipt t1, identical A at t3,
   then different B at t2 can leave historical B and latest A. Test and fix this in the shared
   helper: retain a high-water version even for identical observations, without creating a
   financial revision/event for a no-op. Include the same-key initial-insert race.
2. Existing candle locking explicitly assumes one owned writer. A missing row cannot be
   protected by its current `FOR UPDATE`. Multiple ingestion processes need deterministic
   transaction-scoped source locking before reading/upserting/auditing, with one authorized
   owner per source. Do not add an in-memory lock that fails across workers.
3. Candle `coverage_start` is not moved earlier by its conflict update. Remote historical
   backfill and disjoint coverage windows require an explicit coverage merge path. Unobserved
   bridge intervals remain unknown; min/max candle times do not prove expected completeness.
4. Metadata-only observations do not update `market_candles` under the existing changed-field
   predicate. Preserve imported provenance in the immutable import ledger and define which
   semantic metadata changes (particularly volume units) must update materialized state.

Transaction-scoped locks and consistent acquisition order are supported by PostgreSQL's
[locking documentation](https://www.postgresql.org/docs/17/explicit-locking.html).

## 4. Independent quote qualification

| Question | Evidence-based result |
|---|---|
| Is genuine independent quote acquisition implemented in the checkout? | Yes: `TradingViewGuestProvider` consumes quote deltas; `parse_quote` retains only current delta prices. Fixtures test identity, decimals, timestamps, delayed rejection and reconnect. This is implementation evidence, not a new live qualification. |
| Can the new collector collect native candles and quotes together? | Opt-in `dual` factory builds candle workers plus independent quote workers (`providers/factory.py:66`), sharing collector ownership and a six-socket local bound. Default quote history is false. |
| Is that code installed in the inspected collector? | No. Installed Settings accepts only ticks/candles and its factory selects alternative provider objects. Configuration selects candles. |
| What does candle mode prove about current price? | Only a latest completed candle close. The API labels this `price_type=candle_close`; it is not an independent live quote. |
| What source times are available? | `lp_time` applies to last only. Bid/ask-only deltas can have no source timestamp. Source and receipt clocks must remain distinct. |
| Realtime/delayed behavior? | Current TradingView parsers require explicit validated streaming/realtime metadata and reject delayed/unverified sources. A changing price alone is insufficient. |
| Genuine historical live evidence? | `docs/dual-prior-live-sample.json` records bounded 2026-10-13 probes. It explicitly does not qualify today's availability or the new simultaneous runtime. |
| Production acceptance? | **Blocked for independent quote delivery** until a separately reviewed collector version/mode is installed and genuinely qualified. No new live probe was run. |

## 5. Recommended architecture

Use one authoritative implementation under `src/marketdata`, exported into a new v1.1
distribution. Deploy the same company application image as two processes:

```text
TradingView → existing/local collector → local PostgreSQL
                                       + transactional sync outbox (reviewed collector change)
                                              ↓ committed rows only
                                       independent sync worker + local SQLite delivery state
                                              ↓ verified HTTPS + producer credential
company gateway → v1.1 ingestion factory → shared Store helpers → company PostgreSQL
                                                                    ↓ SELECT-only
                                                        v1.1 read factory → REST / WS
```

Keep `APPLICATION_ROLE=api` and `marketdata.api:create_app` for existing readers. Add
`APPLICATION_ROLE=ingest` with `marketdata.ingest_api:create_app`; do not start TradingView
or import concrete provider transports there. This is role separation within one codebase,
not a second market-data backend. Migration administration stays a separate tools role.

The company DB has no public port. The worker receives a local source DSN and an HTTPS
destination URL/token, never a company DB DSN. The ingestion service receives no upstream
provider credentials. Frontend/BFF read credentials, producer credentials, reader DB role,
writer DB role and schema-owner credentials are independent.

Initial scope is one active collector producer/dataset per company installation, with explicit
instrument grants. A later laptop-to-server handover needs epoch fencing and source ownership
transfer; simultaneous independent authoritative producers are not an implicit v1.1 feature.

## 6. Proposed ingestion protocol v1 (review candidate)

This is the exact proposed initial protocol, not implemented or published OpenAPI yet.
The application release is 1.1; the new ingestion protocol starts at v1 independently.

| Method/path on ingestion process | Purpose |
|---|---|
| `POST /api/ingest/v1/candles` | Atomic completed-native-1m batches; stream is `candles`, `corrections` or `backfill`. |
| `POST /api/ingest/v1/quotes` | Atomic batches of independently observed quote deltas; stream `quotes`. |
| `POST /api/ingest/v1/status` | Captured collector/source status batches; stream `status`. |
| `POST /api/ingest/v1/coverage` | Explicit versioned coverage windows/partitions; stream `coverage`. |
| `GET /api/ingest/v1/receipts/{batch_id}` | Authenticated lookup of this producer's committed receipt after an uncertain response. |
| `POST /api/ingest/v1/reconcile` | Bounded observation-ID/hash and source-key/version reconciliation. No SQL access. |

Require `Authorization: Bearer <producer-secret>` over HTTPS, JSON content type and
`X-Payload-SHA256`. The authenticated credential determines producer identity and grants;
the body must match, and cannot self-authorize mappings. Support credential IDs, overlapping
rotation and revocation via mounted private configuration. Store hashes/key identifiers,
not plaintext secrets in public files or request logs. Recheck revocation on every request.

Envelope fields: `protocol_version` (`1`), `producer_id` (UUID), `source_epoch` (UUID),
`batch_id` (UUID), `stream`, `records` (nonempty bounded array). A record contains stable
`observation_id`, decimal-string `source_position` when available, `kind`, and typed
`observation`. Position is local replication ordering, not a claimed provider sequence.
Backfill snapshot records additionally carry an explicit `snapshot_fence` and capture mode;
these transport fields do not become fabricated upstream timestamps.

Validation and representation:

- Producer submits immutable canonical UTF-8 JSON bytes: sorted object keys, compact
  separators, UTC `Z` dates, monetary values as finite plain decimal strings. Worker persists
  those exact request bytes before sending. Server hashes raw bytes (including envelope),
  verifies the header, rejects duplicate JSON keys/NaN/unknown fields, and bounds the body
  while reading even if Content-Length is missing. Compression is initially unsupported.
- Financial JSON numbers/booleans are rejected. Decimal strings have a configurable length
  bound (initially 128 characters), no exponent, no rounding. Quote source nanoseconds and
  replication positions use unsigned decimal strings bounded by signed BIGINT range.
- Observation schemas reuse `MarketCandle`/`Tick` invariants plus strict wire validators.
  UUID/canonical symbol/provider/exact provider symbol/type/venue must agree with the
  pre-provisioned registry and grant. Unknown mapping is an error, not automatic registration.
- Preserve source timestamp/null, source ns, original collector receipt, original status and
  bounded provenance. Native candles retain bucket start/end; optional genuine upstream
  publication time stays null when absent. Record `server_ingested_at` separately using the
  company DB clock. Reject implausibly future receipts under a documented skew allowance;
  historical source/receipt times have no arbitrary freshness rejection.
- Candles must be completed, UTC minute-aligned, interval `1m`, basis `native`, valid OHLC,
  and supported source status. First release accepts the collector's verified realtime-native
  semantics; delayed-native acquisition is not claimed. Quotes may carry honestly classified
  delayed status only under an explicitly provisioned grant. Test fixtures may use simulated
  data under test-only producer policy. No null fields are filled from candle closes or caches.
- Metadata is a typed/allowlisted bounded provenance object (initially 16 KiB per record),
  including native volume classification and unknown raw volume strings. No arbitrary secrets.
- Starting limits: 500 candle/quote records, 50 status/coverage records, 2 MiB body, bounded
  coverage range count (500). Limits are configurable. A worker splits only unsent batches;
  it never changes bytes under an already submitted batch ID. Initial admission budget is
  5 requests/second/producer with burst 10 and at most two in-flight data requests. Enforce
  across workers through gateway policy and a PostgreSQL-backed producer admission counter.
  These are conservative configuration defaults, not measured throughput claims.

Idempotency and acknowledgments:

1. Batch key is `(producer_id,batch_id)` across all ingestion endpoints. Store raw-body
   SHA-256, stream/epoch and immutable acknowledgment. Same key/same bytes returns that
   receipt; same key/different bytes returns `409 batch_payload_conflict`, including JSON
   formatting-only differences. A batch with any invalid record has no data/receipt commit.
2. Observation key is `(producer_id,source_epoch,observation_id)`. Quotes preserve original
   `tick_id`; outbox observations use stable persisted IDs; legacy candle IDs are UUIDv5 over
   source identity, bucket, original receipt and canonical content fingerprint. Repeated
   observation IDs with different observation content return `409 observation_payload_conflict`.
   Hash the canonical typed observation, excluding delivery batch/stream/scan-fence fields.
3. A newer candle version is determined by a persisted source-position/state-version gate per
   natural candle key, never upload order. Equal-version conflicting content is rejected;
   older versions remain durably recorded with a superseded outcome and cannot change current
   state. No-op versions still advance the ordering barrier. Quotes keep the existing
   source-time/receipt/tick-ID latest guard; stale quotes can persist without a latest event.
4. A `200` acknowledgment is returned only after the owning transaction context has exited
   successfully. It includes producer/epoch/batch, fingerprint, `state=committed`, counts and
   per-record outcomes (`applied`, `recorded_no_change`, `superseded`, `duplicate`). Persist the
   original receipt so timeout/restart retries return the same result. Do not label a
   transaction-internal timestamp as the exact physical commit time.
5. Keep compact batch receipts and observation identity/hash tombstones for the producer
   epoch's supported replay lifetime. Initial implementation does not expire them automatically.
   Storage sizing/archival is explicit; bounded WS retention never authorizes receipt deletion.

Errors use a separate ingestion error schema with stable code, request ID, record index where
safe, and retryability. Use 400 malformed JSON/digest, 401 absent/invalid credential, 403 revoked
or out-of-scope producer/source, 404 unknown receipt/mapping, 409 identity/version/epoch conflict,
413 body/record-count limit, 415 unsupported content encoding/type, 422 typed semantic violation,
429 admission limit with Retry-After, and 503 unavailable/uncertain DB commit. Existing read
API error bodies/status conventions remain unchanged. Never expose database errors or secrets.

## 7. Atomic persistence and proposed additive migration

New `0007` follows the inspected `0006`; do not edit historical migrations. Proposed tables:

| Table(s) | Purpose and keys |
|---|---|
| `ingest_producers`, `ingest_source_grants` | Stable producer/active dataset epoch, enabled state, authorized exact source/kinds/statuses. Enforce a single active owner per source. Provisioned administratively. |
| `ingest_admission_budgets` | Per-producer shared rate/concurrency admission state. Runtime updates this table without acquiring permission to change producer policy/grants. |
| `ingest_batches` | PK producer/batch; raw fingerprint, immutable receipt, DB ingestion timestamp. A committed row is the durable acknowledgment. |
| `ingest_observations` | PK producer/epoch/observation; canonical hash, original typed observation, source position, source key, batch and outcome. Preserves provenance and accepted historical/superseded records without creating another serving schema. |
| `ingest_candle_heads`, `ingest_coverage_heads` | Latest accepted source version/fingerprint per natural key or coverage window, including no-op barriers. Index by producer/epoch/source/window. |
| `candle_observation_heads` | Shared writer receipt high-water state per natural candle key, including identical observations. Seed from existing historical/latest receipts without changing financial observations. |
| `ingest_source_status` | Captured/expiry/server-ingestion timestamps and original health snapshot. Materialize the existing singleton status interface for the initial one-producer scope. |
| `sync_source_clock`, `sync_outbox` | Local opt-in replication feed; epoch/commit-ordered position, stable observation ID/kind/source/full payload/hash. Empty and unused on the company DB. Separate from the bounded frontend journal. |

Use ordinary PostgreSQL tables for receipts/outbox; existing Timescale market tables are reused.
Add only demonstrated lookup/unique indexes. The shared candle receipt head fixes the no-op
ordering case without generating financial events or rewriting original observation timestamps.
Final DDL belongs to milestone 1 review/tests, not this audit.

Destination transaction sequence:

1. Bound/authenticate/parse request, then begin one company DB transaction with durable commit
   settings (`synchronous_commit=on`). Acquire the producer policy lock; verify epoch and grants.
2. Acquire/inspect receipt identity. A concurrent identical request waits and then observes the
   committed receipt; different payload conflicts. No committed pending receipt is left behind.
3. Acquire source locks in a fixed sorted order, then observation/version keys. Check all
   identity conflicts before materializing changes. Use the shared Store helpers on this
   **same connection** to write candles/revisions/coverage or quote history/latest state.
4. Append journal events only for actual materialized changes via existing `append_events`.
   Its clock lock remains at the transaction tail. Write import provenance, version barriers
   and the immutable receipt in this transaction. Status batches project capture/expiry values
   explicitly; never use the old upload-time TTL refresh unchanged.
5. Commit, then return the saved acknowledgment. On lost/uncertain commit response, retry
   the exact batch or query its receipt. Rollback leaves no data/event/receipt split.

For coverage snapshots, define the tracked source window explicitly. Preserve source unknown
and not-expected partitions, subtract actual destination bars as positive evidence, and mark
remaining expected-but-absent destination minutes missing. Keep original source missing
ranges as replication diagnostics. Source completeness and destination delivery progress are
different facts. Merge earlier/disjoint windows without declaring untracked bridges complete.

For remote status, preserve source `captured_at`, `expires_at`, run ID and provider/source
states. Apply a configured maximum lifetime anchored at capture; an old queued heartbeat
cannot become fresh upon arrival or retry. Reject stale status versions; never manufacture
TradingView health from a successful HTTPS upload. Keep sync-link health separate.

Both v1.1 process roles must check schema `0007`, including production read-role privilege
validation over all new tables. Released v1.0 remains exact-`0006` and must not be pointed at
`0007`. Migration tests must cover fresh ordinary PostgreSQL/Timescale installations and
populated `0006 → 0007` upgrades. Any downgrade testing runs only on disposable databases;
downgrade would discard new replication metadata and is not an automatic production rollback.

## 8. Local synchronization and historical reconciliation

Preferred, separately reviewed collector change:

- In each existing local writer transaction, append full eligible observations/coverage/status
  to the outbox with stable IDs. Do not call HTTP in collector code. Quote outbox capture must
  retain selected observations durably before latest-state coalescing could erase them; if all
  accepted quote deltas are in scope, persist each delta to outbox, including those not winning
  latest state. Make the capture policy explicit and test it. Financial state, source outbox
  and existing journal commit together.
- Allocate outbox positions through a locked transactional clock, following the existing
  event-clock principle. A serial/identity or transaction-start timestamp alone can skip a
  late commit. Persist source epoch and fence it on restore/ownership transfer.
- The independent worker uses a SELECT-only local source role and a private SQLite database
  for staged immutable request bodies, batch identities, fingerprints, retry state, and separate
  checkpoints for candles, corrections, quotes, coverage, status and backfill. SQLite is local
  delivery state, not another message service. Lock the worker state to one active process.
- Stage a bounded page from a repeatable-read committed view, fsync its bytes/state, then send.
  Advance delivery checkpoints only after a verified committed receipt is durably saved in
  SQLite. Crash after remote commit but before local save repeats the same ID/bytes safely.
  Resolve prior uncertain batches before issuing a newer status that could hide them.
- Verify HTTPS certificates/hostname with HTTPX defaults or a configured company CA. Never
  use `verify=False`, plaintext production URLs, credential-bearing query strings or automatic
  cross-origin redirects. HTTPX documents verification and custom trust in its
  [SSL guide](https://www.python-httpx.org/advanced/ssl/).
- Retry network timeouts, uncertain responses, 429 and transient 5xx using capped exponential
  backoff with jitter and Retry-After; stop/quarantine auth, mapping, validation and identity
  conflicts. Persist retry state across restarts. Bound requests/bytes/in-flight work, prefer
  live traffic and corrections over backfill with fair quotas, and expose backlog age/disk use.
- Stop staging when the local delivery spool is full; keep source outbox records. Never delete
  unacknowledged source records to meet a storage cap. Cleanup uses a separately permissioned
  local maintenance action after durable acknowledgments and a documented retention margin.
  Finite disk cannot guarantee unlimited outages; surface capacity failure explicitly.

Historical bootstrap must have a defined seam with live delivery. In one local repeatable-read
view, capture outbox fence F and stream the current genuine source candle state plus available
historical ticks/coverage into durable bounded pages. Baseline candle state is explicitly
authoritative as of F; changes after F have greater positions and cannot be overwritten by
late baseline uploads. Retain original observation fields/IDs, not scan/upload times. A
duplicate baseline observation can advance its state barrier without generating a new event.
Use per-source/window backfill progress so a large historical scan cannot block current quotes.
Do not hold an MVCC snapshot open while waiting for the internet.

Reconcile bounded ID/hash manifests and source-key/current-version manifests through HTTPS.
At a declared fence compare immutable imported observations to prove delivery; compare current
state only at equal versions, treating newer destination versions as superseding older scan
results. Return bounded missing/conflicting IDs for repair; counts alone do not prove equality.
Reupload missing records under new batch IDs with the same observation IDs. Hash exact decimal
strings, nulls, identity, status, timestamps and provenance consistently. Never delete company
history automatically because a local row is absent. Document source retention and restore
epochs; a server restart is distinct from catastrophic restore/data loss.

Transitional read-only adapter for an unchanged legacy `0004` source:

- Introspect an explicitly supported schema; do not use today's exact-`0006` Store connection
  against it. Read final candle payloads and any actual historical ticks with local SELECT-only
  credentials; keep all progress/fingerprints in SQLite.
- A rolling recent overlap is insufficient. Repeatedly sweep **all configured historical
  source/time partitions** with keyset pagination and compare durable fingerprints. Rescan
  revision rows as audit evidence as well; their serial IDs/discovered timestamps are not
  commit-order watermarks. This detects late inserts/corrections to old buckets after a prior
  pass when those final rows remain present.
- Legacy `candle_revisions` stores changed fields/old/new OHLCV/status and discovery time,
  not every full original candle payload/receipt/provenance. Do not synthesize missing fields
  or pretend that scanning it recreates all historical observations. Latest-only quotes and
  expired journal rows are unrecoverable without another retained source.
- Therefore this adapter can provide eventual **current candle-state** catch-up/backfill under
  retained-source assumptions. It cannot satisfy every-observation delivery or independent
  quotes from the inspected candle-only installation. It is not the full-acceptance design.

No installed collector migration, mode change or restart is authorized by this report. Build
and test the outbox-capable collector on isolated databases first; present that collector
delta and its eventual rollout separately from the company v1.1 installation.

## 9. Database privileges and deployment design

Provide reviewed SQL scripts with explicit per-table grants, not blanket future-table grants:

| Principal | Required access |
|---|---|
| Schema owner/migration administrator | DDL and registry/producer grant provisioning only in explicit administrative steps. Not a runtime credential. |
| Company reader | SELECT existing market/read journal/status/registry/version objects; no ingest secrets/receipts, INSERT/UPDATE/DELETE/TRUNCATE, CREATE, TEMP or administrative membership. Validate newly added tables for accidental write rights. |
| Company ingestion writer | SELECT schema/version/authorized registry/policy; INSERT market ticks/revisions/import observations/receipts; INSERT/UPDATE market/latest/coverage/status/version heads and admission budgets; UPDATE event clock and INSERT/DELETE bounded event journal. TEMP for existing staging and USAGE on revision sequence. No registry mutation, policy modification, DDL, TRUNCATE, role management or market-history DELETE. |
| Local sync reader | SELECT only required local tables/outbox/version; no company DB credential. SQLite file private to the worker user. |
| Local outbox maintenance | Narrow local cleanup permissions, separate from ordinary SELECT-only worker reads. Never company database access. |

The current production Compose is a useful hardening reference, but its `collector` service is
provider collection, **not** an HTTPS ingest server. Create a company-specific v1.1 Compose file
with DB, read and ingest services from one app image, explicit migration tools, mounted separate
DSNs/tokens, internal database networking, no host DB port, loopback/private API ports and
gateway TLS/body/rate limits. No broker is needed. Keep the laptop worker a separate service.
The existing production migration entrypoint unconditionally requests Timescale; add an explicit
plain-PostgreSQL mode for the company's default fresh installation. Never reuse an existing
collector volume/project or the supplied dev/sample credentials.

Deliver separate copyable PowerShell and Linux guides for company installation and collector
operation, including CA trust, private credential files/ACLs, explicit migrations/grants,
read/ingest role startup, Windows service or Task Scheduler/systemd worker ownership, rotation,
backup/restore epoch handling, outage recovery, reconciliation and verification commands.
Preserve the permanent read credential behind company server/session authorization rather than
putting it in browser code. The supplied BFF remains provisional until wired to company sessions.

Record the operator's existing access approval and the authorized company redistribution scope,
recipient/audience limits and applicable provider/exchange entitlements. This audit does not
revoke the recorded approval or make a new legal determination. Technical access and successful
fixture tests are not evidence that every redistribution use is licensed.

## 10. File-by-file implementation plan and milestones

All paths below are **proposed**, not changes implemented in Phase A.

| Milestone | Existing files to change | New files / deliverable | Required verification |
|---|---|---|---|
| 1. Transactions and schema | `persistence.py`, `events.py` only as needed, migration head checks, relevant persistence/migration tests | `migrations/versions/0007_replication.py`; receipt/version/outbox storage helpers; scoped role SQL | Same-connection rollback, no-op ordering, same-key concurrency, receipt/event/data atomicity; fresh/upgrade on isolated PG and Timescale. |
| 2. Ingestion runtime | `config.py`, `production.py`, observability | `ingest_api.py`, `ingest_models.py`, `ingest_service.py`, `replication_store.py`, `tests/test_ingest_api.py` | Strict validation, limits, committed acknowledgments, reused-ID conflicts, no provider startup, role isolation. Authentication required from the first runnable endpoint. |
| 3. Authorization/hardening | Production role validation and configuration tests | `ingest_auth.py`, private credential example, gateway policy, reader/writer grant scripts and denial tests | Separate read/ingest tokens, producer/source/epoch scopes, rotation/revocation, cross-worker rate limits, wrong principals denied, secrets absent from logs. |
| 4. Durable worker / separately reviewed collector hooks | `persistence.py`, collector status writer, writer protocols/callers only where required | `sync_worker.py`, `sync_source.py`, `sync_state.py`, `sync_transport.py`, `sync_outbox.py`, worker unit/integration tests | No direct company SQL; immutable staged requests; crashes at every acknowledgment/checkpoint boundary; outbox rollback and separate stream progress. |
| 5. Backfill/reconciliation | Shared coverage helper; no replacement native query engine | `sync_reconcile.py`, manifest models/routes, `tests/test_sync_reconciliation.py` | Old-bucket correction/late insertion, baseline/live fence, disjoint windows, exact hashes, retained-source limitations and no silent cursor reset. |
| 6. End-to-end compatibility | Existing API/WS tests extended, existing read models preserved | `compose.sync-test.yaml`, TLS test harness, `tests/test_sync_end_to_end.py`, evidence report | All acceptance cases below with separate local/company DBs, real HTTPS, separate processes and least-privilege roles. |
| 7. Packaging/docs | Exporter parameterized by version/output, `Dockerfile.api`, project metadata/lock, package allowlist/tests | `distributions/marketdata-fastapi-v1.1/`, new ZIP + SHA-256, production company Compose, PowerShell/Linux guides, worker service examples | Extract ZIP outside parent checkout, install locked dependencies afresh, build same-image roles, migrations/grants/smoke from package, manifests and compatibility checks. |

The root remains authoritative; export shared helpers into the v1.1 snapshot. Do not independently
edit two SQL implementations, overwrite `distributions/marketdata-fastapi/`, or regenerate the
v1.0 ZIP. Include `httpx` through an explicit synchronization dependency extra and keep read
startup independent of optional transport/migration tools. Resolve release/package versions
consistently to 1.1.0 while preserving the existing v1 read contracts.

## 11. Acceptance and evidence plan

First deterministic integration scenario uses one configured instrument, two disposable
PostgreSQL databases ending `_test`, task-specific Docker labels/ports and a local trusted TLS
CA with hostname verification. A fixture is visibly simulated even if its wire shape matches
a captured native provider message. It validates software behavior; it does not prove genuine
current TradingView delivery. Genuine provider acceptance is a separate approved, noncompeting
run against the same flow with actual native candles and independent quotes.

| Required acceptance | Phase B/C evidence to collect |
|---|---|
| 1–3. Local commit → authenticated HTTPS → company commit | Independent local DB readback, staged request hash, TLS verification, company receipt/row readback; reject auth/mapping failures without writes. |
| 4–5. REST and Go ChartData | Exact Decimal OHLC comparison and aligned `a`–`h` arrays, documented zeros/null handling, no API-schema regression. |
| 6. Committed WS event | Existing MarketHub path in a separate read process; observe no precommit event and correct replay fence after commit. |
| 7. Duplicate upload | Lost-response retry, concurrent identical batch, same observation in another batch; assert one intended revision/event and conflict on altered bytes/content. |
| 8. Historical correction | Correct an old minute after newer minutes; reverse original/correction upload order; include no-op t3 then stale t2 and equal-version conflicts. Verify coverage/latest/history agree. |
| 9. Independent genuine quote | Prove source quote-delta acquisition and local committed identity before upload; latest reports `price_type=quote`, appropriate source/receipt clocks, status and freshness. No candle-close substitute. |
| 10. Stale/delayed quote | Send older source-time quote after a newer quote, retain it in history/import ledger, assert latest and event tail unchanged. Include receipt-only and nanosecond cases. |
| 11–12. Outage/restart/checkpoints | Disconnect HTTPS, restart ingestion/read/worker separately, drop the response after DB commit, crash before/after SQLite save, then reconcile exact IDs with zero missing committed source observations. |
| Backfill/workload | Full source window sweep, late records beyond overlap, correction/quote fairness during backfill, disk pressure, body/rate limits, sustained realistic source/quote rates with measured latency and backlog. |
| Migrations/permissions/release | Fresh PG/Timescale install, populated upgrades, actual restricted writer and read-only denial tests, package extraction/install, company proxy/TLS and company-session frontend checks. |
| Preservation | Before/after deployed image/config/container/volume identity and source-data manifest from an authorized read-only snapshot; prove task DB allowlists. No fixture may truncate a production-like source. |

Do not mark genuine-data acceptance passed using fixtures. If no genuine independent quote is
available in the approved runtime, the release can pass software integration while full
production acceptance remains blocked. Current source/event tests are reusable regression
coverage, not evidence that HTTPS synchronization already exists.

## 12. Verification actually executed in Phase A

| Check | Result this session |
|---|---|
| Root `uv run --no-sync pytest -q -p no:cacheprovider`, TEST_DATABASE_URL unset | **329 passed, 157 skipped**, 32.81 s. Database/integration cases skipped; one existing Starlette/httpx deprecation warning. |
| Frozen standalone directory tests, explicit local `src` PYTHONPATH, existing root interpreter, TEST_DATABASE_URL unset | **70 passed, 73 skipped**, 2.65 s; local-package/OpenAPI/manifest checks included. One same dependency warning. This was not a new environment installation. |
| `uv run --no-sync ruff check --no-cache .` | Passed. |
| `uv run --no-sync ruff format --check --no-cache .` | Passed, 212 files. |
| `uv run --no-sync mypy --cache-dir nul` | Passed, 38 source files. |
| `uv run --no-sync alembic heads` | `0006 (head)`. Inspects migration source; does not apply migrations. |
| Distribution/ZIP verification | 53 snapshot hash entries valid; 66 archive entries CRC-valid and matching source snapshot after LF normalization; released ZIP SHA-256 unchanged. |
| Installed-code inspection | Stopped-container code/config/migration inventory read without start/exec/provider I/O. No DB SQL executed. |
| Git state before audit documentation | Only pre-existing untracked `TradingViewConvHistory/`; HEAD unchanged. Final documentation diff checked separately. |

Not executed: PostgreSQL/Timescale integration tests, migration application/roundtrip,
database permission tests, real HTTPS upload, durable worker/reconciliation tests, actual
company REST/WS/frontend delivery, genuine candle/quote qualification, fresh v1.1 package
installation or workload benchmarks. These require Phase B implementation and isolated
infrastructure; none is claimed passed. Earlier PG/Timescale results in existing documents
are historical evidence and were not rerun here.

Recommended next action: review this protocol/transaction/outbox proposal, including the
separate collector delta and its limitations, then begin milestone 1 on isolated databases.
Phase A does not authorize any live collector switch or migration.
