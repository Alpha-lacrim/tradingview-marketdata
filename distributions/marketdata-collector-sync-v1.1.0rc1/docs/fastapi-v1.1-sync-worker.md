# Milestone 3B: independent durable HTTPS worker

This guide describes `python -m marketdata.sync_worker` and the `marketdata-sync` entry point.
No provider transport or collector scheduler is started. Company database credentials belong
only on the company server. The worker receives a restricted **local source** DSN and a separate
producer bearer token. See the [milestone report](fastapi-v1.1-milestone3b.md) for actual tests
and qualification limits. These are installation instructions, not authorization to touch the
preserved legacy collector. Its eventual rollout remains separately reviewed.

Milestone 3B currently exists in this uncommitted review workspace. A GitHub main clone or the
frozen v1.0 ZIP does not contain these changes. Future installation must use the reviewed source,
additive migrations and role scripts together. The task-owned wheel contains application code;
it does not provision schemas, database roles, secrets or services. Complete release packaging
and fresh release-package installation qualification belong to Milestone 5. Milestone 4
qualifies the integrated source/worker/company pipeline on disposable infrastructure.

## Verified compatibility and boundaries

| Source | Transfer / destination | Semantics and limits |
|---|---|---|
| Base `0006`, source `sc0001` | Additive source `sc0002` | Existing capture rows/epoch/positions preserved; worker state is separate. Collector accepts both extension versions. Worker requires `sc0002`. |
| Destination `0009` | Additive `0010` | Quote ordering heads and destination identity; no changes to `0007`–`0009`. |
| `source_candle_outbox.record` | Protocol-v1 `CandleRecord` | Original UUID, positive position **string**, epoch, OHLC/volume plain decimal strings, UTC bucket/receipt, native basis and provenance. `change_kind` stays local. |
| `source_quote_state.record`, `version` | Protocol-v1 `QuoteRecord` | Derived full snapshot identity; field-level original identities, clocks/status/metadata. Superseded intermediate updates may coalesce. |
| `source_capture_clock` | Batch `source_epoch`, ordering positions | Shared, commit-serialized candle/quote/metadata position space; numerical candle gaps are valid. |
| `source_candle_exports` | Enrollment anti-join only | Not evidence of destination completeness. Reconciliation reads actual financial rows. |
| `candle_coverage` cumulative window/multiranges | Existing `/coverage` | Unknown, not_expected and source_missing remain disjoint; company derives missing ranges from its actual bars. Fragmentation over the protocol bound fails preflight. |
| `collector_status` + captured provider/symbol health | Existing `/status` | Actual run ID, updated_at, expiry, running/database flags and source health; no worker-generated market heartbeat. |
| Producer, session and complete provisioned manifest | Existing session/renew/recover | Same-producer expiry recovery only; no discovery, takeover or new ownership. |
| Existing Store methods / event journal | Existing READ REST/WS/ChartData | Defaults retained. Trusted snapshot and missing-row restoration options use the shared SQL and journal transaction. |
| Old installed schema `0004` | **Not migrated here** | Future source path is only `0005`, `0006`, then the source extension. Never destination `upgrade head`. |

Current checkout has independent quote parsing, dual candle/quote operation and durable
partial-field capture. This does not prove that the preserved older installation captures
quotes, or that genuine live acquisition, entitlements or redistribution rights are qualified.
Completed candle closes are never used as current quote substitutes.

## Destination ordering and actual-state evidence

Once a producer/epoch/ownership context starts coalesced snapshots, raw quote deltas cannot
overwrite that context's latest state (`quote_snapshot_required`). Legacy raw quote producers
retain their existing behavior until they opt into snapshots. Existing read contracts are unchanged.

`ingest_quote_heads` stores instrument/provider, producer, epoch, ownership generation, source
position, observation ID/hash and materialized full payload. Existing producer/owner row locks,
sorted source advisory locks and observation identity locks serialize changes. Within a context,
position wins over primary-field clock/receipt/UUID. Across contexts, ownership generation must
advance; positions are not compared across epochs. Disabled/replaced producers remain fenced.
Field clocks may not regress; equal clocks use snapshot position, and an unchanged original
field identity cannot be reused with different contents. Omitted/null fields retain prior fields.
Last is primary when present, otherwise bid, then ask. Updating ask never refreshes old last time.
No uploaded financial timestamp is replaced by server time.

Quote receipt records add `quote_state`: producer_id, source_epoch, ownership_generation,
source_position (string), observation_id, observation_sha256, materialized (boolean), payload.
The receipt is atomic evidence **at that transaction**. A saved receipt alone is insufficient
after a restore. The worker separately reads current materialized state and requires its accepted
snapshot to match a locally known current or durably staged snapshot, including field provenance.
It checks version/ownership/epoch and original observation fingerprint before acknowledging.
Unknown newer state, missing materialization or divergent fields stops acceptance.

POST `/api/ingest/v1/reconcile` uses the existing HTTPS/bearer/quota/deadline middleware.
Request: `protocol_version:1`, current `session_id`, `session_generation`, and 1..100 `keys`.
Each key has instrument_id, provider, stream (`candles` or `quotes`), source_epoch,
ownership_generation, optional bucket_start. A candle key without bucket_start is an explicit
high-water probe; it does not create a candle. Body limit is min(256 KiB, configured ceiling).
Response: protocol_version, producer_id, destination_id, server_time, results in request order.
Each result echoes key and `high_water` (producer/epoch committed maximum position, string).
Candle results include candle_sha256 (null if absent), revision_count, revision_sha256 and
revision_truncated; quote results include
quote_state. Financial candle hashes normalize decimal scale/UTC and include OHLC, typed volume,
volume_additive, source/realtime status and source identities. Receipt/ingestion times are excluded.
Only an authenticated current owner/session can inspect its streams. There is no ownership edit.

## Local durable state, locks and permissions

`sc0002` adds source_sync_owner, source_sync_batches, source_sync_archive, source_sync_guard
and narrowly granted SECURITY DEFINER functions. The separate source migration chain remains
based on `0006`. It never imports or runs destination migrations.

The worker holds the exact `SourceCapture.consumer_lock()` session advisory lock throughout
its lifecycle. A claim stores a random owner token, increasing generation, backend PID and
backend start time. Every state operation checks that tuple and the actual advisory lock in
pg_locks. Mutating transactions hold the owner row lock; deferred commit triggers recheck
lock liveness. A lost lock aborts in-flight cursor/state changes. A supervised task checks the
connection every 250 ms and cancels delivery work on loss. A remote upload can already have
committed; its receipt is recovered after a legitimate new local claim. No local ack follows
from an old task's late HTTP response. A dead backend may take a short time to release locks;
another process must wait/restart, never steal them.

The advisory-key representation follows the PostgreSQL 16
[pg_locks contract](https://www.postgresql.org/docs/16/view-pg-locks.html); source/caller locks
follow [transaction lock semantics](https://www.postgresql.org/docs/16/explicit-locking.html).

Run `scripts/sql/v11_source_sync_role.sql` as source schema administrator only after approved
installation. It creates `marketdata_v11_sync` without admin/DDL/TEMP privileges, grants explicit
SELECT tables and only the narrow worker functions. It cannot directly mutate financial records,
registry, epochs, capture head, quote state, outbox payloads, export markers or worker tables.
Startup rejects broad table/column grants and admin roles. The schema administrator owns all
functions and must remain a separate trusted principal. The collector writer retains its
existing financial/capture write privileges. The company INGEST writer and SELECT-only READ role
are separate again, provisioned by the updated `v11_destination_roles.sql` after `0010`.
Before applying PUBLIC privilege revokes, review/grant the actual collector writer's required
TEMP privilege explicitly: Store's existing financial writer uses transaction-local staging
tables. The synchronization role itself must not receive TEMP. No live role script was applied.

Acknowledgment functions require a locally stored committed receipt tied to immutable batch
bytes/hash and selected range/version. Candle acknowledgment also verifies that the staged
records cover all retained events in that range. These functions constrain local capabilities;
the trusted worker verifies HTTPS receipts, not PostgreSQL itself. A compromised worker/token
is a compromised authorized producer, not a cryptographically attested collector.

## Delivery, retries and recovery

1. Use SourceCapture.page/pending_quotes. Encode and validate a bounded prefix, then durably
   stage exact UTF-8 bytes, UUID, SHA-256, original record identities/positions, range, event kinds.
2. Query the original receipt or send those exact bytes over verifying HTTPS. No redirects,
   proxy environment inheritance or disabled certificate checks. A total transport deadline
   bounds streaming and a 4-MiB response ceiling bounds receipts/reconciliation results.
3. Verify producer/epoch/stream/batch/hash/state/record outcomes and actual financial parity;
   commit the receipt locally, then update the separate external continuity anchor.
4. In a separate fenced transaction, acknowledge only that candle prefix or quote version.
   Mark the batch acknowledged; later bounded cleanup removes only acknowledged source events.

An empty candle page can advance through quote/metadata-only positions only when SQL confirms
there are no unacknowledged candles in the range. Each actual sub-batch advances only its selected
last position. An old quote acknowledgment never clears N+1. Staged N remains immutable even
when source latest state has coalesced to N+1. Received-but-unacknowledged batches replay the
saved receipt into source acknowledgment after restart. Uncertain network/local receipt commits
query the original batch ID. Source observation identities and financial timestamps stay fixed.

Batch body ceiling defaults to 2 MiB and records to 100 (maximum 500); coverage/status are capped
at 50. Measure encoded bytes before staging, not just SourceCapture page count. A record that
cannot fit stops with diagnostics; no staged body is shortened or rewritten under its old UUID.

Session state includes the complete manifest, epoch/ownership generations, current UUID/generation,
pending recovery's exact bytes/UUID and validated RecoveryReceipt. Renewal is scheduled from
server-provided remaining lease using monotonic time. After expiry, save the existing M2.1
RecoveryRequest before HTTP; lost responses replay that operation. Persist a valid result before
adopting its session. An expired saved recovery result is saved, then renewed/recovered from that
known identity. Never follow an unknown replacement session. After rotation, inspect old batch
receipts first. A missing old receipt requires a **new** envelope/UUID with identical observation
records; retain old bytes. Session recovery does not refresh status/quote clocks or discard data.

429/503/transport errors retain bytes with persisted capped exponential backoff, jitter and
Retry-After (up to 300 seconds). Requests are spaced at least 650 ms, including receipt/session
traffic. 401/403, unknown session/ownership, identity, receipt or parity failures stop/quarantine;
they do not invent permissions or retries with different financial values. Correct private
credentials/configuration, investigate the durable sanitized error category and restart only
after review. A DBA can use the fenced `source_sync_state('retry', {batch_id})` operation after
review to requeue a quarantined immutable batch; no repair rewrites its bytes. Clock-ahead
observations stay retained: synchronize host/database clocks and wait for trusted time or
review genuine source correction. Never clamp original timestamps. No browser receives tokens.

The full session contract remains [protocol v1](fastapi-v1.1-protocol-v1.md) and
[same-producer recovery](fastapi-v1.1-session-recovery.md).

## Enrollment, coverage, status and fairness

Set `enroll:true` explicitly for bounded seed_candles/seed_quotes enrollment. Export markers
prevent duplicate enrollment after restart. Wire validation runs before staging/enrollment commit;
unknown volume units or invalid legacy status/quantities remain in source history and are reported,
without markers claiming successful enrollment. No volume unit guessing, rounding or timestamp edits.
Durable enrollment/reconciliation indices resume round-robin work across streams after restart.

Each turn has bounded quote, candle, coverage and status slots, then bounded enrollment,
reconciliation and cleanup. Live quotes cannot permanently starve candles/corrections/backfill.
Coverage/status advance their durable round-robin index even when the selected instrument has
no record, unchanged contents or expired health; one idle instrument cannot block later grants.
Coverage/status use bounded batches (`metadata_max_records`, default/max 50, also capped by
`max_records` and actual encoded byte size). Business-key locks and the shared source clock
serialize allocation in READ COMMITTED transactions; one captured collector-status row is reused
throughout a status batch. This avoids keeping an obsolete MVCC clock snapshot while waiting for
continuous quote writers. Capture/allocation/batch/hash/traversal progress commit together.
Unstaged records after a byte split remain unmarked and are revisited. Coverage is cumulative;
incompatible shrinking snapshots or more than 500 ranges stop rather than truncating evidence.
Status uses the collector's actual record/TTL and matching provider/symbol health. Expired or
pre-recovery staged status is quarantined. After recovery, capture must actually update before
status is sent again. No fresh status is fabricated from a running worker or healthy company API.

## Reconciliation, restore and continuity

Startup checks destination high-water, destination identity and the last externally anchored
receipt before local progress advances. The separate private `anchor_file` survives database-only
restores and records identity, observed local head, remote high-water/destination identity and last
verified receipt ID/hash. Missing anchor with existing worker identity, changed epochs/manifests,
remote high-water ahead of source, remote rollback, missing anchored receipt or destination
replacement fails closed. Back up/protect this file separately from the PostgreSQL dataset.

Periodic reconciliation pages retained native source financial rows (50 candle keys/turn) and
actual latest quote state, independently of export markers, receipts and outbox retention.
Quote comparison checks the full typed payload and ownership/epoch/version, not only its derived ID.
It stores missing/divergent findings and compares bounded source/destination correction manifests.
Revision manifests normalize financial decimal scale and changed-field ordering, excluding local
row IDs/discovery clocks. More than 128 revisions for a bucket is explicitly flagged as incomplete
rather than falsely reported equal. A missing revision is detected even if current OHLC matches.
Pre-enrollment legacy revision history can legitimately differ: protocol v1 transfers genuine
candle observations/corrections, not invented historical revision receipts. Such findings require
review; no automatic audit-history fabrication or unsupported revision-history import occurs.
No candle is fabricated to fill a gap. `--repair` explicitly requests a bounded current-source
resnapshot through SourceCapture's existing allocator and writer contract. Missing candles whose
accepted financial head agrees can be restored by a new observation/position without changing
their original timestamps or adding a false financial revision. A quote resnapshot has a new
delivery identity/version while preserving original per-field observations and clocks.

Divergent physical data with intact newer receipt barriers may intentionally remain unrepairable
through ordinary uploads; the worker detects/quarantines rather than bypassing correction order.
Arbitrary partial database corruption requires administrator-reviewed recovery. Whole-destination
rollback/replacement and source restore/clone mismatches require reviewing backups, trusted
receipts/high-water, retained financial data and producer ownership before adopting a new trusted
anchor or explicit administrator epoch/generation transition. There is no automatic epoch reset.
A clone of database **and** private anchor/token is indistinguishable from the original principal;
different installations must have distinct producer credentials and explicit handover.

A reviewed restore/identity transition must follow this order:

1. Stop capture and synchronization under a separately approved maintenance plan; retain a
   consistent source/capture/worker backup and the independent private anchor.
2. Inventory old staged IDs/hashes and company receipts/high-water; compare retained financial
   rows and corrections. Keep unresolved batches and the old database unchanged as evidence.
3. If continuity cannot be established, prepare a **new isolated source dataset/database** with
   a fresh extension-generated epoch. Import only verified financial records with original
   values/timestamps and use bounded validated enrollment. Do not UPDATE the old global epoch,
   reset its head/ack, relabel old outbox records, or reuse its observation IDs for changed bytes.
4. Have the company administrator review the financial comparison and explicitly provision the
   new producer/session/credential and higher ownership generations with that epoch. Revoke the
   replaced installation; no producer-facing HTTP operation can do this.
5. Provision the new complete manifest and a new private continuity anchor after review. Verify
   bounded history/quote parity before routine operation. Preserve old receipts/state until the
   reconciliation and retention decision is signed off. Existing newer company corrections
   must not be overwritten to make an old backup appear complete.

The worker does not execute this administrator procedure or silently adopt an empty anchor.
No comparison can detect a source commit never observed by either the independent anchor or
destination after arbitrary backup rollback; consistent backups/WAL recovery remain necessary.

## Capacity and monitoring

Source limits remain 1,000,000 retained events / 512 MiB canonical candle bytes, 64 KiB per candle
and 16 KiB per quote snapshot. Capacity exhaustion rolls back new capture transactions; it does
not evict pending events. The worker cannot recover observations never acquired/committed.
Set worker `source_max_rows` and `source_max_bytes` to the collector's reviewed capture limits
when lower than defaults, so enrollment observes the same capacity policy.

Immutable staged rows have an additional hard 64-MiB / 10,000-row budget. Delivered batches are
bounded-pruned into smaller archive receipt summaries; the archive removes record payloads so
it is not a permanent local quote-history store. Replaced/quarantined payloads remain audit
evidence and count toward capacity; an operator must reconcile them before a reviewed retention
decision. Receipt-summary/export-marker/company-ledger growth is not automatically purged.

JSON metrics include pending rows/canonical bytes, oldest pending candle, head/ack, quote version
lag (positions, not quote count), staged bytes/count, quarantines/retries, database size, cluster
WAL bytes, private-state filesystem free bytes, captured collector TTL and session lease health.
Durable delivery_counts distinguish completed candles, corrections, history seeds, quote
snapshots, coverage and captured status; a batch's phase transition and counters commit together.
Cluster WAL is shared across databases; state filesystem free space is **not** the remote database
filesystem free space. Monitor that host's disk/WAL separately. Financial freshness remains in
the genuine field clocks and existing read API, separate from upload/session health. Alert before
budgets bind. Metrics/logs contain categories/IDs/counts, never tokens, DSNs or provider secrets.

## Windows PowerShell: reviewed new installation

Use a separate checkout/environment and a separately approved new source database. The preserved
old collector is outside these commands. Install Python 3.12+ and uv, then:

```powershell
Set-Location C:\Marketdata\tradingview-marketdata
uv sync --extra collector --extra migrations
$private='C:\ProgramData\MarketdataSync'
New-Item -ItemType Directory -Path $private -Force | Out-Null
icacls $private /inheritance:r /grant:r "${env:USERNAME}:(OI)(CI)F" 'SYSTEM:(OI)(CI)F'
```

Privately provision `source-dsn`, `producer-token`, company CA (if needed), the complete approved
manifest and known session. Never put company PostgreSQL credentials on this machine. For an
explicitly approved **new disposable** source, with administrator DSN supplied privately:

```powershell
$env:DATABASE_URL=[IO.File]::ReadAllText("$private\source-admin-dsn").Trim()
uv run --no-sync alembic upgrade 0006
uv run --no-sync alembic -c alembic-source.ini upgrade head
# Private libpq files define the reviewed source-admin service/password; no secret arguments.
$env:PGSERVICEFILE="$private\pg_service.conf"
$env:PGPASSFILE="$private\pgpass.conf"
psql 'service=source-admin' -X -v ON_ERROR_STOP=1 -f scripts/sql/v11_source_sync_role.sql
# Set its LOGIN password privately, then provision source-dsn with only this restricted role.
Remove-Item Env:\DATABASE_URL
```

For the company destination only, its administrator upgrades destination head `0010` and applies
the updated destination-role grants (existing roles need reviewed GRANT deltas, not CREATE ROLE
replay). READ and INGEST remain independent services. Gateway HTTPS and private company DB
networking follow the existing runtime guide. No collector can connect to company PostgreSQL.

Create `sync.json` privately using actual provisioned identities, with **every** approved grant:

```json
{
  "endpoint": "https://marketdata.company.example",
  "source_dsn_file": "C:\\ProgramData\\MarketdataSync\\source-dsn",
  "token_file": "C:\\ProgramData\\MarketdataSync\\producer-token",
  "anchor_file": "C:\\ProgramData\\MarketdataSync\\continuity.json",
  "ca_file": "C:\\ProgramData\\MarketdataSync\\company-ca.pem",
  "producer_id": "<provisioned producer UUID>",
  "session_id": "<known provisioned session UUID>",
  "session_generation": 1,
  "streams": [{
    "instrument_id": "<registered instrument UUID>",
    "provider": "<registered provider>",
    "stream": "candles",
    "source_epoch": "<actual source_capture_clock epoch>",
    "ownership_generation": 1
  }],
  "enroll": true,
  "max_records": 100,
  "metadata_max_records": 50,
  "max_body_bytes": 2097152,
  "interval_seconds": 2,
  "timeout_seconds": 30,
  "reconcile_seconds": 300
}
```

Placeholders are deliberately invalid; replace them with the approved complete manifest.
Add quotes/coverage/status entries where authorized, preserving OANDA/FXCM mappings. Omit
ca_file to use the system trust store. The worker refuses HTTP origins or arbitrary URL paths.

```powershell
uv run --no-sync python -m marketdata.sync_worker --config "$private\sync.json" --once
uv run --no-sync python -m marketdata.sync_worker --config "$private\sync.json"
```

For an operator-approved login task (no token/DSN arguments):

```powershell
$python=(Resolve-Path '.venv\Scripts\python.exe').Path
$action=New-ScheduledTaskAction -Execute $python -Argument '-m marketdata.sync_worker --config C:\ProgramData\MarketdataSync\sync.json' -WorkingDirectory $PWD.Path
$trigger=New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings=New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName MarketdataSync -Action $action -Trigger $trigger -Settings $settings -Description 'Independent durable market-data HTTPS synchronization'
```

Use Task Scheduler history for exit failures and an approved rotating metrics log/service wrapper
for persistent diagnostics. This login task is not a Windows service or a tested sleep/reboot claim.

## Linux service

Install the same checkout/dependencies in `/opt/marketdata`, create a dedicated `marketdata-sync`
OS account, and privately provision absolute config/secret/CA paths under `/etc/marketdata-sync`
and the separate anchor under `/var/lib/marketdata-sync`. Restrict ownership/modes (directories
0700, secrets/config 0600) to that account. Use the same reviewed source/destination migration
boundaries and explicit database-role scripts. A service unit example:

```bash
cd /opt/marketdata
uv sync --extra collector --extra migrations
sudo useradd --system --home-dir /var/lib/marketdata-sync --shell /usr/sbin/nologin marketdata-sync
sudo install -d -m 0700 -o marketdata-sync -g marketdata-sync /etc/marketdata-sync /var/lib/marketdata-sync
# Privately place actual source-dsn, producer-token, CA and complete sync.json here.
sudo chown marketdata-sync:marketdata-sync /etc/marketdata-sync/sync.json /etc/marketdata-sync/source-dsn /etc/marketdata-sync/producer-token
sudo chmod 0600 /etc/marketdata-sync/sync.json /etc/marketdata-sync/source-dsn /etc/marketdata-sync/producer-token
sudo -u marketdata-sync /opt/marketdata/.venv/bin/python -m marketdata.sync_worker --config /etc/marketdata-sync/sync.json --once
```

For an approved new disposable source only, an administrator can use privately provisioned
administrator DSN/libpq service files (never the worker account) before the worker run:

```bash
cd /opt/marketdata
export DATABASE_URL="$(cat /private/source-admin-dsn)"
uv run --no-sync alembic upgrade 0006
uv run --no-sync alembic -c alembic-source.ini upgrade head
unset DATABASE_URL
PGSERVICEFILE=/private/pg_service.conf PGPASSFILE=/private/pgpass.conf \
  psql 'service=source-admin' -X -v ON_ERROR_STOP=1 -f scripts/sql/v11_source_sync_role.sql
```

The Linux JSON uses `/etc/marketdata-sync/source-dsn`, `/etc/marketdata-sync/producer-token`,
`/var/lib/marketdata-sync/continuity.json` and the installed CA path. The source schema and
role commands run once through separately approved administrator provisioning, never from the
worker service. Review existing OS accounts before running useradd.

A service unit example:

```ini
[Unit]
Description=Market-data durable HTTPS synchronization
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=marketdata-sync
Group=marketdata-sync
WorkingDirectory=/opt/marketdata
ExecStart=/opt/marketdata/.venv/bin/python -m marketdata.sync_worker --config /etc/marketdata-sync/sync.json
Restart=on-failure
RestartSec=10
UMask=0077
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
ReadWritePaths=/var/lib/marketdata-sync

[Install]
WantedBy=multi-user.target
```

After explicit installation approval, save it as `/etc/systemd/system/marketdata-sync.service`,
run `sudo systemctl daemon-reload`, `sudo systemctl enable --now marketdata-sync`, and inspect
`journalctl -u marketdata-sync`. Retain system journal rotation and monitor durable diagnostics.
No service installation or deployment was executed in Milestone 3B.

## Reproduce isolated tests

```powershell
docker compose -p marketdata-v11-m3b-tests -f compose.v11-m3b-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://m3b_test:disposable_test@127.0.0.1:55985/marketdata_v11_m3b_test'
uv run --no-sync alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m3b-full-pg16.xml
$env:DATABASE_URL='postgresql://m3b_test:disposable_test@127.0.0.1:55986/marketdata_v11_m3b_test'
uv run --no-sync alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m3b-full-timescale.xml
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync mypy src --no-incremental
```

The test fixtures create uniquely named `_test` source databases, apply base `0006` and source
head, inject observations and remove only those databases. They never run TradingView. The
actual workstation run used an independent task-owned WSL Docker daemon because Docker Desktop
was unavailable; see the report for isolation, engine versions and exact evidence. Run engine
suites sequentially to avoid unnecessary contention with existing short-deadline tests.
