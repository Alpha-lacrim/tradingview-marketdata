# Operations, backup/restore, reconciliation and application rollback

These procedures require site authorization; packaging executes none against production.
Preserve immutable batch bytes, producer/epoch/ownership identity and all observations. Never
truncate receipts/outbox or reset a checkpoint to hide a failure. HTTP200 /livez means only
process liveness; it cannot prove genuine source data, freshness, storage, transport or frontend
publication. Keep the company's application databases separate from market data.

## Startup, outage and terminal errors

- A normal collector restart reuses local DB/source epoch. A worker restart reclaims its local
  advisory lock with a new SQL fencing generation, reloads durable staged/received/acknowledged
  phases, checks the external anchor/remote continuity, and resumes bounded enrollment/reconcile.
- Internet/API outages retain committed candle events and newest quote state. Retry/backoff,
  persisted next_attempt and immutable IDs survive restarts. A missing HTTP acknowledgment is
  resolved by prior batch receipt. Old-session bytes are never mutated: after authorized session
  recovery, a missing receipt needs a new batch envelope/UUID preserving the observations.
- Expired sessions recover automatically only for the same enabled still-owned producer, exact
  prior session/generation and complete ownership manifest. Lost recovery response replays its
  durable recovery ID. A healthy session is not a fresh market observation.
- Ownership transfer, revocation, unknown session, clock-ahead validation, epoch/anchor/remote
  rollback or revision divergence stop/quarantine. Retain pending rows and investigate the
  sanitized code. Do not restart endlessly, delete anchors, substitute timestamps or reuse old
  machine credentials on a new collector. Secure handover is in COLLECTOR-TRANSITION.md.
- Lost local PostgreSQL lock connection exits/fences the worker. OS supervisor may restart after
  DB is healthy; the stale process cannot record receipts/acks. A competing worker must fail
  ownership claim. Do not run manual --once/repair alongside the service.

## Monitoring and proposed alerts

Monitor four independent dimensions. Proposed thresholds below are starting points from M4,
not approved SLAs and not guarantees for a real99 universe. Every alert must retain instrument,
provider, source epoch and genuine observation time. Do not mark missing unknown-calendar
minutes as known upstream omissions or fabricate candles for a percentage target.

| Dimension / signal | Where | Proposed action |
|---|---|---|
| Collector transport/reconnect/entitlement and per-symbol failures | Collector metrics/logs and captured collector_status/provider health | Alert unavailable transport/permission failure immediately; distinguish source publication omission from connectivity. |
| Completed native candle age, missing expected-minute ranges | Local native tables/coverage; READ /api/v1/coverage | Alert known expected-minute gaps after approved grace; compare provider calendar and actual source history before blaming sync. |
| Genuine current last/bid/ask age and realtime/delayed status | READ /api/v1/prices/latest, quote snapshots/field_observations | Alert primary-field age>30s independent of API/worker status. A newer bid must not refresh old last; unchanged numeric price alone is not health. |
| Capture status expiry | collector_status.expires_at and ingest source-status/read health | Source template publishes5s with proposedTTL60s. Expired remains expired; no upload-generated heartbeat. |
| Pending candle rows/bytes, oldest captured event, quote pending versions | Worker JSON metrics; source_capture_clock/outbox/quote_state | Alert oldest normal candle backlog>60s or increasing backlog; never evict unacknowledged candles. Pause/repair capacity before configured bounds/disk fill. |
| Latest quote synchronization lag | Source version vs destination quote head/field payload and source clocks | Compare actual states, not just last successful upload. Steady proposed ageP95<=15s, bounded burst<=30s only after approved startup grace. |
| Staged/received/quarantined batches, retries, rejects, 429/503 | source_sync_batches, worker delivery_counts/last_error, INGEST audit | Alert terminal errors immediately and sustained retry/backoff; validate immutable bytes/receipts before source acknowledgment. |
| Session lease/recovery/fencing | Worker session_lease_seconds and sanitized recovery/fence audit | Routine expiry may recover; unknown/replaced owner requires DBA. Never use session health as source freshness. |
| Destination availability/commit errors | Authenticated INGEST health and safe audit, PostgreSQL health | Alert DB errors/503, failed transactions and high commit delay; receipt exists only after atomic financial/event commit. |
| READ availability and WSS delivery/replay lag | /health, authenticated REST and WSS consumer metrics | P95 journal-to-consumer ~1s is proposed tuning target; reconnect/replay bounded; expired cursor requires REST baseline. No exactly-once promise. |
| Actual historical divergence | Worker reconciliation findings and bounded revision manifests | Any unresolved missing/divergent financial/revision state blocks acceptance, even if old receipts exist. |
| Disk/WAL/journal/receipt growth, connections and backups | PostgreSQL/OS and backup scheduler | Alert storage<20% free or insufficient recovery runway; alert missed/untested backup. Pending-outbox limits do not bound total DB growth. |

The worker emits JSON after cycles: backlog/oldest-event/quote state, local storage, per-stream
counts, collector_capture_valid, state_filesystem_free_bytes and session_lease_seconds. Keep
stdout/stderr under rotation and attach process-exit alerts; a terminal error can occur before
a new metrics line. See docs/fastapi-v1.1-sync-worker.md for exact fields. Use dedicated SELECT
monitoring credentials for source/destination SQL; do not give monitoring the runtime writer.
Protect logs containing private market data; avoid token/DSN/full-payload logging.

## Destination backup and restore

Define business-approved RPO/RTO and off-host encrypted retention before enabling ingestion.
Use full logical backup plus roles/extension/image/version manifests and, if required, tested
physical/WAL recovery. Record migration0010, destination_id, journal epoch/head/floor, producer
policy and receipt/observation tables. All financial, journal, receipt and policy tables must
be backed up consistently. A partial financial-only backup cannot prove replay safety.

Copyable dedicated Compose backup (choose NEW private backup filenames; never dump over source):

```text
docker compose --env-file .env exec -T db pg_dump -U marketdata_owner -d marketdata_v11 -Fc -f /tmp/marketdata_v11_review.dump
docker compose --env-file .env cp db:/tmp/marketdata_v11_review.dump PRIVATE_BACKUP_DIRECTORY/marketdata_v11_review.dump
docker compose --env-file .env exec -T db pg_dumpall -U marketdata_owner --roles-only --file=/tmp/marketdata_roles_review.sql
docker compose --env-file .env cp db:/tmp/marketdata_roles_review.sql PRIVATE_BACKUP_DIRECTORY/marketdata_roles_review.sql
```

Use Docker cp, not PowerShell5 binary `>` redirection. Lock backup directory permissions before
copying; roles backup contains password verifiers and is secret. Transfer off-host, hash, encrypt
and rehearse restore. Schedule using the site's protected backup service with separate privileges;
a written backup command is not a scheduled backup. Remove only verified temporary dump files in
the dedicated container after successful protected transfer; never delete live volumes.

Restore rehearsal into a NEW explicitly named review database, not over the production DB:

```text
docker compose --env-file .env exec -T db createdb -U marketdata_owner marketdata_restore_review
docker compose --env-file .env cp PRIVATE_BACKUP_DIRECTORY/marketdata_v11_review.dump db:/tmp/restore_review.dump
docker compose --env-file .env exec -T db pg_restore -U marketdata_owner --exit-on-error -d marketdata_restore_review /tmp/restore_review.dump
```

For Timescale matching the original image/extension, create extension and call
`SELECT timescaledb_pre_restore();` in the new review DB before pg_restore, then
`SELECT timescaledb_post_restore();` afterward. Use no parallel -j restore. Follow the exact
verified Timescale logical restore procedure, not a plain dump-only approximation. Reapply
DB-level CONNECT/TEMP ACLs and schema-role permissions because database-level privileges are
not carried by a table dump. Do not blindly replay role-creation SQL into a cluster with those
roles already present; compare and apply reviewed GRANTs using DBA tooling.

Compare schema/head, destination identity, exact candle/field precision and timestamps,
revision manifests, coverage, journal/receipt counts and producer ownership. When replacing
actual destination storage, pause INGEST and READ under site control; source continues queuing.
Use an isolated restored DB and explicit service DSN change after approval. A rollback destination
may have lost receipts/accepted observations despite source export markers: worker continuity
must STOP, detect missing high-water/receipt or changed destination identity, and require DBA
review. Validate actual company materialization and source originals before bounded resnapshot.
Do not manufacture receipts or simply remove the external anchor so upload can proceed.

## Source backup, lost state and reconciliation

Back up source base0006+sc0002 DB consistently, its financial rows/revisions, capture clock/epoch,
outbox/export/quote state, worker owner/staged/archive tables, registry and matching compatible
binary. Protect the external anchor independently and record its relationship to the DB backup.
Secure tokens separately with authorized recovery access; never include them in a release ZIP.
Use equivalent pg_dump/restore with the source's explicit NEW review DB/container, not company
commands accidentally targeting the source. Verify counts/checksums and all source/collector
clocks after restoration. An old source DB with newer anchor or newer destination must fail
closed; an exact DB+anchor+credential clone may be indistinguishable until remote state advances.
Only an administrator-approved distinct producer/ownership handover resolves that ambiguity.

Source backup/rehearsal commands for the NEW container named in the source installation guides
(replace the backup directory first; never substitute the legacy container implicitly):

```text
docker exec marketdata-source-v11 pg_dump -U marketdata_owner -d marketdata_source -Fc -f /tmp/source_review.dump
docker cp marketdata-source-v11:/tmp/source_review.dump PRIVATE_BACKUP_DIRECTORY/source_review.dump
docker exec marketdata-source-v11 pg_dumpall -U marketdata_owner --roles-only --file=/tmp/source_roles_review.sql
docker cp marketdata-source-v11:/tmp/source_roles_review.sql PRIVATE_BACKUP_DIRECTORY/source_roles_review.sql
docker exec marketdata-source-v11 createdb -U marketdata_owner source_restore_review
docker cp PRIVATE_BACKUP_DIRECTORY/source_review.dump marketdata-source-v11:/tmp/source_restore_review.dump
docker exec marketdata-source-v11 pg_restore -U marketdata_owner --exit-on-error -d source_restore_review /tmp/source_restore_review.dump
```

The last three commands restore only a NEW review DB. Keep every collector/worker stopped for
that restored copy; merely restoring schema is not permission to start a cloned producer. Apply
the Timescale pre/post-restore steps above if relevant. Separately back up the external anchor:
PowerShell `Copy-Item -LiteralPath C:/Marketdata/durable/source-anchor.json -Destination
PRIVATE_NEW_BACKUP_PATH` or Linux `sudo install -m 0600 /var/lib/marketdata-sync/source-anchor.json
PRIVATE_NEW_BACKUP_PATH`, after verifying the destination does not exist and restricting its
parent directory. Record the DB/anchor capture times and hashes. A DB backup and a separately
copied anchor are not assumed transactionally consistent; restoration must run continuity and
actual-destination checks. Never delete or reset an anchor to make a restored copy start.

Lost worker-state DB/tables, missing/inconsistent receipts, source rollback, epoch mismatch or
remote rollback are NOT routine internet outages. Preserve all copies/staged bytes and stop
upload. Inventory source/destination actual financial data and immutable receipt hashes; decide
reviewed identity continuity versus new dataset/producer handover. Never initialize a blank
worker schema over unknown pending state, seed fake receipts or reset an epoch/cursor in place.
Backups cannot reconstruct unobserved commits that were lost from every copy.

Enrollment (`enroll:true`) pages actual historical candles and latest quotes with stable source
positions; export markers avoid repeated seeding but do not prove company completeness.
Reconciliation periodically compares actual company candle/quote payload and ordering and bounded
revision manifests. For approved missing materialization stop the active worker, run its normal
command with --once --repair, inspect the bounded result, and repeat monitored cycles as needed.
Original source records/times are preserved. A repair cannot recreate unavailable historical
revision audit or ticks never durably retained. Over128 revisions/bucket is reported as bounded
incomplete comparison. Invalid volume units/timestamps remain retained and diagnosed.

## Credential rotation and failed release rollback

Rotation is a separately authorized operation, never performed by release preparation. For a
producer generate a NEW key ID/token in a fresh private directory, keep the same authorized
producer/epoch/ownership identity, stage server fingerprint and private worker token, update
SQL credential_key_id under producer lock, restart ALL INGEST processes with the new fingerprint,
and atomically replace the source token file. There may be a short planned401/403 interval;
retain queued bytes and restart only after config aligns. Existing loaded old server files are
blocked by the changed DB key. Do not enable a revoked producer or transfer ownership as part
of rotation. READ service-key rotation is separate and coordinated with company frontend/BFF.
DB-role password rotation similarly requires planned secret changes and restart, not --roles.

For a failed first installation stop only the new read/ingest services; retain dedicated storage,
backups and evidence. There is no production v1.0 cutover to reverse. For later candidate updates,
record previous reviewed image/source hashes, current migration head and verified backups first.
If app-only rollback is schema-compatible, select previous approved v1.1 image and restart roles
independently. Preserve all durable receipts/events and source pending records. If schema or
protocol changed, use a forward fix or separately reviewed restore with reconciliation, never
assume Alembic downgrade recovers lost financial/session state. Before declaring restored service
healthy verify actual source/destination data, read/WSS replay and original field freshness.

## Controlled release / site acceptance checklist

- Verify archive/file hashes, reviewed source digest, fresh dependency/image evidence and two
  migration manifests; compare them with the separately supplied M5 verification sidecar.
- Approve instrument universe/rights and genuine independent quote/native-candle performance;
  a brief BTC run or simulated99 workload is not full upstream qualification.
- Rehearse new dedicated DB/storage, migrations, least-privilege accounts, producer provisioning,
  correct CA/hostnames, gateway trusted-IP/rate/body/time bounds, private secrets and firewall.
- Rehearse actual target OS restart, Windows sleep/reboot or Linux supervision; provision off-host
  backups and test restore/anchor/receipt continuity. Assign alert owners and approved targets.
- Verify separate READ/INGEST restart, TLS auth, durable source->company candle/correction/quote
  parity, exact ChartData arrays, WSS reconnect/replay and expired-cursor baseline.
- Obtain explicit deployment and separate legacy transition authorization. Do not push/commit,
  change existing volumes or transfer producers simply because these packages are built.

Security-policy restore gate: before exposing a restored INGEST service, compare producer enabled
flags, key IDs, owner generations/epochs and current credential fingerprints with the latest
approved revocation/handover audit retained outside that backup. Never reactivate an old producer
or old key merely because an older DB/secrets backup contains it. Keep INGEST paused until the
current authority is restored/reviewed. A simultaneous rollback of all identity/anchor/security
copies cannot be made automatically safe by a prior batch receipt.
