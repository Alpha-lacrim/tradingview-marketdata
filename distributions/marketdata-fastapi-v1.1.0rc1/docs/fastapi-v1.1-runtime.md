> Current destination head: 0010. Preserve migrations 0007–0009 and apply additive 0010.
> Existing database roles require the reviewed quote-head SELECT/INSERT/UPDATE and
> destination-state SELECT grant deltas in scripts/sql/v11_destination_roles.sql.
> Source capture/worker uses a different database and migration chain; see
> [Milestone 3B operations](fastapi-v1.1-sync-worker.md).

# Company ingestion runtime — milestones 2, 2.1 and 3B

This is a source-level operating guide for the reviewed milestone, not a complete v1.1 release
or permission to deploy a collector. The company has not installed v1.0. Frozen v1.0 source/ZIP
remain schema 0006; this root code requires 0010. Never run the frozen binary on the new schema.
Read [protocol v1](fastapi-v1.1-protocol-v1.md) and the milestone verification report first.

## Database and private identities

Use a dedicated company PostgreSQL database reachable only by the company application and DBA.
No laptop-to-company SQL connection, public database port, shared collector DSN or automatic
startup migration. Backups/schema ownership use an administrative principal kept out of both
application processes. Install root dependencies with `uv sync --frozen`; the Docker API runtime
already contains both factories from this single source tree. Administrative tooling remains a
separate target/extra. A v1.1 standalone export/ZIP and final production Compose are later work.

On the new company database, the DBA runs Alembic explicitly through head 0010, using a private
DATABASE_URL environment variable (or a private shell wrapper that loads it without printing).
Use `uv run --no-sync alembic upgrade head` for PostgreSQL and
`uv run --no-sync alembic -x timescale=true upgrade head` for supported TimescaleDB. These
commands are for the dedicated company target; never substitute the legacy local collector DSN.
Review and run `scripts/sql/v11_destination_roles.sql` once using the DBA connection. It creates
fresh marketdata_v11_read and marketdata_v11_ingest roles and fails on existing names. Set their
private login credentials separately, without putting passwords in shell history. Reader SELECT
includes only the named read relations and ingest_read_status view, never receipt/policy tables.
Writer functions allow policy locks, request admission, same-session renewal and fenced expiry
recovery. They grant no general UPDATE on producer policy, owner transfer or direct budget writes.

For a previously provisioned milestone-1 disposable/company candidate only, migrate 0007->0008
as DBA, then review these additional grants (the fresh role script includes them already):

```sql
GRANT SELECT ON public.ingest_read_status TO marketdata_v11_read;
GRANT EXECUTE ON FUNCTION public.ingest_admit_request(UUID,TEXT,INTEGER),
  public.ingest_renew_session(UUID,TEXT,UUID,BIGINT) TO marketdata_v11_ingest;
```

For a milestone-2 candidate at 0008, run the additive 0009 upgrade as DBA, then grant only:

```sql
GRANT EXECUTE ON FUNCTION public.ingest_recover_session(UUID,TEXT,JSONB)
  TO marketdata_v11_ingest;
```

No SELECT or DML grant on ingest_session_recoveries is needed by either runtime role. Recovery
execution is forbidden to the reader. Fresh role setup includes this function grant already.
0008 itself and its renewal/admission functions are unchanged. For a milestone-2.1 candidate at
0009, upgrade to additive 0010 as DBA, then apply only these additional runtime grants:

```sql
GRANT SELECT,INSERT,UPDATE ON public.ingest_quote_heads TO marketdata_v11_ingest;
GRANT SELECT ON public.ingest_destination_state TO marketdata_v11_ingest;
```

Use the current-head application with 0010; do not start an old exact-head binary against it.

Do not grant either role membership in a privileged role. Reader startup rejects runtime write
permissions and renewal/admission execution; ingestion startup rejects administrator/DDL/policy
mutation privileges and requires the actual market/journal/receipt capabilities. PostgreSQL
SESSION read_only is additional protection; SQL grants enforce reader denial even when a caller
disables that setting. Schema downgrade is not an operational recovery procedure: 0008 downgrade
removes request budgets/status projection/functions; 0007 downgrade loses receipts/fences.

## Provision a producer and source ownership

Choose a stable producer UUID, a durable local dataset epoch, a new session UUID and generation.
Use distinct IDs for a replacement producer when moving ownership; do not share its permanent
token between concurrently running collectors. For a fresh private credential directory:

PowerShell (IDs shown are deliberate placeholders; use your provisioned UUID):

```powershell
uv run --no-sync python scripts/create_ingest_credentials.py --directory C:\ProgramData\Marketdata\secrets\producer1 --producer-id <producer-uuid> --key-id producer1-2026-01
$privateDir = 'C:\ProgramData\Marketdata\secrets\producer1'
icacls $privateDir /inheritance:r /grant:r "$($env:USERDOMAIN)\$($env:USERNAME):(OI)(CI)F"
```

Linux:

```bash
umask 077
uv run --no-sync python scripts/create_ingest_credentials.py --directory /etc/marketdata/secrets/producer1 --producer-id <producer-uuid> --key-id producer1-2026-01
chmod 700 /etc/marketdata/secrets/producer1
chmod 600 /etc/marketdata/secrets/producer1/*
```

Replace angle-bracket UUID placeholders before executing. The script creates a fresh directory,
refuses overwrite, prints no token and writes server ingest_credentials.json plus collector
INGEST token file. Windows requires an explicit private ACL; Unix permission modes are not a
Windows ACL. Allow only the necessary service identities when transferring private files.
The collector token and server hash are not frontend API_KEY. Transfer them through the
company's secret-management channel, never commit them or paste them into browser clients.

The administrator provisions policy using bound parameters through its trusted SQL tooling:

```sql
-- Named :parameters are bound values in DBA tooling, not literal SQL strings.
INSERT INTO ingest_producers
  (producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)
VALUES (:producer_id,:label,:key_id,:session_id,1,clock_timestamp()+interval '120 seconds');
INSERT INTO ingest_source_owners
  (instrument_id,provider,stream,producer_id,source_epoch,ownership_generation,allowed_statuses)
VALUES (:instrument_id,:provider,:stream,:producer_id,:source_epoch,1,:allowed_statuses);
```

Registry/mapping IDs must already be provisioned from the reviewed company registry, with exact
source identity and permitted statuses. Initially provision one instrument, independently for
candles/quotes/coverage/status as intended. Do not authorize simulated status on commercial
streams merely to make fixtures pass. There is no credential/ownership management HTTP API.
Renew before the 120-second lease expires, for example every 30 seconds. After ordinary expiry,
use the durable, same-producer `/session/recover` operation described in
[the recovery guide](fastapi-v1.1-session-recovery.md). It preserves epoch/ownership and atomically
changes the session UUID/generation. Reusing the old session for upload remains rejected. Ownership handover updates existing owner
rows and increases ownership_generation; producer and source epochs remain explicit. Coordinate
that handover with the later worker's durable staging/checkpoints. A restarted stale laptop
must retain its old session configuration and receive a fence error, never discover/take over
the new active session via an admission call.

Rotate credentials with a new key ID and freshly generated files. Bind the new key ID in SQL
first to revoke old processes, mount the replacement hash file and restart every ingestion
process. Disabling the producer serializes with in-flight uploads. Previously committed data
and receipts survive rotation, but revoked credentials no longer read them. Session admission
is needed again for the intended producer. Do not delete immutable receipts to rotate a token.

## Start separate read and ingestion processes

Prepare separate private DSN files for reader and writer and an independently generated
frontend read API_KEY file. The ingestion credential file holds the generated server hashes.
Production entrypoints ignore .env. Configure explicit trusted reverse-proxy addresses; a
plain direct request with a spoofed X-Forwarded-Proto is rejected. Firewall backend listeners
to the gateway, which terminates valid HTTPS and sanitizes forwarded headers. The company must
configure its actual hostnames/certificates, CA trust, body/header/connection limits and
unauthenticated IP rate limits. Set the gateway's batch ceiling no higher than 2 MiB. Limit
slow uploads and restrict methods/paths; do not expose backend ports or PostgreSQL publicly.

Ingestion, PowerShell:

```powershell
$env:DATABASE_URL_FILE='C:\ProgramData\Marketdata\secrets\writer_dsn'
$env:INGEST_CREDENTIALS_FILE='C:\ProgramData\Marketdata\secrets\producer1\ingest_credentials.json'
Remove-Item Env:API_KEY -ErrorAction SilentlyContinue
$env:FORWARDED_ALLOW_IPS='127.0.0.1'
$env:API_WORKERS='2'
uv run --no-sync python -m marketdata.production ingest
```

Ingestion, Linux:

```bash
export DATABASE_URL_FILE=/etc/marketdata/secrets/writer_dsn
export INGEST_CREDENTIALS_FILE=/etc/marketdata/secrets/producer1/ingest_credentials.json
unset API_KEY
export FORWARDED_ALLOW_IPS=127.0.0.1
export API_WORKERS=2
uv run --no-sync python -m marketdata.production ingest
```

Use a separate process/container for read with DATABASE_URL_FILE pointing to reader_dsn,
API_KEY_FILE to read_api_key, explicit HTTPS WEBSOCKET_ALLOWED_ORIGINS/API_CORS_ORIGINS and
`python -m marketdata.production api`. Each entrypoint listens on container/backend port 8000;
use separate containers/network identities or separate host bindings when running both.
Do not run these two commands on the same unisolated host port. Existing direct Uvicorn read
factory remains available for explicit host/port configuration. The same Dockerfile.api runtime
image can select either entrypoint; final company Compose/secrets/gateway packaging is a later
milestone and the older compose.production.yaml is a colocated-collector deployment definition.

Authenticated `/api/ingest/v1/health` checks the ingestion DB. `/livez` checks only the process.
Read `/api/v1/health` separately reports collector state, stored-price freshness, coverage and
WebSocket delivery. Healthy HTTP processes or renewed sessions do not imply fresh prices.
Captured status cannot renew itself through HTTP upload time. Logging contains categories only,
not headers/body/DSNs/tokens; production commands disable Uvicorn access logging. Configure the
gateway similarly, including safe error logging and no query-string credential acceptance.

## Reproduce only on disposable infrastructure

These exact commands target task-owned tmpfs databases and never start a collector:

```powershell
docker compose -p marketdata-v11-m2-tests -f compose.v11-m2-test.yaml up -d --wait
$env:DATABASE_URL='postgresql://m2_test:disposable_test@127.0.0.1:55955/marketdata_v11_m2_test'
uv run --no-sync alembic upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m2-full-pg16.xml
$env:DATABASE_URL='postgresql://m2_test:disposable_test@127.0.0.1:55956/marketdata_v11_m2_test'
uv run --no-sync alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$env:DATABASE_URL
uv run --no-sync pytest -q --junitxml=artifacts/v11-m2-full-timescale.xml
```

Linux uses the same Compose/Alembic/pytest commands with export DATABASE_URL=... and
export TEST_DATABASE_URL="$DATABASE_URL". Install OpenSSL for disposable certificate generation;
TLS tests report a skip if it is absent, not a pass. Tests truncate `_test` tables and create/drop
uniquely named `_test` databases only. Do not run suites concurrently against one database.
See verification JSON for exact skips and limitations. Stop/remove only the verified task
project after testing, without volume operations. No full installation/ZIP, WAN outage test,
source outbox, durable worker, live quote acquisition or measured throughput is claimed here.


Milestone 2.1 reproduction uses compose.v11-m21-test.yaml, project marketdata-v11-m21-tests,
loopback ports 55965/55966, user m21_test and database marketdata_v11_m21_test. Follow
[the recovery guide](fastapi-v1.1-session-recovery.md) for exact commands and client failure cases.
The milestone-2 test recipe above remains its historical source recipe; use current head when
running the current application. No source worker or collector rollout is installed by either.
