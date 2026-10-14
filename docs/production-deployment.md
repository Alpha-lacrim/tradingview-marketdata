# Production deployment and recovery

This Batch 4 work is **undeployed**. Use a modest Linux server with Docker Engine/Compose,
disk space for history plus backups, and a trusted HTTPS reverse proxy/company backend.
The existing `tradingview` deployment and its volume are outside these commands. An existing
legacy combined collector must be stopped and verified absent during a separately authorized
replacement: it does not participate in the new advisory-lock protocol. The current source
universe must be preserved; this guide authorizes no instrument promotion or guest access.

## Configuration and secrets

Use [compose.production.yaml](../compose.production.yaml), not the development Compose file
with shared/default credentials. Put only non-secret settings in `/etc/marketdata/deploy.env`:

```dotenv
MARKETDATA_IMAGE=your-reviewed-registry/marketdata@sha256:<reviewed-digest>
DATABASE_IMAGE=timescale/timescaledb@sha256:<reviewed-2.25.1-pg17-digest>
MARKETDATA_VOLUME=<explicitly-verified-existing-volume-name>
DEPLOY_SECRETS_DIR=/etc/marketdata/secrets
API_PORT=8002
WEBSOCKET_ALLOWED_ORIGINS=["https://<company-frontend-host>"]
API_CORS_ORIGINS=[]
FORWARDED_ALLOW_IPS=<actual-trusted-proxy-address-as-seen-by-container>
```

The external volume must already exist. For a **new** installation, an operator explicitly
creates a new named volume first. For an existing installation verify its name, mounts,
PostgreSQL system identifier, database name and reviewed image/configuration before any
service action. Never create a replacement volume because the expected one is missing.
Never run `down -v`, erase a data directory, or restore over the live database.

Provision private files `postgres_password`, `migration_database_url`, `api_database_url`,
`collector_database_url`, `api_key` under the secret directory. Database URL files use separate
DBA/migration, SELECT-only API and writer principals, respectively. URL-encode passwords;
use host `db`, port 5432, database `marketdata` for this topology. Generate the API token using
`openssl rand -base64 48 > /etc/marketdata/secrets/api_key`, without printing it. No token/DSN
is passed as an argument or stored in Compose environment. The non-root app must be able to
read mounted files; provision appropriate owner/group permissions without making the host
directory accessible to unrelated users. Docker access is administrator-equivalent.

The entrypoint loads secret files into its private process environment, rejects bad
configuration without printing its contents, and starts an explicit API/collector/migration
process. Production authentication is always enabled for the API. Startup rejects DB owner,
superuser/administrative, schema-CREATE, database-TEMP and market-table write privileges.
Read-only transactions and statement timeouts provide additional protection. Docker inspection
shows mounted file paths, not credential values. Rotate the API token by securely replacing
the file and recreating only API containers; coordinate the company backend and WS reconnect.

## Database principals and migrations

DBA setup, after migrations and using interactive `psql` password prompts/secret tooling:

```sql
CREATE ROLE marketdata_api LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE marketdata_collector LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE, TEMPORARY ON DATABASE marketdata FROM PUBLIC;
GRANT CONNECT ON DATABASE marketdata TO marketdata_api, marketdata_collector;
GRANT USAGE ON SCHEMA public TO marketdata_api, marketdata_collector;
GRANT SELECT ON public.instruments, public.provider_instruments, public.market_ticks,
  public.latest_prices, public.market_candles, public.latest_candles, public.candle_coverage,
  public.candle_revisions, public.collector_status, public.marketdata_events,
  public.marketdata_event_clock, public.alembic_version TO marketdata_api;
ALTER ROLE marketdata_api SET default_transaction_read_only=on;
GRANT TEMPORARY ON DATABASE marketdata TO marketdata_collector;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.instruments, public.provider_instruments,
  public.market_ticks, public.latest_prices, public.market_candles, public.latest_candles,
  public.candle_coverage, public.candle_revisions, public.collector_status,
  public.marketdata_events, public.marketdata_event_clock TO marketdata_collector;
GRANT SELECT ON public.alembic_version TO marketdata_collector;
GRANT USAGE ON SEQUENCE public.candle_revisions_id_seq TO marketdata_collector;
```

Grant API SELECT on the 12 `candles_{1s,1m,5m,15m,1h,1d}_{last,mid}` views individually if
serving legacy Timescale tick-derived routes. Review memberships, existing principals affected
by PUBLIC revocation, function grants and schema ownership. Do not grant API write-role
membership, arbitrary future relations or internal extension schemas. Migration privileges
remain separate. The collector requires staging-table TEMP, not schema-CREATE. The API
serves native candle data without Timescale internal access. See [API grants](api.md#dedicated-read-only-database-role).

For a new target, pin the reviewed application image, then run these Linux commands from
the reviewed release directory. Keep the Compose project name fixed in all operations:

```bash
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml config --quiet
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml up -d db
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml --profile tools run --rm migrate
```

Run the DBA grants above before API/collector startup. Migrations are explicit, never part of
API startup. Schema head is 0006; Batch 4 adds no migration. Plain PostgreSQL is supported:
use an explicitly selected PostgreSQL image and run Alembic without `-x timescale=true`
instead of the supplied Timescale migration entrypoint. Do not change the database engine
of an existing volume.

API only:

```bash
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml up -d --no-deps app
curl --fail http://127.0.0.1:8002/livez
curl --fail http://127.0.0.1:8002/health
```

Authenticate protected smoke requests through the company's backend or a client that loads
the token privately; verify instruments, a known historical window, latest prices, coverage,
WS subscribe and REST reconciliation before routing frontend traffic. The API defaults to two
workers, each with at most 10 DB connections and 50 WS clients/5,000 subscriptions. These
are per-worker limits, not a global guarantee. Budget DB connections for all workers plus
the collector/maintenance. Worker count and gateway limits require target-host qualification.

Collector only, after reviewing a registry and establishing rights for its selected providers:

```bash
docker compose --env-file /etc/marketdata/deploy.env -p marketdata -f compose.production.yaml --profile ingestion up -d --no-deps collector
```

The profile defaults to disabled providers. Unsupported TradingView production ingestion
is explicitly blocked (`TRADINGVIEW_PRODUCTION_ACCESS_APPROVED=false` in production Compose).
Changing that gate requires documented access/redistribution rights and a reviewed override,
not a benchmark result. No live source access was exercised in Batch 4. Never start a second
collector alongside the existing legacy owner. API restarts do not restart the collector.

## HTTPS, networking and logs

Only the loopback API port and loopback collector metrics are published. PostgreSQL has no
published port and uses an internal network. Host firewall/actual remote reachability still
require operator verification. The collector alone joins an outbound network when enabled.
Terminate HTTPS at the existing trusted proxy, and use an authenticated company BFF for
browser sessions. Support WebSocket Upgrade/Connection headers and HTTP/1.1. Set upstream
connect timeout about 3s, request timeout above the 5s API budget (for example 15s), and WS
idle timeout above the 10s heartbeat (for example 60s). Validate Origin, enforce gateway
connection/request limits, and trust forwarded headers only from the actual proxy address.
Never use a wildcard trusted proxy list. See [Uvicorn settings](https://www.uvicorn.org/settings/).

Uvicorn access logging is disabled by the production entrypoint. Configure proxy logs to use
the path without query parameters, and exclude Authorization/Cookie headers. Log status,
request duration and request ID instead. Application faults log a category without exception
text/SQL/DSNs. Normal market updates are not individually logged. Compose rotates stdout
logs at 10MB × 5 per service. Inspect `docker compose ... logs --tail 100 app` privately.
File-mounted Compose secrets and internal networking follow [Docker secrets](https://docs.docker.com/compose/how-tos/use-secrets/)
and [network configuration](https://docs.docker.com/reference/compose-file/networks/).

## Health and monitoring

`/livez` is public and independent of DB connectivity. `/health` is public DB readiness;
503 means DB access unavailable, not source offline. `/api/v1/health` is authenticated:
API/database readiness, collector running/stopped/unknown, source freshness, historical
coverage and actual journal-reader delivery state (idle/operational/degraded/unknown).
An offline collector or TradingView source never creates a blanket history failure.
Docker's readiness healthcheck does not by itself restart an unhealthy live process;
`unless-stopped` restarts exited processes. Alert on sustained readiness failure and diagnose
DB connectivity rather than restarting a healthy collector automatically.

Scrape authenticated `/metrics` on each API worker, not just a load-balanced aggregate.
Counters include HTTP statuses/slow-client drops; histograms include route request/query
duration and loop lag; gauges include DB readiness, pool size/idle/used, WS clients, queue
total/maximum/high-water, and reader-task running. Static route labels avoid unbounded symbol
or URL labels. Fault logs are structured JSON. Collector metrics are separate; zero ingestion
metrics in an API process do not describe the collector. Monitor freshness/coverage separately
from HTTP availability, and WS latency against the configured one-second poll interval.

## Backup, restore and rollback

Run scheduled custom-format `pg_dump` with a protected libpq service/password file. Capture
PostgreSQL/Timescale versions, schema head, row counts and SHA-256 of the archive. Keep
encrypted off-host backups and an operator-defined retention/RPO. The following libpq
service names must be provisioned privately; credentials are not command arguments:

```bash
pg_dump --dbname=service=marketdata_backup --format=custom --file=/secure-backups/marketdata.dump
sha256sum /secure-backups/marketdata.dump
pg_restore --list /secure-backups/marketdata.dump > /secure-backups/marketdata.contents
```

Restore into a **new, explicitly created recovery database**, with the same supported DB/
extension versions. Preserve the original database/volume. Provision target roles separately;
use `--no-owner --no-privileges` when a DBA will reapply exact grants. For Timescale:

```bash
psql service=marketdata_restore -v ON_ERROR_STOP=1 -c 'CREATE EXTENSION IF NOT EXISTS timescaledb; SELECT timescaledb_pre_restore();'
pg_restore --dbname=service=marketdata_restore --no-owner --no-privileges --exit-on-error /secure-backups/marketdata.dump
psql service=marketdata_restore -v ON_ERROR_STOP=1 -c 'SELECT timescaledb_post_restore();'
```

Plain PostgreSQL omits the extension hooks. Follow [Timescale logical restore](https://www.tigerdata.com/docs/deploy/self-hosted/backup-and-restore/logical-backup)
and [PostgreSQL pg_restore](https://www.postgresql.org/docs/17/app-pgrestore.html).
Before switching clients to a restored database, stop its writers/API readers, assign a
new random UUID to `marketdata_event_clock.epoch` in an explicit DBA transaction, then verify
history/coverage/identities/Decimal values/migration head and privileges. A new epoch forces
old WS cursors to 409 resync. Reapply grants and point mounted DSNs at the validated recovery
target only after operator approval. Restore recovers the backup snapshot, not later writes.

For a Batch 4-only code rollback, preserve schema 0006 and pin the reviewed Batch 3-compatible
image plus its separate credentials/configuration; recreate only `app`. Restart no collector
for API-only changes. For rollback to Batch 2/schema0005, stop all new API/collector owners,
back up, run reviewed `alembic downgrade 0005`, then start compatible code. This drops only
the replay journal/clock. Rollback to the old combined collector additionally requires
`downgrade 0004` and exactly one legacy owner with its preserved registry/image. Both
market-history-preserving migration round trips are exercised by inherited tests; do not
blindly run an old Store requiring 0004 on 0006. Never downgrade live owners or change a volume.
Use [integration rollback](collector-fastapi-integration.md) for the preserved 100/34/17 images.

Production TLS/firewall/backup scheduling and the final company session adapter are not
configured by this local qualification. Actual isolated Linux API startup/restart, DB outage/
recovery, read-only roles and database backup/restore are tested; see [Batch4 report](api-batch4-verification.md).
