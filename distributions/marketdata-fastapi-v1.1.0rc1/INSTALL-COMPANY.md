# Company first-install procedure

Run only after separate company installation authorization. Start with a NEW dedicated
`marketdata_v11` database and NEW named storage volume, never the unrelated application DB.
Prerequisites: Linux Docker/Compose (or approved Windows Server Linux-container host), Python
3.12+, uv==0.11.27, working DNS/HTTPS package registries, OpenSSL for tests, a DBA account,
a reviewed registry, an approved gateway network/IP, company hostnames and valid TLS chain.
No live collector needs to be reachable. No v1.0 installation or cutover is required.

1. Verify the supplied outer archive SHA256, extract to a new versioned directory and compare
   all SHA256SUMS entries. Preserve the previous candidate and verification sidecar. Build the
   exact Dockerfile/base digest with `--no-cache` as README specifies. Pin the resulting local
   image ID or approved registry digest for installation. Do not push without authorization.
2. Prepare NEW private directories and a new dedicated volume. Linux example (replace paths,
   reviewed hostnames and IPs first):

```bash
uv sync --frozen --no-dev
sudo install -d -m 0700 /etc/marketdata
sudo .venv/bin/python scripts/prepare_install.py --component company --directory /etc/marketdata/private --host db --database marketdata_v11
sudo install -d -m 0700 /etc/marketdata/approved
# Copy only the reviewed one-instrument registry and source inventory into approved/.
# Never copy the collector's TradingView connection credentials here.
test ! -e .env && cp .env.example .env
# Edit .env: image ID/digest, new volume name, external gateway network/IP, HTTPS origins,
# SECRETS_DIR=/etc/marketdata/private, CONFIG_DIR=/etc/marketdata/approved.
docker volume create COMPANY_NEW_MARKETDATA_VOLUME
# The company gateway administrator creates/approves its PRIVATE network and fixed proxy IP.
# Example only, after choosing a nonconflicting subnet:
docker network create --subnet COMPANY_APPROVED_SUBNET COMPANY_APPROVED_GATEWAY_NETWORK
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d db
docker compose --env-file .env --profile administration run --rm tools --dsn-file /run/secrets/database_admin_dsn --database marketdata_v11 --migrate --roles --passwords /run/secrets/role_passwords.json --check
```

On Windows PowerShell, preparation/build/Compose commands are equivalent. Set SECRETS_DIR and
CONFIG_DIR to absolute local paths supported by that Docker host. Create the private directory
under the service administrator account with the script (it removes inherited Windows ACLs
before writing files). Example:

```powershell
uv sync --frozen --no-dev
.venv\Scripts\python.exe scripts/prepare_install.py --component company --directory C:/MarketdataCompany/private --host db --database marketdata_v11
New-Item -ItemType Directory C:/MarketdataCompany/approved -ErrorAction Stop
if (Test-Path .env) { throw 'Review existing environment; do not overwrite it' }
Copy-Item .env.example .env
# Edit all placeholders before the following commands.
docker compose --env-file .env config --quiet
docker volume create COMPANY_NEW_MARKETDATA_VOLUME
docker compose --env-file .env up -d db
docker compose --env-file .env --profile administration run --rm tools --dsn-file /run/secrets/database_admin_dsn --database marketdata_v11 --migrate --roles --passwords /run/secrets/role_passwords.json --check
```

The PostgreSQL image initializes the dedicated database with database_admin_password; data
persists in the explicitly named external volume. No PostgreSQL port is published. The schema
administrator owns tables/definer functions. READ/INGEST roles are new non-owner logins, cannot
create roles/databases/schema, and cannot mutate policy. The CLI refuses an existing source
extension, a mismatched database name, unknown schema or unversioned nonempty DB. Roles fail
if names exist; it will not rotate credentials or perform an ownership transfer. Do not use
these initial-role scripts on a shared cluster without DBA review of the fixed role names.

Timescale is optional. For a separate fresh Timescale installation, use a distinct fresh
volume and `-f compose.yaml -f compose.timescale.yaml` on EVERY Compose command, and add
`--timescale` to the tools migration command. That profile uses Timescale2.25.1/PG17.7, not
PG16.1; never mount a PG16 volume in PG17. Plain PostgreSQL16.1 is fully supported. Retain the
exact selected image digest and backup/restore tool version. Do not initialize both profiles
against the same volume. Scripts target destination0010 directly; 0009->0010 is supported but
is not a required first-install sequence.

3. The source operator performs the separately reviewed source setup and sends only its
   non-secret inventory (base0006/sc0002/source_epoch) and exact approved registry. Begin with
   one instrument. Persist a NEW producer UUID for this installation and a non-secret key ID.
   Generate an initial plan and credentials; review every field before applying it:

```bash
.venv/bin/python scripts/create_initial_plan.py --registry /etc/marketdata/approved/instruments.json --source-inventory /etc/marketdata/approved/source-inventory.json --instrument APPROVED_CANONICAL_SYMBOL --provider tradingview_guest --producer-id NEW_PRODUCER_UUID --key-id laptop-installation-01 --statuses APPROVED_COMMA_SEPARATED_STATUSES --output /etc/marketdata/approved/initial-plan.json
.venv/bin/python scripts/create_ingest_credentials.py --directory /etc/marketdata/private/new-producer-credentials --producer-id NEW_PRODUCER_UUID --key-id laptop-installation-01
# Private transfer: ingest_token to source operator only; server fingerprint JSON to company.
# Copy ingest_credentials.json to /etc/marketdata/private/ingest_credentials.json.
docker compose --env-file .env --profile administration run --rm tools --dsn-file /run/secrets/database_admin_dsn --database marketdata_v11 --registry /provision/instruments.json --provision /provision/initial-plan.json --check
```

PowerShell uses `.venv/Scripts/python.exe` and equivalent C:/ private paths. All UUID/key/status
arguments must match the reviewed plan; do not guess realtime entitlement. initial-plan.json
has all four stream grants, stable source epoch, generation1 and a durable initial session UUID.
The CLI inserts a new producer/owners, never updates existing ownership. Foreign keys reject
unknown mappings. A lease expiring before worker startup is ordinary same-producer recovery,
not permission to choose a new unknown session.

4. Split file permissions before service startup. On Linux runtime-mounted `read_dsn`,
   `ingest_dsn`, `api_key` and `ingest_credentials.json` need read access for container UID10001;
   e.g. root:10001 mode0640 and traversal on their dedicated secrets directory, while admin DSN,
   admin password and role_passwords.json remain root-only0600. Docker Compose file secrets are
   bind mounts and do not reliably remap host UID/mode. Use a company secret manager if required.
   Different runtime containers mount only their own files. On Windows grant read ACLs only to
   the chosen Docker/service principal and administrator, not Users/Everyone. Tools run as root
   only in the explicit administration profile; runtime services run nonroot/read-only/cap-drop.
On Linux apply runtime traversal/read permissions explicitly after server fingerprint copy:

```bash
sudo chgrp 10001 /etc/marketdata/private
sudo chmod 0710 /etc/marketdata/private
sudo chown root:10001 /etc/marketdata/private/read_dsn /etc/marketdata/private/ingest_dsn /etc/marketdata/private/api_key /etc/marketdata/private/ingest_credentials.json
sudo chmod 0640 /etc/marketdata/private/read_dsn /etc/marketdata/private/ingest_dsn /etc/marketdata/private/api_key /etc/marketdata/private/ingest_credentials.json
```

5. Configure the company-approved reverse proxy using gateway.nginx.conf as an example. Replace
   both server_names and certificate paths, attach its fixed approved IP to GATEWAY_NETWORK,
   and, after backend services are up in step6, validate with `nginx -t` before reload. READ and INGEST have
   separate hostnames/routes. There is no public HTTP endpoint forwarding bearer credentials.
   Only gateway may reach application listeners. Trust only its exact IP, never '*'. Keep DB
   internal; deny inbound5432 and direct application8000 at firewall/security groups. Allow
   collector outbound443 to INGEST and READ access through the company's frontend/BFF boundary.
6. Start independently and check liveness, DB availability, authorization and data separately:

```text
docker compose --env-file .env up -d read ingest
docker compose --env-file .env ps
docker compose --env-file .env logs --tail 100 read ingest
```

From an authorized machine, use scripts/smoke_read.py with `--endpoint https://READ_HOSTNAME
--token-file PRIVATE_READ_SERVICE_KEY --symbol APPROVED_CANONICAL_SYMBOL --provider tradingview_guest
--origin https://FRONTEND_HOSTNAME` and optional `--ca-file COMPANY_CA_PEM`. It performs no writes.
The INGEST health route `/api/ingest/v1/health` requires a valid producer Authorization bearer;
use a private client that reads the file, not a literal token on the command line. Invalid
credentials return401, revoked/unauthorized producer403, fence conflicts409, validation422,
oversize413, quota429 with Retry-After, transient DB/service503. No financial ack precedes commit.
Verify READ has no ingestion routes and no write/function privileges; use release verification
SQL/tests before authorization. Do not put permanent ingestion tokens into browser code.

Start the independently configured source worker for actual data, then compare native candles,
field timestamps/ages, coverage, receipts and committed WSS events. Correct ChartData endpoint
is `/api/v1/dashboard/candles`: a=time seconds, b=open, c=close, d=high, e=low, f=count, g=volume,
h=value; unavailable metrics0, empty results eight empty arrays. REST financial decimals remain
strings. WSS is `/api/v1/ws/marketdata`, ack type subscription_ack; replay is bounded, expired
cursor requires REST resnapshot, no exactly-once delivery promise. Existing frontend auth stays
unchanged; keep service keys in the company's server-side frontend/BFF boundary.

Separately restart read and ingest (`docker compose restart read`, then `restart ingest`) and
repeat smoke/data checks. Restarting API must not restart collection. Never use `down -v` or
prune against this installation. Backups are required before migrations and regularly; see
OPERATIONS.md for actual restore and application rollback procedures and site sign-off gates.

If initial registry registration succeeds but a later initial plan fails, retain the registry.
Compare its mappings with the approved source, correct the rejected plan, and retry provisioning
without --registry. Do not truncate registered financial data to rerun a setup command.
