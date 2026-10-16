# Linux collector and worker installation

Use a new final installation directory, Python3.12+, uv==0.11.27 and a dedicated PostgreSQL16.1 source
(or separately reviewed Timescale profile). The worker has no TradingView transport dependency
at runtime and no company database credential. No legacy migration is performed by installation.

```bash
# Verify the archive first. Choose a NEW final code directory before creating any .venv.
sudo unzip /PATH_TO_VERIFIED/marketdata-collector-sync-v1.1.0rc1.zip -d /opt
# Only if this alias does not exist; never overwrite a deployed alias implicitly:
test ! -e /opt/marketdata-collector-sync && sudo ln -s /opt/marketdata-collector-sync-v1.1.0rc1 /opt/marketdata-collector-sync
cd /opt/marketdata-collector-sync
sudo "$(command -v uv)" sync --frozen --no-dev --python /PATH_TO_APPROVED_SYSTEM_PYTHON312
sudo useradd --system --home /var/lib/marketdata-sync --shell /usr/sbin/nologin marketdata-sync
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin marketdata-collect
sudo install -d -m 0700 /etc/marketdata-source
sudo .venv/bin/python scripts/prepare_install.py --component collector --directory /etc/marketdata-source/private --host 127.0.0.1 --port 65432 --database marketdata_source
sudo install -d -m 0700 -o marketdata-sync -g marketdata-sync /var/lib/marketdata-sync
# NEW volume/name only. Never substitute legacy volume/container names.
docker volume create COMPANY_APPROVED_NEW_SOURCE_VOLUME
docker run -d --name marketdata-source-v11 --restart unless-stopped -p 127.0.0.1:65432:5432 --mount type=volume,src=COMPANY_APPROVED_NEW_SOURCE_VOLUME,dst=/var/lib/postgresql/data --mount type=bind,src=/etc/marketdata-source/private/database_admin_password,dst=/run/secrets/database_admin_password,readonly -e POSTGRES_USER=marketdata_owner -e POSTGRES_DB=marketdata_source -e POSTGRES_PASSWORD_FILE=/run/secrets/database_admin_password postgres:16.1@sha256:09f23e02d76670d3b346a3c00aa33a27cf57aab8341eedfcdaed41459d14f5c4
# Wait for pg_isready; perform these only against the NEW explicitly named source.
sudo .venv/bin/python -m marketdata.release_admin --dsn-file /etc/marketdata-source/private/database_admin_dsn --database marketdata_source --migrate --roles --passwords /etc/marketdata-source/private/role_passwords.json --check
sudo .venv/bin/python -m marketdata.release_admin --dsn-file /etc/marketdata-source/private/database_admin_dsn --database marketdata_source --check > source-inventory.json
sudo install -d -m 0700 -o marketdata-sync -g marketdata-sync /etc/marketdata-sync
sudo install -d -m 0700 -o marketdata-collect -g marketdata-collect /etc/marketdata-collect
sudo install -m 0600 -o marketdata-sync -g marketdata-sync /etc/marketdata-source/private/sync_dsn /etc/marketdata-sync/source_dsn
sudo install -m 0600 -o marketdata-collect -g marketdata-collect /etc/marketdata-source/private/collector_dsn /etc/marketdata-collect/source_dsn
```

Send non-secret source-inventory.json and the exact approved registry to the company DBA.
Receive this installation's approved complete initial manifest, token and company CA by a
private channel. Install them mode0600 under /etc/marketdata-sync owned by marketdata-sync.
Never copy the server database/admin credentials to this host. The source admin DSN remains
administrator-only, separate from both runtime accounts. Make the package tree root-owned and
read/execute for runtime users, with its .venv under /opt/marketdata-collector-sync as systemd
examples expect. Do not make source code writable by either runtime account.

```bash
sudo -u marketdata-sync /opt/marketdata-collector-sync/.venv/bin/python /opt/marketdata-collector-sync/scripts/configure_worker.py --plan /etc/marketdata-sync/initial-plan.json --output /etc/marketdata-sync/worker.json --endpoint https://INGEST_HOSTNAME --source-dsn-file /etc/marketdata-sync/source_dsn --token-file /etc/marketdata-sync/ingest_token --anchor-file /var/lib/marketdata-sync/source-anchor.json --ca-file /etc/marketdata-sync/company-ca.pem --enroll
# Review collector.example.json; use Linux paths in /etc/marketdata-collect/collector.json.
# Defaults are disabled. Authorize opt-in only after COLLECTOR-TRANSITION.md preconditions.
sudo install -m 0644 marketdata-worker.service /etc/systemd/system/marketdata-worker.service
sudo install -m 0644 marketdata-collector.service /etc/systemd/system/marketdata-collector.service
sudo systemctl daemon-reload
# Start worker independently; collector activation is a separate reviewed action.
sudo systemctl enable --now marketdata-worker
sudo systemctl status marketdata-worker
sudo journalctl -u marketdata-worker --since '10 minutes ago'
# After explicit collector approval and proof no old instance is running:
sudo systemctl enable --now marketdata-collector
sudo systemctl restart marketdata-worker
```

Configure journald retention/forwarding and alert repeated exit/fence failures. Restrict network:
source PostgreSQL loopback/private only, outbound443 to INGEST plus approved upstream acquisition;
metrics loopback or authenticated internal monitoring. No externally published local database.
Default service protection allows worker state writes only under /var/lib/marketdata-sync;
its external continuity anchor must be there. Keep it backed up separately from PostgreSQL.

Publicly trusted HTTPS needs no ca_file; private company PKI needs the complete approved CA
bundle and hostname validation. Never use verify=false. NTP/chrony and UTC are mandatory;
future-dated observations are retained/rejected, not rewritten. No actual Linux systemd boot
or company firewall qualification is implied by shipping the unit files.

For manual enrollment/repair stop the worker service first, run its command as marketdata-sync
with --once (and --repair only after review), inspect durable results, then restart service.
`--once` is one bounded turn, not a full-history guarantee. Historical invalid data remains
unmarked/reported; compare source/company actual rows and revisions. The complete protocol,
recovery, outbox bounds and fair processing are in docs/fastapi-v1.1-sync-worker.md.

Create the virtual environment at this final path. Never move/copy a previously created .venv
from Downloads, another host, Windows or another Python installation. Both service accounts
must be able to read the approved system interpreter/base libraries and code environment.
