# Windows collector and worker installation

Use a new versioned directory and service account on Windows10/11 or supported Windows Server
with Python3.12+, uv==0.11.27, current trust roots, synchronized Windows time and sufficient
local disk. PostgreSQL16.1 is the tested plain version; optional Timescale2.25.1/PG17.7 uses
its separately reviewed container profile. Docker Desktop requires a supported Windows host;
a Windows Server operator may use native PostgreSQL or a dedicated Linux database instead.
Do not assume Docker Desktop is a supported Windows Server production service.

These steps are for a NEW installation or an explicitly reviewed later legacy transition.
Never point them at the legacy database/volume or start two collectors. A worker-only install
does not require a TradingView connection or an active collector. It MUST have the same local
source dataset and stable source epoch as its approved company manifest.

```powershell
# Extract FIRST to C:/Marketdata/marketdata-collector-sync-v1.1.0rc1, then create .venv there.
Set-Location C:/Marketdata/marketdata-collector-sync-v1.1.0rc1
uv sync --frozen --dev --python C:/PATH_TO_APPROVED_SYSTEM_PYTHON312/python.exe
uv run pytest -q tests
uv run marketdata-sync --help
.venv/Scripts/python.exe scripts/prepare_install.py --component collector --directory C:/Marketdata/private --host 127.0.0.1 --port 65432 --database marketdata_source
New-Item -ItemType Directory C:/Marketdata/durable -ErrorAction Stop
# Set restrictive ACLs on the anchor directory before it contains state.
icacls C:/Marketdata/durable /inheritance:r /grant:r "$($env:USERDOMAIN)\$($env:USERNAME):(OI)(CI)F"
# NEW installation only: choose a new dedicated volume; do not reuse a legacy volume.
docker volume create COMPANY_APPROVED_NEW_SOURCE_VOLUME
docker run -d --name marketdata-source-v11 --restart unless-stopped -p 127.0.0.1:65432:5432 --mount type=volume,src=COMPANY_APPROVED_NEW_SOURCE_VOLUME,dst=/var/lib/postgresql/data --mount type=bind,src=C:/Marketdata/private/database_admin_password,dst=/run/secrets/database_admin_password,readonly -e POSTGRES_USER=marketdata_owner -e POSTGRES_DB=marketdata_source -e POSTGRES_PASSWORD_FILE=/run/secrets/database_admin_password postgres:16.1@sha256:09f23e02d76670d3b346a3c00aa33a27cf57aab8341eedfcdaed41459d14f5c4
# Wait for pg_isready in this NEW container; never target the old container name.
.venv/Scripts/python.exe -m marketdata.release_admin --dsn-file C:/Marketdata/private/database_admin_dsn --database marketdata_source --migrate --roles --passwords C:/Marketdata/private/role_passwords.json --check
.venv/Scripts/python.exe -m marketdata.release_admin --dsn-file C:/Marketdata/private/database_admin_dsn --database marketdata_source --check > C:/Marketdata/private/source-inventory.json
```

PowerShell5.1 redirection writes UTF16; normalize the non-secret inventory before transfer:

```powershell
$m5Inventory = Get-Content -Raw C:/Marketdata/private/source-inventory.json
[IO.File]::WriteAllText('C:/Marketdata/private/source-inventory.json',$m5Inventory,[Text.UTF8Encoding]::new($false))
```

The CLI applies only base0006 then independent sc0001/sc0002, and refuses a destination schema.
This is schema installation, not activation. Keep capture false/providers disabled until
backups, registry/provider identity, source_epoch, independent quote entitlement and rollback
preconditions pass. Do not invoke generic destination alembic upgrade head on a source.
Never enable source_capture in READ/INGEST.

Obtain the company's reviewed initial-plan.json, producer token and certificate trust chain
through the approved private channel. These must identify this installation, not another active
laptop/server. Company requires no local PostgreSQL password or TradingView credential. Store
only ingest_token locally; server fingerprint belongs to INGEST. Copy the exact approved
registry to C:/Marketdata/private/approved-instruments.json; do not automatically activate the
bundled99 proposal. The company plan must refer to its exact instrument/provider IDs and epoch.

```powershell
# Private files must already be copied with restricted ACLs; never paste token on command line.
.venv/Scripts/python.exe scripts/configure_worker.py --plan C:/Marketdata/private/initial-plan.json --output C:/Marketdata/private/worker.json --endpoint https://INGEST_HOSTNAME --source-dsn-file C:/Marketdata/private/sync_dsn --token-file C:/Marketdata/private/ingest_token --anchor-file C:/Marketdata/durable/source-anchor.json --ca-file C:/Marketdata/private/company-ca.pem --enroll
.venv/Scripts/python.exe -m marketdata.sync_worker --config C:/Marketdata/private/worker.json --once
```

Omit --ca-file for public system-trusted certificates; never use an empty CA file, insecure TLS,
or a hostname not covered by the certificate. `--once` is one bounded fair work turn, not proof
of full enrollment. `enroll:true` continues bounded historical candle/quote enrollment over
normal worker cycles; it does not fabricate missing records. Invalid legacy timestamp/volume
units stop preflight and remain locally retained. Preserve immutable staged bytes and investigate.

Copy collector.example.json to a NEW private collector.json and review it. Only after separate
source activation authorization set SOURCE_CAPTURE_ENABLED=true, appropriate enabled asset
providers=tradingview_guest and TRADINGVIEW_PRODUCTION_ACCESS_APPROVED=true. Use dual for native
candles plus independent quotes. Keep optional tick history off unless specifically required.
Preserve explicit delayed/unknown statuses and scoped provider aliases; never use candle close
as an independent price. Bind metrics to loopback and confirm source database roles are distinct.

Run collector once interactively under its dedicated account and verify local committed candles,
source outbox, genuine field timestamps and quote snapshots; then stop it before installing its
startup task. This is a future authorized live qualification step, not packaging verification.
Use separate Windows accounts for collector and sync where possible. Grant each account only
its DSN/config/registry plus the worker's token/anchor permissions, never database_admin_dsn or
role_passwords.json. Administrator retains backups. Directory inheritance must not grant Users.

Example Task Scheduler lifecycle (elevated PowerShell; replace absolute package path and
service identities; no password literals):

```powershell
$m5Package = 'C:/Marketdata/marketdata-collector-sync-v1.1.0rc1'
$m5Python = Join-Path $m5Package '.venv/Scripts/python.exe'
$m5WorkerAction = New-ScheduledTaskAction -Execute $m5Python -Argument '-m marketdata.sync_worker --config C:/Marketdata/private/worker.json' -WorkingDirectory $m5Package
$m5CollectorAction = New-ScheduledTaskAction -Execute $m5Python -Argument '-m marketdata.release_runtime collector --environment-file C:/Marketdata/private/collector.json' -WorkingDirectory $m5Package
$m5Trigger = New-ScheduledTaskTrigger -AtStartup
$m5Settings = New-ScheduledTaskSettingsSet -RestartCount 5 -RestartInterval (New-TimeSpan -Minutes 1) -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew
# Use an approved service account with batch-logon and private file permissions.
# Get-Credential prompts securely; no password literal is stored in this script.
$m5Credential = Get-Credential -UserName 'DOMAIN\MARKETDATA_SYNC_ACCOUNT' -Message 'Authorized unattended synchronization account'
Register-ScheduledTask -TaskName MarketdataV11Sync -Action $m5WorkerAction -Trigger $m5Trigger -Settings $m5Settings -User $m5Credential.UserName -Password ($m5Credential.GetNetworkCredential().Password)
# Register collector separately only after stopping old collection; use its approved account.
Start-ScheduledTask -TaskName MarketdataV11Sync
Get-ScheduledTaskInfo -TaskName MarketdataV11Sync
Stop-ScheduledTask -TaskName MarketdataV11Sync
```

Task Scheduler protects the service logon using Windows facilities. Use an approved managed
service account instead where required. Confirm batch-logon, file/network access and password
expiry policies; these commands do not bypass policy or qualify reboot behavior.
Configure stdout/stderr capture
through your approved service wrapper or a private launcher log; enforce rotation and sanitized
retention. Repeated terminal failures should alert and stop, not restart indefinitely until an
ownership fence is bypassed. Verify actual sleep/resume, reboot, service login and file access
on the target host before acceptance; packaging tests do not prove those OS behaviors.

Manual backfill/reconciliation: enable enroll in the reviewed immutable-identity config and
run continuously. The worker emits bounded backlog/age/difference summaries. After reviewing
missing company materialization, `python -m marketdata.sync_worker --config ... --once --repair`
performs one bounded repair pass; repeat monitored cycles only after review. It never invents
revision history. Do not run a repair instance alongside the active service: stop it first;
the advisory lock rejects a contender. Restart reuses DB state and the external anchor.

During an outage keep PostgreSQL/anchor files intact. Session expiry can recover the same still
owned producer automatically. Revocation, transferred ownership, uncertain restore identity or
clock/validator rejection require review, not new IDs or deleted state. See OPERATIONS.md.

Do not move/copy an existing virtual environment. The base Python installation and libraries
must be readable by the chosen unattended service account, not only an interactive user.
