# Reproducing Milestone 4 qualification

These commands create disposable test infrastructure. They do not install the company
release or migrate the preserved collector. Use the reviewed workspace containing milestones
1 through 4; GitHub main and the frozen v1.0 ZIP do not contain the uncommitted implementation.
Read the [results and limitations](fastapi-v1.1-milestone4.md) first.

## Environment and safety

Python 3.12+, uv, Docker with Compose, and OpenSSL are required. On Windows the harness also
accepts Git's `C:/Program Files/Git/usr/bin/openssl.exe`. Install the collector and migration
extras and development dependencies. TLS uses a private disposable CA and certificate with
localhost/127.0.0.1 SANs. Clients verify that CA; the untrusted-CA negative test must fail.

The harness accepts only explicit loopback ports 55995–55998 or 56005–56008 and task database
names starting `marketdata_v11_m4_` or `m4_`, ending `_test`. It creates new UUID databases,
separate restricted principals, random M2M/read credentials and private per-run files. Never
substitute another DSN. `disposable_test` below is a test fixture, never a service credential.
The worker receives only its restricted **source** DSN and destination HTTPS configuration.

Run one qualification or full pytest suite at a time **per PostgreSQL cluster**. Role setup
uses fixed test role names; different databases alone do not isolate concurrent role changes.
Each `--output` must be new. Retained task databases are listed in `report.json`; analyze
them before removing containers. Private directories contain credentials/anchors/dumps and
must not be published. Public evidence does not contain bearer tokens or database passwords.

The four ordinary Compose services use tmpfs: restarting a container erases its database.
Use the separate `recovery` services with labelled disposable volumes for real restart/restore
tests. Neither configuration references any existing collector volume.

## Windows PowerShell

From the reviewed repository, with an explicitly selected disposable Docker context:

```powershell
uv sync --extra collector --extra migrations --dev
docker compose -p marketdata-v11-m4-tests -f compose.v11-m4-test.yaml --profile recovery config --quiet
docker compose -p marketdata-v11-m4-tests -f compose.v11-m4-test.yaml --profile recovery up -d --wait
New-Item -ItemType Directory -Force artifacts/v11-m4-repeat | Out-Null
$m4Pg = @{
  source='postgresql://m4_test:disposable_test@127.0.0.1:55997/marketdata_v11_m4_test'
  destination='postgresql://m4_test:disposable_test@127.0.0.1:55995/marketdata_v11_m4_test'
}
$m4Ts = @{
  source='postgresql://m4_test:disposable_test@127.0.0.1:55998/marketdata_v11_m4_test'
  destination='postgresql://m4_test:disposable_test@127.0.0.1:55996/marketdata_v11_m4_test'
}
$m4Pg | ConvertTo-Json | Set-Content -Encoding utf8 artifacts/v11-m4-repeat/pg.json
$m4Ts | ConvertTo-Json | Set-Content -Encoding utf8 artifacts/v11-m4-repeat/ts.json
# Windows PowerShell 5.1 emits a BOM; normalize JSON files for Python's strict UTF-8 reader.
foreach ($m4File in @('pg.json','ts.json')) {
  $m4Path = Join-Path $PWD ('artifacts/v11-m4-repeat/' + $m4File)
  [IO.File]::WriteAllText($m4Path, [IO.File]::ReadAllText($m4Path), [Text.UTF8Encoding]::new($false))
}
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-one --feeds 1 --soak-seconds 30 --burst-seconds 0 --faults
uv run python scripts/qualify_v11_edges.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-edges --feeds 1 --soak-seconds 0 --burst-seconds 0
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-backfill --feeds 99 --history-minutes 10 --quotes-during-backfill --status-ttl-seconds 60 --soak-seconds 30 --burst-seconds 0
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-soak --feeds 99 --quote-hz 1 --soak-seconds 900 --burst-hz 5 --burst-seconds 120 --status-ttl-seconds 60
uv run python scripts/report_v11_qualification.py --directory artifacts/v11-m4-repeat/pg-soak --database-file artifacts/v11-m4-repeat/pg.json
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/ts.json --output artifacts/v11-m4-repeat/ts-soak --feeds 99 --quote-hz 1 --soak-seconds 900 --burst-hz 5 --burst-seconds 120 --status-ttl-seconds 60 --timescale
uv run python scripts/report_v11_qualification.py --directory artifacts/v11-m4-repeat/ts-soak --database-file artifacts/v11-m4-repeat/ts.json
```

For actual database restart/restore qualification, create recovery configurations:

```powershell
$m4Recovery = @{
  source='postgresql://m4_test:disposable_test@127.0.0.1:56007/marketdata_v11_m4_test'
  destination='postgresql://m4_test:disposable_test@127.0.0.1:56005/marketdata_v11_m4_test'
  docker_prefix=@('docker')
  source_container='marketdata-v11-m4-recovery-source-pg16-test'
  destination_container='marketdata-v11-m4-recovery-destination-pg16-test'
}
[IO.File]::WriteAllText((Join-Path $PWD 'artifacts/v11-m4-repeat/pg-recovery.json'), ($m4Recovery | ConvertTo-Json -Depth 4), [Text.UTF8Encoding]::new($false))
uv run python scripts/qualify_v11_recovery.py --database-file artifacts/v11-m4-repeat/pg-recovery.json --output artifacts/v11-m4-repeat/pg-restore --feeds 1 --soak-seconds 0 --burst-seconds 0
$m4Recovery.source='postgresql://m4_test:disposable_test@127.0.0.1:56008/marketdata_v11_m4_test'
$m4Recovery.destination='postgresql://m4_test:disposable_test@127.0.0.1:56006/marketdata_v11_m4_test'
$m4Recovery.source_container='marketdata-v11-m4-recovery-source-ts-test'
$m4Recovery.destination_container='marketdata-v11-m4-recovery-destination-ts-test'
[IO.File]::WriteAllText((Join-Path $PWD 'artifacts/v11-m4-repeat/ts-recovery.json'), ($m4Recovery | ConvertTo-Json -Depth 4), [Text.UTF8Encoding]::new($false))
uv run python scripts/qualify_v11_recovery.py --database-file artifacts/v11-m4-repeat/ts-recovery.json --output artifacts/v11-m4-repeat/ts-restore --feeds 1 --soak-seconds 0 --burst-seconds 0 --timescale
```

Full suites use the ordinary **destination test database only**, serially with pipeline tests:

```powershell
$env:DATABASE_URL=$m4Pg.destination
uv run alembic upgrade head
$env:TEST_DATABASE_URL=$m4Pg.destination
uv run pytest -q --junitxml=artifacts/v11-m4-repeat/pg-full.xml
$env:DATABASE_URL=$m4Ts.destination
uv run alembic -x timescale=true upgrade head
$env:TEST_DATABASE_URL=$m4Ts.destination
uv run pytest -q --junitxml=artifacts/v11-m4-repeat/ts-full.xml
Remove-Item Env:TEST_DATABASE_URL
Remove-Item Env:DATABASE_URL
uv run ruff check .
uv run ruff format --check .
uv run mypy
```

## Linux

Use a disposable Docker host/context. The following creates configurations equivalent to
PowerShell and runs the same native-process/TLS harness; it is a reproduction procedure,
not evidence that Linux process supervision was tested in this Windows run.

```bash
uv sync --extra collector --extra migrations --dev
docker compose -p marketdata-v11-m4-tests -f compose.v11-m4-test.yaml --profile recovery up -d --wait
mkdir -p artifacts/v11-m4-repeat
umask 077
uv run python - <<'PY'
import json
from pathlib import Path
root = Path('artifacts/v11-m4-repeat')
for engine, ports, containers in [
 ('pg', (55997,55995), None), ('ts', (55998,55996), None),
 ('pg-recovery', (56007,56005), 'pg16'), ('ts-recovery', (56008,56006), 'ts')]:
    cfg = {role: f'postgresql://m4_test:disposable_test@127.0.0.1:{port}/marketdata_v11_m4_test'
           for role, port in zip(('source','destination'), ports)}
    if containers:
        cfg.update(docker_prefix=['docker'])
        for role in ('source','destination'):
            cfg[role+'_container'] = f'marketdata-v11-m4-recovery-{role}-{containers}-test'
    (root/(engine+'.json')).write_text(json.dumps(cfg))
PY
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-one --feeds 1 --soak-seconds 30 --burst-seconds 0 --faults
uv run python scripts/qualify_v11_edges.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-edges --feeds 1 --soak-seconds 0 --burst-seconds 0
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/pg.json --output artifacts/v11-m4-repeat/pg-soak --feeds 99 --quote-hz 1 --soak-seconds 900 --burst-hz 5 --burst-seconds 120 --status-ttl-seconds 60
uv run python scripts/report_v11_qualification.py --directory artifacts/v11-m4-repeat/pg-soak --database-file artifacts/v11-m4-repeat/pg.json
uv run python scripts/qualify_v11_pipeline.py --database-file artifacts/v11-m4-repeat/ts.json --output artifacts/v11-m4-repeat/ts-soak --feeds 99 --quote-hz 1 --soak-seconds 900 --burst-hz 5 --burst-seconds 120 --status-ttl-seconds 60 --timescale
uv run python scripts/qualify_v11_recovery.py --database-file artifacts/v11-m4-repeat/pg-recovery.json --output artifacts/v11-m4-repeat/pg-restore --feeds 1 --soak-seconds 0 --burst-seconds 0
uv run python scripts/qualify_v11_recovery.py --database-file artifacts/v11-m4-repeat/ts-recovery.json --output artifacts/v11-m4-repeat/ts-restore --feeds 1 --soak-seconds 0 --burst-seconds 0 --timescale
DATABASE_URL=postgresql://m4_test:disposable_test@127.0.0.1:55995/marketdata_v11_m4_test uv run alembic upgrade head
TEST_DATABASE_URL=postgresql://m4_test:disposable_test@127.0.0.1:55995/marketdata_v11_m4_test uv run pytest -q --junitxml=artifacts/v11-m4-repeat/pg-full.xml
DATABASE_URL=postgresql://m4_test:disposable_test@127.0.0.1:55996/marketdata_v11_m4_test uv run alembic -x timescale=true upgrade head
TEST_DATABASE_URL=postgresql://m4_test:disposable_test@127.0.0.1:55996/marketdata_v11_m4_test uv run pytest -q --junitxml=artifacts/v11-m4-repeat/ts-full.xml
uv run ruff check .
uv run ruff format --check .
uv run mypy
```

## Profiles, genuine source and cleanup

Default load is 99 feeds at one injected observation/feed/second for 900 seconds, followed
by five/second for 120 seconds. Rates, durations, feed count, bounded history and status TTL
are CLI parameters. Bounds are 1–99 feeds, rates `(0,10]`, soak 0–3600 seconds, burst
0–300 seconds, history 1–120 minutes and at most 300,000 generated load observations.
`--quotes-during-backfill` additionally injects bounded one-Hz observations during initial
catch-up. Each fixture keeps its original observation ID and timestamps over restarts.

`--genuine --feeds 1 --soak-seconds 60 --burst-seconds 0` explicitly replaces the doubles
with the actual TradingView dual acquisition factory, scoped to BINANCE:BTCUSDT. Execute
only where that isolated connection is authorized and cannot compete with an installation.
Do not use `--genuine` for unattended 99-feed activation. A successful injected run proves
transport/persistence semantics, not upstream availability, publication cadence or rights.

After saving `report.json`, run the report script for each completed run while its database
still exists. It produces per-instrument CSV, exact database comparisons, complete verified
quote snapshots, candle/quote ledger-to-receipt-to-journal-to-WebSocket traces and measured
phase/resource distributions. Final acquisition is stopped before independent financial and
coverage drains. An edge run intentionally ends fenced with pending invalid data; that is
not a complete drain. Read each report's checks and failure category, not just process exit.

The actual M4 run used an independent WSL Docker daemon/root/socket because Docker Desktop
and its existing collector installation were stopped and protected. Compose configuration
was validated but these services were created equivalently with explicit labelled Docker
commands. The session-only `scripts/cleanup_v11_m4_runtime.ps1` verifies its exact WSL
registration path, daemon root, eight containers and four volumes before removal. It is not
a generic cleaner. For a reproduction using Compose, first inspect its eight named services
and four labelled volumes, confirm no unrelated resources, then remove that explicit project:

```text
docker compose -p marketdata-v11-m4-tests -f compose.v11-m4-test.yaml --profile recovery down --volumes
```

Do not run broad Docker prune or delete any pre-existing volume. Local evidence and private
files remain for operator review; protect or destroy private credentials according to the
operator's retention policy. These commands are qualification procedures, not deployment.
