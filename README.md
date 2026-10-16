> FastAPI **1.1.0rc1**: [release preparation and verification](docs/fastapi-v1.1-milestone5.md),
> [company first installation](distributions/marketdata-fastapi-v1.1.0rc1/INSTALL-COMPANY.md),
> [Windows worker](distributions/marketdata-collector-sync-v1.1.0rc1/INSTALL-WINDOWS.md),
> [Linux worker](distributions/marketdata-collector-sync-v1.1.0rc1/INSTALL-LINUX.md).
> Destination head is **0010**; source is separate base **0006 + sc0002**.
> Candidate release; genuine 99-feed and target-host production qualification remain outstanding.
> Approved capture, ingestion, recovery and read contracts are preserved. No live rollout.
> The frozen FastAPI v1.0 package remains unchanged at 0006.

# Multi-asset market data

An asynchronous Python platform for ingesting market data into PostgreSQL/TimescaleDB
and serving latest prices, historical ticks, OHLCV candles, provider health and WebSocket
subscriptions through FastAPI.

**FastAPI v1.1 development:** [milestone 1 destination foundation](docs/fastapi-v1.1-milestone1.md)
adds transaction-aware persistence, fenced producer ownership and durable batch receipts.
Root destination schema head is **0010**. Milestone 3B adds an independent durable HTTPS worker;
no deployment or live collector change has occurred. The frozen v1.0 handover below remains
unchanged at schema **0006**.

**Company API-only setup:** [standalone FastAPI guide](docs/standalone-api.md) provides a
minimal independent image, disposable native candles/quotes, separate SELECT-only credentials,
REST/WS examples and the external-producer contract. No TradingView or collector is required.

**Self-contained handover:** copy [distributions/marketdata-fastapi](distributions/marketdata-fastapi/README.md)
for the independently runnable project, with local source, lockfile, migrations, docs, tests,
Docker setup and examples. It has been verified outside this repository and can later become
its own repository when authorized.

**The default configuration uses simulated data only.** It runs offline with 30 instruments:
crypto, FX, gold/silver spot, WTI/Brent CFDs and synthetic concrete futures contracts.
Live adapters require explicit activation; their implementation does not establish live
availability, exchange entitlement or redistribution rights.

This GitHub repository is **public** (verified 2026-10-12). Credentials, local overrides,
database volumes/dumps, logs and raw generated artifacts are excluded from Git. Sanitized
benchmark evidence and parser fixtures are included; recorded database history remains local.

## Current capabilities and limits

Completed collector and FastAPI Batches 1–4 are consolidated through
[PR #2](https://github.com/Alpha-lacrim/tradingview-marketdata/pull/2), retaining their commit history. See
[the consolidation audit](docs/main-consolidation.md) for inclusion, schema compatibility
and deployment boundaries. The operator authorized consolidation into `main` on 2026-10-14.
Code publication and merge do not deploy the running legacy application.

Batch 4 qualifies the company API locally with 100 simulated instruments, real TCP REST/WS
load, actual disposable database outages, read-only roles, Linux container restart and
backup/restore. See [employer report](docs/api-batch4-verification.md),
[frontend integration](docs/frontend-integration.md) and [production operations](docs/production-deployment.md).
Production startup rejects write-capable API credentials. `/livez` separates process liveness
from DB readiness; metrics include route/query latency, pool usage and WebSocket pressure.
The production Compose definition mounts separate secrets and blocks unapproved guest
ingestion. This is an isolated, undeployed qualification; target-host HTTPS/session/source
rights and sustained capacity remain separate gates.

Batch 2 adds a company-facing, versioned **read-only REST API** under `/api/v1` for
instruments, persisted latest prices, completed native/derived candles, coverage and health.
Fresh quotes and candle closes are explicitly distinct; missing data remains unavailable.
Bearer authentication, production configuration checks, an explicit CORS allow-list and
bounded pagination are documented in [the frontend API guide](docs/api.md).
The company BFF browser/session model is **PROVISIONAL**: the frontend and its final
authentication architecture are not completed. Keep the service token server-side.
Dedicated SELECT-only PostgreSQL grants are documented and independently tested.
See [Batch 2 verification](docs/api-batch2-verification.md) for exact results and limits.
This work starts from integration commit `35ab3c03662ad00e8114fb313f8d2d15459a78d9`
and is undeployed. Batch 3 builds from verified Batch 2 `b9c08df5` in the isolated
`fastapi/batch3-websocket` worktree; that qualification used schema **0006**.

`/api/v1/ws/marketdata` provides authenticated, Origin-validated subscriptions, persisted
price snapshots, genuine latest-quote updates, completed/corrected native candles, source
freshness and heartbeats. A bounded transactional PostgreSQL journal and one reconciliation
reader per API worker deliver committed changes across processes, with no new provider
connections. `/api/v1/marketdata/changes` recovers missed inserts and old candle corrections;
expired cursors require refreshing loaded REST chart history. WebSocket delivery is not
exactly once or permanent history. Queue overflow/send timeout closes the client with 1013.
See [commands, every message schema, limits and browser recovery](docs/api.md#websocket-market-data-v1),
[browser JavaScript](examples/browser-marketdata.js), the [provisional BFF adapter](examples/company_bff.py)
and [Batch 3 verification](docs/api-batch3-verification.md). There are no production changes
or new live TradingView tests. Candle-only mode exposes completed candle closes with their
explicit basis. The opt-in collector `dual` mode adds independent genuine quote subscriptions
and latest-state journaling, compatible with the released standalone API. See
[dual ingestion and qualification](docs/dual-ingestion-qualification.md) for verification,
the preserved registries, source limitations and pending live qualification.

| Adapter | Coverage | Current status |
|---|---|---|
| Mock | All initial categories | Simulated; no credentials or network required |
| Binance Spot | Configurable crypto spot trades | Implemented public adapter; connectivity failed in the recorded local environment |
| OANDA v20 | Account-discovered FX, OTC metals and explicit CFDs | Requires token/account; authenticated sessions untested; practice is simulated, live conservatively indicative |
| Databento Live SDK | Concrete outright exchange futures | Requires paid access and verified exchange entitlements; authenticated sessions untested; status unknown unless verified |
| TradingView guest | Default registry of 17 native 1m chart series; diagnostic quotes | Opt-in, experimental, unsupported observed protocol; validates identity and realtime status at runtime; no exchange futures |

The experimental guest collector stores native completed one-minute candles and exposes
coverage gaps. Its application default is **one socket, one ingestion worker and 17 instruments**.
The opt-in multi-socket experiments reached 128/256/512 crypto spot targets, with eventual
commits of 743/768, 1,419/1,536 and 2,663/3,072 respectively. All stages were incomplete;
these results do not demonstrate complete coverage or sustained production capacity.
An earlier unpaced run encountered HTTP 429 and stopped escalation.
The preserved legacy deployment is configured for a fixed **100-instrument universe** against the
existing database, with four paced sockets and global concurrency eight. That accuracy-selected
profile achieved 99.9% eventual coverage in a 30-cycle window plus three settling cycles.
Its app and database were stopped on the 2026-10-15 milestone inspection; they were left stopped.
The current priority is a separate provisional **99-source business proposal**: 50 Binance
markets, both brokers for 21 OANDA/FXCM forex pairs, metals and energy. It is not deployed;
see [business qualification, source gaps and approval decisions](docs/business-universe-qualification.md).
Earlier ordinary 64/128/256 measurements and exact 34/17 image rollbacks are preserved.
See [current scaling measurements and exact rollback](docs/ordinary-collector-scaling.md),
[pilot activation and persistence evidence](docs/production-instrument-expansion.md)
and [the ATOM native-history/availability investigation](docs/native-availability-investigation.md).
Request success, native availability and durable commits are measured separately.

Both completed development streams are integrated locally on
`integration/collector-fastapi-batch1`; see [integration verification and deployment boundaries](docs/collector-fastapi-integration.md).
The default API builds zero providers; only the explicit collector owns ingestion. Schema
0005 adds health only; additive 0006 adds the bounded committed-event journal. This branch is
undeployed. The running legacy collector has no new
advisory lock and must exit before any future approved replacement starts. The provisional
99-source registry is preserved and is never selected automatically.

See [live end-to-end evidence](docs/candle-live-end-to-end.md),
[earlier live/synthetic retrieval results](docs/candle-retrieval-scalability.md) and
[guest limitations and activation](docs/tradingview.md).

Instrument identity distinguishes spot, OTC, CFDs and concrete exchange futures. Prices use
Decimal/PostgreSQL NUMERIC, and source and receipt timestamps remain distinct and UTC-aware.
Physical WTI/Brent spot pricing is not implemented. API workers now read PostgreSQL
independently of a dedicated collector. Committed WebSocket updates and bounded REST
reconciliation are available. Crash-safe ingestion queue spooling and upstream tick
replay/backfill remain outside this version.

## Repository layout

Application files live directly at the Git repository root. Run commands below from this root.

```text
.
├── src/marketdata/           # Domain, providers, ingestion, persistence and FastAPI
├── tests/                   # Deterministic tests, fixtures and database integration tests
├── scripts/                 # Smoke checks, reconciliation and opt-in benchmarks
├── config/                  # Non-secret instrument registries
├── migrations/              # Destination revisions 0001–0010 (frozen v1.0: 0006)
├── source_migrations/       # Separate opt-in sc0001–sc0002 on source base 0006
├── docs/                    # Technical reference, operations and measured evidence
├── .github/workflows/       # PostgreSQL/TimescaleDB CI matrix
├── pyproject.toml, uv.lock  # Python package, tooling and locked dependencies
├── Dockerfile, compose.yaml # Container build and default mock deployment
├── compose.tradingview.yaml # Explicit experimental guest override
└── AGENTS.md, CODEX.md, HANDOFF.md
```

`AGENTS.md` holds repository policy, `CODEX.md` engineering procedures, and `HANDOFF.md`
current implementation state and outstanding work. For an existing nested checkout or PyCharm
project, read [the safe checkout migration guide](docs/repository-layout.md) before switching.

## Quick start

For the current integration verdict, local authentication/WS examples and collector
dependencies, start with the [developer handoff](docs/developer-handoff.md).

Clone the repository and enter it:

```text
git clone https://github.com/Alpha-lacrim/tradingview-marketdata.git
cd tradingview-marketdata
```

Batch 1 separates the collector from the API. This branch is **undeployed**. Use isolated
configuration and a new Compose project; never run these commands against the existing
`tradingview` project or production database while the collector session is active.

For an independent default mock deployment:

```powershell
Copy-Item .env.example .env
$env:POSTGRES_PORT='55483'
$env:API_PORT='8001'
$env:COLLECTOR_METRICS_PUBLISHED_PORT='9001'
docker compose -p fastapi-dev up -d --build
Invoke-RestMethod http://localhost:8001/health
Invoke-RestMethod http://localhost:8001/prices/latest
```

Compose runs an explicit migration service, an API-only `app`, and a separate `collector`.
The database volume belongs to `fastapi-dev`. Provider overrides target the collector.
Read [process separation and safe operations](docs/fastapi-process-separation.md) before
using any older deployment instructions.

For native development, install Python 3.12+ and uv. Prepare a migrated disposable database
using the test Compose file described in that guide. Root destination startup requires head `0010`;
neither process creates tables or runs migrations. Set `DATABASE_URL` in each terminal:

```powershell
uv sync --frozen --extra databento
$env:DATABASE_URL='postgresql://fastapi_test:disposable_test@127.0.0.1:55482/fastapi_batch1_test'
```

Collector-only terminal:

```powershell
$env:APPLICATION_ROLE='collector'
$env:COLLECTOR_METRICS_PORT='9001'
uv run python -m marketdata.collector
```

API-only terminal (can serve persisted history while the collector is stopped):

```powershell
$env:APPLICATION_ROLE='api'
uv run uvicorn marketdata.api:create_app --factory --workers 2 --port 8001 --ws websockets --ws-max-size 65536 --ws-max-queue 16
```

Use both terminals for independent operation. Ctrl+C gracefully stops only that service.
API startup never constructs providers or registers instruments. Its read-only connection
uses persistent source metadata; provider credentials and a local registry file are not needed.
API `/health` tests database readiness and reports ingestion/feed health separately.

API-only legacy `/ws/prices` still closes with 1013. Use `/api/v1/ws/marketdata` for the
committed cross-process contract and configure its Origin allow-list. For legacy local
WebSocket development, use `APPLICATION_ROLE=combined` with
`marketdata.api:create_combined_app`, one worker, and no other collector on that database.
See the guide for full commands, validation, migration behavior and exact rollback steps.
Omit the optional `databento` extra when using other adapters. Live providers require explicit
activation and never fall back to mock.

## Development and validation

```powershell
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/smoke.py --base-url http://localhost:8001
```

The deterministic suite needs no live markets or credentials. Database integration tests
require `TEST_DATABASE_URL` pointing to a migrated disposable database whose name ends in
`_test`; fixtures clear its market-data tables. The smoke script expects a running default
mock API at localhost:8001; add `--combined` to verify local WebSocket fanout.
Database setup, migration checks and load testing are documented
in [validation and load testing](docs/platform-guide.md#validation-and-load-testing).
[GitHub Actions](https://github.com/Alpha-lacrim/tradingview-marketdata/actions) runs lint,
formatting, strict typing, migrations, tests and downgrade/re-upgrade in both database modes.

## Documentation

| Guide | Contents |
|---|---|
| [Independent processes](docs/fastapi-process-separation.md) | Collector/API roles, health, WebSocket limits, safe commands and rollback |
| [Platform reference](docs/platform-guide.md) | Architecture, provider details, configuration, identity, database semantics, APIs and troubleshooting |
| [TradingView guest operations](docs/tradingview.md) | Experimental protocol, activation, status gates, coverage, recovery and maintenance |
| [Native candle verification](docs/native-candle-verification.md) | Original 17-instrument verification and storage measurements |
| [Candle hardening](docs/candle-hardening-verification.md) | Reliability checks and retained failure evidence |
| [Retrieval scalability](docs/candle-retrieval-scalability.md) | Earlier live/synthetic measurements and maximum-history limits |
| [Connection pool](docs/candle-connection-pool.md) | Bounded worker model, local safeguards and synthetic experiments |
| [Live PostgreSQL experiments](docs/candle-live-end-to-end.md) | Actual guest retrieval, durable commits, omissions and resource measurements |
| [Repository migration](docs/repository-layout.md) | History preservation, existing checkout, PyCharm and Compose compatibility |
| [Current handoff](HANDOFF.md) | Latest implementation decisions, verification and next work |
