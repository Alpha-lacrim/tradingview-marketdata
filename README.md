# Multi-asset market data

An asynchronous Python platform for ingesting market data into PostgreSQL/TimescaleDB
and serving latest prices, historical ticks, OHLCV candles, provider health and WebSocket
subscriptions through FastAPI.

**The default configuration uses simulated data only.** It runs offline with 30 instruments:
crypto, FX, gold/silver spot, WTI/Brent CFDs and synthetic concrete futures contracts.
Live adapters require explicit activation; their implementation does not establish live
availability, exchange entitlement or redistribution rights.

This GitHub repository is **public** (verified 2026-10-12). Credentials, local overrides,
database volumes/dumps, logs and raw generated artifacts are excluded from Git. Sanitized
benchmark evidence and parser fixtures are included; recorded database history remains local.

## Current capabilities and limits

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
See [live end-to-end evidence](docs/candle-live-end-to-end.md),
[earlier live/synthetic retrieval results](docs/candle-retrieval-scalability.md) and
[guest limitations and activation](docs/tradingview.md).

Instrument identity distinguishes spot, OTC, CFDs and concrete exchange futures. Prices use
Decimal/PostgreSQL NUMERIC, and source and receipt timestamps remain distinct and UTC-aware.
Physical WTI/Brent spot pricing is not implemented. Multiple application workers/replicas,
crash-safe queue spooling and automatic tick replay/backfill remain outside this version.

## Repository layout

Application files live directly at the Git repository root. Run commands below from this root.

```text
.
├── src/marketdata/           # Domain, providers, ingestion, persistence and FastAPI
├── tests/                   # Deterministic tests, fixtures and database integration tests
├── scripts/                 # Smoke checks, reconciliation and opt-in benchmarks
├── config/                  # Non-secret instrument registries
├── migrations/              # Alembic revisions 0001–0004
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

Clone the repository and enter it:

```text
git clone https://github.com/Alpha-lacrim/tradingview-marketdata.git
cd tradingview-marketdata
```

Use Docker Compose v2.24+ with a running Linux Docker engine. The following PowerShell
commands start the default mock deployment:

```powershell
Copy-Item .env.example .env
docker compose up -d --build
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/prices/latest
```

The migration service runs before the app. Open [interactive API docs](http://localhost:8000/docs).
Ports bind to localhost; the app runs as an unprivileged user with one Uvicorn worker.
`docker compose down` stops services and retains the named database volume. Replace the
public development defaults in [`.env.example`](.env.example) for deployment.

For native development, install Python 3.12+ and uv:

```powershell
uv sync --frozen --extra databento
docker compose up -d db
uv run alembic -x timescale=true upgrade head
uv run uvicorn marketdata.api:create_app --factory --workers 1 --ws-max-size 16384
```

Omit the optional `databento` extra when using other adapters. Ordinary PostgreSQL uses
`uv run alembic upgrade head` without the Timescale flag. Startup requires schema head
`0004`; it never creates tables. Run either the native app or container app on port 8000.
Configure live providers through [the platform guide](docs/platform-guide.md#configuration-and-live-activation)
or [guest operations](docs/tradingview.md); live feeds never fall back to mock.

## Development and validation

```powershell
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/smoke.py
```

The deterministic suite needs no live markets or credentials. Database integration tests
require `TEST_DATABASE_URL` pointing to a migrated disposable database whose name ends in
`_test`; fixtures clear its market-data tables. The smoke script expects a running default
mock app at localhost:8000. Database setup, migration checks and load testing are documented
in [validation and load testing](docs/platform-guide.md#validation-and-load-testing).
[GitHub Actions](https://github.com/Alpha-lacrim/tradingview-marketdata/actions) runs lint,
formatting, strict typing, migrations, tests and downgrade/re-upgrade in both database modes.

## Documentation

| Guide | Contents |
|---|---|
| [Platform reference](docs/platform-guide.md) | Architecture, provider details, configuration, identity, database semantics, APIs and troubleshooting |
| [TradingView guest operations](docs/tradingview.md) | Experimental protocol, activation, status gates, coverage, recovery and maintenance |
| [Native candle verification](docs/native-candle-verification.md) | Original 17-instrument verification and storage measurements |
| [Candle hardening](docs/candle-hardening-verification.md) | Reliability checks and retained failure evidence |
| [Retrieval scalability](docs/candle-retrieval-scalability.md) | Earlier live/synthetic measurements and maximum-history limits |
| [Connection pool](docs/candle-connection-pool.md) | Bounded worker model, local safeguards and synthetic experiments |
| [Live PostgreSQL experiments](docs/candle-live-end-to-end.md) | Actual guest retrieval, durable commits, omissions and resource measurements |
| [Repository migration](docs/repository-layout.md) | History preservation, existing checkout, PyCharm and Compose compatibility |
| [Current handoff](HANDOFF.md) | Latest implementation decisions, verification and next work |
