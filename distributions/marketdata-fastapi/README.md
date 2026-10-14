# Standalone Market Data FastAPI

**Ready for developer handover.** Copy this entire directory anywhere and run it without
the original repository. Python 3.12+, uv and Docker Compose are the local prerequisites;
Node.js is optional for the browser regression. PostgreSQL/TimescaleDB is the only runtime
service. No scraper process, TradingView credentials or upstream connections are required.

REST/OpenAPI, completed native 1m candles and derived intervals, latest-price freshness,
instrument discovery/coverage, authenticated WebSockets, replay, health and metrics reuse
the verified implementation. Schema head is **0006**. See [API contracts](docs/api.md),
[OpenAPI snapshot](docs/openapi.json), [WS schema](docs/ws-marketdata.schema.json),
[frontend integration](docs/frontend-integration.md) and [producer integration](docs/integration.md).

## Go dashboard compatibility

`GET /api/v1/dashboard/candles` returns exactly `a`–`h`: Unix seconds, Open, Close, High,
Low, Count, Volume, Value. Arrays always align; empty history returns eight empty arrays.
The existing REST and WebSocket contracts stay unchanged. All five native/derived
intervals use the existing persisted candle queries, bounds, authentication and read-only pool.

After the disposable migration/seed steps below:

```sh
curl -H "Authorization: Bearer sample-development-token-32-characters" "http://localhost:8015/api/v1/dashboard/candles?symbol=DEMO%3ABTCUSD&provider=sample&interval=1m&limit=2"
```

Illustrative simulated response (actual seed timestamps/prices vary):

```json
{"a":[1791763200,1791763260],"b":[10.0,11.0],"c":[11.0,12.0],"d":[12.0,13.0],"e":[9.0,10.0],"f":[0,0],"g":[0.0,0.0],"h":[0.0,0.0]}
```

Count and Value have no defined storage mapping in schema 0006, so use the explicitly
approved zeros. Stored volume is used when available; missing volume becomes 0.0.
**Zeros can mean unavailable, not necessarily zero activity.** Count is never the number
of candles; Value is never volume or price × volume. JSON float64 conversion occurs only
at this response boundary and can lose decimal precision. The original API keeps exact
decimal strings and source/status/completeness metadata.
Use `X-Marketdata-Next-Cursor` to page with `cursor`. Read the
[full contract and reserved future metric integration](docs/api.md#go-dashboard-candle-compatibility)
before assigning units to source volume or integrating Count/Value.

## Database compatibility

**Actual isolated tests:** plain PostgreSQL **16.1** and **17.11**, plus optional
TimescaleDB **2.25.1 / PostgreSQL 17.7**: **143 tests passed on each**.
PostgreSQL 16.1 was tested with no Timescale extension. Migrations to 0006, migration
roundtrip, read-only pooling, native/derived candles, and committed-event WebSocket/replay
were exercised. See [verification evidence and limits](docs/verification.md).
Other exact PostgreSQL/Timescale versions and the company target host have not been
qualified in this task. TimescaleDB is never required by plain PostgreSQL API startup.

## Disposable Docker setup

Run from **this directory**, using unused loopback ports. Default API: **8015**, DB: **55915**.
Compose does not forward a repository `.env` into containers. `STANDALONE_API_PORT`,
`STANDALONE_DB_PORT` and `STANDALONE_API_KEY` can override demo values through interpolation.
Plain PostgreSQL 16.1 is the default disposable image; TimescaleDB is optional and requires
its separate override file (currently TimescaleDB 2.25.1 on PostgreSQL 17).
No existing volume/database is used. The database is tmpfs: stopping it discards demo data.
Public sample passwords/token are disposable fixtures, unsuitable for deployment.

```powershell
docker compose -p marketdata-fastapi-demo --profile tools build
docker compose -p marketdata-fastapi-demo up -d --wait db
docker compose -p marketdata-fastapi-demo run --rm migrate
docker compose -p marketdata-fastapi-demo run --rm seed
docker compose -p marketdata-fastapi-demo up -d --wait api
```

Check each command succeeds before continuing. Migrations and sample provisioning are
explicit commands, never API startup actions. The API uses its own SELECT-only `sample_api`
principal; migration/sample commands use `sample_owner`. Seed provisions the reader only in
the disposable `_test` database and inserts simulated candles, quotes, coverage and events.
The API remains operational after the writer exits. Open `http://localhost:8015/docs` and
Authorize with `sample-development-token-32-characters`; `/redoc` and `/openapi.json` are public.

Seed creates 120 completed 1m bars per `DEMO:BTCUSD` and `DEMO:EURUSD` source (`sample` provider).
All data is simulated; volume stays null. Use `--minutes 1440` for previous UTC-day daily buckets.
Missing base minutes remain visible as incomplete rollups. Simulated quotes can be fresh but
never have `realtime_quote_available=true`. No fake provider/collector heartbeat is published.

```powershell
$base='http://localhost:8015'
$headers=@{Authorization='Bearer sample-development-token-32-characters'}
Invoke-RestMethod "$base/api/v1/instruments" -Headers $headers
Invoke-RestMethod "$base/api/v1/prices/latest?symbols=DEMO%3ABTCUSD" -Headers $headers
Invoke-RestMethod "$base/api/v1/candles?symbol=DEMO%3ABTCUSD&provider=sample&interval=1m" -Headers $headers
Invoke-RestMethod "$base/api/v1/coverage?symbols=DEMO%3ABTCUSD" -Headers $headers
```

For a runnable REST/WS snapshot/replay client:

```powershell
uv sync --frozen
uv run python examples/rest_websocket.py
# Wait for committed live quote events instead:
uv run python examples/rest_websocket.py --updates 2
```

While the live client is subscribed, run in a second terminal from this directory:

```powershell
docker compose -p marketdata-fastapi-demo run --rm seed .venv/bin/python -m marketdata.sample_data --events 30 --interval-seconds 2
```

This bounded external sample writer exits automatically. The browser example implements full
reconnect/barrier/replay recovery; the small Python example demonstrates the protocol only.
Use `/api/v1/ws/marketdata`. Legacy `/ws/prices` retains its API-only 1013 behavior.
Cleanup only the named demo with `docker compose -p marketdata-fastapi-demo down`; no volume
operation is needed. After a DB stop, repeat migration/seed before restarting the API.

## Host install and independent image

For runtime-only installation into a fresh environment: `uv sync --frozen --no-dev`, then
run `uv run --no-sync uvicorn marketdata.api:create_app --factory --port 8015 --workers 2
--ws websockets --ws-max-size 65536 --ws-max-queue 16` as one command. Supply a migrated
database, a separate SELECT-only `DATABASE_URL`, `API_KEY` and exact allowed Origins.
`.env.example` describes deployment configuration; copy it only in a fresh checkout and replace
placeholders. No local provider registry is needed by API startup. Plain `pip install .`
or `pip install <built-wheel>` also works; `uv build` builds a self-contained wheel/sdist.

`docker build -t marketdata-fastapi .` builds only the API runtime. The separate `tools`
target includes migration dependencies and files. Runtime dependencies omit httpx, Databento,
Alembic, SQLAlchemy and psycopg. Dev includes httpx for examples/tests; administrative installs
use `uv sync --frozen --no-dev --extra migrations`. Connect remotely to the operator’s independently prepared
market-data database, separate from the company application database; the tmpfs Compose setup is only a developer demo. See [integration](docs/integration.md)
for scoped grants, mounted secrets, external writer transactions and production configuration.

## Tests from this project only

Use a **separate disposable project/database** from your demo. Fixtures truncate its market
tables, and migration tests downgrade/re-upgrade schema 0006. Never use a company DB.

```powershell
uv sync --frozen
$env:STANDALONE_DB_PORT='55916'
docker compose -p marketdata-fastapi-tests up -d --wait db
docker compose -p marketdata-fastapi-tests run --rm migrate
$env:TEST_DATABASE_URL='postgresql://sample_owner:sample_disposable@127.0.0.1:55916/marketdata_standalone_test'
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run mypy src examples/company_bff.py
docker compose -p marketdata-fastapi-tests down
```

For TimescaleDB, use a fresh project and append `-f compose.yaml -f compose.timescale.yaml`
to every Docker Compose command before its first migration. It uses identical tests and
the optional Timescale migration flag. Run `uv run pytest -q -m 'not integration'` for
DB-independent checks; integration cases skip when `TEST_DATABASE_URL` is absent.
[Verification](docs/verification.md) records the actual isolated-copy results and limitations.

## Rebuild a clean handoff ZIP

Run from this project with new output/extraction locations:

```powershell
uv run python scripts/package_distribution.py --output ../marketdata-fastapi-v1.0.zip --extract-to ../marketdata-fastapi-verify
```

The helper excludes secrets/data/environments/caches, verifies all archive bytes and writes
`marketdata-fastapi-v1.0.zip.sha256`. The extraction target must not already exist.
Public demo credentials and `.env.example` placeholders remain available for disposable use.

## Source ownership and future repository

This is a regular directory, with no symlinks, parent installs, path dependencies or Git
submodules. All required code, migrations, fixtures, docs, examples and tests are local.
`SOURCE_MANIFEST.json` records the verified parent source hashes and small test/doc adaptations.
API/domain/persistence/migration source is copied unchanged with LF line endings;
manifest hashes normalize CRLF/LF for future Git checkouts. Concrete upstream transports and
the collector entrypoint are excluded. Generic compatibility/type definitions remain dormant.
Only the API factory is a supported application mode.

Until a separate repository is explicitly authorized, the parent remains the implementation
source of truth. Its `scripts/export_fastapi_distribution.py` refreshes managed snapshot files
and `--check` verifies parity; it refuses to overwrite manual distribution edits. The exporter
is not required to install/run/test this distribution. After an approved repository split,
coordinate API/schema/writer versions and choose one implementation owner rather than
maintaining two divergent copies. Do not publish a repository or deploy from this handoff
without authorization.

Remaining company work: real browser/session authorization for the provisional BFF, production
roles/secrets, TLS/Origins and target-host qualification. Genuine live prices require a qualified
external producer. Synthetic tests do not prove live feed availability or sustained capacity.
