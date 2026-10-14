# Developer integration handoff — 2026-10-14

**Verdict: READY WITH LIMITATIONS.** Developers can integrate the persisted REST and
WebSocket contracts locally. This is not approval to deploy or a claim of current live
TradingView connectivity, complete source coverage, or sustained production capacity.

## Local setup (PowerShell)

For an **API-only handover**, use the [standalone setup](standalone-api.md): minimal API
image, explicit migrations, SELECT-only credentials and native candle/quote sample data,
without any collector or upstream connections. It is the recommended company developer setup.
The following older full-platform setup is useful when developing the mock collector too.

Prerequisites: Python 3.12+, uv, Docker Compose; Node.js for the browser regression checks.
Run from the repository root. Use unused loopback ports and an independent project.
Do not overwrite an existing `.env` or point these commands at a deployed database.

```powershell
uv sync --frozen # add --extra databento only if developing that adapter
Copy-Item .env.example .env # only on a fresh checkout without a local .env
$env:POSTGRES_PORT='55484'
$env:API_PORT='8004'
$env:COLLECTOR_METRICS_PUBLISHED_PORT='9004'
```

In the copied `.env`, set the following values (Compose reads that file; unrelated host
environment values are not automatically forwarded into containers):

```dotenv
API_ENVIRONMENT=development
APPLICATION_ROLE=api
API_KEY=local-development-only-token-32-characters
WEBSOCKET_ALLOWED_ORIGINS=["http://localhost:8004"]
```

Then run `docker compose -p marketdata-handoff-local up -d --build`.

Compose runs migrations through **0006**, then separate API and mock collector processes.
The default registry contains 30 simulated instruments. No provider credentials are needed.
For host execution instead, start only `db`, set `DATABASE_URL` to that project's loopback
database, run `uv run alembic -x timescale=true upgrade head`, then run these in separate
terminals with the same DSN/configuration:

```powershell
# API terminal
$env:APPLICATION_ROLE='api'
uv run uvicorn marketdata.api:create_app --factory --port 8004 --workers 2 --ws websockets --ws-max-size 65536 --ws-max-queue 16
# Collector terminal
$env:APPLICATION_ROLE='collector'
uv run python -m marketdata.collector
```

API startup does not register sources or ingest data. An empty API-only database is valid;
run the mock collector to populate it. Mock mode generates quote events and legacy tick-derived
candles; **v1 native candle history is empty** unless native candles have been persisted.
Completed/corrected native candle delivery is independently covered by simulated DB/TCP tests.
Swagger `/docs`, ReDoc `/redoc`, and `/openapi.json` are public contracts. Swagger's
Authorize button accepts the bearer token. Development may omit `API_KEY`; production must
explicitly select `API_ENVIRONMENT=production`, use a strong secret and SELECT-only API DSN.

## REST requests

```powershell
$base='http://localhost:8004'
$env:API_KEY='local-development-only-token-32-characters' # matches local .env only
$headers=@{Authorization="Bearer $env:API_KEY"}
Invoke-RestMethod "$base/health"
Invoke-RestMethod "$base/api/v1/health" -Headers $headers
$sources=Invoke-RestMethod "$base/api/v1/instruments?limit=100" -Headers $headers
$source=$sources.items[0] # retain exact instrument_id/provider/provider_symbol
$symbol=[uri]::EscapeDataString($source.canonical_symbol)
$provider=[uri]::EscapeDataString($source.provider)
$providerSymbol=[uri]::EscapeDataString($source.provider_symbol)
Invoke-RestMethod "$base/api/v1/prices/latest?symbols=$symbol" -Headers $headers
# Defaults to the preceding aligned UTC day; known source without native bars returns []:
Invoke-RestMethod "$base/api/v1/candles?symbol=$symbol&provider=$provider&provider_symbol=$providerSymbol&interval=1m&limit=100" -Headers $headers
Invoke-RestMethod "$base/api/v1/coverage?symbols=$symbol" -Headers $headers
```

Follow `next_after` as `after` for source/bulk pages; `next_cursor` as `cursor` for candle
history, retaining the source/interval/window. Dates require offsets, aligned UTC boundaries
and a positive range of at most 31 days by default. Native history is completed 1m; 5m/15m/1h/1d
are UTC rollups, with missing minutes explicitly incomplete. Prices and volume are decimal
strings; unknown values are null. Sources, brokers, CFDs and concrete futures remain distinct.

## Subscribe and recover

This server-side Python example uses the local service bearer and explicit approved Origin:

```python
import asyncio, json, os
import httpx
from websockets.asyncio.client import connect


async def main():
    headers = {"Authorization": "Bearer " + os.environ["API_KEY"]}
    async with httpx.AsyncClient(base_url="http://localhost:8004", headers=headers) as client:
        sources = (await client.get("/api/v1/instruments")).raise_for_status().json()
        symbol = sources["items"][0]["canonical_symbol"]
        checkpoint = (
            (await client.get("/api/v1/marketdata/changes")).raise_for_status().json()["cursor"]
        )
        async with connect(
            "ws://localhost:8004/api/v1/ws/marketdata",
            origin="http://localhost:8004",
            additional_headers=headers,
        ) as ws:
            await ws.send(json.dumps({"action": "subscribe", "symbols": [symbol]}))
            for _ in range(5):
                message = json.loads(await asyncio.wait_for(ws.recv(), 30))
                print(message)  # demonstration only; see recovery procedure below
        page = (
            (await client.get("/api/v1/marketdata/changes", params={"after": checkpoint}))
            .raise_for_status()
            .json()
        )
        print(page)


asyncio.run(main())
```

For a real client, save the last applied cursor. On reconnect restore subscriptions, take
the acknowledgement's `snapshot_cursor` barrier, buffer live events within a bound, and page
`/marketdata/changes?after=<saved>&until=<barrier>` until `has_more=false`, using each page's
`cursor`. Then drain buffered events and advance the checkpoint. Replay includes all sources;
filter selected canonical/source identities yourself. Apply old recovered minutes/corrections,
upsert by source/interval/bucket, and avoid overwriting newer receipt versions. A 409 requires
refreshing the whole loaded REST chart window, prices and coverage. Retention is event-count
bounded, not permanent replay. Disconnect/1013 requires recovery; delivery can duplicate.

The [browser example](../examples/browser-marketdata.js) implements this sequence, retry,
watchdog and bounded buffering. Native browser WebSockets cannot set bearer headers. Keep
the permanent token in the company backend; wire the [provisional relay](../examples/company_bff.py)
to the company's real session authorizer and an authorized session-check endpoint. No login
system is supplied. Exact Origin allow-lists are required for WS even in local development;
CORS is a separate REST browser policy. Final browser authentication is a company dependency.

## Semantics and outstanding dependencies

- `price_type=quote` describes a persisted quote; only `realtime_quote_available=true`
  establishes a fresh observation classified realtime. Simulated/delayed/unknown stays explicit.
- `price_type=candle_close` is a completed historical close, even when fresh or its source
  status is realtime. Candle WS events update charts; they are not quote events.
- TradingView chooses `candles` or `ticks` storage mode today. Simultaneous candles/quotes
  can remain a **separate collector milestone** without changing the API handoff. It must be
  completed and qualified before promising continuous TradingView quotes alongside native
  candles. Do not start a second scheduler against the deployed legacy collector.
- Production still needs reviewed deployment/migration and legacy collector replacement,
  separate principals/secrets, operator hostname/TLS/Origins, real company session integration,
  live source smoke and sustained target-host/source-coverage qualification. Existing commercial
  access approval does not establish current feed health or every source's entitlements.
- Only supported journal-aware Store writes produce replay events. Old binaries/direct SQL
  do not. API health/readiness is independent of collector health and data freshness.

Full contracts: [REST/WS reference](api.md), [frontend integration](frontend-integration.md),
[WS JSON schema](ws-marketdata.schema.json). Production procedures:
[deployment](production-deployment.md), [collector replacement](collector-fastapi-integration.md).

## Audit and verification

The audit covered REST routing/response models/OpenAPI authentication, WS commands/envelopes
and committed replay, source selection, settings, migrations, frontend examples and local
instructions. No API redesign, infrastructure, migration or provider activation was needed.
Fixed the obsolete API-only assertion in `scripts/smoke.py` and browser recovery snapshot
ordering, UTC fractional receipt ordering and terminal configuration-error retries. Browser
regressions exercise those failures without network/provider dependencies. Current test
results and exact unverified checks are recorded in [HANDOFF.md](../HANDOFF.md).
