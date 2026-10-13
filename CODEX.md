# CODEX.md

## Purpose

This file defines the engineering operating procedure for AI coding agents working in this repository.

`AGENTS.md` defines repository-wide policy and invariants.

`CODEX.md` defines how engineering work should be carried out.

`HANDOFF.md` records the current implementation state and what the next coding session needs to know.

Before substantive work, read:

1. all applicable `AGENTS.md` and `AGENTS.override.md` files;
2. this `CODEX.md`;
3. `HANDOFF.md`;
4. the relevant source files, tests, configuration and migrations;
5. the current Git status.

Do not begin implementation based only on the user prompt without inspecting the repository.

---

# 1. Working method

For each engineering task:

1. Read the applicable repository instructions.
2. Read `HANDOFF.md`.
3. Inspect Git status.
4. Locate the relevant code, tests and configuration.
5. Understand existing behaviour before editing it.
6. Identify the smallest coherent solution.
7. Implement the solution.
8. Add or update tests.
9. Run targeted validation.
10. Run broader validation appropriate to the scope.
11. Inspect the final diff.
12. Update documentation when behaviour or architecture changed.
13. Update `HANDOFF.md`.

Do not spend excessive time writing speculative plans when repository inspection can answer the question.

Do not repeatedly ask the user about reversible implementation details.

For ordinary engineering choices, use sound judgement and proceed.

Pause only when a decision:

- materially changes the product;
- has significant external cost;
- creates irreversible state;
- requires credentials or permissions that are unavailable;
- cannot reasonably be inferred from the repository or stated requirements.

---

# 2. Source-of-truth hierarchy

For engineering behaviour, use this order:

1. applicable `AGENTS.override.md`;
2. nearest applicable `AGENTS.md`;
3. repository-root `AGENTS.md`;
4. `CODEX.md`;
5. established project architecture and conventions;
6. `HANDOFF.md` for current state only.

`HANDOFF.md` describes what currently exists.

It does not override engineering policy.

For provider-specific behaviour, current official provider or exchange documentation is authoritative.

Do not rely on assumptions about market-data APIs when official documentation can be checked.

---

# 3. Project objective

This repository implements a provider-agnostic real-time market-data platform covering:

- cryptocurrency;
- forex;
- precious metals;
- energy;
- exchange-traded futures.

The platform should:

- connect to legitimate market-data providers;
- normalize provider-specific messages;
- persist market data into PostgreSQL/TimescaleDB;
- maintain latest-price state;
- provide historical queries;
- provide OHLCV/candle data;
- expose provider health;
- optionally expose normalized real-time prices to application clients.

Documented provider adapters are the supported integrations. The operator has explicitly
authorized an experimental TradingView guest quote adapter; its observed protocol and
limitations belong in `docs/tradingview.md`. It must not bypass access controls or infer
real-time status from a changing number alone. No domestic provider is currently selected.

---


# 5. Market-data semantics

Do not treat all financial prices as equivalent.

The system must preserve the distinction between:

- cryptocurrency spot;
- cryptocurrency perpetual futures;
- forex spot/OTC;
- precious-metal spot/reference prices;
- CFDs;
- indices;
- exchange-traded futures;
- synthetic instruments.

Never normalize semantically different products into the same instrument merely because they display similar prices.

Examples that must remain distinct:

- XAU/USD spot versus COMEX Gold futures;
- WTI CFD/reference pricing versus NYMEX CL futures;
- BTC spot versus BTC perpetual futures;
- S&P 500 index versus E-mini S&P 500 futures.

---

# 6. Provider architecture

The application must not be tightly coupled to a single provider.

Providers should be independently selectable for:

- crypto;
- forex;
- metals;
- energy;
- futures.

A provider adapter should conceptually own:

- authentication;
- endpoint selection;
- connection lifecycle;
- subscription management;
- WebSocket or streaming transport;
- provider message parsing;
- heartbeat handling;
- provider symbol mapping;
- provider-specific timestamps;
- sequence handling;
- reconnect behaviour;
- resynchronisation behaviour where applicable;
- provider-specific error handling.

Provider adapters should emit normalized domain objects.

They should not contain application API logic or unrelated persistence logic.

A common provider abstraction should expose behaviour conceptually similar to:

```python
connect()
disconnect()
subscribe()
unsubscribe()
stream_ticks()
get_snapshot()
health_check()
```

Use an appropriate abstraction such as `Protocol` or `ABC`.

---

# 7. Provider development procedure

When adding or changing a live provider:

1. Check current official documentation.
2. Verify authentication requirements.
3. Verify endpoint URLs.
4. Verify WebSocket message formats.
5. Verify symbol conventions.
6. Verify subscription formats.
7. Verify heartbeat requirements.
8. Verify sequence semantics if present.
9. Verify reconnect/resynchronisation behaviour.
10. Verify whether data is:
   - realtime;
   - delayed;
   - indicative;
   - end-of-day.
11. Verify relevant exchange entitlement requirements.
12. Add configuration.
13. Add symbol mappings.
14. Add parser fixtures.
15. Add provider tests.
16. Add reconnect/error-path tests.
17. Update provider documentation.
18. Update `HANDOFF.md`.

Never fabricate an endpoint, symbol or provider capability.

For the explicitly authorized undocumented guest adapter, verify assumptions against the
site's own current served JavaScript and captured responses. Mark it experimental, retain
protocol evidence and reject unverified/delayed delivery; do not treat fixtures as proof of
current live access or permission to reuse licensed data.

If something cannot be verified, mark it explicitly as unverified.

---

# 8. Real-time classification

The system should explicitly distinguish data status where known:

- `realtime`
- `delayed`
- `indicative`
- `simulated`
- `end_of_day`
- `unknown`

Never label delayed data as real-time.

If genuine real-time futures data requires exchange entitlements, document this clearly.

Examples may include:

- CME;
- COMEX;
- NYMEX;
- ICE.

Never attempt to bypass exchange licensing, provider subscriptions or entitlement checks.

---

# 9. Instrument identity

Use a proper instrument registry.

Do not rely on display symbols as primary identity.

A canonical instrument should support fields such as:

- internal ID;
- canonical symbol;
- display name;
- asset class;
- instrument type;
- base asset;
- quote asset;
- exchange;
- currency;
- root symbol;
- contract code;
- contract month;
- expiry;
- tick size;
- multiplier;
- active status;
- market-data status;
- metadata.

Provider mappings should preserve:

- provider;
- provider symbol;
- exchange or venue;
- provider metadata.

Prefer separating canonical instrument identity from provider-specific mappings when appropriate.

Possible canonical naming conventions include:

```text
CRYPTO:BTCUSDT
CRYPTO:ETHUSDT
FX:EURUSD
FX:GBPUSD
SPOT:XAUUSD
SPOT:XAGUSD
FUTURES:GC:<CONTRACT>
FUTURES:SI:<CONTRACT>
FUTURES:CL:<CONTRACT>
FUTURES:ES:<CONTRACT>
FUTURES:NQ:<CONTRACT>
```

The exact convention may evolve, but it must remain deterministic and documented.

---

# 10. Futures

Futures correctness is a first-class requirement.

A futures root such as:

```text
GC
SI
CL
ES
NQ
```

is not an individual tradable contract.

Concrete futures contracts must have independent identities.

Store relevant information including:

- root symbol;
- provider contract symbol;
- exchange;
- contract month;
- expiry;
- multiplier;
- tick size.

Support concepts such as:

- active contracts;
- front month;
- next contract;
- rollover.

Front-month is a dynamic mapping, not a permanent instrument identity.

Do not write front-month ticks directly under an ambiguous permanent root symbol.

Persist data under the concrete contract.

A logical alias such as:

```text
FUTURES:CL:FRONT
```

may resolve to a concrete contract, but historical ticks must remain associated with their real contract.

Do not assume rollover happens on expiration day.

Where possible, derive contract metadata from authoritative exchange/provider data rather than hard-coded assumptions.

Rollover logic must be deterministic and tested.

---

# 11. Normalized tick model

Provider messages should normalize into a common internal tick representation.

Relevant fields may include:

- instrument ID;
- canonical symbol;
- provider symbol;
- asset class;
- instrument type;
- provider;
- exchange;
- event timestamp;
- received timestamp;
- bid;
- ask;
- last;
- bid size;
- ask size;
- last size;
- volume;
- sequence number;
- provider metadata.

Not every provider supplies every field.

Unavailable values should remain null rather than being fabricated.

---

# 12. Financial numeric handling

Do not use binary floating-point for persisted financial prices or quantities where exact decimal representation matters.

Use an appropriate decimal-safe pipeline, such as:

- Python `Decimal`;
- PostgreSQL `NUMERIC`.

Do not silently round provider values unless a documented schema constraint requires it.

Do not convert values to float merely for convenience before persistence.

---

# 13. Timestamp handling

Internally use UTC.

All persisted timestamps must be timezone-aware.

Distinguish among:

## Event timestamp

Timestamp assigned by the market-data source or exchange.

## Received timestamp

Timestamp when the local ingestion service received the event.

## Persistence timestamp

Timestamp when the database wrote the record, if this is separately useful.

Do not conflate these timestamps.

Do not store naive datetimes.

Do not rewrite provider event timestamps simply to make events appear ordered.

---

# 14. Ingestion architecture

The preferred ingestion path is:

```text
Provider stream
    |
    v
Provider parser
    |
    v
Normalizer
    |
    v
Bounded async queue
    |
    +----> Real-time subscriber fan-out
    |
    v
Batch persistence
    |
    v
PostgreSQL / TimescaleDB
```

The provider receive loop must not block on ordinary database latency.

Use bounded queues.

Do not use unbounded queues.

Backpressure behaviour must be explicit.

If data must eventually be dropped because all bounded recovery capacity is exhausted:

- log the event;
- increment an observable counter;
- identify the provider;
- identify the affected stream if practical;
- never silently pretend persistence succeeded.

---

# 15. Database persistence

Market-data ingestion is write-heavy.

Do not execute one transaction per incoming tick.

Use batching.

Prefer an efficient PostgreSQL insertion mechanism appropriate to the existing stack, such as:

- PostgreSQL `COPY`;
- batched `asyncpg`;
- carefully batched SQLAlchemy inserts.

Choose based on measured needs and repository conventions.

Maintain a database connection pool.

Temporary database latency must not stop provider WebSocket reading.

---

# 16. Database model

The system should normally provide at least:

## `instruments`

Canonical instrument metadata.

## `provider_instruments`

Provider-specific mappings where useful.

## `market_ticks`

Normalized market observations.

## `latest_prices`

Efficient current state.

Latest-price lookup must not require scanning tick history.

## OHLCV / candles

Support useful intervals such as:

- 1 second;
- 1 minute;
- 5 minutes;
- 15 minutes;
- 1 hour;
- 1 day.

If TimescaleDB continuous aggregates solve this cleanly, prefer them over unnecessary duplicate application-managed tables.

---

# 17. TimescaleDB

Use TimescaleDB when it materially improves the system.

If used, consider:

- hypertables;
- sensible time partitioning;
- compression;
- retention policies;
- continuous aggregates.

Do not introduce complicated TimescaleDB features without a clear operational benefit.

Keep ordinary PostgreSQL compatibility in mind where practical.

---

# 18. Latest-price state

Latest-price queries should be efficient.

Maintain a dedicated latest-state representation using a safe method such as:

- PostgreSQL UPSERT;
- a well-defined current-state table;
- another documented mechanism.

Do not derive every latest-price request by sorting the entire tick table.

Preserve relevant state such as:

- bid;
- ask;
- last;
- timestamp;
- provider;
- realtime status.

---

# 19. Price semantics

Do not expose a generic `price` field without defining what it means.

Preserve when available:

- bid;
- ask;
- last;
- mid.

Only calculate:

```text
mid = (bid + ask) / 2
```

when both bid and ask exist.

Do not silently substitute `last` for `mid`.

If fallback behaviour is implemented, document it explicitly.

---

# 20. Sequence handling

Where the provider supplies sequence numbers:

- retain them where useful;
- detect gaps;
- log sequence gaps;
- follow documented provider recovery procedures.

Do not fabricate sequence numbers.

Do not deduplicate ticks simply because consecutive price values are identical.

Only perform deterministic deduplication when provider semantics support it.

---

# 21. Reliability

Long-running provider services should handle:

- network failures;
- WebSocket disconnects;
- stale connections;
- heartbeat failures;
- provider rate limits;
- authentication failures;
- entitlement failures;
- malformed messages;
- temporary database failures;
- graceful shutdown.

Use:

- exponential backoff;
- jitter;
- connection timeouts;
- ping/pong or provider heartbeat mechanisms;
- automatic resubscription after reconnect.

Do not endlessly retry permanent invalid configuration or invalid credentials as though they were transient failures.

One failing provider should not unnecessarily terminate unrelated provider streams.

---

# 22. Async task lifecycle

Background tasks must have explicit ownership.

Do not create detached asynchronous tasks whose exceptions disappear silently.

Cancellation should propagate cleanly.

Shutdown should:

1. stop accepting new work;
2. stop provider subscriptions safely;
3. allow reasonable queued work to flush where appropriate;
4. close database connections;
5. close provider connections;
6. terminate cleanly.

---

# 23. Error classification

Where practical, classify failures into categories such as:

- transient network;
- authentication;
- entitlement;
- provider rate limit;
- malformed payload;
- provider protocol;
- sequence gap;
- database;
- configuration;
- internal invariant.

Retries should target failures that are genuinely retryable.

Logs should include enough context to diagnose the failure without exposing secrets.

---

# 24. Provider health

Track feed/provider status.

Useful states include:

- healthy;
- stale;
- degraded;
- reconnecting;
- disconnected.

Useful fields may include:

- provider;
- connection state;
- last message time;
- last successful message time;
- reconnect count;
- error count;
- last error;
- update timestamp.

Health information should distinguish application health from market-feed health.

---

# 25. REST API

Where the repository exposes an HTTP API, useful endpoints include:

```text
GET /health
GET /providers
GET /instruments
GET /instruments/{symbol}
GET /prices/latest
GET /prices/latest/{symbol}
GET /candles/{symbol}
GET /ticks/{symbol}
```

Historical endpoints must use:

- bounded result sizes;
- sensible pagination;
- validated timestamps;
- validated intervals.

Do not allow accidental unbounded market-tick queries.

Use typed request/response models where appropriate.

---

# 26. Client WebSocket API

Where real-time application clients need updates, expose an application-controlled WebSocket endpoint such as:

```text
/ws/prices
```

A subscription might conceptually look like:

```json
{
  "action": "subscribe",
  "symbols": [
    "CRYPTO:BTCUSDT",
    "FX:EURUSD",
    "SPOT:XAUUSD"
  ]
}
```

A normalized outbound message might conceptually look like:

```json
{
  "type": "tick",
  "symbol": "CRYPTO:BTCUSDT",
  "event_timestamp": "2026-01-01T00:00:00Z",
  "received_timestamp": "2026-01-01T00:00:00Z",
  "bid": "100000.00",
  "ask": "100001.00",
  "last": "100000.50",
  "provider": "example",
  "realtime_status": "realtime"
}
```

Never expose upstream provider credentials or authenticated provider URLs to clients.

---

# 27. Configuration

Use validated typed configuration.

Configuration should support:

- PostgreSQL connection;
- enabled providers;
- provider API credentials;
- provider URLs where configurable;
- enabled asset classes;
- enabled instruments;
- queue capacity;
- batch size;
- batch flush interval;
- reconnect parameters;
- stale-feed thresholds;
- logging level.

Subscriptions should be configurable without editing source code.

Maintain `.env.example` with variable names and safe placeholders only.

---

# 28. Secrets

Never:

- commit API keys;
- print API secrets;
- include credentials in test fixtures;
- expose credentials through API responses;
- store secrets in `HANDOFF.md`;
- disable TLS verification merely to make a connection work.

Use environment variables or an appropriate secret-management mechanism.

---

# 29. Mock provider

Maintain a `MockMarketDataProvider` or equivalent local simulation provider.

It should allow the complete architecture to run without:

- paid credentials;
- live provider access;
- Internet connectivity.

It should support:

- multiple instruments;
- configurable tick frequency;
- bid;
- ask;
- last;
- timestamps;
- plausible market movement.

All mock data must be clearly marked as simulated.

The local development pipeline should support:

```text
Mock feed
→ normalization
→ ingestion
→ database
→ latest state
→ API
→ client WebSocket
```

---

# 30. Testing

Tests should be deterministic wherever possible.

The normal test suite must not require:

- paid credentials;
- live Internet connectivity;
- live exchange sessions.

## Unit tests

Cover relevant behaviour including:

- provider parsing;
- normalization;
- symbol mapping;
- Decimal preservation;
- timestamp conversion;
- configuration validation;
- futures contract parsing;
- futures rollover resolution.

## Persistence integration tests

Cover:

- instrument creation;
- provider mappings;
- tick insertion;
- latest-price UPSERT;
- historical queries;
- duplicate handling.

## Provider behaviour tests

Mock:

- normal messages;
- malformed messages;
- disconnects;
- reconnects;
- sequence gaps where relevant;
- entitlement failures where applicable.

## Regression tests

When fixing a reproducible bug, add a regression test whenever practical.

---

# 31. Live-provider tests

Live-provider tests, if implemented, must be opt-in.

Do not make them part of the normal CI test suite unless the environment is specifically configured for them.

Clearly distinguish:

- parser/fixture tests;
- mocked integration tests;
- live external tests.

---

# 32. Observability

Use structured logs.

Useful context includes:

- provider;
- canonical symbol;
- provider symbol;
- queue depth;
- batch size;
- reconnect attempt;
- error category.

Do not log secrets.

Where appropriate expose lightweight metrics including:

- messages received per second;
- rows persisted per second;
- queue depth;
- batch persistence latency;
- reconnect count;
- parse failure count;
- dropped tick count.

Do not introduce a large monitoring stack unless justified.

---

# 33. Performance

The initial architecture should be capable of supporting:

- hundreds of instruments;
- several thousand incoming updates per second;

on reasonable hardware.

These are design targets, not benchmark claims.

Never claim throughput without measurement.

Provide a reproducible benchmark or load-test mechanism where practical.

Measure:

- incoming message rate;
- persisted row rate;
- queue depth;
- database batch latency;
- dropped messages.

---

# 34. Dependency discipline

Reuse existing dependencies where sensible.

Before adding a production dependency, determine whether:

- the standard library;
- an existing dependency;
- the current framework

already solves the problem adequately.

Do not add Redis, Kafka, RabbitMQ or another infrastructure layer simply because it is common in financial systems.

Introduce new infrastructure only when there is a demonstrated need.

---

# 35. Docker and local development

Prefer a simple local environment.

For a greenfield implementation, Docker Compose should normally include:

- application;
- PostgreSQL or TimescaleDB.

Add additional services only when justified.

Where useful, provide:

- service health checks;
- deterministic startup;
- migration instructions;
- local mock-provider execution.

---

# 36. Database migrations

Schema changes require proper migrations.

Prefer Alembic when compatible with the existing Python stack.

Do not depend on production startup automatically creating or mutating the database schema.

Verify migration behaviour.

Where relevant, test:

- upgrade;
- downgrade;
- fresh database creation.

---

# 37. Code quality

Follow established repository style.

For a greenfield Python implementation, sensible defaults include:

- Python 3.12+;
- asyncio;
- FastAPI;
- Pydantic v2;
- SQLAlchemy 2.x async and/or asyncpg;
- Alembic;
- pytest;
- Ruff;
- mypy or pyright where useful.

These are defaults, not reasons to rewrite a functioning existing stack.

Keep modules cohesive.

Avoid:

- giant classes;
- giant files;
- provider-specific database code;
- database-specific provider code;
- speculative abstractions.

---

# 38. Change discipline

Before editing:

1. understand the relevant code path;
2. inspect existing abstractions;
3. search for callers;
4. inspect related tests;
5. inspect current configuration;
6. inspect Git status.

While editing:

- make cohesive changes;
- avoid unrelated refactors;
- preserve existing behaviour unless intentionally changing it;
- add tests alongside behaviour changes;
- preserve unrelated working-tree changes.

After editing:

1. inspect the diff;
2. run targeted tests;
3. run broader relevant tests;
4. run configured linting;
5. run configured type checking;
6. verify migrations where relevant;
7. perform an appropriate smoke test.

Do not claim a command passed unless it actually ran successfully.

---

# 39. Git discipline

Never discard unrelated user changes.

Do not:

- reset the working tree;
- rewrite repository history;
- force-push;
- amend unrelated commits;

unless explicitly instructed.

Do not create commits unless the user or repository workflow requires them.

Always inspect Git status before and after significant work.

---

# 40. Documentation

Documentation must describe implemented reality.

Do not describe planned providers as operational.

Clearly distinguish:

- implemented and working;
- implemented but awaiting credentials;
- implemented but awaiting exchange entitlement;
- delayed;
- simulated;
- planned.

Update documentation when changing:

- architecture;
- provider support;
- configuration;
- schema;
- APIs;
- futures behaviour;
- operational procedures.

---

# 41. HANDOFF maintenance

`HANDOFF.md` is the live state document for future coding sessions.

Before ending a meaningful coding session:

1. Read the current `HANDOFF.md`.
2. Update it to match the repository's actual current state.
3. Remove or rewrite stale information.
4. Record significant decisions.
5. Record what was actually implemented.
6. Record relevant tests/checks actually run.
7. Record known issues.
8. Record external blockers.
9. Record the next highest-priority actions.

Do not turn `HANDOFF.md` into an append-only diary.

Prefer current truth over chronological history.

Do not paste large source files, logs or diffs into it.

Never include credentials or secrets.

`HANDOFF.md` is project state, not policy.

It must not override `AGENTS.md` or `CODEX.md`.

---

# 42. Completion standard

Do not consider a task complete merely because code was written.

Depending on task scope, verify as many of the following as reasonably apply:

- application starts;
- migrations apply;
- relevant tests pass;
- lint passes;
- type checking passes;
- mock provider operates;
- provider parsing works;
- normalized ticks reach persistence;
- latest-price state updates;
- historical queries work;
- candles work;
- health information is correct;
- application WebSocket subscriptions work;
- documentation reflects actual behaviour;
- `HANDOFF.md` reflects current state.

If some checks cannot be completed, state exactly which ones and why.

---

# 43. End-of-session report

At the end of meaningful work, report concisely:

- what was implemented;
- important files changed;
- architecture decisions made;
- migrations added;
- tests/checks run;
- smoke tests performed;
- live providers currently operational;
- credentials or entitlements still required;
- known limitations;
- next recommended work.

Do not report planned work as completed.

---

# Implemented execution protocol

Source map: `src/marketdata/domain.py` and `registry.py` own identity/normalization;
`providers/` owns documented transports/parsers; `ingestion.py` owns task/queue lifecycle;
`persistence.py` owns PostgreSQL; `api.py` owns REST/WebSocket serving. `README.md` is the
setup/repository overview; `docs/platform-guide.md` documents activation, operational limits,
schema semantics and provider references. `config/` holds
non-secret instrument registries. `migrations/versions/` is the schema source of truth.

Alembic head is `0005`; 0005 adds only a collector health snapshot table. `candles.py` owns
completed native candle validation/scheduling;
`candle_ingestion.py` owns bounded minute collection/recovery through the optional
`providers/candles.py` capability. The experimental guest default stores native closed 1m
candles. Tick ingestion remains available explicitly and is unchanged for other providers.
Session classification belongs in `sessions.py`; 0004 adds tri-state coverage and correction
audit provenance. See `docs/tradingview.md` for polling health, recovery, latency and live checks.

Run development commands from the Git repository root, which now directly contains
`pyproject.toml`, `src/`, `tests/`, `config/` and `migrations/`. Existing home-rooted/nested
checkouts must follow `docs/repository-layout.md` before adopting the relocation commit;
use a separate clone to preserve the original working directory, IDE and database volume.

Install using `uv sync --frozen --extra databento` (omit the extra when not needed).
Local database: `docker compose up -d db`. Migrate explicitly with
`uv run alembic -x timescale=true upgrade head`, or omit `-x timescale=true` for ordinary
PostgreSQL mode. API: `APPLICATION_ROLE=api` and
`uv run uvicorn marketdata.api:create_app --factory --workers 2`.
Collector: `APPLICATION_ROLE=collector` and `uv run python -m marketdata.collector`.
Use `APPLICATION_ROLE=combined` with `marketdata.api:create_combined_app` and one worker only
for development fanout compatibility. The API factory always serves database reads and never
starts ingestion. See docs/fastapi-process-separation.md for isolated configuration, separate
metrics, snapshot health, migrations and shutdown. Use explicit separate project names/ports
when another deployment exists; never run these generic development commands against it.

Required quality commands: `uv run pytest -q`, `uv run ruff check .`,
`uv run ruff format --check .`, `uv run mypy`.
Persistence/API integration tests require `TEST_DATABASE_URL` pointing at a migrated,
disposable database whose name ends in `_test`; its market-data tables are cleared.
Verify downgrade/re-upgrade only on a disposable database. Force-refresh historical
continuous aggregates in tests after writes: materialized corrections are eventually
consistent, unlike the unmaterialized real-time tail.

Full isolated mock container smoke: configure unused ports,
`docker compose -p fastapi-dev up -d --build`, then
`uv run python scripts/smoke.py --base-url http://localhost:8001`.
Load test against a separate migrated database:
`uv run python -m marketdata.benchmark --seconds 10 --rate 5000 --instruments 300`.
These commands generate simulated observations; they are not live-provider tests.

## Live guest pool experiment

The operator authorized bounded guest multi-WebSocket investigation on 2026-10-12.
`scripts/benchmark_live_pool.py` explicitly constructs experimental guest workers; the
application factory keeps the original single-socket default and supports explicit
TRADINGVIEW_CANDLE_POOL_ENABLED activation through validated Settings. Counts come from
the selected registry, never benchmark count switches. See docs/production-instrument-expansion.md
for the separate 34 pilot and original-17 rollback. Guest app pool safeguards are at most six
connections, 100 instruments/jobs per worker, 600 total and 12 concurrent requests; these are
local bounds within the operator-authorized investigation, not upstream entitlements.
The pilot uses two paced workers, local windows two, global four. Do not increase a deployed
universe merely because these ceilings allow it. Experimental `PoolLimits`
are local safeguards, never documented upstream allowances or an entitlement claim.
Keep the 100-instrument/job cap per worker, native identity/status validation, UTC cutoffs,
Decimal parsing and access rejection behavior. Do not infer completed bars from successful
requests. HTTP 429 reduces global concurrency and imposes at least 60s provider cooldown
(honor longer numeric/date Retry-After); the live harness stops escalation and checkpoints.
Connection starts can be paced with `CANDLE_POOL_CONNECT_INTERVAL_SECONDS` (default 0).

Stop the ordinary collector while experimenting. Use a dedicated migrated PostgreSQL
database ending `_test`, separate from databases used by pytest fixtures (which truncate
tables). The harness registers benchmark-only identities in unique provider namespaces,
uses the actual `CandleIngestion.cycle` and `Store.write_candles` transaction, and reads back
exact native OHLCV/status from another acquired connection after commit. No production
table truncation, registry replacement or deployment is needed. For an isolated split-process
development project, restore its collector with `docker compose -p fastapi-dev start collector`.
The existing combined production deployment follows its owner's preserved operations; this
FastAPI branch must not stop, restore or rebuild it during parallel development.

Run `uv run python scripts/benchmark_live_pool.py --collector-stopped --discover
--counts 128 256 512 --cycles 3 --connections 6 --connect-interval 2
--output artifacts/live-pool-paced.json` with `TEST_DATABASE_URL` set. Supply this as one
shell command. `--discover` records current TradingView scanner identities and ranks spot
USDT candidates by reported volume * close; this ranks candidates only, not native proof.
Use the same saved candidate file for repeated comparisons. Chart responses independently
validate every source. Never swap sparse instruments after seeing results within a run.

Each stage has baseline cycles, one labelled client socket disconnect plus one pretransaction
DB failure, and two recovery cycles. Preserve immediate and eventual durable completeness,
all first-commit candle timestamps/latencies, source omissions, worker/connection diagnostics,
CPU/RSS/traced memory/event-loop lag and cleanup. Commit time means local acknowledgement
after transaction return, not a server-side/exchange timestamp. Short-run percentiles are
descriptive, not sustained-capacity claims. Preserve failed checkpoints; publish sanitized
measured evidence and mark stages never reached as NOT TESTED. See
`docs/candle-live-end-to-end.md` for actual results and remaining limitations.
Publish both successful/partial and stopped checkpoints with `uv run python
scripts/summarize_live_pool.py artifacts/live-pool.json artifacts/live-pool-paced.json`.
Preserve raw checkpoint hashes and every candle's first commit timestamp; report request
success separately from native-minute completeness. Use a quiet host for capacity comparisons
and disclose concurrent development workload when reporting exploratory runs.
