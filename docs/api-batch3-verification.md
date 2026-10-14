# FastAPI Batch 3 verification

Batch 3 continues verified Batch 2 `b9c08df51655f4767dd0669838f2d221c4854933`
(integration base `35ab3c03662ad00e8114fb313f8d2d15459a78d9`) in a new isolated worktree,
`C:/Users/Pouyan/PycharmProjects/tradingview-fastapi-batch3`, branch
`fastapi/batch3-websocket`. Changes remain local/uncommitted, unmerged and undeployed.
The original dirty checkout and other worktrees were preserved. No live TradingView access,
production SQL/grants/migrations, collector restart, Docker production service or volume
operation was performed. No provider/registry or ingestion scheduling/parsing algorithm
changed; persistence adds journal writes inside its existing transactions.

## Inspected baseline and resulting contract

Repository AGENTS.md, CODEX.md, HANDOFF.md, Batch 1 process/integration guides and Batch 2
API/verification documents were inspected before editing. The old combined `/ws/prices`
uses bounded local Fanout: ticks are observations published before their DB transaction;
native candles are published only after successful Store.write_candles return. Collector
and API processes cannot share that Fanout. API-only legacy WS closes 1013. It remains intact.
0005 collector_status is a periodic expiring health snapshot, not a durable market-event
notification system. There was no LISTEN/NOTIFY mechanism. Both latest/history update in
atomic Store transactions; corrections already have candle_revisions provenance.

Final new interfaces:

- **`/api/v1/ws/marketdata`**: subscribe/unsubscribe/snapshot/ping/disconnect, authenticated
  service bearer plus exact Origin allow-list. Schemas and commands in [api.md](api.md#websocket-market-data-v1)
  and [generated JSON Schema](ws-marketdata.schema.json).
- **`GET /api/v1/marketdata/changes`**: bounded retained-journal replay with a fixed `until`
  fence; 409 requires full refresh of the loaded REST chart window.
- New native inserts and corrections emit candle_completed/candle_corrected after commit.
  Genuine persisted latest quote changes emit price_update, including unchanged numerical
  values at new observation timestamps. Batch latest state is sampled, not every raw tick.
  Completed candle closes are never relabeled live quotes. TradingView candle mode itself
  has no current-price quote stream; quote delivery is proven with explicitly simulated data.
- Price snapshots use the same source selection/decimal/UTC schemas as REST v1. Periodic
  source_status distinguishes persisted freshness, DB availability, collector state,
  provider transport and positively reported closed-session status. Expired session health
  becomes UNKNOWN, never a fabricated market closure.

## Journal correctness and recovery

Additive **0006** creates two regular PostgreSQL tables, equally supported on TimescaleDB.
Existing migrations 0001-0005 are unchanged. Store appends inside the candle/tick transaction,
not after commit in an unreliable callback. A singleton counter UPDATE assigns each batch
an ID block and holds its row lock until transaction end. No SERIAL/IDENTITY is used for
commit ordering: writer B cannot allocate a later cursor while writer A is uncommitted.
Rollback undoes counter and rows together. IDs are JSON strings; cursor includes version,
epoch and position. Immutable journal payloads preserve every retained correction version.

The deterministic concurrent regression pauses A after cursor allocation, starts B on a
different instrument, and waits for actual pg_stat_activity transaction-lock evidence.
Neither a REST reader nor the live worker advances past A while uncommitted. After release,
both events arrive in order over real WebSocket. Paginated reconnect from before A sees A
then B; reconnect after A sees exactly B. Separate rollback-after-append regression proves
no market/latest/journal changes become visible. Exact retry/older-tick/identical-price tests
cover logical deduplication and genuine observations independently.

Each API worker polls only a bounded journal tail and owns its queues; no extra provider
connection or infrastructure. Notification loss is irrelevant because no notification is
required. Polling defaults to one second, pages 250. Retention defaults to 100,000 events
(count, not duration), pruned on writes without deleting history. A worker beyond retention
closes affected clients 1013; REST responds 409. Adding a subscription retains earlier
subscriptions' barriers; heartbeat cursor waits until the reader reaches all snapshot
barriers. A snapshot plus cursor is one repeatable-read view. REST replay restores old
recovered minutes/corrections, not merely new candle timestamps.

## Authentication remains PROVISIONAL

The actual company frontend/session architecture is unfinished and unknown. The BFF model
is explicitly **PROVISIONAL**; market delivery is independently based on bearer headers and
Origin validation. The example is an optional adapter with an injected real-session
validator, no login/user/cookie/session implementation. Browser JavaScript has no service
API key. It supports REST pages, persisted prices, subscriptions, stale/disconnected status,
heartbeat watchdog, bounded synchronization buffer, jittered reconnect and old-correction
replay. See [browser module](../examples/browser-marketdata.js) and
[optional BFF adapter](../examples/company_bff.py).

Tests use explicitly simulated cookies/expiry and verify actual REST/socket relay, session
revocation, missing cookie and wrong Origin rejection before upstream connection. These
are adapter-pattern tests, not final frontend authentication qualification. API SELECT-only
role tests independently receive committed events and deny clock updates/event deletes even
with session read-only disabled. Production DSN/grants/keys/Origins/TLS/session integration
are unprovisioned. No ticket or API write role was introduced.

## Limits and failure checks

All limits are documented in api.md: clients, 100 instruments/client, total subscription
pairs, source rows, snapshot batches, UTF-8 message bytes, queues, send/close deadlines,
query budgets, event pages and retention. Limits are per worker; deployment-wide admission
belongs to the eventual gateway. Queue overflow/oversize/send timeout explicitly closes
1013 and requires REST resync; binary commands close 1003 and oversize input 1009.
No socket can block the collector transaction. Failed handshakes release client slots.

The Docker API command and test configuration select Uvicorn's bounded websockets backend,
max-size 65536 and max-queue 16. The currently installed default SansIO backend has an
unbounded internal receive queue, so ASGI message limits alone would be insufficient.
The bounded legacy backend has dependency deprecation warnings; its queue behavior must
be requalified when changing versions/backends. This is a known limitation, not a production
change or a reason to pretend complete delivery.

| Requested failure / behavior | Executed evidence |
| --- | --- |
| Normal single-worker delivery | Real network API + separate Store/test publisher, exact candle/quote/correction REST comparison |
| Concurrent commit ordering | Controlled A/B transaction barrier, actual PG lock wait, no missed live/reconnected event |
| Multiple API workers | Real Uvicorn two-worker supervisor/shared listener; both worker PIDs subscribe and receive the same event; provider construction/registration forbidden |
| API restart/client disconnect | Socket reconnect with retained cursor; new and old corrected bars recovered through REST |
| Collector restart | Actual mock collector subprocess stops/restarts while same API/socket survives; event ID exceeds stopped collector journal watermark |
| PostgreSQL outage | Exact task-owned DB container paused/unpaused; unavailable socket status, REST 503, subsequent commit and REST recovery |
| Missed notifications | Reader operates with no notification dependency; missed journal pages reconcile from persistent cursor |
| Retention expiry | 409 and explicit worker-client failure; all candle history retained |
| Slow clients | Bounded queue isolated from another subscriber; real blocked sender times out 1013 while fast client receives commit |
| Unknown/malformed/binary/oversize | Typed atomic errors, authentication/Origin denial, 1003/1009 and no admission leak |
| Repeated subscribe/unsubscribe | Idempotent membership, snapshot request and safe close 1000 |
| Multiple clients/symbol addition | Same event IDs, total limits, snapshot batching; existing symbol events not skipped by a new symbol barrier |
| 100 instruments | Four snapshot batches plus 100 committed native fixture candles; every payload equals REST latest candle |
| Staleness/closed market/source outage | No-event transition to stale, simulated current NOT_EXPECTED/closed health, historical reads with source/collector stopped |
| Read-only permissions/migrations | Exact SELECT-role socket, denied writes; populated 0006->0005->0006 and inherited 0004/head round trips preserve market rows |

## Final validation

| Final check | Actual result |
| --- | --- |
| TimescaleDB 2.25.1 / PostgreSQL 17.7 | **431 passed**, 23 dependency warnings, **103.32s** |
| Ordinary PostgreSQL 17.11 | **428 passed, 3 expected Timescale-only skips**, 23 dependency warnings, **107.72s** |
| New Batch 3 cases | **50**, all included in the full suites; explicitly simulated publishers/sessions |
| Ruff lint / formatting | Passed, 130 Python files formatted |
| Strict mypy | Passed, 36 source files; source + provisional BFF example also passed (37 files) |
| Browser JavaScript | `node --check examples/browser-marketdata.js` passed; no real company browser/login run |
| Schema / OpenAPI | Outbound serialization JSON Schema exported; 16 authenticated/legacy GET operations; single 0006 head |
| Migrations / roles | Fresh both-engine upgrades; populated 0006/0005 and inherited 0004/head preservation; SELECT-only event delivery and denied writes passed |
| Git / isolation | Whitespace passed; providers/collector/ingestion/registry and 0001-0005 unchanged; native test processes exited; disposable services removed |

Warnings are the inherited Starlette/httpx warning and bounded legacy websockets/Uvicorn
deprecations documented above. No final behavioral failures. These are local deterministic
correctness checks, not sustained production/live market-data capacity or licensing evidence.

Reproduction uses
only `compose.batch3-test.yaml`, project **marketdata-batch3-tests**, loopback **55791/55792**,
DB **marketdata_batch3_test**, separate network/tmpfs and no production mounts. Native test
processes are owned, stop-file-controlled and joined. The outage test validates the exact
project/container/loopback DSN before pausing; finally always resumes it.

The command set is in api.md. Ruff lint/format, strict mypy including the Python BFF example,
browser JS syntax, generated schema export, single 0006 Alembic head and Git whitespace
checks are included. JUnit evidence remains under ignored artifacts. Earlier 420-pass run
had a too-short two-worker harness HTTP/shutdown timeout; readiness/deadlines/cleanup were
fixed and targeted reruns passed. One original unit fixture exposed shared header mutation
by TestClient; passing a copied header mapping fixed that test-only issue. Initial fake
Store/REST query-count interference was fixed by keeping an idle worker reader idle.

## Remaining limitations and next actions

No exactly-once/permanent WebSocket history promise; replay/upserts and REST history are
required. Count retention can be short under quote load; sustained host/network/concurrent
client capacity and serialization-lock overhead are not qualified by the 100-instrument
fixture. Direct administrative SQL/old collectors do not journal changes. Derived interval
notifications are not supplied; refetch/recompute affected derived buckets. A database restore
must use a new journal epoch to avoid cursor reuse. Legacy-backend dependency deprecations
need requalification during future upgrades. The browser example is type/syntax/adapter
verified, not a finished UI or a run against real company login.

Future review must decide browser auth/session/BFF or another secure adapter, provision
SELECT-only grants for the two new tables, secrets/Origins/TLS, set aggregate admission and
rate limits, choose retention for actual traffic and load-test the deployment host. Any
collector/schema replacement must follow the earlier integration stop-and-replace/legacy
rollback procedure. 0006->0005 drops only the replay journal; it preserves market/health
records but invalidates cursors (new epoch after upgrade). Original legacy combined code
still requires 0004 and cannot be restarted blindly on 0006. No deployment, production
promotion, live provider qualification or business-universe activation is authorized here.
