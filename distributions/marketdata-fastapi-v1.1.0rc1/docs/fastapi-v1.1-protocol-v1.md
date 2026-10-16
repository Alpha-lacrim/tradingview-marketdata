# Company ingestion protocol v1 (FastAPI v1.1, milestone 2)

Implemented by `marketdata.ingest_api:create_app`, separately from the unchanged read factory
`marketdata.api:create_app`. One codebase, separate processes, credentials and database roles.
Destination schema head is 0010 (0007–0009 are preserved). No endpoint connects to TradingView or the collector database.

## Transport and authentication

HTTPS is mandatory for every route except unauthenticated `/livez`, which reports process
liveness only. There is no HTTP redirect carrying credentials. The application checks the ASGI
scheme, ignoring raw forwarded headers. With TLS termination, only explicitly allowlisted
proxy addresses may supply Uvicorn forwarded scheme information. Restrict the application's
network listener to that gateway. Never use a wildcard proxy trust list. See the
[FastAPI proxy documentation](https://fastapi.tiangolo.com/advanced/behind-a-proxy/).

Supply `Authorization: Bearer <key_id>.<secret>` on every ingestion request. Generate a random
256-bit secret using `scripts/create_ingest_credentials.py`; the server's private mounted JSON
file contains only `producer_id`, non-secret `key_id`, and SHA-256 of the entire token. The
collector receives the separate token file. Hash verification is constant-time. File definitions
are loaded at startup, bounded to 1000 unique producers/keys and 256 KiB; rotations require
restarting all ingestion processes. SQL `credential_key_id` must match the file definition.
Disabled producers and old key IDs fail even while an old process still holds its loaded file.
The upload transaction checks the key again under the same producer policy lock as storage.
No permanent producer token belongs in browser JavaScript. Origin-bearing requests are rejected;
no CORS access is provided. Frontend API_KEY and producer credentials must be independently
created; ingestion configuration rejects API_KEY. Read routes do not mount ingestion routes.

Credentials authenticate a producer, never an arbitrary body-supplied producer ID. Ownership
and allowed data-status authorization still come from the destination database. Mapping UUID,
canonical symbol, provider, provider symbol and quote instrument/venue identity must agree.
OANDA and FXCM mappings remain separate. Entitlements and redistribution rights remain an
operator/company responsibility; transport access is not evidence of redistribution rights.

## Routes

All paths below are under `/api/ingest/v1`.

| Method / path | Meaning |
|---|---|
| POST `/session` | Validate an administrator-provisioned active session and retrieve its bounded ownership manifest. |
| POST `/session/renew` | Renew that same live session to at least 120 seconds from server now. |
| POST `/session/recover` | Recover an expired, unchanged producer session atomically using a durable recovery ID and expected ownership manifest. |
| POST `/candles` | Commit completed native one-minute candles, including backfill and corrections. |
| POST `/quotes` | Commit independently captured quote observations and safely update eligible latest state. |
| POST `/coverage` | Commit cumulative coverage snapshots. |
| POST `/status` | Commit captured per-source collector status. |
| GET `/receipts/{batch_id}` | Retrieve this authenticated producer's original committed acknowledgment, including after session rotation. |
| POST `/reconcile` | Bounded current-owner financial/head comparison; see the Milestone 3B worker guide. |
| GET `/health` | Authenticated ingestion/database readiness, without any feed-freshness claim. |

JSON requests require `Content-Type: application/json` (optional `; charset=utf-8`). Content
encoding must be absent or identity. No gzip/decompression, multipart or compressed batches.
The client verifies the server certificate and hostname using its system/company CA bundle;
never disable TLS verification. Clients should disable automatic redirects for token-bearing
requests. Browser sessions and upstream provider credentials are outside this protocol.

## Session and ownership fencing

Admission and renewal retain their approved request and behavior:

```json
{"protocol_version":1,"session_id":"<provisioned UUID>","session_generation":1}
```

They require the supplied UUID/generation to match a live, enabled session. Admission never
reveals a replacement session to a stale client. Renewal cannot revive an expired session.

Milestone 2.1 adds POST `/session/recover` for an expired session held by the same enabled,
currently authenticated producer with unchanged expected ownership. The request carries a
durable recovery_id, previous_session_id/generation, and its complete expected stream manifest
(instrument_id/provider/stream/source_epoch/ownership_generation). PostgreSQL locks the producer
and stream rows, checks all ownership/epochs/generations, advances the session generation once,
creates a random replacement session UUID and commits its durable recovery receipt atomically.
It changes no ownership, dataset epoch, market observation, event or upload receipt.

An exact logical retry of that recovery ID returns its original acknowledgment only if that
replacement session is still current and the stream checks still match. It never extends the
lease. A later recovery, administrator session rotation or ownership change makes obsolete
retries fail closed, without revealing a subsequent session. Revoked keys and disabled producers
remain denied. Different competing recovery IDs expecting the same previous session have one
winner; losers must stop instead of following the winner. Shared credentials plus cloned winning
recovery state cannot identify distinct physical machines; never share installation identities.
Use distinct producer credentials and administrator-controlled handover between installations.

See [the exact recovery contract and Windows/Linux examples](fastapi-v1.1-session-recovery.md).
The [ingestion OpenAPI snapshot](fastapi-v1.1-ingest-openapi.json) includes the typed request and
response; public HTTP documentation remains disabled. The read API OpenAPI stays unchanged.

A successful admission/renewal response includes producer/session identity, expires_at,
server_time and at most 1000 current grant rows. It is a point-in-time manifest; uploads recheck
authorization. Source status expires independently. Neither renewal nor recovery refreshes
captured market timestamps or source-status TTL. An ordinary extended WAN outage can recover
automatically without DBA intervention when the expected identity and grants remain valid.
Intentional ownership/epoch/installation changes remain administrator-controlled. Keep all
pending source observations durable while disconnected or fenced.

## Batch envelope and canonical values

Each upload includes `X-Payload-SHA256`: lowercase SHA-256 hex over the exact UTF-8 request bytes.
The route and envelope stream must match. Duplicate JSON keys, nonfinite JSON constants,
unknown model fields, invalid data and mismatched hashes fail without a market-data commit.

```json
{
  "protocol_version": 1,
  "producer_id": "<authenticated producer UUID>",
  "session_id": "<provisioned session UUID>",
  "session_generation": 1,
  "source_epoch": "<durable local dataset UUID>",
  "batch_id": "<durable staged batch UUID>",
  "stream": "candles",
  "records": [{
    "observation_id": "<durable source observation UUID>",
    "source_position": "42",
    "kind": "candle",
    "observation": {"...": "the complete typed CandleObservation object"}
  }]
}
```

The skeleton is descriptive, not a copyable financial observation. Concrete schemas live in
`replication_models.py`; existing domain/candle identity fields are reused. Corrections are
candle records in the same ordering domain. Kinds are candle, quote, coverage and status.
Quote observation_id must equal the original explicit tick_id. No identity is generated by
parsing a request. Positions are positive BIGINT decimal strings, without leading zeros;
they describe durable local change order, never an invented provider sequence. Protocol and
session generation are strict JSON integers. Batch/observation identities survive retries.

All monetary inputs are finite plain decimal strings (max 128 characters), not JSON floats or
exponent notation. Nullable missing prices/sizes/volume remain null. Non-null native volume
requires a supported verified volume_type (base or tick); preserve volume_additive semantics.
Source nanoseconds are canonical decimal strings, separately retained from the datetime's
microseconds. Timestamps use timezone-aware RFC3339 strings with seconds, at most six fractional
digits, and Z or an explicit offset; normalize to UTC. Numeric/naive timestamp coercion is
rejected for market/coverage/status and captured health timestamp fields. The separate source
nanosecond field preserves finer genuine precision. Do not replace source or collector time
with server/upload time. Market source/collector timestamps over five seconds ahead of server
admission fail explicitly rather than being rewritten.

A candle must already be a completed native one-minute interval, with coherent OHLC bounds.
Coverage is a cumulative per-source snapshot within one ownership generation: newer windows
must contain the previous accepted window. Explicit unknown/not_expected/source_missing
ranges must be disjoint, minute-aligned and within the declared window. Destination missing
coverage is derived from actual stored bars, not from the source's claim of completeness.
Do not truncate a snapshot that exceeds the range ceiling; manifest/chunking is future work.

Status records retain run_id, captured_at, expires_at (lifetime >0 and <=120 seconds), running,
database_healthy, queue_depth and a health object restricted to that source. Retry/upload never
refreshes that TTL. Company read status checks current ownership, producer/session fences and
expiry. Missing/stale status does not fall back to an unrelated local singleton. Existing read
REST/WS fields remain compatible: readiness, collector status and price freshness are distinct.
Per-source WebSocket status remains independent of a missing status on another instrument;
global summaries are conservative. Source status is periodically projected, not journal-replayed
as a new financial event. No exactly-once WebSocket delivery guarantee is added.

## Receipts and retry policy

Success is HTTP 200 with the durable acknowledgment from milestone 1: protocol_version,
producer_id/source_epoch/batch_id/stream, payload_sha256, state=committed, server_ingested_at,
per-record outcomes and outcome counts. This is returned only after market rows, revisions,
coverage, latest state, imported-observation ledger, existing event journal and receipt commit
atomically. server_ingested_at is operational transaction time, not the source timestamp or an
exact physical commit timestamp. Replaying identical bytes under the same batch ID returns
that same saved response. Whitespace/envelope changes under a reused batch ID conflict.

Retransmission of an identical observation in another batch returns duplicate without a new
financial revision/event. Older candle versions are superseded; legacy raw quote ordering retains
the existing source-time/receipt/UUID rules. Coalesced source snapshots use the fenced
source-position barrier and field-level provenance specified in the
[Milestone 3B contract](fastapi-v1.1-sync-worker.md). Their receipt adds current quote_state
evidence; a worker must also verify actual materialization before acknowledging. Delayed quotes lacking source time are history-only.
An accepted ordering barrier may recorded_no_change if the shared candle receipt guard prevents
regression; reconciliation must compare actual materialized state as well as ledger outcomes.

After a timeout, connection loss, 429 or 503, retain the exact immutable staged bytes. Apply
bounded exponential backoff with jitter and Retry-After, then query the receipt or retry those
bytes. A missing receipt is not proof an in-flight transaction cannot subsequently commit.
After lease rotation, query the old receipt first; a replacement envelope uses a new batch ID,
while preserving the original observation identities/positions/epoch. Never mutate an existing
batch in place to update its session. Disabled/revoked producer credentials cannot query receipts.
The independent [Milestone 3B worker](fastapi-v1.1-sync-worker.md) implements staging,
checkpoints, recovery and financial reconciliation using these endpoints.

## Limits and errors

Settings can lower the hard ceilings: 2 MiB batch body, 500 candle/quote records, 50 status/
coverage records, 500 total coverage ranges, 16 KiB observation metadata. Admission/renewal bodies are
limited to 2048 bytes. Recovery bodies are bounded to the smaller of 256 KiB and the configured
body ceiling, with 1..1000 unique stream grants. Bounds apply to chunked bodies as well as Content-Length. A total request
deadline (default 30 seconds) includes authentication, database waits, streaming body reads and
commit. Cancellation reaches the same task's transaction, allowing rollback. A commit completed
just before an uncertain response is recovered through its durable receipt.

The database enforces a shared fixed-minute request budget per authenticated producer, default
120/minute, across processes/restarts. Every authenticated route counts, including bad payloads,
receipt polling and session renewal/recovery. Budget consumption is a separate small operational commit;
a rejected/rolled-back market batch can still consume quota. Fixed windows permit a boundary
burst up to two adjacent windows. Use identical limits across workers. Requests beyond quota
return 429 and Retry-After:60. Authenticated pending work is bounded per process (default 8);
overflow returns 503. This bounds application work, not unauthenticated connection attempts.
The gateway must independently limit unauthenticated requests/connections, header/body sizes,
slow clients and aggregate instance traffic. No broker/shared in-memory service is required.

Every error body is `{"error":{"code":"...","retryable":false}}`; retryable is true only
for 429/503. No input value, Pydantic detail, database exception text, DSN or token is echoed.
All responses have Cache-Control:no-store. Database faults, deadlocks, connection loss and
request deadlines return sanitized 503 with Retry-After:2; an unexpected invariant returns 500.

| HTTP | Codes / handling |
|---|---|
| 400 | https_required, invalid_content_length, payload_hash_required, payload_hash_mismatch |
| 401 | authentication_required (WWW-Authenticate:Bearer), including disabled/old-key definitions |
| 403 | browser_access_denied, producer_not_authorized, source_not_authorized, source_status_not_authorized |
| 404 | receipt_not_found, not_found |
| 409 | producer_session_fenced, batch_payload_conflict, observation_payload_conflict, source_version_conflict, source_identity_conflict, coverage_window_conflict, quote_snapshot_required, producer_session_active, recovery_payload_conflict, recovery_ownership_changed, recovery_obsolete, session_generation_exhausted |
| 413 | payload_too_large, record_limit_exceeded, ownership_manifest_too_large |
| 415 | json_required, unsupported_content_encoding |
| 422 | invalid_batch, invalid_session, invalid_request, stream_mismatch, collector_clock_ahead, source_clock_ahead, invalid_recovery, invalid_quote_snapshot, invalid_reconciliation |
| 429 | rate_limited; back off |
| 503 | ingestion_busy, database_unavailable; retry exact bytes or inspect receipt |
| 500 | internal_error; preserve batch and investigate rather than blindly changing its contents |

No full quote-history replication, genuine provider acquisition, WAN recovery or sustained
throughput claim follows from these destination HTTP tests. Durable source capture before
coalescing, retention/backpressure and a real worker remain separately reviewed milestones.
