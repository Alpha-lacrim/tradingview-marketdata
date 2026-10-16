# TradingView collector / durable synchronization — v1.1.0rc1 candidate

Independent component for Windows laptop/server or Linux dedicated collector. Contains compatible
collector source, source migration track, worker and operational templates. It does not need a
company PostgreSQL DSN, password, tunnel or driver connection. Transport is authenticated HTTPS
with certificate validation. Collector and worker use different local database roles and separate
service identities where supported. The worker can run while collection is stopped and does
not open a TradingView connection.

Read [Windows](INSTALL-WINDOWS.md), [Linux](INSTALL-LINUX.md),
[legacy transition and handover](COLLECTOR-TRANSITION.md), and [operations](OPERATIONS.md).
Do not migrate the installed legacy collector while unpacking this release. Defaults disable
live providers and capture. Installing schema does not enable collection. A source with the
extension requires explicit compatible collector opt-in; an old binary must not write it.

Source installation is base 0006 then independent sc0001 -> sc0002. sc0002 supplies worker
ownership/staging/archive state and is required by the worker. source_capture_version is
separate from alembic_version. Company 0007–0010 are deliberately absent from this package.
The worker cannot directly rewrite financial history, epochs or original outbox payloads;
its SQL functions require the consumer session lock and generation. It exits on ownership
connection loss; a supervised service may restart it, but must retain its DB and external anchor.

A completed candle/correction and its outbox event commit together. Latest genuine quotes are
field-aware coalesced snapshots; intermediate ticks are not guaranteed historical retention.
An older acknowledgment cannot clear a newer pending quote. Delivery success never updates
source/collector clocks or makes a stale field fresh. OANDA and FXCM identities stay separate.

Use Python 3.12+ and uv==0.11.27. In a newly extracted directory:

```text
uv sync --frozen --dev
uv run pytest -q tests
uv run marketdata-sync --help
```

Use `uv sync --frozen --no-dev` for a service installation. A worker wheel can also be installed
in a dedicated virtualenv; keep the complete archive beside it for migrations/configuration.
RELEASE.json and SHA256SUMS identify this candidate and each payload. M5 verification is a
separate sidecar; do not confuse packaging with company deployment or genuine 99-feed approval.

Before installing, run `python scripts/verify_payload.py` from the extracted root, after checking
the outer archive hash from the supplied sidecar. For restricted package networks, Docker accepts
`--build-arg PACKAGE_INDEX=https://APPROVED_HTTPS_INDEX/simple/`. All runtime wheels must match
requirements.lock hashes. Never use --trusted-host, disable TLS, or put repository passwords in
build arguments. A public mirror may be used only if allowed by company policy; the candidate
verification sidecar records the index actually tested and default-index failures separately.
