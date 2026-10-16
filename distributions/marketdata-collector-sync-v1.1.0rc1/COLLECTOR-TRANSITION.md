# Controlled legacy collector transition and later laptop-to-server handover

DOCUMENTED PROCEDURE ONLY. Packaging does not authorize or execute it. Existing collector was
historically reported base0004/candle-only; inventory the ACTUAL installation before any decision.
M5 source requires a compatible v1.1 binary, base0006 and source sc0001+sc0002. Company requires0010.
Never install destination0007–0010 into a collector to make version numbers match.

Preconditions and explicit stop points:

1. Read-only inventory: operator records application/image hashes, actual DB/server version,
   alembic_version, table definitions, provider registry/UUIDs/symbol aliases, storage mode,
   quote availability, containers/volumes, collector process/ownership lock, disk size and last
   known candle/quote times. Do not start a stopped collector simply to inspect it. Do not
   claim its quote capability from the new source package's code. Never disclose secret values.
2. Verify license/entitlements, provider calendars and genuine independent quotes on a safe
   isolated profile. Compare OANDA/FXCM identities separately. Stage schema upgrade on a restored
   disposable COPY, test representative old volume units/timestamps, and report ineligible
   enrollment records without rewriting them. Keep the real installation untouched until review.
3. Make verified logical/physical backups of source data, schema, roles, registry and exact old
   compatible binary; rehearse restore into a separate DB and compare counts/financial checksums.
   If source capture/worker already exists, back up its epoch, all worker/outbox/receipts plus the
   separately protected external anchor/identity/token. Preserve old data and backup logs.
4. Freeze collection under an approved maintenance window: stop exactly the existing collector,
   prove no other host/process/legacy container can write, and record final durable financial
   high-water. The old legacy collector may lack the new advisory lock, so a new lock alone
   does NOT stop it. Disable its scheduled/container auto-start for the reviewed transition.
5. On the reviewed source ONLY, run the collector archive's guarded release_admin --migrate
   with an exact expected database and compatible PostgreSQL/extensions. It explicitly upgrades
   base0004->0005->0006, then separate sc0001->sc0002. Installing schema is not activation.
   Old binary exact-head checks will reject the new schema; never run old/new binaries together.
6. Validate preserved historical counts, OHLC/volume precision, provider mappings, revision
   evidence and UTC receipt/source timestamps. Record the newly created source epoch exactly
   once for this dataset; migration/restarts must not regenerate it. Uncertain restore/identity
   continuity stops and needs DBA review. No silent epoch reset/cursor deletion.
7. Provision separate collector writer and restricted sync roles; protect each secret. Validate
   the compatible binary locally, then explicitly enable capture + dual operation and approved
   provider scope. A schema with the source extension cannot be used through READ/INGEST or a
   capture-disabled collector. Start ONE collector, verify atomic outbox and genuine quote clocks,
   then give the company only the non-secret epoch/identity/approved manifest for policy setup.
8. Start worker under its separate account with complete owner manifest, approved CA/token,
   durable anchor and enroll=true. Observe bounded backfill, current-price fairness, invalid
   records, backlog/oldest age and receipt/fence errors. Compare actual company financial state,
   fields/clocks, revision manifests and coverage independently of prior export markers.
9. Accept only after restart/outage/receipt replay and genuine-source/freshness checks. Keep old
   backup/binary quarantined; do not delete historical observations or production volumes.

Rollback is not `alembic downgrade`. sc0002 intentionally refuses blind downgrade because its
staging/receipts/identity cannot be discarded safely. If activation fails before new financial
writes, restore the verified pre-change source into a separately named DB/volume and use the exact
old binary after confirming new collector/worker are stopped. If new writes/deliveries occurred,
retain that database and staged bytes/anchor, account for company receipts, and perform a reviewed
merge/reconciliation or forward fix. Blind backup overwrite may lose committed candles or reuse
source positions. Do not restore over the only copy or truncate delivery state to make startup pass.

Later Windows laptop -> Linux server move:

- Give the new physical installation a DISTINCT durable producer UUID/key/token. Never copy the
  old credential to a second active collector. An exact cloned anchor/DB/token cannot prove which
  physical machine is authoritative; fencing must come from approved company ownership.
- Freeze the laptop's collector and worker and preserve pending bytes/receipts. Prefer draining
  authorized pending data and reconciling actual company contents BEFORE handover. Otherwise
  retain pending original records and decide their disposition with the DBA; never rewrite old
  staged envelopes under their original IDs. Do not let both producers write the same stream.
- Validate the new dataset: restore all trusted source durability state only with reviewed
  continuity, or initialize a distinct dataset epoch and re-enroll actual records. New epoch is
  an explicit administrative transition, never an automatic restore workaround. Preserve timestamps.
- DBA uses EXISTING ingest_source_owners mechanism: lock affected owner/producer rows, verify
  expected current producer/epoch/generation, assign the new producer/validated epoch with a
  strictly higher ownership_generation, and admit its new known session. Disable old producer/key
  as policy requires. Triggers reject regression. Record the full complete manifest and audit.
  No ingestion/recovery endpoint grants takeover rights. This packaging CLI only creates new
  unowned grants; intentional handover remains reviewed DBA tooling/SQL from the accepted protocol.
- Verify the old retained credential/session remains fenced, new worker reconciles materialized
  candles/quotes/coverage/revisions and REST/WSS replay, and no old pending data is silently
  discarded. A pending historical correction requires reviewed ordering relative to the accepted
  new context; never treat a blind resnapshot as recovery of missing audit history.
- Validate the Linux service, CA/secret permissions, reboot, backup/anchor continuity and field
  freshness separately. Retain rollback evidence and old observations until authorized retention.

Local identity transition limitation: an existing source_sync_owner.identity and external anchor
bind the OLD producer/manifest. Merely changing worker.json/token on a cloned DB is rejected.
This candidate has no automatic local identity-reset/handover command. Prefer draining the old
installation, then prepare a NEW source database/capture epoch with administrator-reviewed
transfer of actual historical financial rows and a new producer/ownership generation. Preserve
the complete old DB, pending batches and receipts. If preserving a restored dataset epoch is
required, a DBA must review the local worker-state/anchor transition and every pending envelope
before authorizing it; do not edit identity JSON/reset a cursor as a routine installation step.
Stop at that review gate rather than pretending company-side handover alone changes local identity.
