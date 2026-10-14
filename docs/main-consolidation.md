# Completed-development consolidation into main

This review consolidates completed collector and FastAPI development without deploying it.
Integration branch: `integration/completed-main`; target: `main`. The operator retains the
final merge decision. No published history, prior branch or production resource is replaced.

## Exact inputs and retained history

| Input | Commit | Inclusion |
| --- | --- | --- |
| Target/main and merge base | `2c5d0e6e51f51d0ae7298501df79e38859f3a464` | Ancestor of the API source |
| Completed collector scaling/business work | `368bce1293db59c7623bcb6331232dc525ff943e` | Parent of the existing integration merge |
| FastAPI Batch 1 | `6901bfadd44cb83ccdca49aac559e6f97a93a91d` | Parent of the existing integration merge |
| Reviewed collector/API integration | `35ab3c03662ad00e8114fb313f8d2d15459a78d9` | Both parents retained |
| FastAPI Batch 2 | `b9c08df51655f4767dd0669838f2d221c4854933` | Ancestor of Batch 4 |
| Completed Batch 3 snapshot and Batch 4 | `cb4a06332adda936421a00b1b87155e7834cd24f` | Published source; complete journal/stream/security/qualification files |

Batch 3 had no separate published commit: its branch pointer still references Batch 2 and
its verified pending files were incorporated into cb4a0633. Do not merge the dirty Batch 3
worktree again. All five commits ahead of main are retained; no cherry-pick, squash, rebase,
force push or branch deletion is needed. Because main is an ancestor of the source, the
merge tree equals the integration head tree. The PR's server-generated merge SHA and tree
are verified separately; final checks/results are recorded on the PR.

## Completeness and exclusions

Completed collector work includes paced bounded pool profiles, preserved 100/34/17 rollback
evidence, loop-lag metrics, recovery set membership, scoped FX/FXCM identity validation,
unknown-volume provenance and the provisional independent-broker business registry/ledger.
No 99-source proposal or larger experimental registry is automatically activated.

Completed API work includes independent locked collector/API ownership, persistent collector
health, read-only REST v1, truthful quotes/candle closes/freshness, bounded cursor history and
tri-state coverage, transactional replay journal, independent worker WebSockets, reconnect/
correction handling, production credentials/Origin/CORS checks, health/metrics and frontend/
deployment/backup/rollback guides. Existing integration conflict resolutions are documented
in [collector-fastapi-integration.md](collector-fastapi-integration.md); no new code conflict
exists against the target. This branch changes consolidation documentation only over cb4a0633.

Excluded: the paused/uncommitted `batch3-database-efficiency` experiment, dirty home-rooted
deployment changes, local captures/logs/databases/secret files, and unrelated root refs.
`chore/root-layout` is already merged. Earlier API/integration/ordinary-scale branches are
superseded by ancestry, not deleted. Frozen sanitized source/scaling/API qualification JSON
and CSV are intentional published evidence; raw generated artifacts remain ignored.

## Schema and historical compatibility

0001–0004 remain unchanged from main. 0005 remains unchanged from Batch 1 and adds only the
collector-status snapshot. 0006 remains unchanged from cb4a0633 and adds the commit-ordered
journal/clock. There is one linear Alembic head. Native history, source identity, activation
starts, EXPECTED/NOT_EXPECTED/UNKNOWN coverage, correction audit and Decimal/UTC semantics
are preserved. Populated round-trip and replay/rollback tests run against disposable engines.
No production migration runs on checkout, push or merge.

The deployed legacy Store requires 0004 and does not use the collector advisory lock.
A future approved deployment must stop the legacy owner before starting a lock-aware one,
back up and explicitly migrate the verified database. Old code cannot be blindly restarted
on 0006; schema-compatible rollback is in [production-deployment.md](production-deployment.md).

## Validation and production boundary

Validate the exact PR merge tree with full suites on task-owned plain PostgreSQL and
TimescaleDB, including opt-in actual disposable DB outage, backup/restore and Linux API
container smoke. Run Ruff, format, strict mypy, JS syntax and migration checks. GitHub's
existing read-only `project tests` matrix validates plain-table and Timescale modes; its
guarded local-container fault tests are covered by the separate full local runs.
No load/source experiment is repeated for this documentation-only consolidation.

Repository audit found one hosted-runner workflow, triggered by push/PR, with contents:read;
it installs dependencies, checks/tests and migrates only an ephemeral test service. It has
no deploy job, production environment, SSH/registry publishing or self-hosted runner.
GitHub reported no repository webhooks, environments or deployment records during review.
Current production containers run prebuilt images and named DB volumes, with no source bind
mounts. Container identity/image/start/mount metadata is compared before/after validation;
production SQL, service restart, image replacement and live registry access are not used.
Repository changes therefore provide no observed automatic deployment path. An undisclosed
external deployment integration cannot be established from repository/host metadata alone.

Remaining production gates are unchanged: source rights/redistribution, genuine live quote
availability, final instrument selection, production TLS/session/DBA provisioning and target-
host/sustained capacity. These do not block a reviewed code consolidation; they block rollout.
Main must remain unchanged until explicit approval of the final PR merge.
