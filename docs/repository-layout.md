# Repository layout and existing checkout migration

The application now lives directly at the repository root: `src/`, `tests/`, `scripts/`,
`config/`, `docs/`, `migrations/`, Python configuration and container files. The actual project
README replaces the publication wrapper. One root CI workflow uses root-relative commands
and `uv.lock`; the duplicate nested workflow is removed. Python packaging, registry paths,
Alembic paths and Docker build contexts were already relative to the application directory
and retain the same relative layout. No application behavior or schema is changed.

## Why the old layout existed

The original Git worktree was rooted at `C:/Users/Pouyan`, while the application was at
`C:/Users/Pouyan/PycharmProjects/tradingview`. Earlier publication preserved that Git tree
and used a root allowlist, wrapper README and CI working-directory override to publish only
the application. That avoided including unrelated home files but exposed the local directory
hierarchy on GitHub. The restructuring is an ordinary descendant commit with file moves;
earlier commits and their original paths remain available. Use `git log --follow -- <file>`
to inspect history across a move.

## Safe local migration

Restructuring was prepared in a separate clone at
`C:/Users/Pouyan/PycharmProjects/tradingview-marketdata`, on `chore/root-layout`.
The original home-rooted checkout, application directory, `.venv`, ignored artifacts,
operator notes, IDE files and running containers were preserved. The review branch changes
the Git tree; it does not relocate the existing local application or deploy a new image.

Until the review branch is merged, a new clone must explicitly select it:

```text
git clone --branch chore/root-layout https://github.com/Alpha-lacrim/tradingview-marketdata.git
cd tradingview-marketdata
```

After merge, normal clones use the new layout automatically. Existing nested/home-rooted
checkouts should use a separate clone rather than pull the relocation commit into the home
directory: a pull there would put the application at the home root and remove the old tracked
paths. Keep the old checkout available until local configuration and data access are checked.
Copy only needed ignored settings/artifacts deliberately; never add credentials or database
dumps to Git. Recreate the virtual environment using `uv sync --frozen --extra databento`
instead of moving an environment containing absolute paths. Do not remove or relocate the
home-level `.git` as part of this migration.

## PyCharm

The inspected local IDE project is the parent `PycharmProjects/.idea` project with an inherited
Python 3.12 SDK; no application-specific `tradingview/.idea` or tradingview VCS mapping was
present. Its configuration was left untouched.

To use the new checkout, open `tradingview-marketdata` as a separate PyCharm project, select
its `.venv/Scripts/python.exe` on Windows (or `.venv/bin/python` on Linux), and set run/test
working directories to that checkout's root. Keep Uvicorn's module/arguments as
`marketdata.api:create_app --factory --workers 1 --ws-max-size 16384`. Configure Git for the
new checkout root. Editable installation through `uv sync` provides imports; `src` can also
be marked as a Sources Root in the IDE. `.idea/` remains ignored. Existing parent-project
run configurations continue to use the original path until deliberately changed.

## Docker Compose and existing data

Compose derives a project name from the checkout directory. The original local project is
`tradingview`, with volume `tradingview_marketdata_pg` and services bound to ports 5432/8000.
A new checkout named `tradingview-marketdata` gets a different Compose project and database
volume by default. Do not start a second deployment on the same ports or interpret a fresh
volume as lost history. `docker compose config --quiet` validates paths without deployment.

Continue operating the existing deployment from the original directory until an intentional
deployment migration. To reuse its existing named volume later, first back up the database,
stop the old project's services, then use `docker compose -p tradingview ...` consistently
from the new checkout with the same local configuration. This is an explicit deployment
change; the repository restructuring itself does not perform it. Never use `down -v` for
this migration. Experimental live benchmarks also require explicitly stopping/restoring the
intended collector; commands run from a new checkout otherwise target a different project.

Validation uses a separate `marketdata-root-review` Compose project, alternate host ports
55432/18000, a separate named volume and disposable databases. It does not restart the
existing collector, alter its registry or touch its database history.
