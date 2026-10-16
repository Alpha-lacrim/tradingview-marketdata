"""Release role separation, explicit migration boundaries and collector writer grants."""

# Bounded local test fixtures, never application-request IO.
# ruff: noqa: ASYNC240

import argparse
import builtins
import json
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import pytest
from test_native_candles import make_candle
from test_source_capture import source  # noqa: F401

from marketdata.config import Settings
from marketdata.domain import normalize, utc_now
from marketdata.persistence import Store
from marketdata.release_admin import boundary, execute, provision
from marketdata.release_runtime import launch


@pytest.mark.integration
async def test_destination_snapshot_persists_without_source_modules(store, pair, monkeypatch):
    original = builtins.__import__

    def isolated_import(name, *args, **kwargs):
        if name == "marketdata.source_capture":
            raise ModuleNotFoundError("source capture is absent from the company release")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", isolated_import)
    tick = normalize(*pair, utc_now(), last="100.1234567890123456789")
    async with store.connection() as c, c.transaction():
        await store.write_observations_in_transaction(
            c, [tick], history=False, ordered_snapshots=[tick]
        )
    async with store.connection() as c:
        assert json.loads(await c.fetchval("SELECT payload FROM latest_prices")) == tick.model_dump(
            mode="json"
        )
        assert await c.fetchval("SELECT count(*) FROM marketdata_events") == 1


@pytest.mark.parametrize(
    "component,role",
    [("company", "collector"), ("collector", "read"), ("collector", "ingest"), ("unknown", "read")],
)
def test_release_rejects_role_before_loading_secrets(tmp_path, component, role):
    (tmp_path / "RELEASE.json").write_text(json.dumps(dict(component=component)))
    with pytest.raises(ValueError):
        launch(role, tmp_path)


def test_company_capture_opt_in_rejected_before_secret_loading(tmp_path, monkeypatch):
    (tmp_path / "RELEASE.json").write_text('{"component":"company"}')
    monkeypatch.setenv("SOURCE_CAPTURE_ENABLED", "true")
    with pytest.raises(ValueError, match="source capture"):
        launch("read", tmp_path)


def test_release_collector_requires_capture_before_secret_loading(tmp_path, monkeypatch):
    (tmp_path / "RELEASE.json").write_text('{"component":"collector"}')
    monkeypatch.delenv("SOURCE_CAPTURE_ENABLED", raising=False)
    with pytest.raises(ValueError, match="opt-in"):
        launch("collector", tmp_path)


@pytest.mark.integration
async def test_actual_destination_source_boundary(store):
    async with store.connection() as c:
        assert (await boundary(c, "company", complete=True))["base"] == "0010"
        with pytest.raises(ValueError, match="destination"):
            await boundary(c, "collector")


@pytest.mark.integration
async def test_source_boundary_and_new_collector_writer_permissions(source, pair):  # noqa: F811
    local, _ = source
    role = "m5_collect_" + uuid4().hex
    sql = (
        Path("scripts/sql/v11_source_collector_role.sql")
        .read_text()
        .replace("marketdata_v11_collect", role)
    )
    parsed = urlsplit(local.settings.database_url.get_secret_value())
    async with local.connection() as c:
        assert (await boundary(c, "collector", complete=True))["source_extension"] == "sc0002"
        with pytest.raises(ValueError, match="source capture"):
            await boundary(c, "company")
        await c.execute(sql)
        await c.execute(f"ALTER ROLE {role} PASSWORD 'disposable_test'")
    config = dict(
        _env_file=None,
        database_url=urlunsplit(
            parsed._replace(netloc=f"{role}:disposable_test@{parsed.hostname}:{parsed.port}")
        ),
        application_role="collector",
        source_capture_enabled=True,
    )
    writer = Store(Settings(**config))
    try:
        await writer.connect()
        await writer.write_candles([make_candle(pair, realtime_status="simulated")], [])
        await writer.write_quotes([normalize(*pair, utc_now(), last="100.1234567890123456789")])
        async with writer.connection() as c:
            assert await c.fetchval("SELECT count(*) FROM source_candle_outbox") == 1
            assert await c.fetchval("SELECT count(*) FROM source_quote_state") == 1
            for statement in [
                "UPDATE source_sync_owner SET generation=generation",
                "CREATE TABLE public.not_allowed(id int)",
                "DELETE FROM market_candles",
                "TRUNCATE source_candle_outbox",
                "UPDATE source_candle_outbox SET record=record",
                "UPDATE source_capture_clock SET source_epoch=source_epoch",
                "UPDATE source_quote_state SET acked_version=acked_version",
            ]:
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await c.execute(statement)
    finally:
        await writer.close()
        async with local.connection() as c:
            await c.execute(f"DROP OWNED BY {role}")
            await c.execute(f"DROP ROLE {role}")


@pytest.mark.integration
async def test_initial_provisioning_rejects_transfer_and_rolls_back(store, pair):
    plan = dict(
        producer_id=str(uuid4()),
        label="M5 injected initial policy",
        key_id="m5test",
        session_id=str(uuid4()),
        allowed_statuses=["simulated"],
        streams=[
            dict(
                instrument_id=str(pair[0].id),
                provider=pair[1].provider,
                stream="candles",
                source_epoch=str(uuid4()),
                ownership_generation=2,
            )
        ],
    )
    async with store.connection() as c:
        with pytest.raises(ValueError, match="handover"):
            await provision(c, plan)
        assert await c.fetchval("SELECT count(*) FROM ingest_producers") == 0
        plan["streams"][0]["ownership_generation"] = 1
        await provision(c, plan)
        with pytest.raises(asyncpg.UniqueViolationError):
            await provision(c, plan)
        assert await c.fetchval("SELECT count(*) FROM ingest_producers") == 1
        assert await c.fetchval("SELECT ownership_generation FROM ingest_source_owners") == 1


@pytest.mark.integration
@pytest.mark.parametrize("component", ["company", "collector"])
async def test_guarded_first_install_from_separate_tracks(store, tmp_path, component):
    name = "m5_install_" + uuid4().hex + "_test"
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    dsn = urlunsplit(parsed._replace(path="/" + name))
    root = tmp_path / component
    root.mkdir()
    (root / "RELEASE.json").write_text(json.dumps(dict(component=component)))
    shutil.copytree("migrations", root / "migrations", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copyfile("alembic.ini", root / "alembic.ini")
    if component == "collector":
        shutil.copytree(
            "source_migrations",
            root / "source_migrations",
            ignore=shutil.ignore_patterns("__pycache__"),
        )
        shutil.copyfile("alembic-source.ini", root / "alembic-source.ini")
        for migration in (root / "migrations/versions").glob("*.py"):
            if migration.name[:4].isdigit() and int(migration.name[:4]) > 6:
                migration.unlink()  # Only this generated test copy; never the repository.
    secret = tmp_path / "dsn"
    secret.write_text(dsn)
    args = argparse.Namespace(
        root=root,
        dsn_file=secret,
        database=name,
        migrate=True,
        timescale=store.has_aggregates,
        roles=False,
        passwords=None,
        registry=None,
        provision=None,
        check=True,
    )
    async with store.connection() as c:
        await c.execute(f'CREATE DATABASE "{name}"')
    try:
        state = await execute(args)
        assert state["base"] == ("0010" if component == "company" else "0006")
        assert state["source_extension"] == (None if component == "company" else "sc0002")
        assert await execute(args) == state  # Repeated migration retains the source epoch.
        args.database = "wrong_test"
        with pytest.raises(ValueError, match="expected database"):
            await execute(args)
    finally:
        async with store.connection() as c:
            await c.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
