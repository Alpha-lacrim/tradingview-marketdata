"""Explicit first-install tooling; reads only private files and never transfers ownership."""

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import asyncpg
from pydantic import SecretStr

from marketdata.config import Settings
from marketdata.persistence import Store
from marketdata.registry import Registry
from marketdata.session_recovery import RecoveryStream


async def boundary(c: Any, component: str, *, complete: bool = False) -> dict[str, Any]:
    base = None
    if await c.fetchval("SELECT to_regclass('public.alembic_version')"):
        base = await c.fetchval("SELECT version_num FROM alembic_version")
    capture = bool(await c.fetchval("SELECT to_regclass('public.source_capture_version')"))
    destination = bool(await c.fetchval("SELECT to_regclass('public.ingest_producers')"))
    extension = (
        await c.fetchval("SELECT version_num FROM source_capture_version") if capture else None
    )
    if component == "company":
        if capture or base not in {None, *(f"{n:04}" for n in range(1, 11))}:
            raise ValueError("company database must not contain source capture")
        if complete and base != "0010":
            raise ValueError("company requires destination 0010")
    elif component == "collector":
        if destination or base not in {None, *(f"{n:04}" for n in range(1, 7))}:
            raise ValueError("collector database must not contain destination schema")
        if extension not in {None, "sc0001", "sc0002"}:
            raise ValueError("unknown source extension")
        if complete and (base != "0006" or extension != "sc0002"):
            raise ValueError("collector/worker requires 0006 plus sc0002")
    else:
        raise ValueError("unknown release component")
    required = (
        ["instruments", "provider_instruments", "market_ticks", "latest_prices"] if base else []
    )
    if complete:
        required += (
            [
                "ingest_quote_heads",
                "ingest_destination_state",
                "ingest_batches",
                "ingest_observations",
            ]
            if component == "company"
            else [
                "source_capture_clock",
                "source_candle_outbox",
                "source_quote_state",
                "source_sync_owner",
                "source_sync_batches",
                "source_sync_archive",
            ]
        )
    for table in required:
        if not await c.fetchval("SELECT to_regclass('public.' || $1::text)", table):
            raise ValueError("versioned database is missing required market-data schema")
    if complete:
        singleton = "ingest_destination_state" if component == "company" else "source_capture_clock"
        if await c.fetchval(f"SELECT count(*) FROM {singleton}") != 1:
            raise ValueError("dataset identity missing; restore/administrator review required")
    if base is None and await c.fetchval(
        "SELECT count(*) FROM pg_tables WHERE schemaname='public'"
    ):
        raise ValueError("unversioned nonempty database is not a clean installation")
    return dict(base=base, source_extension=extension, component=component)


async def provision(c: Any, plan: dict[str, Any]) -> None:
    """New producer only. Existing ownership triggers remain the handover authority."""
    if set(plan) != {"producer_id", "label", "key_id", "session_id", "streams", "allowed_statuses"}:
        raise ValueError("invalid initial provisioning plan")
    producer, session = UUID(plan["producer_id"]), UUID(plan["session_id"])
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", plan["key_id"]):
        raise ValueError("invalid key ID")
    streams = [RecoveryStream.model_validate(s) for s in plan["streams"]]
    if not 1 <= len(streams) <= 1000 or len({s.source_epoch for s in streams}) != 1:
        raise ValueError("bounded single-epoch initial manifest required")
    statuses = plan["allowed_statuses"]
    if not statuses or not set(statuses) <= {
        "realtime",
        "delayed",
        "unknown",
        "indicative",
        "simulated",
        "end_of_day",
    }:
        raise ValueError("explicit permitted observation statuses required")
    async with c.transaction():
        await c.execute("SET LOCAL synchronous_commit=on")
        await c.execute(
            "INSERT INTO ingest_producers(producer_id,label,credential_key_id,session_id,"
            "session_generation,session_expires_at) VALUES($1,$2,$3,$4,1,"
            "clock_timestamp()+interval '120 seconds')",
            producer,
            plan["label"],
            plan["key_id"],
            session,
        )
        for s in streams:
            if s.ownership_generation != 1:
                raise ValueError("initial provisioning cannot perform a handover")
            await c.execute(
                "INSERT INTO ingest_source_owners(instrument_id,provider,stream,producer_id,"
                "source_epoch,ownership_generation,allowed_statuses) VALUES($1,$2,$3,$4,$5,1,$6)",
                s.instrument_id,
                s.provider,
                s.stream,
                producer,
                s.source_epoch,
                statuses,
            )


async def execute(args: argparse.Namespace) -> dict[str, Any]:
    root = args.root.resolve()
    component = json.loads((root / "RELEASE.json").read_text("utf-8"))["component"]
    dsn = args.dsn_file.read_text("utf-8").strip()
    if urlsplit(dsn).path.removeprefix("/") != args.database:
        raise ValueError("private DSN does not name the explicitly expected database")
    c = await asyncpg.connect(dsn, timeout=10)
    try:
        if not await c.fetchval("SELECT pg_try_advisory_lock(332680217676)"):
            raise ValueError("another release administrator holds the installation lock")
        result = await boundary(c, component)
        if args.migrate:
            commands = [[sys.executable, "-m", "alembic"]]
            if args.timescale:
                commands[0] += ["-x", "timescale=true"]
            commands[0] += ["upgrade", "0010" if component == "company" else "0006"]
            if component == "collector":
                commands.append(
                    [
                        sys.executable,
                        "-m",
                        "alembic",
                        "-c",
                        "alembic-source.ini",
                        "upgrade",
                        "sc0002",
                    ]
                )
            for command in commands:
                run = await asyncio.to_thread(
                    subprocess.run,
                    command,
                    cwd=root,
                    env={**os.environ, "DATABASE_URL": dsn},
                    capture_output=True,
                    timeout=180,
                )
                if run.returncode:
                    raise RuntimeError(
                        "migration failed; review database using private DBA tooling"
                    )
            result = await boundary(c, component, complete=True)
        if args.roles:
            await boundary(c, component, complete=True)
            scripts = (
                ["v11_destination_roles.sql"]
                if component == "company"
                else ["v11_source_sync_role.sql", "v11_source_collector_role.sql"]
            )
            async with c.transaction():
                for name in scripts:
                    await c.execute((root / "scripts/sql" / name).read_text("utf-8"))
                if args.passwords:
                    passwords = json.loads(args.passwords.read_text("utf-8"))
                    names = (
                        {"marketdata_v11_read", "marketdata_v11_ingest"}
                        if component == "company"
                        else {"marketdata_v11_collect", "marketdata_v11_sync"}
                    )
                    if set(passwords) != names:
                        raise ValueError("initial role password set does not match component")
                    for name, password in passwords.items():
                        if not re.fullmatch(r"[A-Za-z0-9_-]{43,128}", password):
                            raise ValueError("random initial role passwords required")
                        literal = await c.fetchval("SELECT quote_literal($1::text)", password)
                        await c.execute(f'ALTER ROLE "{name}" PASSWORD {literal}')
        elif args.passwords:
            raise ValueError("password provisioning requires new role creation, never rotation")
        if args.registry:
            if component != "company":
                raise ValueError(
                    "source registry is registered only by its explicitly enabled collector"
                )
            await boundary(c, component, complete=True)
            # Initial enrollment only: never silently change approved provider mappings.
            if await c.fetchval("SELECT count(*) FROM instruments"):
                raise ValueError(
                    "registry already exists; compare it with DBA tooling before changes"
                )
            settings: dict[str, Any] = dict(
                _env_file=None,
                database_url=SecretStr(dsn),
                application_role="api",
                api_environment="test",
                source_capture_enabled=False,
            )
            store = Store(Settings(**settings))
            try:
                await store.connect()
                await store.register(Registry.load(args.registry))
            finally:
                await store.close()
        if args.provision:
            if component != "company":
                raise ValueError("producer policy belongs only in the company database")
            await boundary(c, component, complete=True)
            await provision(c, json.loads(args.provision.read_text("utf-8")))
        if args.check:
            result = await boundary(c, component, complete=True)
        if component == "collector" and result["source_extension"]:
            result["source_epoch"] = str(
                await c.fetchval("SELECT source_epoch FROM source_capture_clock")
            )
        return result
    finally:
        await c.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--dsn-file", required=True, type=Path)
    parser.add_argument("--database", required=True)
    parser.add_argument("--migrate", action="store_true")
    parser.add_argument("--timescale", action="store_true")
    parser.add_argument("--roles", action="store_true")
    parser.add_argument("--passwords", type=Path)
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--provision", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(asyncio.run(execute(args))))
    except Exception as error:
        parser.exit(
            1, f"release administration rejected ({type(error).__name__}); inspect privately\n"
        )


if __name__ == "__main__":
    main()
