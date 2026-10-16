"""Fresh exported-CLI installations on guarded M5 disposable clusters only."""

# Test orchestration, never called by the application.
# ruff: noqa: ASYNC240

import argparse
import asyncio
import json
import os
import secrets
import subprocess
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg

ROOT = Path(__file__).resolve().parents[1]


async def main(args):
    config = json.loads(args.database_file.read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    private = args.output / "private"
    private.mkdir(mode=0o700)
    if os.name == "nt":
        import getpass

        await asyncio.to_thread(
            subprocess.run,
            [
                "icacls",
                str(private),
                "/inheritance:r",
                "/grant:r",
                getpass.getuser() + ":(OI)(CI)F",
            ],
            capture_output=True,
            check=True,
        )
    result = {}
    for component, database_role, package in [
        ("collector", "source", "marketdata-collector-sync-v1.1.0rc1"),
        ("company", "destination", "marketdata-fastapi-v1.1.0rc1"),
    ]:
        parsed = urlsplit(config[database_role])
        assert parsed.hostname == "127.0.0.1" and parsed.port in {55995, 55996, 55997, 55998}
        assert parsed.path == "/marketdata_v11_m5_test"
        name = "m5_package_" + uuid4().hex + "_test"
        dsn = urlunsplit(parsed._replace(path="/" + name))
        pkg = (args.install_root / package).resolve()
        python = (args.install_root / (component + "-env/Scripts/python.exe")).resolve()
        if os.name != "nt":
            python = (args.install_root / (component + "-env/bin/python")).resolve()
        admin = await asyncpg.connect(config[database_role])
        roles = (
            ["marketdata_v11_collect", "marketdata_v11_sync"]
            if component == "collector"
            else ["marketdata_v11_read", "marketdata_v11_ingest"]
        )
        for role in roles:
            assert not await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname=$1", role)
        await admin.execute(f'CREATE DATABASE "{name}"')
        secret = private / (component + "-dsn")
        secret.write_text(dsn)
        passwords = {role: secrets.token_urlsafe(32) for role in roles}
        password_file = private / (component + "-passwords.json")
        password_file.write_text(json.dumps(passwords))
        local = None
        try:
            command = [
                str(python),
                "-m",
                "marketdata.release_admin",
                "--root",
                str(pkg),
                "--dsn-file",
                str(secret.resolve()),
                "--database",
                name,
                "--migrate",
                "--roles",
                "--passwords",
                str(password_file.resolve()),
                "--check",
            ]
            if args.timescale:
                command.append("--timescale")
            completed = await asyncio.to_thread(
                subprocess.run, command, cwd=pkg, capture_output=True, timeout=120
            )
            (private / (component + "-install.log")).write_bytes(
                completed.stdout + completed.stderr
            )
            assert completed.returncode == 0, component + " exported fresh installation failed"
            state = json.loads(completed.stdout)
            assert state["base"] == ("0006" if component == "collector" else "0010")
            assert state["source_extension"] == ("sc0002" if component == "collector" else None)
            local = await asyncpg.connect(dsn)
            if component == "company":
                inventory = private / "source-inventory.json"
                inventory.write_text(json.dumps(result["collector"]["state"]))
                registry = private / "registry.json"
                registry.write_text(
                    json.dumps(json.loads((pkg / "config/instruments.mock.json").read_text())[:1])
                )
                plan = private / "initial-plan.json"
                planned = await asyncio.to_thread(
                    subprocess.run,
                    [
                        str(python),
                        str(pkg / "scripts/create_initial_plan.py"),
                        "--registry",
                        str(registry.resolve()),
                        "--source-inventory",
                        str(inventory.resolve()),
                        "--instrument",
                        "CRYPTO:BTCUSDT",
                        "--provider",
                        "mock",
                        "--producer-id",
                        str(uuid4()),
                        "--key-id",
                        "m5-fresh-test",
                        "--statuses",
                        "simulated",
                        "--output",
                        str(plan.resolve()),
                    ],
                    cwd=pkg,
                    capture_output=True,
                    timeout=30,
                )
                assert planned.returncode == 0
                provisioned = await asyncio.to_thread(
                    subprocess.run,
                    [
                        str(python),
                        "-m",
                        "marketdata.release_admin",
                        "--root",
                        str(pkg),
                        "--dsn-file",
                        str(secret.resolve()),
                        "--database",
                        name,
                        "--registry",
                        str(registry.resolve()),
                        "--provision",
                        str(plan.resolve()),
                        "--check",
                    ],
                    cwd=pkg,
                    capture_output=True,
                    timeout=60,
                )
                (private / "initial-provision.log").write_bytes(
                    provisioned.stdout + provisioned.stderr
                )
                assert provisioned.returncode == 0
                assert await local.fetchval("SELECT count(*) FROM ingest_producers") == 1
                assert await local.fetchval("SELECT count(*) FROM ingest_source_owners") == 4
            role_flags = []
            for role in roles:
                role_dsn = urlunsplit(
                    parsed._replace(
                        netloc=f"{role}:{passwords[role]}@{parsed.hostname}:{parsed.port}",
                        path="/" + name,
                    )
                )
                runtime = await asyncpg.connect(role_dsn)
                assert await runtime.fetchval("SELECT current_user") == role
                await runtime.close()
                flags = dict(
                    await local.fetchrow(
                        "SELECT rolname,rolsuper,rolinherit,rolcreatedb,rolcreaterole,"
                        "rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1",
                        role,
                    )
                )
                assert not any(value for key, value in flags.items() if key != "rolname")
                for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                    if role.endswith(("_read", "_sync")):
                        assert not await local.fetchval(
                            "SELECT has_table_privilege($1,'market_candles',$2)", role, privilege
                        )
                assert not await local.fetchval(
                    "SELECT has_schema_privilege($1,'public','CREATE')", role
                )
                role_flags.append(flags)
            if component == "company":
                assert not await local.fetchval("SELECT to_regclass('source_capture_clock')")
                assert not await local.fetchval(
                    "SELECT has_table_privilege($1,'ingest_source_owners','UPDATE')", roles[1]
                )
            else:
                assert not await local.fetchval("SELECT to_regclass('ingest_producers')")
                assert not await local.fetchval(
                    "SELECT has_column_privilege($1,'source_capture_clock',"
                    "'source_epoch','UPDATE')",
                    roles[0],
                )
                state["source_epoch"] = str(
                    await local.fetchval("SELECT source_epoch FROM source_capture_clock")
                )
            result[component] = dict(
                state=state,
                roles=role_flags,
                server=await local.fetchval("SELECT version()"),
                timescale=await local.fetchval(
                    "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
                ),
                cli_exit_code=completed.returncode,
            )
        finally:
            if local is not None:
                for role in roles:
                    await local.execute(f"DROP OWNED BY {role}")
                    await local.execute(f"DROP ROLE {role}")
                await local.close()
            # Retain the generated DB for evidence. Timescale internal background workers
            # can reconnect during FORCE drop. The labelled task cluster/volume is removed
            # only after all evidence is extracted, like the full pipeline fixtures.
            result.setdefault(component, {})["retained_task_database"] = name
            await admin.close()
    (args.output / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    print("Exported fresh installs, migration separation and runtime role restrictions passed")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--database-file", type=Path, required=True)
    p.add_argument("--install-root", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--timescale", action="store_true")
    asyncio.run(main(p.parse_args()))
