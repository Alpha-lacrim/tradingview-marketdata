"""Actual logical restore and task-container restart qualification; no live provider IO."""

# Bounded private task configuration, dump files and diagnostic markers.
# ruff: noqa: ASYNC240, ASYNC230

import asyncio
import json
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import asyncpg
from qualify_v11_pipeline import Pipeline, arguments, database_url, write_json

from marketdata.domain import utc_now


class TaskDocker:
    def __init__(self, config):
        self.prefix = config["docker_prefix"]
        self.failure_log = None
        self.persistent = False
        self.containers = {role: config[role + "_container"] for role in ("source", "destination")}

    async def run(self, *args, data=None):
        if args[0] == "restart" and not self.persistent:
            raise ValueError("real restart qualification requires verified task-owned volumes")
        result = await asyncio.to_thread(
            subprocess.run, [*self.prefix, *args], input=data, capture_output=True, timeout=60
        )
        if result.returncode:
            if self.failure_log is not None:
                self.failure_log.write_bytes(result.stderr[:65536])
            raise RuntimeError("task Docker command failed; inspect private runtime evidence")
        return result.stdout

    async def verify(self):
        persistent = []
        for name in self.containers.values():
            if not name.startswith("marketdata-v11-m4-") or not name.endswith("-test"):
                raise ValueError("refusing non-M4 container")
            value = json.loads(await self.run("inspect", name))[0]
            if value["Config"]["Labels"].get("marketdata.task") != "v11-m4":
                raise ValueError("refusing unlabelled container")
            durable = False
            for mount in value["Mounts"]:
                if mount["Type"] == "volume" and mount["Name"] == name + "-data":
                    volume = json.loads(await self.run("volume", "inspect", mount["Name"]))[0]
                    if volume["Labels"].get("marketdata.task") != "v11-m4":
                        raise ValueError("refusing unlabelled task volume")
                    durable = mount["Destination"] == "/var/lib/postgresql/data"
                elif mount["Type"] != "tmpfs":
                    raise ValueError("refusing unrelated mount")
            persistent.append(durable)
        self.persistent = all(persistent)
        if not self.persistent:
            raise ValueError("restart tests require task-owned persistent PGDATA volumes")

    async def dump(self, role, dsn):
        parsed = urlsplit(dsn)
        return await self.run(
            "exec",
            "-e",
            "PGPASSWORD=disposable_test",
            self.containers[role],
            "pg_dump",
            "-Fc",
            "-U",
            "m4_test",
            "-h",
            "127.0.0.1",
            "-p",
            str(parsed.port),
            "-d",
            parsed.path[1:],
        )

    async def restore(self, role, dsn, payload):
        if len(payload) > 134217728:
            raise ValueError("task restore exceeds 128 MiB bound")
        parsed = urlsplit(dsn)
        return await self.run(
            "exec",
            "-i",
            "-e",
            "PGPASSWORD=disposable_test",
            self.containers[role],
            "pg_restore",
            "--exit-on-error",
            "-U",
            "m4_test",
            "-h",
            "127.0.0.1",
            "-p",
            str(parsed.port),
            "-d",
            parsed.path[1:],
            data=payload,
        )


async def expected_stop(pipeline, reason, codes):
    path = Path(pipeline.configs["worker"]["trace"])
    before = path.stat().st_size if path.exists() else 0
    pipeline.launch("worker")
    process = pipeline.children["worker"]
    result = await asyncio.wait_for(asyncio.to_thread(process.wait), 30)
    pipeline.running.discard("worker")
    errors = [
        json.loads(line)
        for line in Path(pipeline.logs[-1].name).read_text("utf-8").splitlines()
        if line.startswith('{"qualification_process_error"')
    ]
    actual = errors[-1].get("diagnostic_codes", []) if errors else []
    pipeline.check(
        reason, result != 0 and bool(set(actual) & set(codes)), exit_code=result, codes=actual
    )
    # Failure must occur before any financial transport or local receipt mutation.
    if path.exists():
        with path.open("rb") as incoming:
            incoming.seek(before)
            rows = [json.loads(line) for line in incoming if line.endswith(b"\n")]
        pipeline.check(
            reason + " rejects financial upload",
            not any(
                r.get("method") == "POST" and r.get("operation") in ("candles", "quotes")
                for r in rows
            ),
        )


async def run(args):
    if args.genuine or args.feeds != 1:
        raise ValueError("restore qualification is injected and bounded to one feed")
    pipeline = Pipeline(args)
    clones = []
    docker = TaskDocker(json.loads(args.database_file.read_text("utf-8")))
    await docker.verify()
    try:
        await pipeline.initialize()
        docker.failure_log = pipeline.private / "docker-error.log"
        await pipeline.acceptance()
        for role in ("worker", "collector"):
            await pipeline.stop(role)
        backups = {}
        for role in ("source", "destination"):
            backups[role] = await docker.dump(role, getattr(pipeline, role + "_dsn"))
            (pipeline.private / f"{role}.dump").write_bytes(backups[role])
        original_anchor = (pipeline.private / "anchor.json").read_bytes()
        pipeline.check("consistent source/destination logical backups", all(backups.values()))

        # Advance the original databases beyond the backup, preserving the external anchor.
        old = pipeline.candles[0]
        pipeline.candles[0] = old.model_copy(
            update=dict(close=old.open + 1, received_timestamp=utc_now())
        )
        write_json(pipeline.candles_path, [b.model_dump(mode="json") for b in pipeline.candles])
        pipeline.quotes(8000)
        pipeline.launch("collector")
        await pipeline.launch_worker()

        async def advanced():
            return (
                await pipeline.count("destination", "candle_revisions") >= 2
                and await pipeline.destination_caught_up()
            )

        await pipeline.wait("post-backup changes committed", advanced)
        for role in ("worker", "collector", "ingest", "read"):
            await pipeline.stop(role)
        pipeline.ws_task.cancel()
        await asyncio.gather(pipeline.ws_task, return_exceptions=True)
        anchor_path = pipeline.private / "anchor.json"
        advanced_anchor = anchor_path.read_bytes()
        source_count = await pipeline.count("source", "market_candles")
        for role in ("source", "destination"):
            original = getattr(pipeline, role + "_dsn")
            name = urlsplit(original).path[1:].replace("_test", "_clone_test")
            c = pipeline.connections[role]
            await c.execute(f'CREATE DATABASE "{name}"')
            dsn = database_url(original, name)
            clones.append((role, name, dsn))
            if args.timescale:
                clone = await asyncpg.connect(dsn)
                try:
                    await clone.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
                    await clone.execute("SELECT timescaledb_pre_restore()")
                finally:
                    await clone.close()
            await docker.restore(role, dsn, backups[role])
            clone = await asyncpg.connect(dsn)
            try:
                if args.timescale:
                    await clone.execute("SELECT timescaledb_post_restore()")
                await clone.execute(f'REVOKE CREATE,TEMPORARY ON DATABASE "{name}" FROM PUBLIC')
                if role == "destination":
                    await clone.execute(
                        f'GRANT TEMPORARY ON DATABASE "{name}" TO marketdata_v11_ingest'
                    )
            finally:
                await clone.close()
        pipeline.check(
            "actual pg_restore completed for both databases",
            True,
            clones=[name for _, name, _ in clones],
        )

        source_clone = next(dsn for role, _, dsn in clones if role == "source")
        (pipeline.private / "dsn").write_text(
            database_url(source_clone, role="marketdata_v11_sync"), encoding="utf-8"
        )
        await expected_stop(
            pipeline,
            "source restore stops against newer external anchor",
            ["source_restore_or_identity_mismatch"],
        )
        anchor_path.unlink()
        await expected_stop(
            pipeline, "source clone without trusted anchor stops", ["external_anchor_missing"]
        )
        anchor_path.write_bytes(original_anchor)
        # A self-consistent older source+anchor must still detect the company's newer high water.
        pipeline.launch("ingest")
        await pipeline.wait("original destination ready", lambda: pipeline.health(pipeline.ingest))
        await expected_stop(
            pipeline,
            "old source and old anchor detect newer destination",
            ["source_restore_high_water_mismatch"],
        )
        await pipeline.stop("ingest")
        anchor_path.write_bytes(advanced_anchor)
        (pipeline.private / "dsn").write_text(
            database_url(pipeline.source_dsn, role="marketdata_v11_sync"), encoding="utf-8"
        )

        original_ingest_dsn = pipeline.configs["ingest"]["settings"]["database_url"]
        destination_clone = next(dsn for role, _, dsn in clones if role == "destination")
        pipeline.configs["ingest"]["settings"]["database_url"] = database_url(
            destination_clone, role="marketdata_v11_ingest"
        )
        pipeline.launch("ingest")
        await pipeline.wait("restored destination ready", lambda: pipeline.health(pipeline.ingest))
        await expected_stop(
            pipeline,
            "destination rollback stops despite retained export markers",
            ["destination_rollback_requires_review", "destination_receipt_missing_requires_review"],
        )
        await pipeline.stop("ingest")
        pipeline.configs["ingest"]["settings"]["database_url"] = original_ingest_dsn
        pipeline.launch("ingest")
        pipeline.launch("read")
        await pipeline.wait("original read ready", lambda: pipeline.health(pipeline.reader))
        await pipeline.wait("original ingest ready", lambda: pipeline.health(pipeline.ingest))

        # Receipt/export markers remain intact while actual destination rows are missing.
        dest = pipeline.connections["destination"]
        before_receipts = await pipeline.count("destination", "ingest_batches")
        await dest.execute("DELETE FROM latest_prices")
        await dest.execute(
            "DELETE FROM market_candles WHERE bucket_start="
            "(SELECT min(bucket_start) FROM market_candles)"
        )
        pipeline.configs["worker"].update(reconcile_all=2, repair=False)
        pipeline.launch("worker")
        result = await asyncio.wait_for(asyncio.to_thread(pipeline.children["worker"].wait), 40)
        pipeline.running.discard("worker")
        findings = [
            json.loads(line)["findings"]
            for line in (pipeline.directory / "worker.ndjson").read_text("utf-8").splitlines()
            if json.loads(line)["kind"] == "reconciliation"
        ]
        pipeline.check(
            "reconciliation inspects actual missing financial rows",
            result == 0 and any(findings),
            findings=findings,
        )
        pipeline.check(
            "missing data not hidden by receipts",
            await pipeline.count("destination", "ingest_batches") == before_receipts,
        )
        pipeline.configs["worker"].update(reconcile_all=2, repair=True)
        pipeline.launch("worker")
        result = await asyncio.wait_for(asyncio.to_thread(pipeline.children["worker"].wait), 40)
        pipeline.running.discard("worker")
        pipeline.check("explicit bounded resnapshot staged", result == 0)
        pipeline.configs["worker"].pop("reconcile_all")
        pipeline.configs["worker"].pop("repair")
        await pipeline.launch_worker()
        await pipeline.wait("actual repaired destination contents", pipeline.destination_caught_up)
        await pipeline.parity()

        # Restart only positively identified, tmpfs task databases.
        for role in ("destination", "source"):
            if role == "source":
                await pipeline.stop("worker")
            await pipeline.connections[role].close()
            await docker.run("restart", docker.containers[role])
            dsn = getattr(pipeline, role + "_dsn")
            async with asyncio.timeout(20):
                while True:
                    try:
                        pipeline.connections[role] = await asyncpg.connect(dsn)
                        break
                    except (OSError, asyncpg.PostgresError, asyncpg.InterfaceError):
                        await asyncio.sleep(0.2)
            if role == "source":
                await pipeline.launch_worker()
            await pipeline.wait(role + " DB restart recovery", pipeline.destination_caught_up)
            pipeline.check(
                role + " real database restart preserves history",
                await pipeline.count(role, "market_candles") == source_count,
            )
        await pipeline.parity()
        pipeline.report["completed_at"] = utc_now()
    except BaseException as error:
        import traceback

        pipeline.report.update(
            failure_category=type(error).__name__,
            failure_frames=[
                dict(file=Path(f.filename).name, line=f.lineno, function=f.name)
                for f in traceback.extract_tb(error.__traceback__)
            ],
        )
        write_json(pipeline.directory / "report.json", pipeline.report)
        raise
    finally:
        for role in list(pipeline.children):
            await pipeline.stop(role)
        for role, name, _ in clones:
            # Names derive only from this run's generated, validated UUID database.
            assert (
                name.startswith("m4_")
                and name.endswith("_clone_test")
                and name.replace("_", "").isalnum()
            )
            c = pipeline.connections[role]
            if c.is_closed():
                c = await asyncpg.connect(getattr(pipeline, role + "_dsn"))
                pipeline.connections[role] = c
            await c.execute(f'DROP DATABASE "{name}" WITH (FORCE)')
        await pipeline.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(run(arguments()))
    except Exception as error:
        print(json.dumps(dict(recovery_qualification_failed=type(error).__name__)), flush=True)
        raise SystemExit(1) from None
