"""Verify exported images + freshly installed source package using M4's injected-data oracle."""

# Test-only orchestration; bounded private files. No live TradingView connection.
# ruff: noqa: ASYNC240

import argparse
import asyncio
import json
import os
import subprocess
import time
import traceback
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import httpx
import qualify_v11_pipeline as q


def linux_path(path):
    path = Path(path).resolve().as_posix()
    if os.name == "nt":
        return "/mnt/" + path[0].lower() + path[2:]
    return path


original_guard = q.task_database


def task_guard(dsn):
    parsed = urlsplit(dsn)
    if parsed.path == "/marketdata_v11_m5_test":
        original_guard(dsn.replace("/marketdata_v11_m5_test", "/marketdata_v11_m4_test"))
        return parsed
    return original_guard(dsn)


class ReleasePipeline(q.Pipeline):
    async def initialize(self):
        await super().initialize()
        self.container_names = {}
        self.role_logfiles = {}
        self.run_id = uuid4().hex[:12]
        self.prefix = json.loads(self.args.database_file.read_text())["docker_prefix"]
        c = self.connections["source"]
        await c.execute(
            (self.args.collector_package / "scripts/sql/v11_source_collector_role.sql").read_text()
        )
        await c.execute("ALTER ROLE marketdata_v11_collect PASSWORD 'disposable_test'")
        self.configs["collector"]["settings"]["database_url"] = q.database_url(
            self.source_dsn, role="marketdata_v11_collect"
        )
        self.durable = self.private / "worker-durable"
        self.durable.mkdir()
        config = json.loads(self.sync_path.read_text())
        config.update(
            source_dsn_file="/run/secrets/source_dsn",
            token_file="/run/secrets/token",
            ca_file="/run/secrets/ca.pem",
            anchor_file="/durable/anchor.json",
        )
        self.linux_sync = self.private / "linux-sync.json"
        q.write_json(self.linux_sync, config)
        self.report["release_images"] = {
            "company": self.args.company_image,
            "collector": self.args.collector_image,
        }

    def mount(self, path, target, *, readonly=True):
        return [
            "--mount",
            f"type=bind,src={linux_path(path)},dst={target}" + (",readonly" if readonly else ""),
        ]

    def launch(self, role):
        assert role not in self.children or self.children[role].poll() is not None
        if role == "collector":
            q.write_json(self.private / "collector.json", self.configs[role])
            log = (self.private / f"collector-{time.time_ns()}.log").open("wb")
            self.logs.append(log)
            self.children[role] = subprocess.Popen(
                [
                    str(self.args.collector_python),
                    str(q.ROOT / "scripts/v11_qualification_process.py"),
                    str(self.private / "collector.json"),
                ],
                cwd=q.ROOT,
                env={**os.environ, "PYTHONUTF8": "1"},
                stdout=log,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            self.running.add(role)
            return
        name = f"marketdata-v11-m5-{self.run_id}-{role}"
        self.container_names[role] = name
        command = [
            *self.prefix,
            "run",
            "--rm",
            "--name",
            name,
            "--label",
            "marketdata.task=v11-m5",
            "--label",
            "marketdata.run=" + self.run_id,
            "--network",
            "host",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--no-healthcheck",
        ]
        if role == "worker":
            command += ["--entrypoint", "python"]
            for path, target in [
                (self.linux_sync, "/run/secrets/worker.json"),
                (self.private / "dsn", "/run/secrets/source_dsn"),
                (self.private / "token", "/run/secrets/token"),
                (Path(self.configs["read"]["cert"]), "/run/secrets/ca.pem"),
            ]:
                command += self.mount(path, target)
            command += self.mount(self.durable, "/durable", readonly=False)
            command += [
                self.args.collector_image,
                "-m",
                "marketdata.sync_worker",
                "--config",
                "/run/secrets/worker.json",
            ]
        else:
            config = self.configs[role]
            settings = config["settings"]
            dsn_file = self.private / (role + "-dsn")
            dsn_file.write_text(settings["database_url"])
            command += self.mount(dsn_file, "/run/secrets/database_url")
            command += self.mount(config["cert"], "/run/secrets/tls_cert")
            command += self.mount(config["key"], "/run/secrets/tls_key")
            if role == "read":
                key = self.private / "read-key"
                key.write_text(self.read_key)
                command += self.mount(key, "/run/secrets/api_key")
                command += ["-e", "WEBSOCKET_ALLOWED_ORIGINS=" + json.dumps([q.ORIGIN])]
            else:
                command += self.mount(
                    self.private / "credentials.json", "/run/secrets/ingest_credentials.json"
                )
                command += ["-e", "INGEST_CREDENTIALS_FILE=/run/secrets/ingest_credentials.json"]
            command += [
                "-e",
                "API_WORKERS=2",
                "-e",
                "MARKETDATA_POLL_SECONDS=0.05",
                self.args.company_image,
                role,
                "--bind-host",
                "127.0.0.1",
                "--port",
                str(config["port"]),
                "--tls-cert-file",
                "/run/secrets/tls_cert",
                "--tls-key-file",
                "/run/secrets/tls_key",
            ]
        log = (self.private / f"{role}-{time.time_ns()}.log").open("wb")
        self.role_logfiles[role] = Path(log.name)
        self.logs.append(log)
        self.children[role] = subprocess.Popen(
            command,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.running.add(role)

    async def stop(self, role):
        if role == "collector" and os.name == "nt":
            # Windows venv launchers can have a different PID from the interpreter.
            # Stop only a confirmed test driver using this run's private config.
            marker = Path(self.configs[role]["pid"])
            if marker.exists():
                pid = int(marker.read_text())
                expected = str(self.private / "collector.json").replace("'", "''")
                script = (
                    "$item=Get-CimInstance Win32_Process -Filter 'ProcessId=" + str(pid) + "';"
                    "if($item) { if($item.Name -ne 'python.exe' -or "
                    "$item.CommandLine.IndexOf('" + expected + "',"
                    "[StringComparison]::OrdinalIgnoreCase) -lt 0 -or "
                    "$item.CommandLine -notmatch 'v11_qualification_process.py') "
                    "{throw 'Refusing unrelated process'};Stop-Process -Id $item.ProcessId -Force }"
                )
                result = await asyncio.to_thread(
                    subprocess.run,
                    ["powershell.exe", "-NoProfile", "-Command", script],
                    capture_output=True,
                    timeout=15,
                )
                if result.returncode:
                    raise RuntimeError("owned collector cleanup rejected")
        name = getattr(self, "container_names", {}).get(role)
        process = self.children.get(role)
        if name and process is not None and process.poll() is None:
            inspection = await asyncio.to_thread(
                subprocess.run, [*self.prefix, "inspect", name], capture_output=True
            )
            if inspection.returncode == 0:
                value = json.loads(inspection.stdout)[0]
                assert value["Config"]["Labels"].get("marketdata.run") == self.run_id
                assert value["Config"]["Labels"].get("marketdata.task") == "v11-m5"
                await asyncio.to_thread(
                    subprocess.run,
                    [*self.prefix, "stop", "-t", "5", name],
                    capture_output=True,
                    timeout=15,
                )
        await super().stop(role)

    async def cleanup(self):
        for role in list(self.children):
            await self.stop(role)
        if "source" in self.connections and hasattr(self, "run_id"):
            c = self.connections["source"]
            await c.execute("DROP OWNED BY marketdata_v11_collect")
            await c.execute("DROP ROLE marketdata_v11_collect")
        await super().cleanup()


async def main(args):
    q.task_database = task_guard
    args.quote_hz, args.burst_hz = 1, 5
    args.feeds, args.history_minutes = 1, 3
    args.status_ttl_seconds = 60
    args.genuine = args.faults = args.quotes_during_backfill = False
    args.soak_seconds, args.burst_seconds = 15, 0
    pipeline = ReleasePipeline(args)
    try:
        await pipeline.initialize()
        await pipeline.acceptance()
        smoke = await asyncio.to_thread(
            subprocess.run,
            [
                str(args.company_python),
                str(args.company_package / "scripts/smoke_read.py"),
                "--endpoint",
                pipeline.read_url,
                "--token-file",
                str(pipeline.private / "read-key"),
                "--ca-file",
                str(pipeline.private / "cert.pem"),
                "--symbol",
                pipeline.pairs[0][0].canonical_symbol,
                "--provider",
                pipeline.pairs[0][1].provider,
                "--origin",
                q.ORIGIN,
            ],
            capture_output=True,
            timeout=45,
        )
        (pipeline.private / "shipped-smoke.log").write_bytes(smoke.stdout + smoke.stderr)
        pipeline.check("shipped HTTPS REST ChartData WSS smoke", smoke.returncode == 0)
        boundaries = {}
        for role, name in pipeline.container_names.items():
            inspected = await asyncio.to_thread(
                subprocess.run,
                [*pipeline.prefix, "inspect", name],
                capture_output=True,
                check=True,
            )
            value = json.loads(inspected.stdout)[0]
            boundaries[role] = dict(
                image=value["Image"],
                user=value["Config"]["User"],
                readonly_root=value["HostConfig"]["ReadonlyRootfs"],
                capabilities_dropped=value["HostConfig"]["CapDrop"],
                security_options=value["HostConfig"]["SecurityOpt"],
                mounts=[dict(target=m["Destination"], writable=m["RW"]) for m in value["Mounts"]],
                command=value["Config"]["Cmd"],
            )
            assert value["Config"]["User"] == "appuser"
            assert value["HostConfig"]["ReadonlyRootfs"]
        pipeline.report["container_security_boundaries"] = boundaries
        await pipeline.stop("worker")
        ticks = pipeline.quotes(51)
        await pipeline.wait(
            "source commits while worker stopped", lambda: pipeline.quotes_committed(ticks)
        )
        pipeline.check(
            "source retains newest state while worker stopped",
            not await pipeline.destination_caught_up(),
        )
        await pipeline.launch_worker()
        await pipeline.wait("packaged worker durable restart", pipeline.destination_caught_up)
        await pipeline.parity()
        for role in ("ingest", "read"):
            await pipeline.stop(role)
            other = pipeline.reader if role == "ingest" else pipeline.ingest
            pipeline.check(
                "independent role remains healthy: " + role, await pipeline.health(other)
            )
            pipeline.launch(role)
            await pipeline.wait(
                "role restart: " + role,
                lambda current=role: pipeline.health(
                    pipeline.ingest if current == "ingest" else pipeline.reader
                ),
            )
            # An intentional server restart invalidates pooled HTTP connections.
            # Reconnect the read-only test observer, as a frontend client must do.
            previous = pipeline.ingest if role == "ingest" else pipeline.reader
            await previous.aclose()
            replacement = httpx.AsyncClient(
                base_url=pipeline.ingest_url if role == "ingest" else pipeline.read_url,
                verify=pipeline.context,
                trust_env=False,
                headers={
                    "Authorization": "Bearer "
                    + (pipeline.token if role == "ingest" else pipeline.read_key)
                },
                timeout=10,
            )
            if role == "ingest":
                pipeline.ingest = replacement
            else:
                pipeline.reader = replacement
        await asyncio.gather(pipeline.ws_task, return_exceptions=True)
        pipeline.ws_task = asyncio.create_task(pipeline.ws_consumer())
        await pipeline.soak(15, 1, "packaged-steady")
        await pipeline.finish()
        await pipeline.stop("worker")
        config = json.loads(args.database_file.read_text())
        for role in ("source", "destination"):
            name = config[role + "_container"]
            assert name.startswith("marketdata-v11-m5-") and name.endswith("-test")
            inspection = await asyncio.to_thread(
                subprocess.run, [*pipeline.prefix, "inspect", name], capture_output=True, check=True
            )
            value = json.loads(inspection.stdout)[0]
            assert value["Config"]["Labels"].get("marketdata.task") == "v11-m5"
            assert all(
                m["Type"] == "volume" and m["Name"] == name + "-data" for m in value["Mounts"]
            )
            await asyncio.to_thread(
                subprocess.run,
                [*pipeline.prefix, "restart", name],
                capture_output=True,
                check=True,
                timeout=30,
            )
            await pipeline.connections[role].close()

            async def reconnect(current=role):
                try:
                    pipeline.connections[current] = await asyncpg.connect(
                        getattr(pipeline, current + "_dsn")
                    )
                    return True
                except (OSError, asyncpg.PostgresError):
                    return False

            await pipeline.wait("database restart: " + role, reconnect)
        await pipeline.wait("read after database restart", lambda: pipeline.health(pipeline.reader))
        await pipeline.wait(
            "ingest after database restart", lambda: pipeline.health(pipeline.ingest)
        )
        await pipeline.launch_worker()

        async def worker_metrics():
            return '"session_lease_seconds"' in pipeline.role_logfiles["worker"].read_text(
                encoding="utf-8", errors="replace"
            )

        await pipeline.wait("worker completes cycle without live collector", worker_metrics)
        await pipeline.wait("worker without live collector", pipeline.destination_caught_up)
        (await pipeline.reader.get("/api/v1/prices/latest")).raise_for_status()
        (await pipeline.ingest.get("/api/ingest/v1/health")).raise_for_status()
        await pipeline.parity()
        await pipeline.snapshot("post-database-restart")
        pipeline.check(
            "packaged roles survive actual database restart without active collector", True
        )
        pipeline.report["completed_at"] = q.utc_now()
    except BaseException as error:
        pipeline.report["failure_category"] = type(error).__name__
        pipeline.report["failure_frames"] = [
            dict(file=Path(f.filename).name, line=f.lineno, function=f.name)
            for f in traceback.extract_tb(error.__traceback__)
        ]
        if pipeline.private.exists():
            (pipeline.private / "failure.log").write_text(traceback.format_exc())
        raise
    finally:
        await pipeline.cleanup()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--database-file", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--company-image", required=True)
    p.add_argument("--collector-image", required=True)
    p.add_argument("--collector-package", type=Path, required=True)
    p.add_argument("--collector-python", type=Path, required=True)
    p.add_argument("--company-package", type=Path, required=True)
    p.add_argument("--company-python", type=Path, required=True)
    p.add_argument("--timescale", action="store_true")
    try:
        asyncio.run(main(p.parse_args()))
    except Exception as error:
        p.exit(1, f"release pipeline failed ({type(error).__name__}); inspect private test logs\n")
