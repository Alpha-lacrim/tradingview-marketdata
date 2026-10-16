"""Private-file configured task processes. Test hooks never enter application code."""

# Bounded task-owned trace/config files; fault markers must precede os._exit.
# ruff: noqa: ASYNC240

import asyncio
import importlib
import json
import os
import ssl
import sys
import traceback
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn
from v11_qualification_source import InjectedCandles, InjectedQuotes, record, tracing_store

from marketdata.config import Settings
from marketdata.domain import utc_now
from marketdata.persistence import Store
from marketdata.source_capture import CaptureConflict
from marketdata.sync_config import SyncConfig
from marketdata.sync_http import SyncProblem
from marketdata.sync_worker import SyncWorker


def diagnostic_codes(error):
    if isinstance(error, BaseExceptionGroup):
        return [code for child in error.exceptions for code in diagnostic_codes(child)]
    if isinstance(error, SyncProblem):
        return [error.code]
    if isinstance(error, CaptureConflict):
        return [
            {
                "source restore or identity mismatch requires review": (
                    "source_restore_or_identity_mismatch"
                ),
                "external continuity anchor is missing": "external_anchor_missing",
                "configured source epoch mismatch": "configured_epoch_mismatch",
                "worker requires a restricted source synchronization role": (
                    "restricted_role_required"
                ),
            }.get(str(error), "capture_conflict")
        ]
    return [type(error).__name__]


def private(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def check_database(dsn):
    parsed = urlsplit(dsn)
    if (
        parsed.hostname != "127.0.0.1"
        or parsed.port not in {55995, 55996, 55997, 55998, 56005, 56006, 56007, 56008}
        or not parsed.path.removeprefix("/").startswith(("marketdata_v11_m4_", "m4_"))
        or not parsed.path.endswith("_test")
    ):
        raise ValueError("qualification processes require loopback task databases ending _test")


async def collector(config):
    import marketdata.collector as module

    settings = Settings(_env_file=None, **config["settings"])
    trace = Path(config["trace"])
    if not config.get("genuine"):

        async def providers(settings, registry, metrics):
            candles = InjectedCandles(Path(config["candles"]), trace, metrics)
            quotes = InjectedQuotes(Path(config["quotes"]), trace, metrics)
            pairs = registry.subscriptions("tradingview_guest")
            await candles.subscribe(pairs)
            await quotes.subscribe(pairs)
            return [candles, quotes]

        module.build_providers = providers
    module.Store = tracing_store(trace)
    async with module.collector_lifespan(settings):
        Path(config["ready"]).write_text(utc_now().isoformat(), encoding="utf-8")
        await asyncio.Event().wait()


async def worker(config):
    sync = SyncConfig.load(Path(config["sync_config"]))
    dsn = sync.source_dsn_file.read_text(encoding="utf-8").strip()
    check_database(dsn)
    store = Store(
        Settings(
            _env_file=None,
            database_url=dsn,
            application_role="collector",
            source_capture_enabled=True,
        )
    )
    trace = Path(config["trace"])

    class ObservedWorker(SyncWorker):
        current_stream = None
        last_batch = None

        async def stage(self, stream):
            self.current_stream = stream
            return await super().stage(stream)

        async def deliver(self, stream):
            self.current_stream = stream
            pending = await self.local.pending(stream)
            self.last_batch = str(pending["batch_id"]) if pending else None
            return await super().deliver(stream)

        async def checkpoint(self, boundary):
            pending = await self.local.pending(self.current_stream) if self.current_stream else None
            batch_id = str(pending["batch_id"]) if pending else self.last_batch
            record(
                trace,
                "worker_boundary",
                boundary=boundary,
                stream=self.current_stream,
                batch_id=batch_id,
            )
            if boundary == "after_staging_commit" and pending:
                body = json.loads(pending["payload"])
                record(
                    trace,
                    "immutable_batch_staged",
                    batch_id=batch_id,
                    payload_sha256=pending["payload_sha256"],
                    stream=body["stream"],
                    records=[
                        dict(
                            observation_id=r["observation_id"],
                            source_position=r["source_position"],
                            kind=r["kind"],
                            instrument_id=r["observation"]["instrument_id"],
                            source_timestamp=r["observation"].get("event_timestamp"),
                            bucket_start=r["observation"].get("bucket_start"),
                            bucket_end=r["observation"].get("bucket_end"),
                            received_timestamp=r["observation"].get("received_timestamp"),
                            field_observations=r["observation"]
                            .get("metadata", {})
                            .get("field_observations", {}),
                        )
                        for r in body["records"]
                    ],
                )
            fault = private(config["fault"]) if Path(config["fault"]).exists() else {}
            if fault.get("crash_at") == boundary:
                Path(config["marker"]).write_text(
                    json.dumps(dict(boundary=boundary, batch_id=batch_id)), encoding="utf-8"
                )
                os._exit(81)

        async def request(self, method, path, payload=None, **kwargs):
            started = utc_now()
            try:
                result = await super().request(method, path, payload, **kwargs)
            except Exception as error:
                record(
                    trace,
                    "worker_http_error",
                    method=method,
                    operation=path.split("/")[1],
                    started_at=started,
                    category=getattr(error, "code", type(error).__name__),
                )
                raise
            record(
                trace,
                "worker_http_result",
                method=method,
                operation=path.split("/")[1],
                started_at=started,
                batch_id=result.get("batch_id"),
                state=result.get("state"),
                payload_bytes=len(payload) if payload else 0,
            )
            return result

    instance = ObservedWorker(store, sync)
    try:
        await store.connect()
        if config.get("reconcile_all"):
            async with instance.capture.consumer_lock() as connection:
                await instance.local.claim(connection)
                instance.state = await instance.local.initialize()
                await instance.session()
                await instance.continuity()
                for _ in range(config["reconcile_all"]):
                    record(
                        trace,
                        "reconciliation",
                        findings=await instance.reconciliation(repair=config.get("repair", False)),
                    )
        else:
            await instance.supervise(once=config.get("once", False))
    finally:
        await instance.http.close()
        await store.close()


def server(config):
    import marketdata.ingest_api as ingestion
    from marketdata.api import create_app
    from marketdata.replication import DestinationWriter

    trace = Path(config["trace"])
    settings = Settings(_env_file=None, **config["settings"])
    if settings.application_role == "ingest":
        original = DestinationWriter.commit

        async def commit(self, producer, payload, *args, **kwargs):
            started = utc_now()
            result = await original(self, producer, payload, *args, **kwargs)
            record(
                trace,
                "destination_commit_observed",
                started_at=started,
                batch_id=result["batch_id"],
                stream=result["stream"],
                counts=result["counts"],
            )
            fault = private(config["fault"]) if Path(config["fault"]).exists() else {}
            if fault.get("pause_after_upload_commit"):
                Path(config["marker"]).write_text(result["batch_id"], encoding="utf-8")
                await asyncio.Event().wait()
            return result

        DestinationWriter.commit = commit
        original_session = ingestion.session
        original_recover = ingestion.recover_session

        async def session(*args, **kwargs):
            result = await original_session(*args, **kwargs)
            fault = private(config["fault"]) if Path(config["fault"]).exists() else {}
            if fault.get("pause_after_renewal") and kwargs.get("renew"):
                Path(config["marker"]).write_text("renewed", encoding="utf-8")
                await asyncio.Event().wait()
            return result

        async def recover(*args, **kwargs):
            result = await original_recover(*args, **kwargs)
            fault = private(config["fault"]) if Path(config["fault"]).exists() else {}
            if fault.get("pause_after_recovery"):
                Path(config["marker"]).write_text("recovered", encoding="utf-8")
                await asyncio.Event().wait()
            return result

        ingestion.session, ingestion.recover_session = session, recover
        app = ingestion.create_app(settings)
    else:
        app = create_app(settings)
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=config["port"],
        access_log=False,
        log_level="error",
        proxy_headers=False,
        ssl_certfile=config["cert"],
        ssl_keyfile=config["key"],
        timeout_keep_alive=30,
        timeout_graceful_shutdown=2,
    )


def main():
    config = private(sys.argv[1])
    Path(config["pid"]).write_text(str(os.getpid()), encoding="utf-8")
    # Workstation truststore injection is client-only; retain normal verifying TLS.
    if ssl.SSLContext.__module__.endswith("truststore._api"):
        ssl.SSLContext = importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext
    if config["role"] != "worker":
        check_database(config["settings"]["database_url"])
    if config["role"] == "collector":
        asyncio.run(collector(config))
    elif config["role"] == "worker":
        asyncio.run(worker(config))
    else:
        server(config)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            json.dumps(
                dict(
                    qualification_process_error=type(error).__name__,
                    diagnostic_codes=diagnostic_codes(error),
                    frames=[
                        dict(file=Path(f.filename).name, line=f.lineno, function=f.name)
                        for f in traceback.extract_tb(error.__traceback__)
                    ],
                )
            ),
            flush=True,
        )
        raise SystemExit(1) from None
