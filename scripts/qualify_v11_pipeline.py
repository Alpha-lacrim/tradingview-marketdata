"""Bounded, opt-in qualification against disposable loopback PostgreSQL clusters.

This is a test harness, not a collector deployment or a second sync implementation.
All observations are visibly simulated unless --genuine is explicitly selected.
Secrets are read from a private file; report files contain no credentials.
"""

# Bounded task-owned trace/config files; synchronous fault markers survive os._exit.
# ruff: noqa: ASYNC240

import argparse
import asyncio
import hashlib
import importlib
import json
import os
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import time
import traceback
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import asyncpg
import httpx
from v11_qualification_resources import process_counters
from websockets.asyncio.client import connect

from marketdata.candles import MarketCandle, minute_start
from marketdata.config import Settings
from marketdata.domain import Tick, utc_now
from marketdata.persistence import Store
from marketdata.reconciliation import candle_fingerprint
from marketdata.registry import Registry

ROOT = Path(__file__).resolve().parents[1]
ORIGIN = "https://frontend.example.com"


def write_json(path, value):
    temporary = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    temporary.write_text(json.dumps(value, default=str, indent=2), encoding="utf-8")
    temporary.replace(path)


def task_database(dsn):
    parsed = urlsplit(dsn)
    if (
        parsed.hostname != "127.0.0.1"
        or parsed.port not in {55995, 55996, 55997, 55998, 56005, 56006, 56007, 56008}
        or not parsed.path.removeprefix("/").startswith(("marketdata_v11_m4_", "m4_"))
        or not parsed.path.endswith("_test")
    ):
        raise ValueError("requires the documented task-owned M4 loopback clusters/databases")
    return parsed


def database_url(dsn, database=None, role=None):
    parsed = task_database(dsn)
    return urlunsplit(
        parsed._replace(
            path="/" + database if database else parsed.path,
            netloc=f"{role}:disposable_test@127.0.0.1:{parsed.port}" if role else parsed.netloc,
        )
    )


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def business_registry(count, genuine=False):
    rows = json.loads(
        (ROOT / "config/instruments.tradingview-business-proposal.json").read_text("utf-8")
    )
    assert len(rows) == 99, "business universe changed; review qualification profile"
    if genuine:
        rows = [r for r in rows if r["instrument"]["canonical_symbol"] == "CRYPTO:BTCUSDT"]
        assert len(rows) == count == 1, "genuine qualification is bounded to one authorized feed"
    else:
        rows = rows[:count]
        for row in rows:
            for mapping in row["mappings"]:
                mapping["realtime_status"] = "simulated"
    return rows


def percentiles(values):
    ordered = sorted(values)
    return {
        "count": len(values),
        **{
            f"p{p}": ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p / 100))]
            if ordered
            else None
            for p in (50, 95, 99, 100)
        },
    }


class Pipeline:
    def __init__(self, args):
        self.args, self.directory = args, args.output.resolve()
        self.private = self.directory / "private"
        self.children, self.configs, self.logs = {}, {}, []
        self.running = set()
        self.connections, self.databases = {}, []
        self.samples, self.ws_events, self.results = [], [], []
        self.report = dict(
            evidence="isolated genuine" if args.genuine else "isolated injected",
            parameters={k: v for k, v in vars(args).items() if k != "database_file"},
            started_at=utc_now(),
            checks=self.results,
        )

    def check(self, name, passed, **evidence):
        self.results.append(dict(name=name, passed=bool(passed), at=utc_now(), **evidence))
        write_json(self.directory / "report.json", self.report)
        print(json.dumps(dict(check=name, passed=bool(passed)), default=str), flush=True)
        if not passed:
            raise AssertionError(name)

    def launch(self, role):
        assert role not in self.children or self.children[role].poll() is not None
        logfile = (self.private / f"{role}-{time.time_ns()}.log").open("wb")
        self.logs.append(logfile)
        config_path = self.private / f"{role}.json"
        write_json(config_path, self.configs[role])
        self.children[role] = subprocess.Popen(
            [sys.executable, str(ROOT / "scripts/v11_qualification_process.py"), str(config_path)],
            cwd=ROOT,
            stdout=logfile,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUTF8": "1"},
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        self.running.add(role)

    async def stop(self, role):
        self.running.discard(role)
        process = self.children.get(role)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(process.wait), 8)
            except TimeoutError:
                process.kill()
                await asyncio.to_thread(process.wait)

    async def launch_worker(self):
        source = self.connections["source"]
        previous = await source.fetchval("SELECT generation FROM source_sync_owner")
        self.launch("worker")

        async def claimed():
            return await source.fetchval("SELECT generation FROM source_sync_owner") > previous

        await self.wait("worker local ownership claim", claimed)

    async def wait(self, name, predicate, deadline_seconds=90):
        async with asyncio.timeout(deadline_seconds):
            while not await predicate():
                dead = [
                    r
                    for r, p in self.children.items()
                    if r in self.running and p.poll() is not None
                ]
                if dead:
                    raise RuntimeError(
                        f"qualification child exited while waiting for {name}: {dead}"
                    )
                await asyncio.sleep(0.25)

    async def initialize(self):
        self.directory.mkdir(parents=True, exist_ok=False)
        self.private.mkdir(mode=0o700)
        if os.name == "nt":
            result = await asyncio.to_thread(
                subprocess.run,
                [
                    "icacls",
                    str(self.private),
                    "/inheritance:r",
                    "/grant:r",
                    f"{os.getlogin()}:(OI)(CI)F",
                ],
                capture_output=True,
                check=False,
            )
            if result.returncode:
                raise RuntimeError("private qualification directory ACL could not be restricted")
        private_config = json.loads(self.args.database_file.read_text("utf-8"))
        suffix = uuid4().hex[:12]
        for role in ("source", "destination"):
            admin = private_config[role]
            task_database(admin)
            connection = await asyncpg.connect(admin)
            name = f"m4_{suffix}_{role}_test"
            try:
                await connection.execute(f'CREATE DATABASE "{name}"')
            finally:
                await connection.close()
            dsn = database_url(admin, name)
            self.databases.append((admin, name))
            environment = {**os.environ, "DATABASE_URL": dsn, "PYTHONUTF8": "1"}
            commands = [
                [sys.executable, "-m", "alembic", "upgrade", "0006" if role == "source" else "head"]
            ]
            if self.args.timescale:
                commands[0][3:3] = ["-x", "timescale=true"]
            if role == "source":
                commands.append(
                    [sys.executable, "-m", "alembic", "-c", "alembic-source.ini", "upgrade", "head"]
                )
            for command in commands:
                result = await asyncio.to_thread(
                    subprocess.run,
                    command,
                    cwd=ROOT,
                    env=environment,
                    capture_output=True,
                    check=False,
                )
                (self.private / f"{role}-migration.log").write_bytes(result.stdout + result.stderr)
                if result.returncode:
                    raise RuntimeError(f"isolated {role} migration failed")
            self.connections[role] = await asyncpg.connect(dsn)
            setattr(self, role + "_dsn", dsn)
        self.rows = business_registry(self.args.feeds, self.args.genuine)
        self.registry_path = self.private / "registry.json"
        write_json(self.registry_path, self.rows)
        self.registry = Registry.load(self.registry_path)
        self.pairs = self.registry.subscriptions("tradingview_guest")
        for role in ("source", "destination"):
            store = Store(
                Settings(
                    _env_file=None,
                    database_url=getattr(self, role + "_dsn"),
                    application_role="collector" if role == "source" else "api",
                    source_capture_enabled=role == "source",
                )
            )
            await store.connect()
            await store.register(self.registry)
            if role == "source":
                # Explicit historical enrollment request, not fabricated observations.
                # Normal collector registration starts coverage at activation time.
                end = minute_start(utc_now())
                await store.write_candles(
                    [],
                    [
                        (
                            i.id,
                            m.provider,
                            end - timedelta(minutes=self.args.history_minutes + 2),
                            end,
                            {} if self.args.genuine else {"session": "24x7", "timezone": "Etc/UTC"},
                        )
                        for i, m in self.pairs
                    ],
                )
            await store.close()
            sql_path = (
                ROOT
                / "scripts/sql"
                / ("v11_source_sync_role.sql" if role == "source" else "v11_destination_roles.sql")
            )
            c = self.connections[role]
            await c.execute(sql_path.read_text("utf-8"))
            for name in (
                ("marketdata_v11_sync",)
                if role == "source"
                else ("marketdata_v11_ingest", "marketdata_v11_read")
            ):
                await c.execute(f"ALTER ROLE {name} PASSWORD 'disposable_test'")
        source, dest = self.connections["source"], self.connections["destination"]
        self.epoch = await source.fetchval("SELECT source_epoch FROM source_capture_clock")
        self.producer, self.session = uuid4(), uuid4()
        key_id = str(uuid4())
        token = key_id + "." + secrets.token_urlsafe(32)
        self.token = token
        await dest.execute(
            """INSERT INTO ingest_producers
            (producer_id,label,credential_key_id,session_id,session_generation,session_expires_at)
            VALUES($1,'M4 disposable qualification',$2,$3,1,
                   clock_timestamp()+interval '120 seconds')""",
            self.producer,
            key_id,
            self.session,
        )
        manifest = []
        for instrument, mapping in self.pairs:
            for stream in ("candles", "quotes", "coverage", "status"):
                await dest.execute(
                    """INSERT INTO ingest_source_owners
                    (instrument_id,provider,stream,producer_id,source_epoch,ownership_generation,allowed_statuses)
                    VALUES($1,$2,$3,$4,$5,1,$6)""",
                    instrument.id,
                    mapping.provider,
                    stream,
                    self.producer,
                    self.epoch,
                    ["realtime", "delayed", "unknown", "indicative"]
                    if self.args.genuine
                    else ["simulated"],
                )
                manifest.append(
                    dict(
                        instrument_id=str(instrument.id),
                        provider=mapping.provider,
                        stream=stream,
                        source_epoch=str(self.epoch),
                        ownership_generation=1,
                    )
                )
        write_json(
            self.private / "credentials.json",
            [
                dict(
                    producer_id=str(self.producer),
                    key_id=key_id,
                    token_sha256=hashlib.sha256(token.encode()).hexdigest(),
                )
            ],
        )
        (self.private / "token").write_text(token, encoding="utf-8")
        (self.private / "dsn").write_text(
            database_url(self.source_dsn, role="marketdata_v11_sync"), encoding="utf-8"
        )
        openssl = shutil.which("openssl") or "C:/Program Files/Git/usr/bin/openssl.exe"
        cert, key = self.private / "cert.pem", self.private / "key.pem"
        openssl_config = self.private / "openssl.cnf"
        openssl_config.write_text("[req]\ndistinguished_name=dn\n[dn]\n", encoding="utf-8")
        await asyncio.to_thread(
            subprocess.run,
            [
                openssl,
                "req",
                "-config",
                str(openssl_config),
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
            ],
            capture_output=True,
            check=True,
        )
        self.ingest_url, self.read_url = (
            f"https://127.0.0.1:{free_port()}",
            f"https://127.0.0.1:{free_port()}",
        )
        self.read_key = secrets.token_urlsafe(40)
        for role in ("ingest", "read", "collector", "worker"):
            self.configs[role] = dict(
                role=role,
                pid=str(self.private / f"{role}.pid"),
                trace=str(self.directory / f"{role}.ndjson"),
                fault=str(self.private / f"{role}-fault.json"),
                marker=str(self.private / f"{role}-marker"),
            )
        for role, url in (("ingest", self.ingest_url), ("read", self.read_url)):
            self.configs[role].update(
                cert=str(cert),
                key=str(key),
                port=urlsplit(url).port,
                settings=dict(
                    database_url=database_url(
                        self.destination_dsn,
                        role="marketdata_v11_ingest" if role == "ingest" else "marketdata_v11_read",
                    ),
                    application_role="ingest" if role == "ingest" else "api",
                    api_environment="production",
                ),
            )
        self.configs["ingest"]["settings"].update(
            ingest_credentials_file=str(self.private / "credentials.json")
        )
        self.configs["read"]["settings"].update(
            api_key=self.read_key, websocket_allowed_origins=[ORIGIN], marketdata_poll_seconds=0.05
        )
        self.candles_path, self.quotes_path = (
            self.private / "candles.json",
            self.private / "quotes.ndjson",
        )
        self.quotes_path.touch()
        self.candles = []
        if not self.args.genuine:
            self.add_candles(self.args.history_minutes)
        self.configs["collector"].update(
            genuine=self.args.genuine,
            candles=str(self.candles_path),
            quotes=str(self.quotes_path),
            ready=str(self.private / "collector-ready"),
            settings=dict(
                database_url=self.source_dsn,
                application_role="collector",
                source_capture_enabled=True,
                registry_file=str(self.registry_path),
                collector_metrics_port=0,
                collector_status_ttl_seconds=self.args.status_ttl_seconds,
                **{
                    f"market_data_provider_{a}": "tradingview_guest"
                    for a in ("crypto", "forex", "metals", "energy", "futures")
                },
                tradingview_storage_mode="dual",
                candle_backfill_minutes=max(10, self.args.history_minutes + 5),
                candle_request_timeout_seconds=15,
                tradingview_candle_pool_enabled=True,
                candle_pool_connections=1,
                candle_pool_max_instruments=100,
            ),
        )
        self.sync_path = self.private / "sync.json"
        write_json(
            self.sync_path,
            dict(
                endpoint=self.ingest_url,
                source_dsn_file=str(self.private / "dsn"),
                token_file=str(self.private / "token"),
                anchor_file=str(self.private / "anchor.json"),
                ca_file=str(cert),
                producer_id=str(self.producer),
                session_id=str(self.session),
                session_generation=1,
                streams=manifest,
                interval_seconds=1,
                timeout_seconds=5,
                max_records=100,
                reconcile_seconds=300,
            ),
        )
        self.configs["worker"].update(sync_config=str(self.sync_path))
        if ssl.SSLContext.__module__.endswith("truststore._api"):
            ssl.SSLContext = importlib.import_module(ssl.SSLContext.__module__)._original_SSLContext
        self.context = ssl.create_default_context(cafile=str(cert))
        self.reader = httpx.AsyncClient(
            base_url=self.read_url,
            verify=self.context,
            trust_env=False,
            headers={"Authorization": "Bearer " + self.read_key},
            timeout=10,
        )
        self.ingest = httpx.AsyncClient(
            base_url=self.ingest_url,
            verify=self.context,
            trust_env=False,
            headers={"Authorization": "Bearer " + token},
            timeout=10,
        )
        self.report.update(
            source_epoch=self.epoch,
            producer_id=self.producer,
            migrations={"source": "0006+sc0002", "destination": "0010"},
            engines={
                role: await c.fetchval("SELECT version()") for role, c in self.connections.items()
            },
        )

    def add_candles(self, minutes):
        now = utc_now()
        end = minute_start(now) - timedelta(minutes=1)
        existing = {(b.instrument_id, b.bucket_start) for b in self.candles}
        for index, (instrument, mapping) in enumerate(self.pairs):
            for offset in range(minutes):
                start = end - timedelta(minutes=offset + 1)
                if (instrument.id, start) in existing:
                    continue
                price = Decimal(100 + index) + Decimal("0.123456789012345678")
                self.candles.append(
                    MarketCandle(
                        instrument_id=instrument.id,
                        canonical_symbol=instrument.canonical_symbol,
                        provider=mapping.provider,
                        provider_symbol=mapping.provider_symbol,
                        bucket_start=start,
                        bucket_end=start + timedelta(minutes=1),
                        open=price,
                        high=price + 2,
                        low=price - 2,
                        close=price + 1,
                        volume=Decimal("123.456789012345678"),
                        realtime_status="simulated",
                        source_status="streaming",
                        received_timestamp=now,
                        metadata={"volume_type": "base", "qualification_evidence": "injected"},
                    )
                )
        write_json(self.candles_path, [b.model_dump(mode="json") for b in self.candles])

    def quotes(self, sequence, *, fields=("last", "bid", "ask"), age=0):
        when = utc_now() - timedelta(seconds=age)
        rows = []
        for index, (instrument, mapping) in enumerate(self.pairs):
            price = Decimal(100 + index) + Decimal(sequence % 100) / 1000
            values = {"last": price, "bid": price - Decimal("0.01"), "ask": price + Decimal("0.01")}
            row = Tick(
                instrument_id=instrument.id,
                canonical_symbol=instrument.canonical_symbol,
                provider_symbol=mapping.provider_symbol,
                asset_class=instrument.asset_class,
                instrument_type=instrument.instrument_type,
                provider=mapping.provider,
                exchange=instrument.exchange,
                realtime_status="simulated",
                event_timestamp=when,
                received_timestamp=when,
                metadata={"qualification_evidence": "injected", "sequence": sequence},
                **{name: values[name] for name in fields},
            )
            rows.append(row)
        with self.quotes_path.open("a", encoding="utf-8") as output:
            output.writelines(row.model_dump_json() + "\n" for row in rows)
        self.last_quotes = rows
        return rows

    async def quotes_committed(self, ticks):
        rows = await self.connections["source"].fetch(
            "SELECT instrument_id,fields FROM source_quote_state"
        )
        fields = {r["instrument_id"]: json.loads(r["fields"]) for r in rows}
        return all(
            all(
                fields.get(t.instrument_id, {}).get(n, {}).get("observation_id") == str(t.tick_id)
                for n in ("last", "bid", "ask")
                if getattr(t, n) is not None
            )
            for t in ticks
        )

    async def observe_quotes(self, sequence, **kwargs):
        ticks = self.quotes(sequence, **kwargs)
        await self.wait("source quote commit", lambda: self.quotes_committed(ticks))
        await self.wait("destination quote receipt and acknowledgment", self.destination_caught_up)
        return ticks

    async def health(self, client):
        try:
            return (await client.get("/livez")).status_code == 200
        except httpx.TransportError:
            return False

    async def count(self, role, table):
        return await self.connections[role].fetchval(f"SELECT count(*) FROM {table}")

    async def destination_caught_up(self):
        s, d = self.connections["source"], self.connections["destination"]
        return (
            (await self.count("source", "market_candles")) > 0
            and (await self.count("source", "market_candles"))
            == await self.count("destination", "market_candles")
            and (
                await s.fetchval(
                    "SELECT count(*) FROM source_quote_state WHERE acked_version<version"
                )
            )
            == 0
            and (await s.fetchval("SELECT count(*) FROM source_candle_outbox")) == 0
            and (await d.fetchval("SELECT count(*) FROM latest_prices")) == len(self.pairs)
        )

    async def ws_consumer(self):
        url = self.read_url.replace("https:", "wss:") + "/api/v1/ws/marketdata"
        async with connect(
            url,
            ssl=self.context,
            origin=ORIGIN,
            additional_headers={"Authorization": "Bearer " + self.read_key},
        ) as ws:
            await ws.send(
                json.dumps(
                    dict(action="subscribe", symbols=[i.canonical_symbol for i, _ in self.pairs])
                )
            )
            async for message in ws:
                event = json.loads(message)
                observed = utc_now().isoformat()
                with (self.directory / "websocket.ndjson").open("a", encoding="utf-8") as output:
                    output.write(json.dumps(dict(observed_at=observed, event=event)) + "\n")
                if event["type"] in {"price_update", "candle_completed", "candle_corrected"}:
                    self.ws_events.append(dict(observed_at=observed, **event))

    async def snapshot(self, phase):
        s, d = self.connections["source"], self.connections["destination"]
        response = await self.reader.get(
            "/api/v1/prices/latest", params={"provider": "tradingview_guest", "limit": 100}
        )
        response.raise_for_status()
        body = response.json()
        items = body["items"]
        sample = dict(
            at=utc_now(),
            phase=phase,
            prices=[
                dict(
                    symbol=r["canonical_symbol"],
                    age_ms=r["age_ms"],
                    freshness=r["freshness"],
                    quote_freshness=r["quote_freshness"],
                    price_type=r["price_type"],
                    source_timestamp=r["source_timestamp"],
                    received_timestamp=r["received_timestamp"],
                )
                for r in items
            ],
            candle_pending=await self.count("source", "source_candle_outbox"),
            quote_pending=await s.fetchval(
                "SELECT count(*) FROM source_quote_state WHERE acked_version<version"
            ),
            source_bytes=await s.fetchval("SELECT pg_database_size(current_database())"),
            destination_bytes=await d.fetchval("SELECT pg_database_size(current_database())"),
            source_connections=await s.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()"
            ),
            destination_connections=await d.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()"
            ),
            status_streams=await d.fetchval("SELECT count(*) FROM ingest_source_status"),
            live_status_streams=await d.fetchval(
                "SELECT count(*) FROM ingest_source_status WHERE expires_at>clock_timestamp()"
            ),
            processes={
                role: process_counters(int(Path(config["pid"]).read_text("utf-8")))
                for role, config in self.configs.items()
                if Path(config["pid"]).exists()
            },
        )
        self.samples.append(sample)
        with (self.directory / "samples.ndjson").open("a", encoding="utf-8") as output:
            output.write(json.dumps(sample, default=str) + "\n")

    async def parity(self):
        s, d = self.connections["source"], self.connections["destination"]
        left = await s.fetch(
            "SELECT instrument_id,provider,record->'observation' AS payload "
            "FROM source_quote_state "
            "ORDER BY instrument_id,provider"
        )
        right = await d.fetch(
            "SELECT instrument_id,provider,payload FROM latest_prices "
            "ORDER BY instrument_id,provider"
        )
        local_prices = {
            (r["instrument_id"], r["provider"]): Tick.model_validate_json(r["payload"])
            for r in left
        }
        remote_prices = {
            (r["instrument_id"], r["provider"]): Tick.model_validate_json(r["payload"])
            for r in right
        }
        comparisons = []
        for key, source in local_prices.items():
            actual = remote_prices.get(key)
            comparisons.append(
                dict(
                    instrument_id=key[0],
                    provider=key[1],
                    full_snapshot_equal=actual is not None and source == actual,
                    source=source.model_dump(mode="json"),
                    destination=actual.model_dump(mode="json") if actual else None,
                )
            )
        write_json(self.directory / "quote-parity.json", comparisons)
        self.check(
            "destination full quote snapshot parity",
            len(local_prices) == len(remote_prices) == len(self.pairs)
            and all(v["full_snapshot_equal"] for v in comparisons),
        )
        candles = []
        for c in (s, d):
            rows = await c.fetch(
                "SELECT payload FROM market_candles ORDER BY instrument_id,provider,bucket_start"
            )
            candles.append([candle_fingerprint(json.loads(row["payload"])) for row in rows])
        self.check(
            "candle count and exact financial fingerprint parity",
            candles[0] == candles[1],
            source_count=len(candles[0]),
            destination_count=len(candles[1]),
            checksum=hashlib.sha256(json.dumps(candles[0]).encode()).hexdigest(),
        )
        revisions = [await c.fetchval("SELECT count(*) FROM candle_revisions") for c in (s, d)]
        self.check("candle revision count parity", revisions[0] == revisions[1], counts=revisions)

    async def acceptance(self):
        self.launch("ingest")
        self.launch("read")
        await self.wait("ingest readiness", lambda: self.health(self.ingest))
        await self.wait("read readiness", lambda: self.health(self.reader))
        async with httpx.AsyncClient(trust_env=False) as untrusted:
            try:
                await untrusted.get(self.ingest_url + "/livez")
            except httpx.ConnectError:
                self.check("TLS rejects untrusted certificate", True)
            else:
                self.check("TLS rejects untrusted certificate", False)
        unauthorized = await self.ingest.post(
            "/api/ingest/v1/session", headers={"Authorization": "Bearer invalid"}, json={}
        )
        self.check("invalid authentication rejected", unauthorized.status_code == 401)
        self.launch("collector")
        if not self.args.genuine:
            self.quotes(1)

        async def locally_committed():
            return await self.count("source", "market_candles") > 0 and await self.count(
                "source", "latest_prices"
            ) == len(self.pairs)

        await self.wait("local committed candles and independent quotes", locally_committed, 120)
        self.check(
            "local candles and atomic outbox visible before worker",
            await self.count("source", "source_candle_outbox") > 0,
        )
        self.ws_task = asyncio.create_task(self.ws_consumer())
        await self.launch_worker()
        if self.args.quotes_during_backfill:
            started = time.monotonic()
            next_sample = started

            async def quotes_during_history():
                sequence = 1000000
                while True:
                    self.quotes(sequence)
                    sequence += 1
                    await asyncio.sleep(1)

            generator = asyncio.create_task(quotes_during_history())

            async def history_delivered():
                nonlocal next_sample
                if generator.done():
                    await generator
                # The observer owns one connection per DB. Keep its SQL sequential;
                # the independent file injector never shares those connections.
                if time.monotonic() >= next_sample:
                    await self.snapshot("backfill")
                    next_sample = time.monotonic() + 5
                return (
                    await self.count("source", "market_candles")
                    == await self.count("destination", "market_candles")
                    and await self.count("source", "source_candle_outbox") == 0
                )

            try:
                await self.wait(
                    "bounded history delivery alongside current quotes", history_delivered, 300
                )
            finally:
                generator.cancel()
                outcome = (await asyncio.gather(generator, return_exceptions=True))[0]
                if isinstance(outcome, Exception):
                    raise RuntimeError("backfill injector failed") from outcome
            self.report["backfill_seconds"] = time.monotonic() - started
            await self.wait(
                "terminal backfill quotes committed",
                lambda: self.quotes_committed(self.last_quotes),
            )
        await self.wait("complete source-to-destination delivery", self.destination_caught_up, 180)
        await self.parity()
        instrument, mapping = self.pairs[0]
        first = await self.connections["destination"].fetchrow(
            "SELECT payload FROM market_candles WHERE instrument_id=$1 "
            "ORDER BY bucket_start LIMIT 1",
            instrument.id,
        )
        bar = MarketCandle.model_validate_json(first["payload"])
        params = dict(
            symbol=instrument.canonical_symbol,
            provider=mapping.provider,
            interval="1m",
            start=bar.bucket_start.isoformat(),
            end=bar.bucket_end.isoformat(),
        )
        native = await self.reader.get("/api/v1/candles", params=params)
        items = native.json().get("items", [])
        self.check(
            "native candle REST exact OHLC and identity",
            native.status_code == 200
            and len(items) == 1
            and items[0]["origin"] == "native"
            and items[0]["provider_symbol"] == bar.provider_symbol
            and all(
                Decimal(items[0][field]) == getattr(bar, field)
                for field in ("open", "high", "low", "close")
            ),
            item=items[0] if items else None,
        )
        response = await self.reader.get("/api/v1/dashboard/candles", params=params)
        chart = response.json()
        expected = dict(
            a=[int(bar.bucket_start.timestamp())],
            b=[float(bar.open)],
            c=[float(bar.close)],
            d=[float(bar.high)],
            e=[float(bar.low)],
            f=[0],
            g=[float(bar.volume) if bar.volume else 0],
            h=[0],
        )
        self.check(
            "ChartData exact a-h mapping",
            response.status_code == 200 and chart == expected,
            chart=chart,
        )
        params.update(
            start=(bar.bucket_start - timedelta(days=1)).isoformat(),
            end=(bar.bucket_end - timedelta(days=1)).isoformat(),
        )
        empty = await self.reader.get("/api/v1/dashboard/candles", params=params)
        self.check("ChartData eight empty arrays", empty.json() == {k: [] for k in "abcdefgh"})
        if not self.args.genuine:
            await self.stop("collector")
            old = self.candles[0]
            self.candles[0] = old.model_copy(
                update=dict(close=old.open, received_timestamp=utc_now())
            )
            write_json(self.candles_path, [b.model_dump(mode="json") for b in self.candles])
            self.launch("collector")

            async def corrected():
                return await self.count("destination", "candle_revisions") > 0

            await self.wait("historical correction via collector restart", corrected)
            self.check("historical correction delivered", True)
            await self.observe_quotes(2, fields=("bid", "ask"))
            await self.parity()
        await self.snapshot("acceptance")

    async def soak(self, seconds, hz, phase):
        if seconds <= 0:
            return
        started = time.monotonic()
        next_quote, next_sample, next_candles = started, started, started + 60
        sequence = 10
        while time.monotonic() - started < seconds:
            current = time.monotonic()
            if current >= next_quote and not self.args.genuine:
                self.quotes(sequence)
                sequence += 1
                # No catch-up storm: missed scheduling deadlines are measured as generator lag.
                next_quote = max(next_quote + 1 / hz, current)
            if current >= next_sample:
                await self.snapshot(phase)
                next_sample = current + 5
            if current >= next_candles and not self.args.genuine:
                self.add_candles(1)
                next_candles = current + 60
            if self.ws_task.done():
                await self.ws_task
                raise RuntimeError("WebSocket consumer disconnected")
            for role, process in self.children.items():
                if process.poll() is not None:
                    raise RuntimeError(f"{role} stopped during soak")
            await asyncio.sleep(min(0.05, 1 / hz))
        self.report.setdefault("load_phases", []).append(
            dict(
                phase=phase,
                duration_seconds=time.monotonic() - started,
                requested_hz_per_feed=hz,
                generated_updates=(sequence - 10) * len(self.pairs),
            )
        )
        print(json.dumps(dict(phase=phase, completed=True)), flush=True)

    async def fault_suite(self):
        """Real child-process kills around the four durable delivery transitions."""
        source, dest = self.connections["source"], self.connections["destination"]
        worker_fault = Path(self.configs["worker"]["fault"])
        worker_marker = Path(self.configs["worker"]["marker"])
        ingest_fault = Path(self.configs["ingest"]["fault"])
        ingest_marker = Path(self.configs["ingest"]["marker"])

        async def wait_marker(path, process, seconds=30):
            async with asyncio.timeout(seconds):
                while not path.exists():
                    if process.poll() is not None:
                        raise RuntimeError("fault child stopped before reaching requested boundary")
                    await asyncio.sleep(0.05)

        async def new_quote(sequence):
            previous = await source.fetchval("SELECT head FROM source_capture_clock")
            self.quotes(sequence)

            async def committed():
                return await source.fetchval("SELECT head FROM source_capture_clock") > previous

            await self.wait("new local quote", committed)

        for index, boundary in enumerate(
            (
                "after_staging_commit",
                "after_http_before_receipt",
                "after_receipt_commit",
                "after_source_ack",
            )
        ):
            await self.stop("worker")
            worker_marker.unlink(missing_ok=True)
            write_json(worker_fault, dict(crash_at=boundary))
            await new_quote(1000 + index)
            await self.launch_worker()
            await wait_marker(worker_marker, self.children["worker"])
            await asyncio.to_thread(self.children["worker"].wait, 10)
            self.running.discard("worker")
            marker = json.loads(worker_marker.read_text("utf-8"))
            batch_id = UUID(marker["batch_id"])
            batch = await source.fetchrow(
                "SELECT * FROM source_sync_batches WHERE batch_id=$1", batch_id
            )
            archive = await source.fetchrow(
                "SELECT * FROM source_sync_archive WHERE batch_id=$1", batch_id
            )
            raw = bytes(batch["payload"]) if batch else None
            phase = batch["phase"] if batch else "archived" if archive else "missing"
            expected = {
                "after_staging_commit": "staged",
                "after_http_before_receipt": "staged",
                "after_receipt_commit": "received",
                "after_source_ack": "acknowledged",
            }[boundary]
            self.check(
                "crash boundary durable phase: " + boundary,
                phase == expected,
                phase=phase,
                batch_id=batch_id,
            )
            if boundary == "after_receipt_commit":
                # A pending version N+1 must survive replaying version N's saved receipt.
                await new_quote(2000 + index)
            worker_fault.unlink(missing_ok=True)
            await self.launch_worker()
            await self.wait("restart delivery after " + boundary, self.destination_caught_up)
            now = await source.fetchrow(
                "SELECT payload FROM source_sync_batches WHERE batch_id=$1", batch_id
            )
            if now:
                self.check("staged bytes preserved: " + boundary, bytes(now["payload"]) == raw)
            self.check("restart recovery: " + boundary, True)
        await self.parity()

        # A second process must fail instead of stealing the local advisory lock.
        contender = dict(self.configs["worker"])
        contender_path = self.private / "contender.json"
        write_json(contender_path, contender)
        owner_before = await source.fetchrow("SELECT generation,lock_pid FROM source_sync_owner")
        with (self.private / "contender.log").open("wb") as output:
            contender_process = await asyncio.to_thread(
                subprocess.Popen,
                [
                    sys.executable,
                    str(ROOT / "scripts/v11_qualification_process.py"),
                    str(contender_path),
                ],
                cwd=ROOT,
                stdout=output,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONUTF8": "1"},
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            try:
                async with asyncio.timeout(20):
                    while "CaptureConflict" not in (self.private / "contender.log").read_text(  # noqa: ASYNC110
                        "utf-8"
                    ):
                        await asyncio.sleep(0.1)
                owner_after = await source.fetchrow(
                    "SELECT generation,lock_pid FROM source_sync_owner"
                )
                self.check(
                    "competing worker cannot claim ownership",
                    owner_before == owner_after,
                    rejection="CaptureConflict",
                    owner_before=dict(owner_before),
                    owner_after=dict(owner_after),
                )
            finally:
                if contender_process.poll() is None:
                    contender_process.terminate()
                await asyncio.to_thread(contender_process.wait, 10)

        # Kill INGEST after its financial transaction commits but before HTTP response.
        await self.stop("worker")
        await self.stop("ingest")
        ingest_marker.unlink(missing_ok=True)
        write_json(ingest_fault, dict(pause_after_upload_commit=True))
        self.launch("ingest")
        await self.wait("restarted ingest readiness", lambda: self.health(self.ingest))
        await new_quote(3000)
        await self.launch_worker()
        await wait_marker(ingest_marker, self.children["ingest"])
        committed_id = UUID(ingest_marker.read_text("utf-8"))
        self.check(
            "lost upload response already committed",
            await dest.fetchval(
                "SELECT count(*) FROM ingest_batches WHERE batch_id=$1", committed_id
            )
            == 1,
        )
        await self.stop("ingest")
        ingest_fault.unlink(missing_ok=True)
        self.launch("ingest")
        await self.wait("lost response recovery", self.destination_caught_up)
        self.check(
            "lost upload acknowledgment creates one receipt",
            await dest.fetchval(
                "SELECT count(*) FROM ingest_batches WHERE batch_id=$1", committed_id
            )
            == 1,
        )

        # Lost renewal and recovery responses replay safely across actual server restarts.
        for recovery in (False, True):
            await self.stop("worker")
            await self.stop("ingest")
            ingest_marker.unlink(missing_ok=True)
            before = await dest.fetchrow(
                "SELECT session_id,session_generation FROM ingest_producers WHERE producer_id=$1",
                self.producer,
            )
            if recovery:
                await dest.execute(
                    "UPDATE ingest_producers SET session_expires_at="
                    "clock_timestamp()-interval '1 second' "
                    "WHERE producer_id=$1",
                    self.producer,
                )
            write_json(
                ingest_fault, {"pause_after_recovery" if recovery else "pause_after_renewal": True}
            )
            self.launch("ingest")
            await self.wait("session fault ingest ready", lambda: self.health(self.ingest))
            await self.launch_worker()
            await wait_marker(ingest_marker, self.children["ingest"])
            await self.stop("ingest")
            ingest_fault.unlink(missing_ok=True)
            self.launch("ingest")
            await new_quote(4000 + int(recovery))
            await self.wait("session response loss recovery", self.destination_caught_up, 120)
            after = await dest.fetchrow(
                "SELECT session_id,session_generation FROM ingest_producers WHERE producer_id=$1",
                self.producer,
            )
            self.check(
                "lost recovery acknowledgment" if recovery else "lost renewal acknowledgment",
                after["session_generation"] == before["session_generation"] + int(recovery),
                before_generation=before["session_generation"],
                after_generation=after["session_generation"],
            )

        # In-flight response cannot be used after the local lock connection is terminated.
        await self.stop("worker")
        await self.stop("ingest")
        ingest_marker.unlink(missing_ok=True)
        write_json(ingest_fault, dict(pause_after_upload_commit=True))
        self.launch("ingest")
        await self.wait("lock-loss ingest ready", lambda: self.health(self.ingest))
        await new_quote(5000)
        await self.launch_worker()
        await wait_marker(ingest_marker, self.children["ingest"])
        batch_id = UUID(ingest_marker.read_text("utf-8"))
        owner_pid = await source.fetchval("SELECT lock_pid FROM source_sync_owner")
        await source.execute("SELECT pg_terminate_backend($1)", owner_pid)
        async with asyncio.timeout(10):
            await asyncio.to_thread(self.children["worker"].wait, 10)
        self.check(
            "lost worker lock fences in-flight local receipt",
            await source.fetchval(
                "SELECT phase FROM source_sync_batches WHERE batch_id=$1", batch_id
            )
            == "staged",
        )
        self.running.discard("worker")
        await self.stop("ingest")
        ingest_fault.unlink(missing_ok=True)
        self.launch("ingest")
        await self.launch_worker()
        await self.wait("new owner recovers after lock loss", self.destination_caught_up)
        await self.parity()

        # Transport partition: source continues committing while company API is down.
        await self.stop("ingest")
        for sequence in range(6000, 6010):
            self.quotes(sequence)
            await asyncio.sleep(0.1)
        await asyncio.sleep(3)
        self.check(
            "outage retains newest pending quote",
            await source.fetchval(
                "SELECT count(*) FROM source_quote_state WHERE acked_version<version"
            )
            > 0,
        )
        self.launch("ingest")
        await self.wait("outage durable resume", self.destination_caught_up, 120)
        await self.parity()

    async def finish(self):
        if not self.args.genuine and hasattr(self, "last_quotes"):
            await self.wait(
                "terminal injected observations locally committed",
                lambda: self.quotes_committed(self.last_quotes),
            )
        # Freeze acquisition, including coverage, before comparing the two databases.
        # Financial acknowledgment alone does not prove metadata has caught up.
        await self.stop("collector")
        await self.wait("final durable drain", self.destination_caught_up, 180)
        fields = (
            "instrument_id,provider,price_basis,coverage_start,coverage_end,"
            "missing,unknown,not_expected,session_info"
        )

        async def coverage_equal():
            rows = [
                await self.connections[role].fetch(
                    f"SELECT {fields} FROM candle_coverage "
                    "ORDER BY instrument_id,provider,price_basis"
                )
                for role in ("source", "destination")
            ]
            return rows[0] == rows[1] and len(rows[0]) == len(self.pairs)

        await self.wait("final independent coverage drain", coverage_equal, 180)
        self.check("final coverage snapshot parity", True, streams=len(self.pairs))
        await self.parity()
        await self.snapshot("final")
        self.check(
            "committed WebSocket candle update",
            any(e["type"] == "candle_completed" for e in self.ws_events),
        )
        self.check(
            "committed WebSocket quote update",
            any(e["type"] == "price_update" for e in self.ws_events),
        )
        if not self.args.genuine:
            self.check(
                "committed WebSocket correction update",
                any(e["type"] == "candle_corrected" for e in self.ws_events),
            )
        self.report.update(
            completed_at=utc_now(),
            websocket_events=len(self.ws_events),
            latest_price_age_ms=percentiles(
                [p["age_ms"] for s in self.samples for p in s["prices"] if p["age_ms"] is not None]
            ),
            per_instrument=[
                dict(
                    symbol=i.canonical_symbol,
                    provider=m.provider,
                    provider_symbol=m.provider_symbol,
                    age_ms=percentiles(
                        [
                            p["age_ms"]
                            for s in self.samples
                            for p in s["prices"]
                            if p["symbol"] == i.canonical_symbol and p["age_ms"] is not None
                        ]
                    ),
                    stale_samples=sum(
                        p["quote_freshness"] != "fresh"
                        for s in self.samples
                        for p in s["prices"]
                        if p["symbol"] == i.canonical_symbol
                    ),
                )
                for i, m in self.pairs
            ],
        )
        write_json(self.directory / "report.json", self.report)

    async def cleanup(self):
        for role in list(self.children):
            await self.stop(role)
        if hasattr(self, "ws_task"):
            self.ws_task.cancel()
            await asyncio.gather(self.ws_task, return_exceptions=True)
        for name in ("reader", "ingest"):
            if hasattr(self, name):
                await getattr(self, name).aclose()
        for role, c in self.connections.items():
            for name in (
                ("marketdata_v11_sync",)
                if role == "source"
                else ("marketdata_v11_ingest", "marketdata_v11_read")
            ):
                await c.execute(f"DROP OWNED BY {name}")
                await c.execute(f"DROP ROLE {name}")
            await c.close()
        # Keep financial test databases for review; only task runtime cleanup removes them.
        self.report["retained_task_databases"] = [name for _, name in self.databases]
        if self.directory.exists():
            write_json(self.directory / "report.json", self.report)
        for log in self.logs:
            log.close()


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-file", type=Path, required=True, help="private source/destination admin DSNs"
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="new, private task evidence directory"
    )
    parser.add_argument("--feeds", type=int, default=99, choices=range(1, 100), metavar="1..99")
    parser.add_argument("--soak-seconds", type=int, default=900)
    parser.add_argument("--quote-hz", type=float, default=1)
    parser.add_argument("--burst-seconds", type=int, default=120)
    parser.add_argument("--burst-hz", type=float, default=5)
    parser.add_argument("--history-minutes", type=int, default=3)
    parser.add_argument(
        "--quotes-during-backfill",
        action="store_true",
        help="bounded one-Hz quote injection during initial historical catch-up",
    )
    parser.add_argument(
        "--status-ttl-seconds",
        type=float,
        default=20,
        help="actual source health expiry; never changes market-observation freshness",
    )
    parser.add_argument(
        "--timescale", action="store_true", help="enable hypertables on fresh migration"
    )
    parser.add_argument(
        "--faults", action="store_true", help="bounded process and acknowledgment faults"
    )
    parser.add_argument(
        "--genuine", action="store_true", help="explicit single BTCUSDT source qualification"
    )
    args = parser.parse_args()
    if not 5 <= args.status_ttl_seconds <= 120:
        parser.error("source status TTL must be 5..120 seconds")
    if not 0 <= args.soak_seconds <= 3600 or not 0 <= args.burst_seconds <= 300:
        parser.error("soak 0..3600 and burst 0..300 seconds required")
    if (
        not 0 < args.quote_hz <= 10
        or not 0 < args.burst_hz <= 10
        or not 1 <= args.history_minutes <= 120
    ):
        parser.error("rate (0,10] per feed and history 1..120 minutes required")
    if (
        args.feeds * (args.soak_seconds * args.quote_hz + args.burst_seconds * args.burst_hz)
        > 300000
    ):
        parser.error("bounded qualification supports at most 300000 injected updates")
    return args


async def main(args):
    pipeline = Pipeline(args)
    try:
        await pipeline.initialize()
        await pipeline.acceptance()
        if args.faults:
            await pipeline.fault_suite()
        await pipeline.soak(args.soak_seconds, args.quote_hz, "steady")
        await pipeline.soak(args.burst_seconds, args.burst_hz, "burst")
        await pipeline.finish()
    except BaseException as error:
        pipeline.report["failure_category"] = type(error).__name__
        pipeline.report["failure_frames"] = [
            dict(file=Path(f.filename).name, line=f.lineno, function=f.name)
            for f in traceback.extract_tb(error.__traceback__)
        ]
        raise
    finally:
        await pipeline.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main(arguments()))
    except Exception as error:
        print(json.dumps(dict(qualification_failed=type(error).__name__)), flush=True)
        raise SystemExit(1) from None
