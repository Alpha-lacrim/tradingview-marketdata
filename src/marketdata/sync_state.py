"""Fenced local transactions and immutable staging; never writes market rows."""

import base64
import hashlib
import json
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from marketdata.persistence import Store
from marketdata.source_capture import CaptureConflict, SourceCapture
from marketdata.sync_config import SyncConfig


class SyncState:
    def __init__(self, store: Store, capture: SourceCapture, config: SyncConfig):
        self.store, self.capture, self.config = store, capture, config
        self.token = uuid4()
        self.generation = 0
        self.lock_connection: Any = None

    async def claim(self, connection: Any) -> None:
        self.lock_connection = connection
        self.generation = await connection.fetchval("SELECT source_sync_claim($1)", self.token)
        self.capture.worker_fence = (self.token, self.generation)

    @asynccontextmanager
    async def transaction(self, *, isolation: str = "read_committed") -> AsyncIterator[Any]:
        if self.lock_connection is None or self.lock_connection.is_closed():
            raise CaptureConflict("worker ownership connection lost")
        async with self.store.connection() as c, c.transaction(isolation=isolation):
            await c.execute("SET LOCAL synchronous_commit=on")
            await c.execute("SELECT source_sync_enter($1,$2)", self.token, self.generation)
            yield c

    async def operation(self, operation: str, data: dict[str, Any]) -> dict[str, Any]:
        async with self.transaction() as c:
            value = await c.fetchval(
                "SELECT source_sync_state($1,$2::jsonb)", operation, json.dumps(data)
            )
        return dict(json.loads(value))

    async def get(self) -> dict[str, Any]:
        async with self.transaction() as c:
            row = await c.fetchrow("SELECT identity,state FROM source_sync_owner WHERE singleton")
        return {k: json.loads(v) if v else None for k, v in dict(row).items()}

    async def stage(self, payload: bytes, bookkeeping: dict[str, Any], *, c: Any = None) -> None:
        parsed = json.loads(payload)
        data = dict(
            batch_id=parsed["batch_id"],
            payload=base64.b64encode(payload).decode(),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            stream=parsed["stream"],
            bookkeeping=bookkeeping,
        )
        if c is not None:
            await c.execute("SELECT source_sync_state('stage',$1::jsonb)", json.dumps(data))
        else:
            await self.operation("stage", data)

    async def pending(self, stream: str) -> dict[str, Any] | None:
        async with self.transaction() as c:
            row = await c.fetchrow(
                """SELECT * FROM source_sync_batches WHERE stream=$1
                AND phase IN ('staged','received') ORDER BY created_at,batch_id LIMIT 1""",
                stream,
            )
            if row is None:
                return None
            result = dict(row)
            result["ready"] = row["next_attempt"] <= await c.fetchval("SELECT clock_timestamp()")
        result["bookkeeping"] = json.loads(result["bookkeeping"])
        result["receipt"] = json.loads(result["receipt"]) if result["receipt"] else None
        result["payload"] = bytes(result["payload"])
        return result

    async def initialize(self) -> dict[str, Any]:
        epoch = self.config.streams[0].source_epoch
        async with self.transaction() as c:
            unsafe = await c.fetchval("""SELECT
                EXISTS(SELECT 1 FROM pg_roles WHERE rolname=current_user AND
                    (rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls OR rolreplication))
                OR has_schema_privilege(current_user,'public','CREATE')
                OR has_database_privilege(current_user,current_database(),'TEMP,CREATE')
                OR EXISTS(SELECT 1 FROM unnest(ARRAY['instruments','provider_instruments',
                    'market_candles','latest_prices','market_ticks','candle_revisions',
                    'source_capture_clock','source_candle_outbox','source_candle_exports',
                    'source_quote_state','source_sync_owner','source_sync_batches']) t(n)
                    WHERE has_table_privilege(current_user,n,'INSERT,UPDATE,DELETE,TRUNCATE'))
                OR EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid IN
                    ('source_capture_clock'::regclass,'source_candle_outbox'::regclass,
                     'source_quote_state'::regclass,'market_candles'::regclass,
                     'latest_prices'::regclass,'market_ticks'::regclass,'instruments'::regclass,
                     'provider_instruments'::regclass,'source_candle_exports'::regclass,
                     'candle_revisions'::regclass) AND a.attnum>0 AND NOT a.attisdropped
                    AND has_column_privilege(current_user,a.attrelid,a.attname,'UPDATE,INSERT'))
                """)
            if unsafe:
                raise CaptureConflict("worker requires a restricted source synchronization role")
            clock = await c.fetchrow("SELECT * FROM source_capture_clock")
            identity = await c.fetchval("SELECT identity FROM source_sync_owner")
        if clock["source_epoch"] != epoch:
            raise CaptureConflict("configured source epoch mismatch")
        expected = dict(
            producer_id=str(self.config.producer_id),
            source_epoch=str(epoch),
            streams=[s.model_dump(mode="json") for s in self.config.streams],
        )
        path = self.config.anchor_file
        if path.exists():
            anchor = json.loads(path.read_bytes())
            if anchor["identity"] != expected or anchor["head"] > clock["head"]:
                raise CaptureConflict("source restore or identity mismatch requires review")
        elif identity is not None:
            raise CaptureConflict("external continuity anchor is missing")
        else:
            self.write_anchor(dict(identity=expected, head=clock["head"]))
        result = await self.operation(
            "initialize",
            dict(
                identity=expected,
                state=dict(
                    session_id=str(self.config.session_id),
                    session_generation=self.config.session_generation,
                    pending_recovery=None,
                    recovered_at=None,
                    reconciliation={},
                    enrollment={},
                ),
            ),
        )
        return dict(result["state"])

    def write_anchor(self, value: dict[str, Any]) -> None:
        path = self.config.anchor_file
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = Path(str(path) + ".tmp")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(value, output, sort_keys=True)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)

    async def anchor(self, receipt: dict[str, Any] | None = None) -> None:
        value = json.loads(self.config.anchor_file.read_bytes())
        async with self.transaction() as c:
            value["head"] = await c.fetchval("SELECT head FROM source_capture_clock")
        if receipt:
            value["receipt"] = {k: receipt[k] for k in ("batch_id", "payload_sha256")}
        self.write_anchor(value)

    async def acknowledge(self, batch: dict[str, Any]) -> None:
        parsed = json.loads(batch["payload"])
        self.capture.ack_batch_id = UUID(parsed["batch_id"])
        try:
            if parsed["stream"] == "candles":
                info = batch["bookkeeping"]
                await self.capture.acknowledge_candles(
                    UUID(parsed["source_epoch"]), info["after"], info["through"]
                )
            elif parsed["stream"] == "quotes":
                for record in parsed["records"]:
                    o = record["observation"]
                    await self.capture.acknowledge_quote(
                        UUID(parsed["source_epoch"]),
                        UUID(o["instrument_id"]),
                        o["provider"],
                        int(record["source_position"]),
                    )
            async with self.transaction() as c:
                await c.execute(
                    "SELECT source_sync_capture('ack_batch',$1::jsonb)",
                    json.dumps(dict(batch_id=parsed["batch_id"])),
                )
        finally:
            self.capture.ack_batch_id = None
