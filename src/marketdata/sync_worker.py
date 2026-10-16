"""Independent durable HTTPS synchronization. Never opens a provider connection.

Run: python -m marketdata.sync_worker --config /private/sync.json [--once]
"""

import argparse
import asyncio
import hashlib
import json
import random
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from pydantic import ValidationError

from marketdata.config import Settings
from marketdata.domain import Tick
from marketdata.persistence import Store
from marketdata.reconciliation import candle_fingerprint, revision_summary
from marketdata.replication_models import canonical_bytes, decode_batch, fingerprint
from marketdata.session_recovery import (
    MAX_RECOVERY_BYTES,
    RecoveryReceipt,
    RecoveryRequest,
    RecoveryStream,
)
from marketdata.source_capture import CaptureConflict, SourceCapture
from marketdata.sync_config import SyncConfig
from marketdata.sync_http import SyncHTTP, SyncProblem
from marketdata.sync_state import SyncState


class SyncWorker:
    def __init__(self, store: Store, config: SyncConfig):
        self.store, self.config = store, config
        self.capture = SourceCapture(store)
        self.local = SyncState(store, self.capture, config)
        self.http = SyncHTTP(config)
        self.state: dict[str, Any] = {}
        self.lease_until = 0.0
        self.reconcile_at = 0.0
        self.reconcile_cursor = 0
        self.enroll_cursor = 0
        self.last_request = 0.0
        self.continuity_checked = False

    async def checkpoint(self, boundary: str) -> None:
        """No-op seam for deterministic crash injection; no production environment switch."""

    async def request(
        self, method: str, path: str, payload: bytes | None = None, *, batch: bool = False
    ) -> dict[str, Any]:
        # Admission, receipt polling and reconciliation consume the same server quota.
        await asyncio.sleep(max(0, 0.65 - (time.monotonic() - self.last_request)))
        self.last_request = time.monotonic()
        async with self.local.transaction():
            pass  # Fail before network IO if the ownership backend is already gone.
        result = await self.http.request(method, path, payload, batch=batch)
        async with self.local.transaction():
            pass  # A response received after lock loss cannot mutate local state.
        return result

    async def save(self, **values: Any) -> None:
        result = await self.local.operation("state", values)
        self.state = result["state"]

    def manifest(self) -> list[dict[str, Any]]:
        return [s.model_dump(mode="json") for s in self.config.streams]

    def validate_manifest(self, streams: Any) -> None:
        if not isinstance(streams, list):
            raise SyncProblem("manifest_mismatch")
        names = {"instrument_id", "provider", "stream", "source_epoch", "ownership_generation"}
        actual = [{k: s[k] for k in names} for s in streams]
        if sorted(map(canonical_bytes, actual)) != sorted(map(canonical_bytes, self.manifest())):
            raise SyncProblem("manifest_mismatch")

    async def session(self) -> None:
        if self.state.get("pending_recovery"):
            raw = self.state["pending_recovery"].encode()
            response = await self.request("POST", "/session/recover", raw)
            receipt = RecoveryReceipt.model_validate(response)
            saved = RecoveryRequest.model_validate_json(raw)
            self.validate_manifest(response["streams"])
            if (
                receipt.producer_id != self.config.producer_id
                or receipt.recovery_id != saved.recovery_id
                or receipt.previous_session_id != saved.previous_session_id
                or receipt.previous_session_generation != saved.previous_session_generation
                or receipt.session_generation != saved.previous_session_generation + 1
                or receipt.session_id == saved.previous_session_id
            ):
                raise SyncProblem("recovery_receipt_mismatch")
            await self.save(
                session_id=str(receipt.session_id),
                session_generation=receipt.session_generation,
                pending_recovery=None,
                recovered_at=receipt.server_recovered_at.isoformat(),
                recovery_receipt=response,
            )
            self.lease_until = time.monotonic() + max(
                0, (receipt.expires_at - receipt.server_recovered_at).total_seconds() - 30
            )
            # A replayed old receipt does not tell us how much lease remains now.
            self.lease_until = 0
        if time.monotonic() < self.lease_until:
            return
        request = canonical_bytes(
            dict(
                protocol_version=1,
                session_id=self.state["session_id"],
                session_generation=self.state["session_generation"],
            )
        )
        try:
            response = await self.request("POST", "/session/renew", request)
        except SyncProblem as error:
            if error.code != "producer_session_fenced":
                raise
            recovery = RecoveryRequest(
                protocol_version=1,
                recovery_id=uuid4(),
                previous_session_id=UUID(self.state["session_id"]),
                previous_session_generation=self.state["session_generation"],
                streams=self.config.streams,
            )
            raw = canonical_bytes(recovery.model_dump(mode="json"))
            if len(raw) > MAX_RECOVERY_BYTES:
                raise SyncProblem("recovery_manifest_limit") from None
            await self.save(pending_recovery=raw.decode())
            await self.session()
            return
        self.validate_manifest(response["streams"])
        if (response["producer_id"], response["session_id"], response["session_generation"]) != (
            str(self.config.producer_id),
            self.state["session_id"],
            self.state["session_generation"],
        ):
            raise SyncProblem("session_receipt_mismatch")
        self.lease_until = time.monotonic() + max(
            0,
            (
                datetime.fromisoformat(response["expires_at"])
                - datetime.fromisoformat(response["server_time"])
            ).total_seconds()
            - 30,
        )

    def envelope(self, stream: str, records: list[dict[str, Any]]) -> bytes:
        return canonical_bytes(
            dict(
                protocol_version=1,
                producer_id=str(self.config.producer_id),
                session_id=self.state["session_id"],
                session_generation=self.state["session_generation"],
                source_epoch=str(self.config.streams[0].source_epoch),
                batch_id=str(uuid4()),
                stream=stream,
                records=records,
            )
        )

    def bounded(self, stream: str, records: list[dict[str, Any]]) -> tuple[bytes, int]:
        maximum = min(self.config.max_records, 50 if stream in {"coverage", "status"} else 500)
        # Stable envelope/ID for this construction; only unstaged records may be split.
        value = json.loads(self.envelope(stream, []))
        chosen: list[dict[str, Any]] = []
        for record in records[:maximum]:
            value["records"] = [*chosen, record]
            if len(canonical_bytes(value)) > self.config.max_body_bytes:
                break
            chosen.append(record)
        if not chosen:
            raise SyncProblem("single_record_exceeds_body_limit")
        value["records"] = chosen
        raw = canonical_bytes(value)
        decode_batch(raw)  # Historical compatibility preflight; never coerce bad history.
        return raw, len(chosen)

    async def stage(self, stream: str) -> bool:
        if stream == "candles":
            page = await self.capture.page(self.config.max_records)
            if page["source_epoch"] != self.config.streams[0].source_epoch:
                raise SyncProblem("source_epoch_changed")
            if not page["records"]:
                if page["through"] > page["after"]:
                    await self.capture.acknowledge_candles(
                        page["source_epoch"], page["after"], page["through"]
                    )
                return False
            raw, count = self.bounded(stream, page["records"])
            selected = page["records"][:count]
            bookkeeping = dict(
                after=page["after"],
                through=int(selected[-1]["source_position"]),
                event_kinds={
                    r["observation_id"]: page["event_kinds"][r["observation_id"]] for r in selected
                },
            )
        elif stream == "quotes":
            page = await self.capture.pending_quotes(self.config.max_records)
            if page["source_epoch"] != self.config.streams[0].source_epoch:
                raise SyncProblem("source_epoch_changed")
            if not page["records"]:
                return False
            raw, _ = self.bounded(stream, page["records"])
            bookkeeping = {}
        else:
            return await self.stage_metadata(stream)
        await self.checkpoint("before_staging_commit")
        await self.local.stage(raw, bookkeeping)
        await self.local.anchor()
        await self.checkpoint("after_staging_commit")
        return True

    async def stage_metadata(self, stream: str) -> bool:
        # One bounded snapshot batch per fair slot. Original capture/expiry times
        # are retained; batching must not turn session health into market freshness.
        grants = [s for s in self.config.streams if s.stream == stream]
        if not grants:
            return False
        index = int(self.state.get(stream + "_index", 0)) % len(grants)
        selected = [
            grants[(index + n) % len(grants)]
            for n in range(
                min(len(grants), self.config.metadata_max_records, self.config.max_records)
            )
        ]
        records: list[dict[str, Any]] = []
        progress = dict(self.state.get("metadata", {}))
        visited = 0
        raw = b""
        async with self.local.transaction() as c:
            # Take business locks in the same sorted order as financial persistence,
            # before the shared clock. Rollback cannot expose a partial allocation.
            await self.store.lock_sources(c, [(s.instrument_id, s.provider) for s in selected])
            await c.execute("SELECT source_sync_capture('clock','{}')")
            # Observe the locked clock after concurrent writers finish. Keeping an
            # earlier MVCC snapshot causes avoidable retries during continuous quotes.
            # Business locks protect financial rows; capture the status row once.
            status = (
                await c.fetchrow("SELECT * FROM collector_status WHERE singleton")
                if stream == "status"
                else None
            )
            for grant in selected:
                observation = await self._metadata_observation(c, grant, stream, status)
                if observation is None:
                    visited += 1
                    continue
                key = str(grant.instrument_id) + ":" + grant.provider + ":" + stream
                digest = fingerprint(observation).hex()
                if progress.get(key) == digest:
                    visited += 1
                    continue
                position = json.loads(
                    await c.fetchval("SELECT source_sync_capture('metadata_position','{}')")
                )
                record = dict(
                    kind=stream,
                    observation_id=str(uuid4()),
                    source_position=str(position),
                    observation=observation,
                )
                candidate, count = self.bounded(stream, [*records, record])
                if count != len(records) + 1:
                    # The next record remains unmarked and will be revisited. A
                    # reserved metadata position may be unused; candle cursors do
                    # not assume contiguous metadata/quote positions.
                    break
                records.append(record)
                raw = candidate
                progress[key] = digest
                visited += 1
            if records:
                await self.local.stage(raw, dict(metadata_records=len(records)), c=c)
            # Advance past absent/unchanged/expired grants too. Bytes, hashes and
            # traversal progress commit together; a fenced batch remains durable.
            await c.execute(
                "SELECT source_sync_state('state',$1::jsonb)",
                json.dumps({"metadata": progress, stream + "_index": index + visited}),
            )
        self.state = (await self.local.get())["state"]
        return bool(records)

    async def _metadata_observation(
        self, c: Any, grant: RecoveryStream, stream: str, status: Any
    ) -> dict[str, Any] | None:
        source = await c.fetchrow(
            """SELECT i.canonical_symbol,p.provider_symbol
            FROM instruments i JOIN provider_instruments p ON i.id=p.instrument_id
            WHERE i.id=$1 AND p.provider=$2""",
            grant.instrument_id,
            grant.provider,
        )
        if source is None:
            raise SyncProblem("source_mapping_missing")
        observation = dict(
            instrument_id=str(grant.instrument_id),
            provider=grant.provider,
            canonical_symbol=source["canonical_symbol"],
            provider_symbol=source["provider_symbol"],
        )
        if stream == "coverage":
            row = await c.fetchrow(
                """SELECT * FROM candle_coverage WHERE instrument_id=$1
                AND provider=$2 AND price_basis='native'""",
                grant.instrument_id,
                grant.provider,
            )
            if not row:
                return None
            observation.update(
                start=row["coverage_start"].isoformat(),
                end=row["coverage_end"].isoformat(),
                session_info=json.loads(row["session_info"]),
            )
            for column, name in (
                ("unknown", "unknown"),
                ("not_expected", "not_expected"),
                ("missing", "source_missing"),
            ):
                observation[name] = [
                    dict(start=r.lower.isoformat(), end=r.upper.isoformat()) for r in row[column]
                ]
        else:
            row = status
            now = await c.fetchval("SELECT clock_timestamp()")
            if (
                not row
                or row["expires_at"] <= now
                or (
                    self.state.get("recovered_at")
                    and row["updated_at"] < datetime.fromisoformat(self.state["recovered_at"])
                )
            ):
                return None
            health = next(
                (
                    h
                    for h in json.loads(row["providers"])
                    if (
                        h["provider"] == grant.provider
                        or h["provider"].startswith(grant.provider + "_")
                    )
                    and source["canonical_symbol"] in h["symbols"]
                ),
                None,
            )
            if not health:
                return None
            health["symbols"] = {
                source["canonical_symbol"]: health["symbols"][source["canonical_symbol"]]
            }
            observation.update(
                run_id=str(row["run_id"]),
                captured_at=row["updated_at"].isoformat(),
                expires_at=row["expires_at"].isoformat(),
                running=row["running"],
                database_healthy=row["database_healthy"],
                queue_depth=row["queue_depth"],
                health=health,
            )
        return observation

    async def remote_state(self, records: list[dict[str, Any]], stream: str) -> dict[str, Any]:
        keys = []
        for r in records:
            o = r["observation"]
            grant = next(
                (
                    s
                    for s in self.config.streams
                    if (str(s.instrument_id), s.provider, s.stream)
                    == (o["instrument_id"], o["provider"], stream)
                ),
                None,
            )
            if grant is None:
                raise SyncProblem("source_stream_not_provisioned")
            key = grant.model_dump(mode="json")
            if stream == "candles":
                key["bucket_start"] = o["bucket_start"]
            keys.append(key)
        return await self.request(
            "POST",
            "/reconcile",
            canonical_bytes(
                dict(
                    protocol_version=1,
                    session_id=self.state["session_id"],
                    session_generation=self.state["session_generation"],
                    keys=keys,
                )
            ),
        )

    async def verify_receipt(self, batch: dict[str, Any], receipt: dict[str, Any]) -> None:
        body = json.loads(batch["payload"])
        for name in ("protocol_version", "producer_id", "source_epoch", "batch_id", "stream"):
            if receipt.get(name) != body[name]:
                raise SyncProblem("batch_receipt_identity_mismatch")
        if (
            receipt.get("state") != "committed"
            or receipt.get("payload_sha256") != hashlib.sha256(batch["payload"]).hexdigest()
        ):
            raise SyncProblem("batch_receipt_fingerprint_mismatch")
        results = receipt.get("records", [])
        if (
            len(results) != len(body["records"])
            or {r.get("observation_id") for r in results}
            != {r["observation_id"] for r in body["records"]}
            or any(
                r.get("outcome") not in {"applied", "duplicate", "superseded", "recorded_no_change"}
                for r in results
            )
        ):
            raise SyncProblem("batch_receipt_records_mismatch")
        # A saved receipt can survive while financial rows are lost. Verify actual state
        # on every first local acceptance, including old remote receipt replay.
        if body["stream"] in {"candles", "quotes"}:
            for offset in range(0, len(body["records"]), 100):
                records = body["records"][offset : offset + 100]
                remote = await self.remote_state(records, body["stream"])
                await self.validate_remote(remote)
                for record, actual in zip(records, remote["results"], strict=True):
                    if body["stream"] == "candles":
                        if actual["candle_sha256"] != candle_fingerprint(record["observation"]):
                            # Earlier versions in a page can legitimately be superseded.
                            latest = await self.current_candle(record["observation"])
                            if latest is None or actual["candle_sha256"] != candle_fingerprint(
                                latest
                            ):
                                raise SyncProblem("destination_candle_divergence")
                    else:
                        state = actual.get("quote_state")
                        grant = next(
                            s
                            for s in self.config.streams
                            if s.stream == "quotes"
                            and (str(s.instrument_id), s.provider)
                            == (
                                record["observation"]["instrument_id"],
                                record["observation"]["provider"],
                            )
                        )
                        if (
                            not state
                            or not state.get("materialized")
                            or (
                                state["producer_id"],
                                state["source_epoch"],
                                state["ownership_generation"],
                            )
                            != (
                                body["producer_id"],
                                body["source_epoch"],
                                grant.ownership_generation,
                            )
                            or int(state["source_position"]) < int(record["source_position"])
                        ):
                            raise SyncProblem("destination_quote_parity_failed")
                        if (
                            int(state["source_position"]) == int(record["source_position"])
                            and state["observation_sha256"]
                            != fingerprint(
                                dict(
                                    kind="quote",
                                    observation=decode_batch(batch["payload"])
                                    .records[body["records"].index(record)]
                                    .observation.model_dump(mode="json"),
                                )
                            ).hex()
                        ):
                            raise SyncProblem("destination_quote_identity_failed")
                        # Supersession is safe only when this installation can establish
                        # the accepted newer snapshot, not merely a server-side counter.
                        async with self.local.transaction() as c:
                            known = await c.fetchval(
                                """SELECT record FROM source_quote_state
                                WHERE instrument_id=$1 AND provider=$2 AND version=$3""",
                                grant.instrument_id,
                                grant.provider,
                                int(state["source_position"]),
                            )
                            if known is None:
                                known = await c.fetchval(
                                    """SELECT x::text FROM source_sync_batches b,
                                    LATERAL jsonb_array_elements(
                                        convert_from(b.payload,'UTF8')::jsonb
                                        ->'records') x WHERE b.stream='quotes'
                                    AND x->>'observation_id'=$1 LIMIT 1""",
                                    state["observation_id"],
                                )
                        if known is None:
                            raise SyncProblem("newer_quote_state_not_established")
                        if Tick.model_validate(
                            json.loads(known)["observation"]
                        ) != Tick.model_validate(state["payload"]):
                            raise SyncProblem("destination_quote_fields_diverged")

    async def validate_remote(self, remote: dict[str, Any]) -> None:
        if (
            remote.get("producer_id") != str(self.config.producer_id)
            or remote.get("protocol_version") != 1
        ):
            raise SyncProblem("reconciliation_identity_mismatch")
        anchor = json.loads(self.config.anchor_file.read_bytes())
        if anchor.get("destination_id") and anchor["destination_id"] != remote["destination_id"]:
            raise SyncProblem("destination_replacement_requires_review")
        async with self.local.transaction() as c:
            head = await c.fetchval("SELECT head FROM source_capture_clock")
        if any(int(r["high_water"]) > head for r in remote["results"]):
            raise SyncProblem("source_restore_high_water_mismatch")
        high_water = max((int(r["high_water"]) for r in remote["results"]), default=0)
        if high_water < anchor.get("remote_high_water", 0):
            raise SyncProblem("destination_rollback_requires_review")
        anchor["remote_high_water"] = high_water
        anchor.update(destination_id=remote["destination_id"], head=head)
        self.local.write_anchor(anchor)

    async def continuity(self) -> None:
        grant = next((s for s in self.config.streams if s.stream in {"candles", "quotes"}), None)
        if grant is None:
            raise SyncProblem("financial_stream_required")
        remote = await self.request(
            "POST",
            "/reconcile",
            canonical_bytes(
                dict(
                    protocol_version=1,
                    session_id=self.state["session_id"],
                    session_generation=self.state["session_generation"],
                    keys=[grant.model_dump(mode="json")],
                )
            ),
        )
        await self.validate_remote(remote)
        anchor = json.loads(self.config.anchor_file.read_bytes())
        if anchor.get("receipt"):
            expected = anchor["receipt"]
            try:
                receipt = await self.request("GET", "/receipts/" + expected["batch_id"])
            except SyncProblem as error:
                if error.code == "receipt_not_found":
                    raise SyncProblem("destination_receipt_missing_requires_review") from None
                raise
            if receipt.get("state") != "committed" or any(
                receipt.get(k) != v for k, v in expected.items()
            ):
                raise SyncProblem("continuity_receipt_mismatch")
        self.continuity_checked = True

    async def current_candle(self, observation: dict[str, Any]) -> dict[str, Any] | None:
        async with self.local.transaction() as c:
            value = await c.fetchval(
                """SELECT payload FROM market_candles WHERE instrument_id=$1
                AND provider=$2 AND interval='1m' AND price_basis='native' AND bucket_start=$3""",
                UUID(observation["instrument_id"]),
                observation["provider"],
                datetime.fromisoformat(observation["bucket_start"].replace("Z", "+00:00")),
            )
        return json.loads(value) if value else None

    async def deliver(self, stream: str) -> bool:
        batch = await self.local.pending(stream)
        if not batch or not batch["ready"]:
            return False
        if batch["phase"] == "received":
            await self.local.acknowledge(batch)
            await self.checkpoint("after_source_ack")
            return True
        body = json.loads(batch["payload"])
        if (
            stream == "status"
            and self.state.get("recovered_at")
            and any(
                datetime.fromisoformat(r["observation"]["captured_at"].replace("Z", "+00:00"))
                < datetime.fromisoformat(self.state["recovered_at"])
                for r in body["records"]
            )
        ):
            await self.local.operation(
                "quarantine",
                dict(batch_id=str(batch["batch_id"]), code="captured_status_prior_session"),
            )
            return True
        if stream == "status" and any(
            datetime.fromisoformat(r["observation"]["expires_at"].replace("Z", "+00:00"))
            <= datetime.now().astimezone()
            for r in body["records"]
        ):
            await self.local.operation(
                "quarantine", dict(batch_id=str(batch["batch_id"]), code="captured_status_expired")
            )
            return True
        try:
            try:
                receipt = await self.request("GET", "/receipts/" + str(batch["batch_id"]))
            except SyncProblem as error:
                if error.code != "receipt_not_found":
                    raise
                if (body["session_id"], body["session_generation"]) != (
                    self.state["session_id"],
                    self.state["session_generation"],
                ):
                    # Stage the replacement BEFORE retiring the original; crashes can
                    # leave both, but observation IDs/ledger keep replay harmless.
                    replacement = self.envelope(stream, body["records"])
                    await self.local.stage(replacement, batch["bookkeeping"])
                    await self.local.operation(
                        "replace",
                        dict(batch_id=str(batch["batch_id"]), code="session_envelope_replaced"),
                    )
                    return True
                await self.checkpoint("before_http")
                receipt = await self.request("POST", "/" + stream, batch["payload"], batch=True)
            await self.checkpoint("after_http_before_receipt")
            await self.verify_receipt(batch, receipt)
            await self.local.operation(
                "receipt", dict(batch_id=str(batch["batch_id"]), receipt=receipt)
            )
            await self.local.anchor(receipt)
            await self.checkpoint("after_receipt_commit")
        except SyncProblem as error:
            if error.code == "producer_session_fenced":
                self.lease_until = 0
                return False
            if error.retryable:
                delay = max(error.delay, min(300, 2 ** min(batch["attempts"], 8)))
                await self.local.operation(
                    "attempt",
                    dict(
                        batch_id=str(batch["batch_id"]),
                        code=error.code,
                        delay=delay + random.uniform(0, min(5, delay / 4)),
                    ),
                )
                return False
            await self.local.operation(
                "quarantine", dict(batch_id=str(batch["batch_id"]), code=error.code)
            )
            raise
        updated = await self.local.pending(stream)
        if updated is None:
            raise SyncProblem("staged_batch_disappeared")
        await self.local.acknowledge(updated)
        await self.checkpoint("after_source_ack")
        return True

    async def enroll(self) -> None:
        grants = [s for s in self.config.streams if s.stream == "candles"]
        if grants:
            grant = grants[self.enroll_cursor % len(grants)]
            try:
                count = await self.capture.seed_candles(
                    grant.instrument_id, grant.provider, min(100, self.config.max_records)
                )
            except ValidationError as error:
                await self.save(
                    historical_preflight=dict(
                        instrument_id=str(grant.instrument_id),
                        provider=grant.provider,
                        issues=[
                            dict(location=e["loc"], category=e["type"])
                            for e in error.errors(include_input=False, include_context=False)
                        ][:20],
                    )
                )
                raise SyncProblem("historical_preflight_failed") from None
            self.enroll_cursor += 1
            progress = dict(self.state.get("enrollment", {}))
            progress[str(grant.instrument_id) + ":" + grant.provider] = dict(last_count=count)
            await self.save(enrollment=progress)
            await self.save(enrollment_index=self.enroll_cursor)
        await self.capture.seed_quotes(min(100, self.config.max_records))

    async def reconciliation(self, *, repair: bool = False) -> list[dict[str, Any]]:
        grants = [s for s in self.config.streams if s.stream in {"candles", "quotes"}]
        if not grants:
            return []
        grant = grants[self.reconcile_cursor % len(grants)]
        self.reconcile_cursor += 1
        key = str(grant.instrument_id) + ":" + grant.provider + ":" + grant.stream
        progress = dict(self.state.get("reconciliation", {}))
        cursor = progress.get(key, {}).get("cursor")
        async with self.local.transaction() as c:
            if grant.stream == "candles":
                rows = await c.fetch(
                    """SELECT payload FROM market_candles WHERE instrument_id=$1
                    AND provider=$2 AND interval='1m' AND price_basis='native'
                    AND ($3::timestamptz IS NULL OR bucket_start>$3)
                    ORDER BY bucket_start LIMIT 50""",
                    grant.instrument_id,
                    grant.provider,
                    datetime.fromisoformat(cursor) if cursor else None,
                )
                records = [dict(observation=json.loads(r["payload"])) for r in rows]
            else:
                row = await c.fetchval(
                    "SELECT record FROM source_quote_state WHERE instrument_id=$1 AND provider=$2",
                    grant.instrument_id,
                    grant.provider,
                )
                records = [json.loads(row)] if row else []
        if not records:
            progress[key] = dict(cursor=None, discrepancies=[])
            await self.save(reconciliation=progress, reconciliation_index=self.reconcile_cursor)
            return []
        remote = await self.remote_state(records, grant.stream)
        await self.validate_remote(remote)
        differences = []
        for record, result in zip(records, remote["results"], strict=True):
            o = record["observation"]
            if grant.stream == "candles":
                if result["candle_sha256"] != candle_fingerprint(o):
                    differences.append(
                        dict(
                            bucket_start=o["bucket_start"],
                            reason="missing" if result["candle_sha256"] is None else "divergent",
                        )
                    )
                async with self.local.transaction() as c:
                    local_revisions = await revision_summary(
                        c,
                        grant.instrument_id,
                        grant.provider,
                        datetime.fromisoformat(o["bucket_start"].replace("Z", "+00:00")),
                    )
                if result.get("revision_truncated") or local_revisions["revision_truncated"]:
                    differences.append(
                        dict(bucket_start=o["bucket_start"], reason="revision_manifest_bound")
                    )
                elif result.get("revision_sha256") != local_revisions["revision_sha256"]:
                    differences.append(
                        dict(bucket_start=o["bucket_start"], reason="revision_history_mismatch")
                    )
            else:
                state = result.get("quote_state")
                if (
                    not state
                    or not state["materialized"]
                    or (
                        state["producer_id"],
                        state["source_epoch"],
                        state["ownership_generation"],
                    )
                    != (
                        str(self.config.producer_id),
                        str(grant.source_epoch),
                        grant.ownership_generation,
                    )
                    or state["source_position"] != record["source_position"]
                    or state["observation_id"] != record["observation_id"]
                    or Tick.model_validate(state["payload"]) != Tick.model_validate(o)
                ):
                    differences.append(dict(reason="quote_parity_mismatch"))
        if (
            repair
            and grant.stream == "candles"
            and any(d["reason"] in {"missing", "divergent"} for d in differences)
        ):
            await self.capture.resnapshot_candles(
                grant.instrument_id,
                grant.provider,
                [
                    datetime.fromisoformat(d["bucket_start"].replace("Z", "+00:00"))
                    for d in differences
                    if d["reason"] in {"missing", "divergent"}
                ],
            )
        if repair and grant.stream == "quotes" and differences:
            await self.capture.resnapshot_quote(grant.instrument_id, grant.provider)
        progress[key] = dict(
            cursor=records[-1]["observation"].get("bucket_start"),
            discrepancies=differences,
            repair_requested=repair,
        )
        await self.save(reconciliation=progress, reconciliation_index=self.reconcile_cursor)
        return differences

    async def metrics(self) -> dict[str, Any]:
        async with self.local.transaction() as c:
            row = dict(
                await c.fetchrow("""SELECT source_epoch::text,head,candle_ack,
                    pending_rows,pending_bytes,
                pg_database_size(current_database()) AS database_bytes,
                (SELECT min(captured_at) FROM source_candle_outbox
                    WHERE position>candle_ack) AS oldest_candle,
                (SELECT coalesce(max(version-acked_version),0) FROM source_quote_state)
                    AS quote_version_lag,
                (SELECT count(*) FROM source_sync_batches WHERE phase='staged') AS staged,
                (SELECT count(*) FROM source_sync_batches WHERE phase='quarantined') AS quarantined,
                (SELECT coalesce(sum(attempts),0) FROM source_sync_batches) AS retries,
                (SELECT coalesce(sum(octet_length(payload)),0) FROM source_sync_batches)
                    AS staged_bytes
                FROM source_capture_clock""")
            )
            row.update(json.loads(await c.fetchval("SELECT source_sync_storage()")))
            row["delivery_counts"] = json.loads(
                await c.fetchval(
                    "SELECT coalesce(state->'delivery_counts','{}')::text FROM source_sync_owner"
                )
            )
            row["collector_capture_valid"] = bool(
                await c.fetchval("""SELECT running AND
                database_healthy AND expires_at>clock_timestamp() FROM collector_status
                WHERE singleton""")
            )
        row["state_filesystem_free_bytes"] = shutil.disk_usage(self.config.anchor_file.parent).free
        row["session_lease_seconds"] = max(0, self.lease_until - time.monotonic())
        return row

    async def cycle(self) -> None:
        await self.session()
        if not self.continuity_checked:
            await self.continuity()
        # Bounded fair slots: a continuously changing quote stream cannot starve history.
        for stream in ("quotes", "candles", "coverage", "status"):
            if not any(s.stream == stream for s in self.config.streams):
                continue
            if await self.local.pending(stream) is None:
                await self.stage(stream)
            await self.deliver(stream)
        if self.config.enroll:
            await self.enroll()
        if time.monotonic() >= self.reconcile_at:
            await self.reconciliation()
            self.reconcile_at = time.monotonic() + self.config.reconcile_seconds
        await self.capture.cleanup()
        await self.local.operation("prune", {})

    async def supervise(self, *, once: bool = False, repair: bool = False) -> None:
        async with self.capture.consumer_lock() as connection:
            await self.local.claim(connection)
            self.state = await self.local.initialize()
            self.enroll_cursor = int(self.state.get("enrollment_index", 0))
            self.reconcile_cursor = int(self.state.get("reconciliation_index", 0))

            async def work() -> None:
                failures = 0
                while True:
                    try:
                        await self.cycle()
                        if repair:
                            await self.reconciliation(repair=True)
                        print(json.dumps(await self.metrics(), default=str), flush=True)
                        failures = 0
                    except SyncProblem as error:
                        await self.save(
                            last_error=dict(code=error.code, retryable=error.retryable),
                            upload_health="retrying" if error.retryable else "stopped",
                        )
                        if not error.retryable or once:
                            raise
                        failures += 1
                        await asyncio.sleep(max(error.delay, min(60, 2 ** min(failures, 6))))
                    except (
                        asyncpg.SerializationError,
                        asyncpg.DeadlockDetectedError,
                        asyncpg.ConnectionDoesNotExistError,
                        asyncpg.CannotConnectNowError,
                        ConnectionError,
                        TimeoutError,
                    ):
                        failures += 1
                        await asyncio.sleep(min(60, 2 ** min(failures, 6)))
                    if once:
                        return
                    await asyncio.sleep(self.config.interval_seconds)

            async def watch() -> None:
                while True:
                    await asyncio.sleep(0.25)
                    if connection.is_closed():
                        raise CaptureConflict("worker ownership connection lost")
                    await connection.fetchval("SELECT 1")

            async with asyncio.TaskGroup() as group:
                monitor = group.create_task(watch())
                await group.create_task(work())
                monitor.cancel()


async def run(config: SyncConfig, *, once: bool = False, repair: bool = False) -> None:
    settings: dict[str, Any] = dict(
        _env_file=None,
        database_url=config.source_dsn_file.read_text().strip(),
        application_role="collector",
        source_capture_enabled=True,
        source_capture_max_rows=config.source_max_rows,
        source_capture_max_bytes=config.source_max_bytes,
    )
    store = Store(Settings(**settings))
    worker = SyncWorker(store, config)
    try:
        await store.connect()
        await worker.supervise(once=once, repair=repair)
    finally:
        await worker.http.close()
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Durable source HTTPS synchronization")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--repair", action="store_true", help="explicit bounded candle resnapshot")
    args = parser.parse_args()
    try:
        asyncio.run(run(SyncConfig.load(args.config), once=args.once, repair=args.repair))
    except KeyboardInterrupt:
        pass
    except Exception:
        # Tracebacks/config/Pydantic inputs can contain private DSNs or tokens.
        raise SystemExit(
            "synchronization stopped; retain durable state and inspect private configuration"
        ) from None


if __name__ == "__main__":
    main()
