"""Destination transaction foundation shared by authenticated HTTPS ingestion.

The caller supplies a producer identity already authenticated by trusted server code.
Never mount this interface directly as an unauthenticated route.
"""

import hashlib
import json
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

from marketdata.domain import Instrument, Tick
from marketdata.persistence import Store
from marketdata.quote_snapshots import is_snapshot, materialize
from marketdata.replication_models import (
    Batch,
    CandleRecord,
    CoverageObservation,
    CoverageRecord,
    ObservationRecord,
    QuoteRecord,
    StatusRecord,
    decode_batch,
    fingerprint,
)


class ReplicationError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class DestinationWriter:
    def __init__(self, store: Store):
        self.store = store

    async def validate_permissions(self) -> None:
        """Required at ingestion startup, separate from collector Store semantics."""
        async with self.store.connection() as c:
            unsafe = await c.fetchval("""
                SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=current_user
                    AND (rolsuper OR rolcreatedb OR rolcreaterole
                        OR rolreplication OR rolbypassrls))
                OR has_schema_privilege(current_user,'public','CREATE')
                OR has_database_privilege(current_user,current_database(),'CREATE')
                OR EXISTS(SELECT 1 FROM unnest(ARRAY['market_ticks','candle_revisions',
                    'ingest_batches','ingest_observations']) t(name)
                    WHERE has_table_privilege(current_user,name,'UPDATE'))
                OR EXISTS(SELECT 1 FROM unnest(ARRAY['instruments','provider_instruments',
                    'ingest_producers','ingest_source_owners','alembic_version',
                    'ingest_destination_state']) t(name)
                    WHERE has_table_privilege(current_user,name,'INSERT,UPDATE,DELETE,TRUNCATE'))
                OR EXISTS(SELECT 1 FROM unnest(ARRAY['market_ticks','market_candles',
                    'latest_prices','latest_candles','candle_coverage','candle_revisions',
                    'ingest_batches','ingest_observations','ingest_candle_heads',
                    'ingest_coverage_heads','ingest_source_status','ingest_quote_heads','candle_observation_heads'])
                        t(name)
                    WHERE has_table_privilege(current_user,name,'DELETE,TRUNCATE'))
            """)
            required = await c.fetchval("""
                SELECT has_database_privilege(current_user,current_database(),'TEMP')
                AND has_function_privilege(current_user,'ingest_lock_producer(uuid)','EXECUTE')
                AND has_function_privilege(current_user,
                    'ingest_lock_owner(uuid,text,text)','EXECUTE')
                AND has_sequence_privilege(current_user,'candle_revisions_id_seq','USAGE')
                AND NOT EXISTS(SELECT 1 FROM (VALUES
                    ('ingest_quote_heads','SELECT'),('ingest_quote_heads','INSERT'),
                    ('ingest_quote_heads','UPDATE'),('ingest_destination_state','SELECT'),
                    ('instruments','SELECT'),('provider_instruments','SELECT'),
                    ('alembic_version','SELECT'),('ingest_producers','SELECT'),
                    ('ingest_source_owners','SELECT'),
                    ('candle_revisions','SELECT'),('candle_revisions','INSERT'),
                    ('candle_observation_heads','SELECT'),('candle_observation_heads','INSERT'),
                    ('candle_observation_heads','UPDATE'),
                    ('ingest_candle_heads','SELECT'),('ingest_candle_heads','INSERT'),
                    ('ingest_candle_heads','UPDATE'),
                    ('ingest_coverage_heads','SELECT'),('ingest_coverage_heads','INSERT'),
                    ('ingest_coverage_heads','UPDATE'),
                    ('ingest_source_status','SELECT'),('ingest_source_status','INSERT'),
                    ('ingest_source_status','UPDATE'),('marketdata_events','SELECT'),
                    ('market_ticks','SELECT'),('market_ticks','INSERT'),
                    ('ingest_batches','SELECT'),('ingest_batches','INSERT'),
                    ('ingest_observations','SELECT'),('ingest_observations','INSERT'),
                    ('market_candles','SELECT'),('market_candles','INSERT'),('market_candles','UPDATE'),
                    ('latest_prices','SELECT'),('latest_prices','INSERT'),('latest_prices','UPDATE'),
                    ('latest_candles','SELECT'),('latest_candles','INSERT'),('latest_candles','UPDATE'),
                    ('candle_coverage','SELECT'),('candle_coverage','INSERT'),('candle_coverage','UPDATE'),
                    ('marketdata_events','INSERT'),('marketdata_events','DELETE'),
                    ('marketdata_event_clock','SELECT'),('marketdata_event_clock','UPDATE')
                ) p(name,privilege) WHERE NOT has_table_privilege(current_user,name,privilege))
            """)
        if unsafe or not required:
            raise RuntimeError("ingestion requires a restricted destination writer role")

    async def receipt(self, producer_id: UUID, batch_id: UUID) -> dict[str, Any] | None:
        """Authenticated producer-only lookup; remains usable after a session rotation."""
        async with self.store.connection() as c:
            value = await c.fetchval(
                """SELECT b.receipt FROM ingest_batches b JOIN ingest_producers p
                    USING(producer_id) WHERE b.producer_id=$1 AND b.batch_id=$2 AND p.enabled""",
                producer_id,
                batch_id,
            )
        return json.loads(value) if value else None

    async def commit(
        self, authenticated_producer: UUID, payload: bytes, *, credential_key_id: str | None = None
    ) -> dict[str, Any]:
        """Hash exact bytes and return only after data, events and receipt commit together."""
        batch = decode_batch(payload)
        if batch.producer_id != authenticated_producer:
            raise ReplicationError("producer_not_authorized")
        digest = hashlib.sha256(payload).digest()
        async with self.store.connection() as c, c.transaction():
            await c.execute("SET LOCAL synchronous_commit=on")
            if credential_key_id is not None:
                producer = await c.fetchrow(
                    "SELECT * FROM ingest_lock_producer($1)", authenticated_producer
                )
                if producer is None or producer["credential_key_id"] != credential_key_id:
                    raise ReplicationError("producer_not_authorized")
            result = await self._commit(c, batch, digest)
        return result

    async def _commit(self, c: Any, batch: Batch, digest: bytes) -> dict[str, Any]:
        producer = await c.fetchrow("SELECT * FROM ingest_lock_producer($1)", batch.producer_id)
        now = await c.fetchval("SELECT clock_timestamp()")
        if producer is None or not producer["enabled"]:
            raise ReplicationError("producer_not_authorized")
        if (producer["session_id"], producer["session_generation"]) != (
            batch.session_id,
            batch.session_generation,
        ) or producer["session_expires_at"] <= now:
            raise ReplicationError("producer_session_fenced")
        await c.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"marketdata:batch:{batch.producer_id}:{batch.batch_id}",
        )
        previous = await c.fetchrow(
            "SELECT payload_sha256,receipt FROM ingest_batches "
            "WHERE producer_id=$1 AND batch_id=$2",
            batch.producer_id,
            batch.batch_id,
        )
        if previous:
            if previous["payload_sha256"] != digest:
                raise ReplicationError("batch_payload_conflict")
            return cast(dict[str, Any], json.loads(previous["receipt"]))

        # Tick-history UUID keys are shared even across producer namespaces. Reserve these
        # before market writes too, so conflicting cross-producer UUIDs cannot silently drop.
        for tick_id in sorted(
            r.observation.tick_id for r in batch.records if isinstance(r, QuoteRecord)
        ):
            await c.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"marketdata:quote:{tick_id}",
            )
        # Lock every identity before any market write/journal-clock lock. This also covers
        # hostile reused UUIDs on different sources without opposite-order unique waits.
        for observation_id in sorted(r.observation_id for r in batch.records):
            await c.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"marketdata:observation:{batch.producer_id}:{batch.source_epoch}:{observation_id}",
            )
        sources = {(r.observation.instrument_id, r.observation.provider) for r in batch.records}
        owners: dict[tuple[UUID, str], Any] = {}
        for instrument_id, provider in sorted(sources, key=lambda key: (str(key[0]), key[1])):
            owner = await c.fetchrow(
                "SELECT * FROM ingest_lock_owner($1,$2,$3)",
                instrument_id,
                provider,
                batch.stream,
            )
            if owner is None or (owner["producer_id"], owner["source_epoch"]) != (
                batch.producer_id,
                batch.source_epoch,
            ):
                raise ReplicationError("source_not_authorized")
            owners[instrument_id, provider] = owner
        await self.store.lock_sources(c, list(sources))

        outcomes = []
        # Preserve each distinct candle version inside a batch, in source order.
        for record in sorted(batch.records, key=lambda r: r.source_position):
            observation = record.observation
            owner = owners[observation.instrument_id, observation.provider]
            await self._validate_source(c, record, owner, now)
            content = record.observation.model_dump(mode="json")
            observation_digest = fingerprint({"kind": record.kind, "observation": content})
            duplicate = await c.fetchrow(
                """SELECT observation_sha256,source_position FROM ingest_observations
                    WHERE producer_id=$1 AND source_epoch=$2 AND observation_id=$3""",
                batch.producer_id,
                batch.source_epoch,
                record.observation_id,
            )
            if duplicate:
                if (duplicate["observation_sha256"], duplicate["source_position"]) != (
                    observation_digest,
                    record.source_position,
                ):
                    raise ReplicationError("observation_payload_conflict")
                outcome = "duplicate"
            else:
                outcome = await self._apply(c, batch, record, owner, observation_digest)
                await c.execute(
                    """INSERT INTO ingest_observations
                        (producer_id,source_epoch,observation_id,batch_id,instrument_id,provider,
                         stream,source_position,ownership_generation,observation_sha256,payload,outcome)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)""",
                    batch.producer_id,
                    batch.source_epoch,
                    record.observation_id,
                    batch.batch_id,
                    observation.instrument_id,
                    observation.provider,
                    batch.stream,
                    record.source_position,
                    owner["ownership_generation"],
                    observation_digest,
                    json.dumps(content),
                    outcome,
                )
            result_record: dict[str, Any] = {
                "observation_id": str(record.observation_id),
                "outcome": outcome,
            }
            if isinstance(record, QuoteRecord) and is_snapshot(record.observation):
                result_record["quote_state"] = await self.quote_state(
                    c, observation.instrument_id, observation.provider
                )
            outcomes.append(result_record)
        # A long-running transaction cannot finish after its admitted lease has expired.
        now = await c.fetchval("SELECT clock_timestamp()")
        if producer["session_expires_at"] <= now:
            raise ReplicationError("producer_session_fenced")
        result = dict(
            protocol_version=1,
            stream=batch.stream,
            producer_id=str(batch.producer_id),
            source_epoch=str(batch.source_epoch),
            batch_id=str(batch.batch_id),
            payload_sha256=digest.hex(),
            state="committed",
            server_ingested_at=now.isoformat(),
            records=outcomes,
            counts={
                outcome: sum(r["outcome"] == outcome for r in outcomes)
                for outcome in ("applied", "recorded_no_change", "superseded", "duplicate")
            },
        )
        await c.execute(
            """INSERT INTO ingest_batches
                (producer_id,batch_id,source_epoch,stream,payload_sha256,receipt,server_ingested_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7)""",
            batch.producer_id,
            batch.batch_id,
            batch.source_epoch,
            batch.stream,
            digest,
            json.dumps(result),
            now,
        )
        return result

    async def _validate_source(
        self, c: Any, record: ObservationRecord, owner: Any, now: Any
    ) -> None:
        o = record.observation
        source = await c.fetchrow(
            """SELECT i.payload,p.provider_symbol FROM instruments i JOIN provider_instruments p
                ON p.instrument_id=i.id WHERE i.id=$1 AND p.provider=$2""",
            o.instrument_id,
            o.provider,
        )
        if source is None:
            raise ReplicationError("source_not_authorized")
        instrument = Instrument.model_validate_json(source["payload"])
        if (
            o.canonical_symbol != instrument.canonical_symbol
            or o.provider_symbol != source["provider_symbol"]
        ):
            raise ReplicationError("source_identity_conflict")
        if isinstance(record, QuoteRecord):
            quote = record.observation
            if (quote.asset_class, quote.instrument_type, quote.exchange) != (
                instrument.asset_class,
                instrument.instrument_type,
                instrument.exchange,
            ):
                raise ReplicationError("source_identity_conflict")
        if isinstance(record, (CandleRecord, QuoteRecord)):
            observation = record.observation
            if observation.realtime_status.value not in owner["allowed_statuses"]:
                raise ReplicationError("source_status_not_authorized")
            if observation.received_timestamp > now + timedelta(seconds=5):
                raise ReplicationError("collector_clock_ahead")
        if isinstance(record, QuoteRecord) and record.observation.event_timestamp is not None:
            if record.observation.event_timestamp > now + timedelta(seconds=5):
                raise ReplicationError("source_clock_ahead")
        if isinstance(record, StatusRecord) and record.observation.captured_at > now + timedelta(
            seconds=5
        ):
            raise ReplicationError("collector_clock_ahead")

    async def _apply(
        self, c: Any, batch: Batch, record: ObservationRecord, owner: Any, digest: bytes
    ) -> str:
        if isinstance(record, QuoteRecord):
            quote = record.observation
            existing = await c.fetchval(
                "SELECT payload FROM market_ticks WHERE storage_timestamp=$1 AND tick_id=$2",
                quote.storage_timestamp,
                quote.tick_id,
            )
            if existing and Tick.model_validate_json(existing) != Tick.model_validate_json(
                quote.model_dump_json()
            ):
                raise ReplicationError("observation_payload_conflict")
            if is_snapshot(quote):
                return await self._snapshot(c, batch, record, owner, digest)
            if await c.fetchval(
                """SELECT EXISTS(SELECT 1 FROM ingest_quote_heads
                WHERE instrument_id=$1 AND provider=$2 AND producer_id=$3 AND source_epoch=$4
                    AND ownership_generation=$5)""",
                quote.instrument_id,
                quote.provider,
                batch.producer_id,
                batch.source_epoch,
                owner["ownership_generation"],
            ):
                raise ReplicationError("quote_snapshot_required")
            # A delayed observation without source time cannot prove recency. Keep its
            # history/provenance, but do not replace current state using receipt time.
            count = await self.store.write_observations_in_transaction(
                c,
                [quote],
                update_latest=not (
                    quote.realtime_status.value == "delayed" and quote.event_timestamp is None
                ),
            )
            if not count:
                # A direct Store writer may have won the PK race after the earlier lookup.
                existing = await c.fetchval(
                    "SELECT payload FROM market_ticks WHERE storage_timestamp=$1 AND tick_id=$2",
                    quote.storage_timestamp,
                    quote.tick_id,
                )
                if not existing or Tick.model_validate_json(existing) != Tick.model_validate_json(
                    quote.model_dump_json()
                ):
                    raise ReplicationError("observation_payload_conflict")
            return "applied" if count else "recorded_no_change"

        o = record.observation
        table = {
            "candle": "ingest_candle_heads",
            "coverage": "ingest_coverage_heads",
            "status": "ingest_source_status",
        }[record.kind]
        key_columns = ["instrument_id", "provider"]
        keys: list[Any] = [o.instrument_id, o.provider]
        if isinstance(record, CandleRecord):
            key_columns += ["interval", "bucket_start", "price_basis"]
            candle = record.observation
            keys += [candle.interval, candle.bucket_start, candle.price_basis]
        where = " AND ".join(f"{name}=${i}" for i, name in enumerate(key_columns, 1))
        head = await c.fetchrow(f"SELECT * FROM {table} WHERE {where}", *keys)
        version = (owner["ownership_generation"], record.source_position)
        if head:
            old_version = (head["ownership_generation"], head["source_position"])
            if version < old_version:
                return "superseded"
            if version == old_version:
                if head["observation_sha256"] != digest:
                    raise ReplicationError("source_version_conflict")
                return "recorded_no_change"

        outcome = "applied"
        if isinstance(record, CandleRecord):
            restore_missing = False
            if head and head["ownership_generation"] == owner["ownership_generation"]:
                accepted = await c.fetchval(
                    """SELECT payload FROM ingest_observations WHERE
                    producer_id=$1 AND source_epoch=$2 AND instrument_id=$3 AND provider=$4
                    AND stream='candles' AND source_position=$5
                    AND observation_sha256=$6 LIMIT 1""",
                    head["producer_id"],
                    head["source_epoch"],
                    o.instrument_id,
                    o.provider,
                    head["source_position"],
                    head["observation_sha256"],
                )
                if accepted:
                    from marketdata.reconciliation import candle_fingerprint

                    restore_missing = candle_fingerprint(
                        json.loads(accepted)
                    ) == candle_fingerprint(record.observation.model_dump(mode="json"))
            result = await self.store.write_candles_in_transaction(
                c, [record.observation], [], restore_missing=restore_missing
            )
            outcome = "applied" if result["changed"] else "recorded_no_change"
        elif isinstance(record, CoverageRecord):
            coverage = record.observation
            # Versions cover a cumulative whole-source snapshot, not independent windows.
            # Otherwise a newer disjoint window could silently supersede an older backfill.
            if head and head["ownership_generation"] == owner["ownership_generation"]:
                previous_payload = await c.fetchval(
                    """SELECT payload FROM ingest_observations
                        WHERE producer_id=$1 AND source_epoch=$2 AND instrument_id=$3
                        AND provider=$4 AND stream='coverage' AND source_position=$5
                        AND observation_sha256=$6 LIMIT 1""",
                    head["producer_id"],
                    head["source_epoch"],
                    coverage.instrument_id,
                    coverage.provider,
                    head["source_position"],
                    head["observation_sha256"],
                )
                if previous_payload is None:
                    raise ReplicationError("coverage_state_missing")
                previous = CoverageObservation.model_validate_json(previous_payload)
                if coverage.start > previous.start or coverage.end < previous.end:
                    raise ReplicationError("coverage_window_conflict")
            await self.store.apply_coverage_in_transaction(
                c,
                coverage.instrument_id,
                coverage.provider,
                coverage.start,
                coverage.end,
                [(w.start, w.end) for w in coverage.unknown],
                [(w.start, w.end) for w in coverage.not_expected],
                coverage.session_info,
            )
        values = [*keys, batch.producer_id, batch.source_epoch, *version, digest]
        columns = [
            *key_columns,
            "producer_id",
            "source_epoch",
            "ownership_generation",
            "source_position",
            "observation_sha256",
        ]
        if isinstance(record, StatusRecord):
            status = record.observation
            columns += ["session_id", "session_generation", "captured_at", "expires_at", "payload"]
            values += [
                batch.session_id,
                batch.session_generation,
                status.captured_at,
                status.expires_at,
                status.model_dump_json(),
            ]
        update = ",".join(f"{name}=EXCLUDED.{name}" for name in columns if name not in key_columns)
        if isinstance(record, StatusRecord):
            update += ",server_ingested_at=clock_timestamp()"
        placeholders = ",".join(f"${i}" for i in range(1, len(values) + 1))
        written = await c.fetchval(
            f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({','.join(key_columns)}) DO UPDATE SET {update} "
            f"WHERE (EXCLUDED.ownership_generation,EXCLUDED.source_position) > "
            f"({table}.ownership_generation,{table}.source_position) RETURNING source_position",
            *values,
        )
        if written is None:
            raise ReplicationError("source_version_conflict")
        return outcome

    async def quote_state(self, c: Any, instrument: UUID, provider: str) -> Any:
        row = await c.fetchrow(
            """SELECT h.*, p.payload AS actual FROM ingest_quote_heads h
                LEFT JOIN latest_prices p USING(instrument_id,provider)
                WHERE instrument_id=$1 AND provider=$2""",
            instrument,
            provider,
        )
        if row is None:
            return None
        return dict(
            producer_id=str(row["producer_id"]),
            source_epoch=str(row["source_epoch"]),
            ownership_generation=row["ownership_generation"],
            source_position=str(row["source_position"]),
            observation_id=str(row["observation_id"]),
            observation_sha256=bytes(row["observation_sha256"]).hex(),
            materialized=(
                row["actual"] is not None
                and Tick.model_validate_json(row["actual"])
                == Tick.model_validate_json(row["payload"])
            ),
            payload=json.loads(row["actual"]) if row["actual"] else None,
        )

    async def _snapshot(
        self, c: Any, batch: Batch, record: QuoteRecord, owner: Any, digest: bytes
    ) -> str:
        quote = record.observation
        head = await c.fetchrow(
            "SELECT * FROM ingest_quote_heads WHERE instrument_id=$1 AND provider=$2",
            quote.instrument_id,
            quote.provider,
        )
        same_context = head is not None and (
            head["producer_id"],
            head["source_epoch"],
            head["ownership_generation"],
        ) == (batch.producer_id, batch.source_epoch, owner["ownership_generation"])
        if head:
            if owner["ownership_generation"] < head["ownership_generation"]:
                raise ReplicationError("source_version_conflict")
            if same_context and record.source_position < head["source_position"]:
                return "superseded"
            if same_context and record.source_position == head["source_position"]:
                if digest != head["observation_sha256"]:
                    raise ReplicationError("source_version_conflict")
                return "recorded_no_change"
            if not same_context and owner["ownership_generation"] <= head["ownership_generation"]:
                raise ReplicationError("source_version_conflict")
        try:
            snapshot = materialize(
                quote, Tick.model_validate_json(head["payload"]) if same_context else None
            )
        except (ValueError, TypeError, KeyError, StopIteration):
            raise ReplicationError("invalid_quote_snapshot") from None
        # Field-level clocks/status are checked too, including non-primary fields.
        now = await c.fetchval("SELECT clock_timestamp()")
        for field in snapshot.metadata["field_observations"].values():
            if field["realtime_status"] not in owner["allowed_statuses"]:
                raise ReplicationError("source_status_not_authorized")
            from datetime import datetime

            if datetime.fromisoformat(field["received_timestamp"]) > now + timedelta(seconds=5):
                raise ReplicationError("collector_clock_ahead")
            if field["event_timestamp"] and datetime.fromisoformat(
                field["event_timestamp"]
            ) > now + timedelta(seconds=5):
                raise ReplicationError("source_clock_ahead")
        await self.store.write_observations_in_transaction(c, [quote], ordered_snapshots=[snapshot])
        await c.execute(
            """INSERT INTO ingest_quote_heads
                (instrument_id,provider,producer_id,source_epoch,ownership_generation,
                 source_position,observation_id,observation_sha256,payload)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
                ON CONFLICT(instrument_id,provider) DO UPDATE SET
                    producer_id=EXCLUDED.producer_id,source_epoch=EXCLUDED.source_epoch,
                    ownership_generation=EXCLUDED.ownership_generation,
                    source_position=EXCLUDED.source_position,observation_id=EXCLUDED.observation_id,
                    observation_sha256=EXCLUDED.observation_sha256,payload=EXCLUDED.payload""",
            quote.instrument_id,
            quote.provider,
            batch.producer_id,
            batch.source_epoch,
            owner["ownership_generation"],
            record.source_position,
            record.observation_id,
            digest,
            snapshot.model_dump_json(),
        )
        return "applied"

    async def source_status(self, instrument_id: UUID, provider: str) -> dict[str, Any] | None:
        """Separate source freshness from upload receipt; no singleton/REST projection yet."""
        async with self.store.connection() as c:
            row = await c.fetchrow(
                """SELECT s.*,s.expires_at > clock_timestamp() AND p.enabled
                    AND p.session_expires_at > clock_timestamp()
                    AND (s.session_id,s.session_generation)=(p.session_id,p.session_generation)
                    AND (s.producer_id,s.source_epoch,s.ownership_generation)=
                        (o.producer_id,o.source_epoch,o.ownership_generation) AS fresh
                    FROM ingest_source_status s JOIN ingest_source_owners o
                    ON (s.instrument_id,s.provider)=(o.instrument_id,o.provider)
                        AND o.stream='status'
                    JOIN ingest_producers p ON p.producer_id=s.producer_id
                    WHERE s.instrument_id=$1 AND s.provider=$2""",
                instrument_id,
                provider,
            )
        return dict(row) if row else None
