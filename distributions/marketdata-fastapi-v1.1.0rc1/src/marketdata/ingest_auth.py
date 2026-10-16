"""File-mounted hashed producer credentials and database-controlled session admission."""

import hashlib
import hmac
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from marketdata.persistence import Store
from marketdata.replication import ReplicationError


def strict_json(payload: bytes) -> Any:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    def invalid(value: str) -> Any:
        raise ValueError("nonfinite JSON number")

    return json.loads(payload.decode("utf-8"), object_pairs_hook=unique, parse_constant=invalid)


class Credential(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    producer_id: UUID
    key_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,64}$")
    token_sha256: SecretStr


@dataclass(frozen=True)
class Principal:
    producer_id: UUID
    key_id: str


class Credentials:
    def __init__(self, path: Path):
        # Loaded only at startup; rotation requires rolling all ingestion processes.
        with path.open("rb") as file:
            payload = file.read(262145)
        if len(payload) > 262144:
            raise ValueError("credential file exceeds limit")
        values = strict_json(payload)
        if not isinstance(values, list) or not 1 <= len(values) <= 1000:
            raise ValueError("credential file must contain 1..1000 definitions")
        self.keys = [Credential.model_validate(value) for value in values]
        if len({key.key_id for key in self.keys}) != len(self.keys):
            raise ValueError("duplicate credential key ID")
        if len({key.producer_id for key in self.keys}) != len(self.keys):
            raise ValueError("one active credential per producer is required")
        for key in self.keys:
            if not re.fullmatch(r"[0-9a-f]{64}", key.token_sha256.get_secret_value()):
                raise ValueError("credential fingerprint must be lowercase SHA-256")

    def authenticate(self, header: str) -> Principal:
        scheme, _, token = header.partition(" ")
        key_id, _, secret = token.partition(".")
        if scheme.lower() != "bearer" or not re.fullmatch(r"[A-Za-z0-9_-]{43,128}", secret):
            raise ReplicationError("authentication_required")
        digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
        for key in self.keys:
            matched = hmac.compare_digest(digest, key.token_sha256.get_secret_value())
            if matched and key.key_id == key_id:
                return Principal(key.producer_id, key.key_id)
        raise ReplicationError("authentication_required")


class SessionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    protocol_version: int = Field(strict=True, ge=1, le=1)
    session_id: UUID
    session_generation: int = Field(strict=True, gt=0, le=9223372036854775807)


async def session(
    store: Store, principal: Principal, request: SessionRequest, *, renew: bool
) -> dict[str, Any]:
    async with store.connection() as c, c.transaction():
        if renew:
            row = await c.fetchrow(
                "SELECT * FROM ingest_renew_session($1,$2,$3,$4)",
                principal.producer_id,
                principal.key_id,
                request.session_id,
                request.session_generation,
            )
        else:
            row = await c.fetchrow("SELECT * FROM ingest_lock_producer($1)", principal.producer_id)
        now = await c.fetchval("SELECT clock_timestamp()")
        if (
            row is None
            or not row["enabled"]
            or row["credential_key_id"] != principal.key_id
            or (row["session_id"], row["session_generation"])
            != (request.session_id, request.session_generation)
            or row["session_expires_at"] <= now
        ):
            raise ReplicationError("producer_session_fenced")
        owners = await c.fetch(
            """SELECT instrument_id,provider,stream,source_epoch,ownership_generation,
                allowed_statuses FROM ingest_source_owners WHERE producer_id=$1
                ORDER BY instrument_id,provider,stream LIMIT 1001""",
            principal.producer_id,
        )
        if len(owners) > 1000:
            raise ReplicationError("ownership_manifest_too_large")
        result = dict(
            protocol_version=1,
            producer_id=str(principal.producer_id),
            session_id=str(request.session_id),
            session_generation=request.session_generation,
            expires_at=row["session_expires_at"].isoformat(),
            server_time=now.isoformat(),
            streams=[
                {
                    key: str(value) if isinstance(value, UUID) else value
                    for key, value in dict(owner).items()
                }
                for owner in owners
            ],
        )
    return result
