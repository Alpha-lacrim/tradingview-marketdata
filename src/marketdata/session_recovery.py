"""Same-producer expiry recovery; PostgreSQL owns authorization, fencing and receipts."""

import json
from datetime import datetime
from typing import Any, Literal, Self, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from marketdata.ingest_auth import Principal
from marketdata.persistence import Store
from marketdata.replication import ReplicationError
from marketdata.replication_models import MAX_POSITION, Stream

MAX_RECOVERY_BYTES = 262144


class RecoveryStream(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    instrument_id: UUID
    provider: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")
    stream: Stream
    source_epoch: UUID
    ownership_generation: int = Field(strict=True, gt=0, le=MAX_POSITION)


class RecoveryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    protocol_version: int = Field(strict=True, ge=1, le=1)
    recovery_id: UUID
    previous_session_id: UUID
    previous_session_generation: int = Field(strict=True, gt=0, le=MAX_POSITION)
    streams: list[RecoveryStream] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_streams(self) -> Self:
        if len({(s.instrument_id, s.provider, s.stream) for s in self.streams}) != len(
            self.streams
        ):
            raise ValueError("recovery streams must be unique")
        return self


class RecoveryReceipt(BaseModel):
    protocol_version: Literal[1]
    producer_id: UUID
    recovery_id: UUID
    previous_session_id: UUID
    previous_session_generation: int
    session_id: UUID
    session_generation: int
    expires_at: datetime
    server_recovered_at: datetime
    state: Literal["committed"]
    streams: list[RecoveryStream]


async def recover_session(
    store: Store, principal: Principal, request: RecoveryRequest
) -> dict[str, Any]:
    async with store.connection() as c, c.transaction():
        await c.execute("SET LOCAL synchronous_commit=on")
        encoded = await c.fetchval(
            "SELECT ingest_recover_session($1,$2,$3)",
            principal.producer_id,
            principal.key_id,
            request.model_dump_json(),
        )
        result = json.loads(encoded)
        if "error" in result:
            raise ReplicationError(result["error"])
    # Session change and durable acknowledgment have committed before returning.
    return cast(dict[str, Any], result)
