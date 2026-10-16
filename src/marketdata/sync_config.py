"""Private-file configuration for the independent source synchronization process."""

import json
from pathlib import Path
from typing import Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from marketdata.session_recovery import RecoveryRequest, RecoveryStream


class SyncConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)
    endpoint: str
    source_dsn_file: Path
    token_file: Path
    anchor_file: Path
    ca_file: Path | None = None
    producer_id: UUID
    session_id: UUID
    session_generation: int = Field(strict=True, gt=0)
    streams: list[RecoveryStream] = Field(min_length=1, max_length=1000)
    max_body_bytes: int = Field(default=2097152, ge=4096, le=2097152)
    max_records: int = Field(default=100, ge=1, le=500)
    metadata_max_records: int = Field(default=50, ge=1, le=50)
    interval_seconds: float = Field(default=2, ge=1, le=60)
    timeout_seconds: float = Field(default=30, ge=1, le=120)
    reconcile_seconds: float = Field(default=300, ge=30)
    enroll: bool = False
    source_max_rows: int = Field(default=1000000, ge=1, le=1000000)
    source_max_bytes: int = Field(default=536870912, ge=65536, le=536870912)

    @model_validator(mode="after")
    def valid(self) -> Self:
        url = urlsplit(self.endpoint)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or url.path not in {"", "/"}
        ):
            raise ValueError("HTTPS origin without credentials, path, query or fragment required")
        self.endpoint = self.endpoint.rstrip("/")
        self.streams = sorted(
            self.streams, key=lambda s: (str(s.instrument_id), s.provider, s.stream)
        )
        RecoveryRequest(
            protocol_version=1,
            recovery_id=self.producer_id,
            previous_session_id=self.session_id,
            previous_session_generation=self.session_generation,
            streams=self.streams,
        )
        if len({s.source_epoch for s in self.streams}) != 1:
            raise ValueError("one worker database must have one stable dataset epoch")
        return self

    @classmethod
    def load(cls, path: Path) -> Self:
        if path.stat().st_size > 262144:
            raise ValueError("configuration exceeds manifest bound")
        return cls.model_validate(json.loads(path.read_bytes()))
