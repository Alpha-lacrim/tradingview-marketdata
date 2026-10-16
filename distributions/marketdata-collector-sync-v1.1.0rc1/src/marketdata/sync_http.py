"""TLS-only bounded transport. Errors never include credentials or response text."""

import asyncio
import hashlib
import json
import re
import ssl
from typing import Any

import httpx

from marketdata.sync_config import SyncConfig


class SyncProblem(RuntimeError):
    def __init__(self, code: str, *, retryable: bool = False, delay: float = 2):
        super().__init__(code)
        self.code, self.retryable, self.delay = code, retryable, min(300, max(0, delay))


class SyncHTTP:
    def __init__(self, config: SyncConfig):
        self.config = config
        context = ssl.create_default_context(cafile=str(config.ca_file) if config.ca_file else None)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        self.client = httpx.AsyncClient(
            base_url=config.endpoint,
            verify=context,
            trust_env=False,
            follow_redirects=False,
            timeout=config.timeout_seconds,
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def request(
        self, method: str, path: str, payload: bytes | None = None, *, batch: bool = False
    ) -> dict[str, Any]:
        try:
            async with asyncio.timeout(self.config.timeout_seconds):
                return await self._request(method, path, payload, batch=batch)
        except TimeoutError:
            raise SyncProblem("transport_unavailable", retryable=True) from None

    async def _request(
        self, method: str, path: str, payload: bytes | None = None, *, batch: bool = False
    ) -> dict[str, Any]:
        token = self.config.token_file.read_text().strip()
        if len(token) > 300 or not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43,128}", token):
            raise SyncProblem("invalid_private_token_file")
        headers = {"Authorization": "Bearer " + token, "Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if batch and payload is not None:
            headers["X-Payload-SHA256"] = hashlib.sha256(payload).hexdigest()
        try:
            async with self.client.stream(
                method, "/api/ingest/v1" + path, content=payload, headers=headers
            ) as response:
                raw = bytearray()
                async for chunk in response.aiter_bytes():
                    raw.extend(chunk)
                    if len(raw) > 4194304:
                        raise SyncProblem("response_limit_exceeded")
                if response.status_code != 200:
                    code = "http_" + str(response.status_code)
                    try:
                        candidate = json.loads(raw)["error"]["code"]
                        if isinstance(candidate, str) and re.fullmatch(r"[a-z_]{1,100}", candidate):
                            code = candidate
                    except (ValueError, KeyError, TypeError):
                        pass
                    retry_after = response.headers.get("retry-after", "2")
                    raise SyncProblem(
                        code,
                        retryable=response.status_code in {429, 503},
                        delay=float(retry_after) if retry_after.isdigit() else 2,
                    )
                result = json.loads(raw)
                if not isinstance(result, dict):
                    raise SyncProblem("invalid_response")
                return result
        except (httpx.HTTPError, OSError, TimeoutError):
            raise SyncProblem("transport_unavailable", retryable=True) from None
        except (ValueError, RecursionError):
            raise SyncProblem("invalid_response") from None
