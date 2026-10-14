"""Production-only entrypoint with file-mounted secrets and explicit process ownership."""

import argparse
import os
import sys
from pathlib import Path

from pydantic_settings import SettingsConfigDict

from marketdata.config import Settings


class DeploymentSettings(Settings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", hide_input_in_errors=True)


def command(role: str) -> list[str]:
    os.environ["API_ENVIRONMENT"] = "production"
    os.environ["APPLICATION_ROLE"] = "api" if role == "api" else "collector"
    fields = ("DATABASE_URL", "API_KEY") if role == "api" else ("DATABASE_URL",)
    for field in fields:
        path = Path(os.environ.get(field + "_FILE", "/run/secrets/" + field.lower()))
        value = path.read_text(encoding="utf-8").strip()
        if not value:
            raise ValueError("empty secret file")
        os.environ[field] = value
    settings = DeploymentSettings()
    if role == "collector":
        return [sys.executable, "-m", "marketdata.collector"]
    if role == "migrate":
        return [sys.executable, "-m", "alembic", "-x", "timescale=true", "upgrade", "head"]
    workers = int(os.environ.get("API_WORKERS", "2"))
    if not 1 <= workers <= 8:
        raise ValueError("API_WORKERS must be 1..8")
    trusted = os.environ.get("FORWARDED_ALLOW_IPS", "127.0.0.1")
    if "*" in trusted:
        raise ValueError("explicit trusted proxy addresses required")
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "marketdata.api:create_app",
        "--factory",
        "--host",
        "0.0.0.0",
        "--port",
        "8000",
        "--workers",
        str(workers),
        "--ws",
        "websockets",
        "--ws-max-size",
        str(settings.websocket_message_max_bytes),
        "--ws-max-queue",
        "16",
        "--no-access-log",
        "--proxy-headers",
        "--forwarded-allow-ips",
        trusted,
        "--limit-concurrency",
        "128",
        "--timeout-keep-alive",
        "5",
        "--timeout-graceful-shutdown",
        "30",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("api", "collector", "migrate"))
    args = parser.parse_args()
    try:
        arguments = command(args.role)
    except Exception as error:
        # Input validation/IO exceptions may embed DSNs. Never print exception text.
        print(f"production configuration rejected ({type(error).__name__})", file=sys.stderr)
        raise SystemExit(1) from None
    os.execv(sys.executable, arguments)


if __name__ == "__main__":
    main()
