"""Release role launcher; no migration, producer provisioning or implicit capture activation."""

import argparse
import json
import os
import sys
from pathlib import Path

from marketdata.production import command


def launch(role: str, root: Path) -> list[str]:
    component = json.loads((root / "RELEASE.json").read_text("utf-8"))["component"]
    if component not in {"company", "collector"}:
        raise ValueError("unknown release component")
    if (component == "company" and role not in {"read", "ingest"}) or (
        component == "collector" and role != "collector"
    ):
        raise ValueError("role is not available in this release component")
    if role == "collector" and os.environ.get("SOURCE_CAPTURE_ENABLED", "false").lower() not in {
        "true",
        "1",
        "yes",
    }:
        raise ValueError("release collector requires explicit source capture opt-in")
    if role in {"read", "ingest"} and os.environ.get(
        "SOURCE_CAPTURE_ENABLED", "false"
    ).lower() not in {"false", "0", "no"}:
        raise ValueError("source capture cannot be enabled in company roles")
    return command("api" if role == "read" else role)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("role", choices=("read", "ingest", "collector"))
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--environment-file", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--bind-host")
    parser.add_argument("--tls-cert-file", type=Path)
    parser.add_argument("--tls-key-file", type=Path)
    args = parser.parse_args()
    try:
        if args.environment_file:
            values = json.loads(args.environment_file.read_text("utf-8"))
            if not isinstance(values, dict) or any(
                not isinstance(k, str) or not isinstance(v, str) or k in {"DATABASE_URL", "API_KEY"}
                for k, v in values.items()
            ):
                raise ValueError("environment requires strings and file-mounted secrets")
            os.environ.update(values)
        arguments = launch(args.role, args.root)
        if args.port is not None:
            if args.role == "collector" or not 1 <= args.port <= 65535:
                raise ValueError("explicit port applies only to HTTP roles")
            arguments[arguments.index("--port") + 1] = str(args.port)
        if args.bind_host:
            if args.role == "collector":
                raise ValueError("HTTP bind options do not apply to collector")
            arguments[arguments.index("--host") + 1] = args.bind_host
        if bool(args.tls_cert_file) != bool(args.tls_key_file):
            raise ValueError("TLS requires certificate and key together")
        if args.tls_cert_file:
            if args.role == "collector":
                raise ValueError("HTTP TLS options do not apply to collector")
            arguments += [
                "--ssl-certfile",
                str(args.tls_cert_file),
                "--ssl-keyfile",
                str(args.tls_key_file),
            ]
    except Exception as error:
        parser.exit(1, f"release configuration rejected ({type(error).__name__})\n")
    os.execv(sys.executable, arguments)


if __name__ == "__main__":
    main()
