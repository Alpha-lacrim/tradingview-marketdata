"""Create NEW private first-install credentials. Never overwrites or rotates existing files."""

import argparse
import getpass
import json
import os
import re
import secrets
import subprocess
from pathlib import Path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--directory", type=Path, required=True)
    p.add_argument("--component", choices=("company", "collector"), required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--database", required=True)
    args = p.parse_args()
    if (
        not re.fullmatch(r"[a-zA-Z0-9_.-]+", args.host)
        or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", args.database)
        or not 1 <= args.port <= 65535
    ):
        p.error("explicit hostname, database identifier and valid port required")
    args.directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    if os.name == "nt":
        subprocess.run(
            [
                "icacls",
                str(args.directory),
                "/inheritance:r",
                "/grant:r",
                getpass.getuser() + ":(OI)(CI)F",
            ],
            check=True,
            capture_output=True,
        )
    roles = ["read", "ingest"] if args.component == "company" else ["collect", "sync"]
    passwords = {"marketdata_v11_" + role: secrets.token_urlsafe(32) for role in roles}
    admin = secrets.token_urlsafe(32)
    files = {"database_admin_password": admin + "\n"}
    for role, password in {"marketdata_owner": admin, **passwords}.items():
        name = (
            "database_admin_dsn"
            if role == "marketdata_owner"
            else role.removeprefix("marketdata_v11_") + "_dsn"
        )
        if role == "marketdata_v11_collect":
            name = "collector_dsn"
        files[name] = f"postgresql://{role}:{password}@{args.host}:{args.port}/{args.database}\n"
    files["role_passwords.json"] = json.dumps(passwords)
    if args.component == "company":
        files["api_key"] = secrets.token_urlsafe(48) + "\n"
    for name, value in files.items():
        fd = os.open(args.directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(value)
    print("Created private install files. Apply service-specific permissions.")


if __name__ == "__main__":
    main()
