"""Test-only TLS destination killed before/after upload commit; reuses TLS harness."""

import asyncio
import json
import runpy
import sys
from pathlib import Path

from marketdata.replication import DestinationWriter

config = json.loads(Path(sys.argv[1]).read_text())
boundary = config.get("upload_boundary")
original = DestinationWriter._commit if boundary == "before" else DestinationWriter.commit


async def paused(self, *args, **kwargs):
    result = await original(self, *args, **kwargs)
    await asyncio.to_thread(Path(config["marker"]).write_text, "pending")
    await asyncio.Event().wait()
    return result


if boundary == "before":
    DestinationWriter._commit = paused
elif boundary == "after":
    DestinationWriter.commit = paused
runpy.run_path("tests/process_recovery_worker.py", run_name="__main__")
