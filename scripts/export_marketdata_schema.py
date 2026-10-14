"""Export authoritative version 1 WebSocket JSON Schema from public models."""

import json
from pathlib import Path

from pydantic import TypeAdapter

from marketdata.ws_models import MarketEvent


def main() -> None:
    Path("docs/ws-marketdata.schema.json").write_text(
        json.dumps(TypeAdapter(MarketEvent).json_schema(mode="serialization"), indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
