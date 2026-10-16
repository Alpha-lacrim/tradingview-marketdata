"""Shared field clocks for local capture and destination snapshot validation."""

from datetime import UTC, datetime
from typing import Any

PRICE_FIELDS = ("last", "bid", "ask", "last_size", "bid_size", "ask_size", "volume")


def field_order(field: dict[str, Any]) -> tuple[int, datetime, str]:
    received = datetime.fromisoformat(field["received_timestamp"])
    source = (
        datetime.fromisoformat(field["event_timestamp"]) if field["event_timestamp"] else received
    )
    delta = source - datetime(1970, 1, 1, tzinfo=UTC)
    ns = (
        int(field["event_timestamp_ns"])
        if field["event_timestamp_ns"] is not None
        else ((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds) * 1000
    )
    return ns, received, field["observation_id"]
