"""Validation/materialization for source-position ordered quote snapshots.

No timestamps are advanced by delivery. Each non-null field must carry its own
genuine observation. Omitted fields retain the previously accepted observation.
"""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from uuid import UUID

from marketdata.domain import Tick
from marketdata.quote_fields import PRICE_FIELDS, field_order
from marketdata.replication_models import timestamp_wire


def is_snapshot(quote: Tick) -> bool:
    return quote.metadata.get("quote_semantics") == "coalesced_partial_fields"


def materialize(quote: Tick, previous: Tick | None = None) -> Tick:
    fields = quote.metadata.get("field_observations")
    if not isinstance(fields, dict) or not fields or set(fields) - set(PRICE_FIELDS):
        raise ValueError("invalid snapshot fields")
    for name in PRICE_FIELDS:
        value = getattr(quote, name)
        if value is None:
            if name in fields:
                raise ValueError("field provenance without a value")
            continue
        field = fields.get(name)
        if not isinstance(field, dict) or set(field) != {
            "value",
            "observation_id",
            "event_timestamp",
            "event_timestamp_ns",
            "received_timestamp",
            "realtime_status",
            "metadata",
            "sequence_number",
        }:
            raise ValueError("missing field provenance")
        raw = field["value"]
        if not isinstance(raw, str) or len(raw) > 128:
            raise ValueError("invalid field decimal")
        try:
            number = Decimal(raw)
        except InvalidOperation:
            raise ValueError("invalid field decimal") from None
        if not number.is_finite() or number != value:
            raise ValueError("field value mismatch")
        if not isinstance(field["observation_id"], str):
            raise ValueError("invalid field identity")
        UUID(field["observation_id"])
        timestamp_wire(field["event_timestamp"])
        if not isinstance(field["received_timestamp"], str):
            raise ValueError("field receipt time required")
        timestamp_wire(field["received_timestamp"])
        ns = field["event_timestamp_ns"]
        if ns is not None and (
            not isinstance(ns, str)
            or not ns.isascii()
            or not ns.isdigit()
            or len(ns) > 19
            or str(int(ns)) != ns
        ):
            raise ValueError("invalid field nanoseconds")
        sequence = field["sequence_number"]
        if sequence is not None and (
            type(sequence) is not int or not -9223372036854775808 <= sequence <= 9223372036854775807
        ):
            raise ValueError("invalid field sequence")
        # The domain model verifies timezone, nanoseconds, status and financial bounds.
        Tick.model_validate(
            {
                **quote.model_dump(),
                **{n: None for n in PRICE_FIELDS},
                name: value,
                "last": value if name not in {"last", "bid", "ask"} else None,
                **({name: value} if name in {"last", "bid", "ask"} else {}),
                "tick_id": field["observation_id"],
                "event_timestamp": field["event_timestamp"],
                "event_timestamp_ns": field["event_timestamp_ns"],
                "received_timestamp": field["received_timestamp"],
                "realtime_status": field["realtime_status"],
                "sequence_number": field["sequence_number"],
                "metadata": field["metadata"],
            }
        )
        field_order(field)
    primary = next((fields[n] for n in ("last", "bid", "ask") if n in fields), None)
    if primary is None:
        raise ValueError("snapshot has no price")
    if (
        quote.event_timestamp
        != (
            datetime.fromisoformat(primary["event_timestamp"])
            if primary["event_timestamp"]
            else None
        )
        or quote.event_timestamp_ns
        != (
            int(primary["event_timestamp_ns"])
            if primary["event_timestamp_ns"] is not None
            else None
        )
        or quote.realtime_status.value != primary["realtime_status"]
        or quote.received_timestamp
        != max(datetime.fromisoformat(f["received_timestamp"]) for f in fields.values())
    ):
        raise ValueError("snapshot primary clock mismatch")
    merged = dict(previous.metadata["field_observations"]) if previous else {}
    for name, field in fields.items():
        if name in merged and field_order(field)[:2] < field_order(merged[name])[:2]:
            raise ValueError("snapshot field clock regressed")
        if (
            name in merged
            and field_order(field) == field_order(merged[name])
            and field != merged[name]
        ):
            raise ValueError("field observation identity changed")
        merged[name] = field
    primary = next(merged[n] for n in ("last", "bid", "ask") if n in merged)
    data: dict[str, Any] = quote.model_dump()
    data.update(
        **{n: merged[n]["value"] if n in merged else None for n in PRICE_FIELDS},
        event_timestamp=primary["event_timestamp"],
        event_timestamp_ns=primary["event_timestamp_ns"],
        received_timestamp=max(
            datetime.fromisoformat(f["received_timestamp"]) for f in merged.values()
        ),
        realtime_status=primary["realtime_status"],
        metadata={
            **quote.metadata,
            "field_observations": merged,
            "source_capture_primary": {
                k: primary[k] for k in ("received_timestamp", "event_timestamp")
            },
        },
    )
    return Tick.model_validate(data)
