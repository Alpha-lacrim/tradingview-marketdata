import json
import logging
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        data: dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        data.update(getattr(record, "context", {}))
        return json.dumps(data, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
    # HTTP/SDK debug logs may include authenticated URLs or headers.
    for name in ("httpx", "httpcore", "databento", "websockets"):
        logging.getLogger(name).setLevel(logging.CRITICAL)


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.messages = Counter(
            "marketdata_messages_total",
            "Upstream messages including control records",
            ["provider"],
            registry=self.registry,
        )
        self.received = Counter(
            "marketdata_received_total", "Normalized ticks", ["provider"], registry=self.registry
        )
        self.persisted = Counter(
            "marketdata_persisted_total", "Inserted rows", registry=self.registry
        )
        self.dropped = Counter(
            "marketdata_dropped_total",
            "Explicitly lost ticks",
            ["provider", "reason"],
            registry=self.registry,
        )
        self.reconnects = Counter(
            "marketdata_reconnects_total",
            "Reconnect attempts",
            ["provider"],
            registry=self.registry,
        )
        self.parse_failures = Counter(
            "marketdata_parse_failures_total",
            "Malformed messages",
            ["provider"],
            registry=self.registry,
        )
        self.sequence_gaps = Counter(
            "marketdata_sequence_gaps_total",
            "Observed trade ID gaps",
            ["provider"],
            registry=self.registry,
        )
        self.db_errors = Counter(
            "marketdata_database_errors_total", "Persistence failures", registry=self.registry
        )
        self.queue_depth = Gauge("marketdata_queue_depth", "Pending ticks", registry=self.registry)
        self.batch_latency = Histogram(
            "marketdata_batch_seconds", "Database batch duration", registry=self.registry
        )
        self.ws_dropped = Counter(
            "marketdata_ws_dropped_total", "Slow client queue losses", registry=self.registry
        )
