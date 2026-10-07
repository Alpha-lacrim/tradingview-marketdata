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
        self.candles = Counter(
            "marketdata_candles_total",
            "Candle operations",
            ["provider", "operation"],
            registry=self.registry,
        )
        self.candle_latency = Histogram(
            "marketdata_candle_seconds",
            "Candle request/write duration",
            ["operation"],
            registry=self.registry,
        )
        self.candle_lateness = Gauge(
            "marketdata_candle_scheduler_lateness_seconds",
            "Collection start lateness",
            ["provider"],
            registry=self.registry,
        )
        self.candle_age = Gauge(
            "marketdata_candle_age_seconds",
            "Latest completed bucket end age; -1 if absent",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.candle_gaps = Gauge(
            "marketdata_candle_missing_minutes",
            "Unresolved missing minutes since coverage start",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.candle_cycle = Counter(
            "marketdata_candle_fetch_cycle_total",
            "Minute collection cycles",
            ["provider"],
            registry=self.registry,
        )
        self.candle_timing = Histogram(
            "marketdata_candle_timing_seconds",
            "Separated monotonic stage durations / clamped UTC close latencies",
            ["provider", "stage"],
            buckets=(0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13, 21, 34, 60, 120),
            registry=self.registry,
        )
        self.candle_symbol_latency = Histogram(
            "marketdata_candle_symbol_fetch_seconds",
            "Per configured symbol retrieval duration",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.candle_behind = Gauge(
            "marketdata_candle_minutes_behind",
            "Expected completed coverage lag",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.candle_expectation = Gauge(
            "marketdata_candle_expected_state",
            "One-hot current completed minute classification",
            ["provider", "symbol", "expectation"],
            registry=self.registry,
        )
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
        self.connection_up = Gauge(
            "marketdata_connection_up", "Transport connected", ["provider"], registry=self.registry
        )
        self.connection_age = Gauge(
            "marketdata_connection_age_seconds",
            "Current connection age; zero offline",
            ["provider"],
            registry=self.registry,
        )
        self.feed_healthy = Gauge(
            "marketdata_feed_healthy",
            "All configured symbols current according to storage mode",
            ["provider"],
            registry=self.registry,
        )
        self.symbol_observations = Counter(
            "marketdata_symbol_observations_total",
            "Valid normalized observations",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.symbol_age = Gauge(
            "marketdata_symbol_observation_age_seconds",
            "Receipt age; -1 when never observed",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.symbol_fresh = Gauge(
            "marketdata_symbol_fresh",
            "Fresh observation in current connection",
            ["provider", "symbol"],
            registry=self.registry,
        )
        self.symbol_count = Gauge(
            "marketdata_symbols",
            "Configured/fresh/stale symbol counts",
            ["provider", "state"],
            registry=self.registry,
        )
        self.rejections = Counter(
            "marketdata_rejected_observations_total",
            "Suppressed price observations",
            ["provider", "reason"],
            registry=self.registry,
        )
        self.unknown_messages = Counter(
            "marketdata_unknown_messages_total",
            "Unrecognized message types",
            ["provider"],
            registry=self.registry,
        )
        self.transport_timeouts = Counter(
            "marketdata_transport_timeouts_total",
            "No application frames within timeout",
            ["provider"],
            registry=self.registry,
        )
