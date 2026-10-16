from pathlib import Path
from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from marketdata.domain import AssetClass, InstrumentType, RegistryEntry
from marketdata.registry import Registry

ProviderName = Literal["mock", "binance", "oanda", "databento", "tradingview_guest", "disabled"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    application_role: Literal["api", "ingest", "collector", "combined"] = "api"
    ingest_credentials_file: Path | None = None
    ingest_max_body_bytes: int = Field(default=2097152, ge=1024, le=2097152)
    ingest_max_records: int = Field(default=500, ge=1, le=500)
    ingest_requests_per_minute: int = Field(default=120, ge=1, le=10000)
    ingest_max_inflight: int = Field(default=8, ge=1, le=64)
    ingest_request_timeout_seconds: float = Field(default=30, ge=1, le=120)
    collector_status_interval_seconds: float = Field(default=5, gt=0, le=30)
    collector_status_ttl_seconds: float = Field(default=20, gt=0, le=120)
    collector_metrics_port: int = Field(default=9000, ge=0, le=65535)
    collector_metrics_host: str = "127.0.0.1"
    database_url: SecretStr = SecretStr(
        "postgresql://marketdata:marketdata@localhost:5432/marketdata"
    )
    registry_file: Path = Path("config/instruments.mock.json")
    market_data_provider_crypto: ProviderName = "mock"
    market_data_provider_forex: ProviderName = "mock"
    market_data_provider_metals: ProviderName = "mock"
    market_data_provider_energy: ProviderName = "mock"
    market_data_provider_futures: ProviderName = "mock"
    enabled_asset_classes: list[AssetClass] = list(AssetClass)
    queue_capacity: int = Field(default=50000, ge=1, le=1000000)
    batch_size: int = Field(default=1000, ge=1, le=10000)
    batch_flush_seconds: float = Field(default=0.1, gt=0, le=10)
    db_timeout_seconds: float = Field(default=10, gt=0, le=120)
    db_pool_min: int = Field(default=2, ge=1)
    db_pool_max: int = Field(default=10, ge=1, le=100)
    source_capture_enabled: bool = False
    source_capture_max_rows: int = Field(default=1000000, ge=1, le=100000000)
    source_capture_max_bytes: int = Field(default=536870912, ge=1024, le=107374182400)
    shutdown_timeout_seconds: float = Field(default=20, gt=0)
    reconnect_min_seconds: float = Field(default=1, ge=1)
    reconnect_max_seconds: float = Field(default=60, ge=1)
    stale_seconds: float = Field(default=30, ge=10)
    mock_ticks_per_second: float = Field(default=100, gt=0, le=100000)
    mock_seed: int = 42
    websocket_queue_capacity: int = Field(default=256, ge=1, le=10000)
    websocket_max_clients: int = Field(default=1000, ge=1, le=10000)
    websocket_max_symbols: int = Field(default=500, ge=1, le=5000)
    websocket_send_timeout_seconds: float = Field(default=5, gt=0, le=30)
    websocket_allowed_origins: list[str] = Field(default_factory=list)
    websocket_max_total_subscriptions: int = Field(default=10000, ge=1, le=100000)
    websocket_message_max_bytes: int = Field(default=65536, ge=1024, le=1048576)
    websocket_snapshot_max_rows: int = Field(default=1000, ge=1, le=1000)
    websocket_snapshot_batch_size: int = Field(default=25, ge=1, le=100)
    websocket_heartbeat_seconds: float = Field(default=10, gt=0, le=60)
    marketdata_poll_seconds: float = Field(default=1, ge=0.01, le=30)
    marketdata_event_batch_size: int = Field(default=250, ge=1, le=1000)
    marketdata_event_retention_count: int = Field(default=100000, ge=1, le=1000000)
    api_key: SecretStr | None = None
    api_environment: Literal["development", "test", "production"] = "development"
    api_cors_origins: list[str] = Field(default_factory=list)
    api_history_max_days: int = Field(default=31, ge=1, le=366)
    api_max_rows: int = Field(default=1000, ge=1, le=1000)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    binance_ws_url: str = "wss://stream.binance.com:9443"
    binance_rest_url: str = "https://api.binance.com"
    oanda_environment: Literal["practice", "live"] = "practice"
    oanda_api_token: SecretStr | None = None
    oanda_account_id: SecretStr | None = None
    databento_api_key: SecretStr | None = None
    tradingview_ws_url: str = "wss://data.tradingview.com/socket.io/websocket"
    tradingview_storage_mode: Literal["ticks", "candles", "dual"] = "candles"
    tradingview_quote_history: bool = False
    # Operator confirmed commercial public-site access on 2026-10-14.
    # An explicit false still disables production TradingView ingestion.
    tradingview_production_access_approved: bool = True
    candle_interval: Literal["1m"] = "1m"
    candle_fetch_grace_seconds: float = Field(default=2, ge=0, le=30)
    candle_backfill_minutes: int = Field(default=300, ge=3, le=1440)
    candle_correction_lookback_minutes: int = Field(default=3, ge=1, le=30)
    candle_request_timeout_seconds: float = Field(default=15, ge=1, le=60)
    tradingview_candle_concurrency: int = Field(default=2, ge=1, le=12)
    tradingview_candle_pool_enabled: bool = False
    # Integration declares ceilings; experimental harness ceilings are local safeguards.
    candle_pool_connections: int = Field(default=1, ge=1, le=32)
    candle_pool_worker_capacity: int = Field(default=100, ge=1, le=4096)
    candle_pool_max_instruments: int = Field(default=100, ge=1, le=4096)
    candle_pool_concurrency: int = Field(default=2, ge=1, le=128)
    candle_pool_deadline_seconds: float = Field(default=48, gt=0, le=60)
    candle_pool_cleanup_seconds: float = Field(default=3, gt=0, le=10)
    candle_pool_connect_interval_seconds: float = Field(default=0, ge=0, le=10)
    candle_cycle_safety_margin_seconds: float = Field(default=10, ge=1, le=30)
    tradingview_frame_timeout_seconds: float = Field(default=45, ge=10, le=3600)
    tradingview_observation_timeout_seconds: float = Field(default=60, ge=10, le=86400)
    tradingview_symbol_stale_seconds: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid(self) -> Self:
        if self.source_capture_enabled and self.application_role != "collector":
            raise ValueError("source capture is restricted to an explicitly configured collector")
        if self.application_role == "ingest":
            if self.ingest_credentials_file is None:
                raise ValueError("ingest requires INGEST_CREDENTIALS_FILE")
            if self.api_key is not None:
                raise ValueError("ingest must use separate producer credentials, without API_KEY")
        if self.api_key is not None and not self.api_key.get_secret_value().strip():
            raise ValueError("API_KEY must not be empty")
        if (
            self.api_environment == "production"
            and self.application_role in {"api", "combined"}
            and (self.api_key is None or len(self.api_key.get_secret_value()) < 32)
        ):
            raise ValueError("production requires an API_KEY of at least 32 characters")
        for origin in self.api_cors_origins + self.websocket_allowed_origins:
            parsed_origin = urlsplit(origin)
            if (
                parsed_origin.scheme not in ("http", "https")
                or not parsed_origin.hostname
                or parsed_origin.username
                or parsed_origin.password
                or parsed_origin.path
                or parsed_origin.query
                or parsed_origin.fragment
                or "*" in origin
                or (self.api_environment == "production" and parsed_origin.scheme != "https")
            ):
                raise ValueError("CORS requires explicit origins; production requires HTTPS")
        if self.websocket_snapshot_batch_size > self.websocket_snapshot_max_rows:
            raise ValueError("snapshot batch size must fit snapshot row limit")
        if self.collector_status_ttl_seconds <= self.collector_status_interval_seconds:
            raise ValueError("collector status TTL must exceed its publish interval")
        if self.application_role != "api" and self.db_pool_max < 2:
            raise ValueError("collector requires at least two pool connections")
        if self.tradingview_candle_pool_enabled:
            if self.tradingview_storage_mode not in {"candles", "dual"}:
                raise ValueError("TradingView candle pool requires candle storage mode")
            # Bounded operator-authorized guest scope, not upstream entitlements.
            if (
                self.candle_pool_connections > 6
                or self.candle_pool_worker_capacity > 100
                or self.candle_pool_max_instruments > 600
                or self.candle_pool_concurrency > 12
                or self.candle_pool_max_instruments
                > self.candle_pool_worker_capacity * self.candle_pool_connections
                or self.candle_pool_concurrency
                > self.tradingview_candle_concurrency * self.candle_pool_connections
            ):
                raise ValueError("TradingView pool exceeds bounded local capacity/request scope")
            if (
                self.candle_fetch_grace_seconds
                + self.candle_pool_deadline_seconds
                + self.candle_pool_cleanup_seconds
                >= 60
            ):
                raise ValueError(
                    "TradingView pool deadline and cleanup must fit the minute cadence"
                )
        if self.candle_fetch_grace_seconds + self.candle_cycle_safety_margin_seconds >= 60:
            raise ValueError("candle grace + safety margin must leave a positive minute budget")
        if any(
            not 1 <= seconds <= 86400 for seconds in self.tradingview_symbol_stale_seconds.values()
        ):
            raise ValueError("TradingView symbol stale thresholds must be 1..86400 seconds")
        if self.candle_correction_lookback_minutes > self.candle_backfill_minutes:
            raise ValueError("candle correction lookback must fit within backfill horizon")
        if self.db_pool_min > self.db_pool_max or self.batch_size > self.queue_capacity:
            raise ValueError("pool min <= max and batch_size <= queue_capacity required")
        if self.reconnect_min_seconds > self.reconnect_max_seconds:
            raise ValueError("reconnect minimum must not exceed maximum")
        if not self.database_url.get_secret_value().startswith(("postgresql://", "postgres://")):
            raise ValueError("DATABASE_URL must use PostgreSQL")
        for value, scheme in (
            (self.binance_ws_url, "wss"),
            (self.binance_rest_url, "https"),
            (self.tradingview_ws_url, "wss"),
        ):
            parsed = urlsplit(value)
            if parsed.scheme != scheme or not parsed.hostname or parsed.username or parsed.query:
                raise ValueError("provider URLs must use TLS and contain no credentials/query")
        return self

    def selected_registry(self, registry: Registry) -> Registry:
        entries: list[RegistryEntry] = []
        for entry in registry.entries:
            i = entry.instrument
            if not i.active or i.asset_class not in self.enabled_asset_classes:
                continue
            category = (
                "futures" if i.instrument_type == InstrumentType.FUTURE else i.asset_class.value
            )
            provider = getattr(self, f"market_data_provider_{category}")
            if provider == "disabled":
                continue
            if (
                provider == "tradingview_guest"
                and self.api_environment == "production"
                and not self.tradingview_production_access_approved
            ):
                raise ValueError(
                    "production TradingView ingestion requires established access rights"
                )
            mappings = [m for m in entry.mappings if m.provider == provider]
            if not mappings:
                raise ValueError(f"missing {provider} mapping for {i.canonical_symbol}")
            entries.append(RegistryEntry(instrument=i, mappings=mappings))
        return Registry(entries)
