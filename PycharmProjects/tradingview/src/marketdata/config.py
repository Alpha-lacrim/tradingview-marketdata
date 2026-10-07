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
    shutdown_timeout_seconds: float = Field(default=20, gt=0)
    reconnect_min_seconds: float = Field(default=1, ge=1)
    reconnect_max_seconds: float = Field(default=60, ge=1)
    stale_seconds: float = Field(default=30, ge=10)
    mock_ticks_per_second: float = Field(default=100, gt=0, le=100000)
    mock_seed: int = 42
    websocket_queue_capacity: int = Field(default=256, ge=1, le=10000)
    websocket_max_clients: int = Field(default=1000, ge=1, le=10000)
    websocket_max_symbols: int = Field(default=500, ge=1, le=5000)
    websocket_send_timeout_seconds: float = Field(default=5, gt=0)
    api_key: SecretStr | None = None
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    binance_ws_url: str = "wss://stream.binance.com:9443"
    binance_rest_url: str = "https://api.binance.com"
    oanda_environment: Literal["practice", "live"] = "practice"
    oanda_api_token: SecretStr | None = None
    oanda_account_id: SecretStr | None = None
    databento_api_key: SecretStr | None = None
    tradingview_ws_url: str = "wss://data.tradingview.com/socket.io/websocket"
    tradingview_storage_mode: Literal["ticks", "candles"] = "candles"
    candle_interval: Literal["1m"] = "1m"
    candle_fetch_grace_seconds: float = Field(default=2, ge=0, le=30)
    candle_backfill_minutes: int = Field(default=300, ge=3, le=1440)
    candle_correction_lookback_minutes: int = Field(default=3, ge=1, le=30)
    candle_request_timeout_seconds: float = Field(default=15, ge=1, le=60)
    tradingview_candle_concurrency: int = Field(default=2, ge=1, le=12)
    candle_cycle_safety_margin_seconds: float = Field(default=10, ge=1, le=30)
    tradingview_frame_timeout_seconds: float = Field(default=45, ge=10, le=3600)
    tradingview_observation_timeout_seconds: float = Field(default=60, ge=10, le=86400)
    tradingview_symbol_stale_seconds: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def valid(self) -> Self:
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
            mappings = [m for m in entry.mappings if m.provider == provider]
            if not mappings:
                raise ValueError(f"missing {provider} mapping for {i.canonical_symbol}")
            entries.append(RegistryEntry(instrument=i, mappings=mappings))
        return Registry(entries)
