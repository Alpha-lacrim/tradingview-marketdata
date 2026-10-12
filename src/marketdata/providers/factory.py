from collections.abc import Callable

from marketdata.config import Settings
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.binance import BinanceProvider
from marketdata.providers.candle_pool import CandleConnectionPool, PoolLimits
from marketdata.providers.databento import DatabentoProvider
from marketdata.providers.mock import MockMarketDataProvider
from marketdata.providers.oanda import OandaProvider
from marketdata.providers.tradingview import TradingViewGuestProvider
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


def tradingview_provider(settings: Settings, metrics: Metrics) -> MarketDataProvider:
    if settings.tradingview_storage_mode != "candles":
        return TradingViewGuestProvider(settings, metrics)
    if settings.tradingview_candle_pool_enabled:
        return CandleConnectionPool(
            "tradingview_guest",
            settings,
            metrics,
            lambda worker: TradingViewCandleProvider(settings, metrics, experimental_pool=True),
            PoolLimits(6, 100, 600, 12),
        )
    if settings.candle_pool_connections != 1:
        raise ValueError("enable TRADINGVIEW_CANDLE_POOL_ENABLED for multiple candle connections")
    return TradingViewCandleProvider(settings, metrics)


async def build_providers(
    settings: Settings, registry: Registry, metrics: Metrics
) -> list[MarketDataProvider]:
    providers: list[MarketDataProvider] = []
    provider: MarketDataProvider
    factories: list[tuple[str, Callable[[], MarketDataProvider]]] = [
        (
            "mock",
            lambda: MockMarketDataProvider(
                metrics, settings.mock_ticks_per_second, settings.mock_seed
            ),
        ),
        ("binance", lambda: BinanceProvider(settings, metrics)),
        ("oanda", lambda: OandaProvider(settings, metrics)),
        (
            "tradingview_guest",
            lambda: tradingview_provider(settings, metrics),
        ),
    ]
    for name, factory in factories:
        subscriptions = registry.subscriptions(name)
        if subscriptions:
            if name == "tradingview_guest" and settings.tradingview_storage_mode == "candles":
                capacity = min(
                    settings.candle_pool_max_instruments,
                    settings.candle_pool_worker_capacity * settings.candle_pool_connections,
                    600 if settings.tradingview_candle_pool_enabled else 100,
                )
                if len(subscriptions) > capacity:
                    raise ValueError("TradingView registry exceeds configured candle capacity")
            provider = factory()
            await provider.subscribe(subscriptions)
            providers.append(provider)
    futures = registry.subscriptions("databento")
    datasets = {str(m.metadata["dataset"]) for _, m in futures}
    for dataset in sorted(datasets):
        provider = DatabentoProvider(settings, metrics, dataset)
        await provider.subscribe(
            [pair for pair in futures if pair[1].metadata["dataset"] == dataset]
        )
        providers.append(provider)
    return providers
