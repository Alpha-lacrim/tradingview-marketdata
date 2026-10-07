from marketdata.config import Settings
from marketdata.observability import Metrics
from marketdata.providers.base import MarketDataProvider
from marketdata.providers.binance import BinanceProvider
from marketdata.providers.databento import DatabentoProvider
from marketdata.providers.mock import MockMarketDataProvider
from marketdata.providers.oanda import OandaProvider
from marketdata.providers.tradingview import TradingViewGuestProvider
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry


async def build_providers(
    settings: Settings, registry: Registry, metrics: Metrics
) -> list[MarketDataProvider]:
    providers: list[MarketDataProvider] = []
    provider: MarketDataProvider
    for name, factory in (
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
            lambda: (
                TradingViewCandleProvider(settings, metrics)
                if settings.tradingview_storage_mode == "candles"
                else TradingViewGuestProvider(settings, metrics)
            ),
        ),
    ):
        subscriptions = registry.subscriptions(name)
        if subscriptions:
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
