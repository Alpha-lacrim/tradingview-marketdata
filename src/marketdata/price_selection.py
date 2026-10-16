"""Selection of persisted observations, independent of collector transport health."""

from datetime import datetime

from marketdata.candles import MarketCandle
from marketdata.config import Settings
from marketdata.domain import Tick


def observation_age(observation: Tick | MarketCandle, now: datetime) -> tuple[int, bool]:
    source_time = (
        observation.bucket_end
        if isinstance(observation, MarketCandle)
        else observation.event_timestamp or observation.received_timestamp
    )
    skew = source_time > now or observation.received_timestamp > now
    # A delayed event or delayed local persistence cannot appear freshly observed.
    age = max(
        (now - source_time).total_seconds(),
        (now - observation.received_timestamp).total_seconds(),
        0,
    )
    if isinstance(observation, Tick):
        # Coalesced fields retain their individual clocks. A new bid must not make
        # an old selected last price fresh, including when its source time is absent.
        primary = observation.metadata.get("source_capture_primary")
        if isinstance(primary, dict):
            for key in ("event_timestamp", "received_timestamp"):
                value = primary.get(key)
                if isinstance(value, str):
                    try:
                        timestamp = datetime.fromisoformat(value)
                        if timestamp.tzinfo is not None:
                            skew = skew or timestamp > now
                            age = max(age, (now - timestamp).total_seconds())
                    except ValueError:
                        pass
    return int(age * 1000), skew


def stale_threshold(observation: Tick | MarketCandle, settings: Settings) -> float:
    if isinstance(observation, MarketCandle):
        return 60 + settings.candle_fetch_grace_seconds
    return settings.tradingview_symbol_stale_seconds.get(
        observation.provider_symbol, settings.stale_seconds
    )


def select_price(
    tick: Tick | None, candle: MarketCandle | None, now: datetime, settings: Settings
) -> Tick | MarketCandle | None:
    if candle is not None and candle.bucket_end > now:
        candle = None
    if tick is None:
        return candle
    age, skew = observation_age(tick, now)
    if not skew and age <= stale_threshold(tick, settings) * 1000:
        return tick
    if candle is None:
        return tick
    tick_time = tick.event_timestamp or tick.received_timestamp
    primary = tick.metadata.get("source_capture_primary")
    if isinstance(primary, dict) and tick.event_timestamp is None:
        value = primary.get("received_timestamp")
        if isinstance(value, str):
            try:
                timestamp = datetime.fromisoformat(value)
                if timestamp.tzinfo is not None:
                    tick_time = min(tick_time, timestamp)
            except ValueError:
                pass
    return candle if skew or candle.bucket_end >= tick_time else tick
