"""Ordinary provider activation and durable coverage, without live access."""

import json
from datetime import timedelta
from pathlib import Path

import pytest
from test_native_candles import NOW, Replay, make_candle

from marketdata.candle_ingestion import CandleIngestion
from marketdata.config import Settings
from marketdata.domain import RegistryEntry
from marketdata.fanout import Fanout
from marketdata.observability import Metrics
from marketdata.providers.candle_pool import CandleConnectionPool
from marketdata.providers.factory import build_providers
from marketdata.providers.tradingview_candles import TradingViewCandleProvider
from marketdata.registry import Registry

PILOT = Path("config/instruments.tradingview-pilot.json")
ORIGINAL = Path("config/instruments.tradingview.json")


def test_pilot_preserves_original_identities_and_has_native_evidence():
    original, pilot = Registry.load(ORIGINAL), Registry.load(PILOT)
    assert pilot.entries[:17] == original.entries
    assert len(pilot.entries) == 34
    evidence = json.loads(Path("docs/production-instrument-expansion.json").read_text())
    samples = {r["source_symbol"]: r for r in evidence["additions"]}
    for i, m in pilot.subscriptions("tradingview_guest")[17:]:
        row = samples[m.provider_symbol]
        assert row["prior_baseline_native_targets"] == 3
        assert row["prior_eventual_native_targets"] == 6
        assert row["canonical_symbol"] == i.canonical_symbol
        assert row["native_sample"]["metadata"]["bar_fillgaps"] is False
        assert m.realtime_status == "unknown"  # Runtime source status must still be checked.


async def test_ordinary_factory_uses_configured_pool_and_rejects_capacity():
    registry = Registry.load(PILOT)
    settings = Settings(
        _env_file=None,
        tradingview_candle_pool_enabled=True,
        candle_pool_connections=2,
        candle_pool_concurrency=4,
    )
    providers = await build_providers(settings, registry, Metrics())
    pool = providers[0]
    assert isinstance(pool, CandleConnectionPool)
    assert len(pool.subscriptions) == 34
    assert [len(w.provider.subscriptions) for w in pool.workers] == [17, 17]
    assert all(isinstance(w.provider, TradingViewCandleProvider) for w in pool.workers)
    assert len(pool.owners) == 34 and not pool.tasks
    with pytest.raises(ValueError, match="capacity"):
        await build_providers(
            settings.model_copy(update={"candle_pool_max_instruments": 33}), registry, Metrics()
        )
    with pytest.raises(ValueError, match="TRADINGVIEW_CANDLE_POOL_ENABLED"):
        await build_providers(
            Settings(_env_file=None, candle_pool_connections=2), registry, Metrics()
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"tradingview_storage_mode": "ticks"},
        {"candle_pool_connections": 7},
        {"candle_pool_worker_capacity": 101},
        {"candle_pool_concurrency": 13},
        {"candle_pool_max_instruments": 101},
        {"candle_pool_concurrency": 3},
        {"candle_pool_deadline_seconds": 55},
    ],
)
def test_pool_configuration_rejects_unusable_or_out_of_scope_limits(changes):
    with pytest.raises(ValueError):
        Settings(_env_file=None, tradingview_candle_pool_enabled=True, **changes)


@pytest.mark.integration
async def test_activation_restart_recovery_removal_and_source_identity(store, monkeypatch):
    pairs = Registry.load(PILOT).subscriptions("tradingview_guest")
    old, new = pairs[0], pairs[17]
    registry = Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in [old, new]])
    await store.register(Registry(registry.entries[:1]))
    old_start = NOW.replace(second=0) - timedelta(days=5)
    await store.write_candles(
        [make_candle(old, old_start)],
        [(old[0].id, old[1].provider, old_start, NOW.replace(second=0))],
    )
    old_state = await store.candle_progress(old[0].id, old[1].provider)
    monkeypatch.setattr("marketdata.persistence.utc_now", lambda: NOW)
    await store.register(registry, candle_providers=["tradingview_guest"])
    activation = NOW.replace(second=0) + timedelta(minutes=1)
    state = await store.candle_progress(new[0].id, new[1].provider)
    assert state["coverage_start"] == state["coverage_end"] == activation
    assert state["missing_count"] == state["unknown_count"] == 0
    assert await store.candle_progress(old[0].id, old[1].provider) == old_state

    p = Replay()
    await p.subscribe([new])
    p.session_info[new[0].canonical_symbol] = {"session": "24x7", "timezone": "Etc/UTC"}
    service = CandleIngestion(Settings(_env_file=None), store, p, Fanout(100, 2, p.metrics))
    await service.cycle(NOW)  # No closed bucket at/after activation yet.
    assert not p.calls
    p.fail = True
    await service.cycle(NOW + timedelta(minutes=2))
    failed = await store.candle_progress(new[0].id, new[1].provider)
    assert failed["missing_count"] == 1 and failed["coverage_start"] == activation
    monkeypatch.setattr("marketdata.persistence.utc_now", lambda: NOW + timedelta(days=1))
    await store.register(registry, candle_providers=["tradingview_guest"])
    assert await store.candle_progress(new[0].id, new[1].provider) == failed
    resumed = Replay()
    await resumed.subscribe([new])
    restarted = CandleIngestion(
        Settings(_env_file=None), store, resumed, Fanout(100, 2, resumed.metrics)
    )
    await restarted.cycle(NOW + timedelta(minutes=4))
    recovered = await store.candle_progress(new[0].id, new[1].provider)
    assert recovered["coverage_start"] == activation and recovered["missing_count"] == 0
    assert resumed.calls[0][0] == activation
    assert (
        len(
            await store.native_candles(
                new[0].id, new[1].provider, activation, activation + timedelta(minutes=3), "1m", 100
            )
        )
        == 3
    )
    await store.register(Registry(registry.entries[:1]), candle_providers=["tradingview_guest"])
    assert await store.candle_progress(new[0].id, new[1].provider) == recovered
    changed = Registry(
        [
            RegistryEntry(
                instrument=new[0],
                mappings=[new[1].model_copy(update={"provider_symbol": "BINANCE:OTHERUSDT"})],
            )
        ]
    )
    with pytest.raises(ValueError, match="source identity"):
        await store.register(changed, candle_providers=["tradingview_guest"])
    assert await store.candle_progress(old[0].id, old[1].provider) == old_state


@pytest.mark.integration
async def test_restart_before_first_collection_recovers_from_activation(store, monkeypatch):
    import importlib.util

    pair = Registry.load(PILOT).subscriptions("tradingview_guest")[17]
    registry = Registry([RegistryEntry(instrument=pair[0], mappings=[pair[1]])])
    monkeypatch.setattr("marketdata.persistence.utc_now", lambda: NOW)
    await store.register(registry, candle_providers=["tradingview_guest"])
    activation = NOW.replace(second=0) + timedelta(minutes=1)
    monkeypatch.setattr("marketdata.persistence.utc_now", lambda: NOW + timedelta(minutes=10))
    await store.register(registry, candle_providers=["tradingview_guest"])
    p = Replay()
    await p.subscribe([pair])
    p.session_info[pair[0].canonical_symbol] = {"session": "24x7", "timezone": "Etc/UTC"}
    service = CandleIngestion(Settings(_env_file=None), store, p, Fanout(100, 2, p.metrics))
    await service.cycle(NOW + timedelta(minutes=10))
    assert p.calls[0][0] == activation
    state = await store.candle_progress(pair[0].id, pair[1].provider)
    assert state["coverage_start"] == activation
    assert state["missing_count"] == state["unknown_count"] == 0
    spec = importlib.util.spec_from_file_location("reconciliation", "scripts/reconcile_candles.py")
    utility = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(utility)
    result = await utility.reconcile(
        store,
        NOW.replace(second=0) - timedelta(days=5),
        NOW.replace(second=0) + timedelta(minutes=10),
    )
    row = next(r for r in result if r["symbol"] == pair[0].canonical_symbol)
    assert row["completed_wall_clock_minutes"] == 7210
    assert row["evaluated_minutes"] == row["persisted_native_candles"] == row["EXPECTED"] == 9
    assert row["missing_expected_minutes"] == 0 and row["invariant_holds"]
