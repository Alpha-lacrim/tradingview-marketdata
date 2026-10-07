"""Measure the real COPY/UPSERT pipeline using synthetic, provider-isolated instruments."""

import argparse
import asyncio
import json
import time
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from marketdata.config import Settings
from marketdata.domain import (
    AssetClass,
    DataStatus,
    Instrument,
    InstrumentType,
    ProviderInstrument,
    RegistryEntry,
)
from marketdata.fanout import Fanout
from marketdata.ingestion import IngestionService
from marketdata.observability import Metrics, configure_logging
from marketdata.persistence import Store
from marketdata.providers.mock import MockMarketDataProvider
from marketdata.registry import Registry


async def benchmark(seconds: float, rate: float, instruments: int) -> dict[str, float | int | str]:
    settings = Settings()
    configure_logging(settings.log_level)
    run = uuid4().hex[:8].upper()
    entries = [
        RegistryEntry(
            instrument=Instrument(
                canonical_symbol=f"BENCH:SIM:{run}:{n:05}",
                display_name="Synthetic benchmark",
                asset_class=AssetClass.CRYPTO,
                instrument_type=InstrumentType.SPOT,
                exchange="SIM",
                currency="USD",
                tick_size=Decimal("0.01"),
                metadata={"benchmark": True},
            ),
            mappings=[
                ProviderInstrument(
                    provider="mock",
                    provider_symbol=f"BENCH-{run}-{n}",
                    realtime_status=DataStatus.SIMULATED,
                )
            ],
        )
        for n in range(instruments)
    ]
    registry = Registry(entries)
    store = Store(settings)
    metrics = Metrics()
    provider = MockMarketDataProvider(metrics, rate)
    await provider.subscribe(registry.subscriptions("mock"))
    service = IngestionService(settings, store, [provider], Fanout(1, 1, metrics), metrics)
    try:
        await store.connect()
        await store.register(registry)
        start = time.perf_counter()
        await service.start()
        await asyncio.sleep(seconds)
        queued_at_stop = service.queue.qsize()
        await service.stop()
        elapsed = time.perf_counter() - start
        received = metrics.received.labels("mock")._value.get()
        persisted = metrics.persisted._value.get()
        dropped = sum(
            sample.value
            for metric in metrics.dropped.collect()
            for sample in metric.samples
            if sample.name.endswith("_total")
        )
        batch_samples = {
            s.name: s.value
            for m in metrics.batch_latency.collect()
            for s in m.samples
            if s.name.endswith(("_sum", "_count"))
        }
        return {
            "run": run,
            "instruments": instruments,
            "requested_rate": rate,
            "elapsed_seconds_including_drain": elapsed,
            "received": int(received),
            "persisted": int(persisted),
            "received_per_second": received / elapsed,
            "persisted_per_second": persisted / elapsed,
            "dropped": int(dropped),
            "queue_depth_at_stop": queued_at_stop,
            "mean_batch_seconds": batch_samples["marketdata_batch_seconds_sum"]
            / max(1, batch_samples["marketdata_batch_seconds_count"]),
        }
    finally:
        if not service.stopping:
            await service.stop()
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--rate", type=float, default=5000)
    parser.add_argument("--instruments", type=int, default=300)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        not 0 < args.seconds <= 3600
        or not 0 < args.rate <= 100000
        or not 1 <= args.instruments <= 10000
    ):
        parser.error("seconds: (0,3600], rate: (0,100000], instruments: [1,10000]")
    result = asyncio.run(benchmark(args.seconds, args.rate, args.instruments))
    rendered = json.dumps(result, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
