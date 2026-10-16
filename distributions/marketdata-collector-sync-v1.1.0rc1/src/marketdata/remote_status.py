"""Conservative projection of source-captured status into the existing read contracts."""

import json
from typing import Any

from marketdata.domain import utc_now
from marketdata.providers.base import HealthState, ProviderHealth, SymbolHealth


def project_status(rows: list[dict[str, Any]]) -> dict[str, Any]:
    now = utc_now()
    groups: dict[str, list[ProviderHealth]] = {}
    running, healthy = True, True
    queues: dict[str, int] = {}
    unavailable: set[str] = set()
    captured = []
    fresh = True
    source_health: dict[tuple[str, str], ProviderHealth] = {}
    source_fresh: dict[tuple[str, str], bool] = {}
    for row in rows:
        payload = json.loads(row["payload"]) if row["payload"] else {}
        live = bool(row["fresh"])
        is_running = live and payload.get("running", False)
        running = running and is_running
        healthy = healthy and live and payload.get("database_healthy", False)
        fresh = fresh and live
        if payload:
            run = payload["run_id"]
            queues[run] = max(queues.get(run, 0), payload["queue_depth"])
        if row["captured_at"] is not None:
            captured.append(row["captured_at"])
        name = row["canonical_symbol"]
        source_fresh[name, row["provider"]] = live
        if not payload:
            unavailable.add(row["provider"])
            continue
        health = ProviderHealth.model_validate(payload["health"])
        symbol = health.symbols.get(name) or SymbolHealth(
            canonical_symbol=name, provider_symbol=row["provider_symbol"]
        )
        health.symbols = {name: symbol}
        if symbol.last_observation_at is not None:
            symbol.age_ms = max(0, int((now - symbol.last_observation_at).total_seconds() * 1000))
            symbol.stale = (
                symbol.stale
                or symbol.last_observation_at > now
                or symbol.age_ms > symbol.stale_after_seconds * 1000
            )
        else:
            symbol.stale = True
        if not is_running:
            health.state = "disconnected"
            health.transport = "disconnected"
            health.subscription_restored = False
            health.degradation_reasons = [
                "collector_stopped" if live else "collector_status_expired"
            ]
            symbol.stale, symbol.observed_in_session = True, False
            symbol.session_expectation, symbol.candle_state = "UNKNOWN", None
        elif symbol.stale and health.state == "healthy":
            health.state = "stale"
        groups.setdefault(row["provider"], []).append(health)
        source_health[name, row["provider"]] = health
        source_fresh[name, row["provider"]] = live
    providers = []
    for provider, parts in sorted(groups.items()):
        symbols = {symbol: value for part in parts for symbol, value in part.symbols.items()}
        state: HealthState = (
            "disconnected"
            if provider in unavailable or any(p.state == "disconnected" for p in parts)
            else "healthy"
            if all(p.state == "healthy" for p in parts)
            else "degraded"
        )
        providers.append(
            ProviderHealth(
                provider=provider,
                state=state,
                transport="connected"
                if all(p.transport == "connected" for p in parts)
                else "disconnected",
                updated_at=min(p.updated_at for p in parts),
                symbols=symbols,
                configured_symbol_count=len(symbols),
                requested_symbol_count=len(symbols),
                fresh_symbol_count=sum(not s.stale for s in symbols.values()),
                stale_symbol_count=sum(s.stale for s in symbols.values()),
                stale_symbols=sorted(k for k, s in symbols.items() if s.stale),
                subscription_restored=all(p.subscription_restored for p in parts),
                degradation_reasons=sorted(
                    {reason for p in parts for reason in p.degradation_reasons}
                ),
                last_success_at=max(
                    (p.last_success_at for p in parts if p.last_success_at), default=None
                ),
                last_message_at=max(
                    (p.last_message_at for p in parts if p.last_message_at), default=None
                ),
                storage_mode=parts[0].storage_mode
                if len({p.storage_mode for p in parts}) == 1
                else "mixed",
            )
        )
    return dict(
        fresh=fresh,
        running=running,
        database_healthy=healthy,
        queue_depth=sum(queues.values()),
        updated_at=min(captured) if captured else None,
        providers=providers,
        source_health=source_health,
        source_fresh=source_fresh,
    )
