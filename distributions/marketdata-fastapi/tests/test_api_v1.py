"""Real database read-back for the company API; fixtures never contact a source."""

import asyncio
import base64
import importlib.util
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import asyncpg
import httpx
import pytest
from helpers import forbidden, make_candle, snapshot
from pydantic import SecretStr, ValidationError

from marketdata.api import create_app
from marketdata.api_models import HistoricalCandlePage
from marketdata.api_v1 import chart_data, completed_candle
from marketdata.config import Settings
from marketdata.domain import DataStatus, RegistryEntry, normalize
from marketdata.persistence import Store
from marketdata.price_selection import select_price
from marketdata.providers.base import ProviderHealth
from marketdata.registry import Registry

NOW = datetime(2026, 10, 13, 10, 0, 2, tzinfo=UTC)
START = NOW.replace(hour=0, minute=0, second=0) - timedelta(days=1)
TOKEN = "deterministic-test-token-32-characters"


@pytest.fixture
async def brokers(store, monkeypatch):
    registry = Registry.load(Path("config/instruments.tradingview-business-proposal.json"))
    pairs = [registry.by_provider["tradingview_guest", s] for s in ("OANDA:EURUSD", "FX:EURUSD")]
    await store.register(Registry([RegistryEntry(instrument=i, mappings=[m]) for i, m in pairs]))
    monkeypatch.setattr("marketdata.api_v1.utc_now", lambda: NOW)
    return pairs


@asynccontextmanager
async def api_client(store, **kwargs):
    app = create_app(
        Settings(
            _env_file=None,
            database_url=kwargs.pop("database_url", store.settings.database_url),
            **kwargs,
        )
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            yield app, client


def candle(pair, bucket, **kwargs):
    return make_candle(
        pair,
        bucket,
        received_timestamp=NOW,
        volume=None,
        metadata={"volume_type": "unknown", "volume_additive": False},
        **kwargs,
    )


async def persist(store, pair, buckets, **kwargs):
    candles = [candle(pair, bucket, **kwargs) for bucket in buckets]
    plan = [
        (
            pair[0].id,
            pair[1].provider,
            min(buckets),
            max(buckets) + timedelta(minutes=1),
            {"session": "24x7", "timezone": "Etc/UTC"},
        )
    ]
    await store.write_candles(candles, plan)
    return candles


@pytest.mark.integration
async def test_instruments_filter_identity_and_keyset_pagination(store, brokers):
    await persist(store, brokers[0], [START])
    async with api_client(store) as (_, client):
        page = (
            await client.get(
                "/api/v1/instruments",
                params={"asset_class": "forex", "provider": "tradingview_guest", "limit": 1},
            )
        ).json()
        assert len(page["items"]) == 1 and page["next_after"]
        second = (
            await client.get(
                "/api/v1/instruments",
                params={
                    "asset_class": "forex",
                    "provider": "tradingview_guest",
                    "limit": 1,
                    "after": page["next_after"],
                },
            )
        ).json()
        assert second["next_after"] is None
        items = page["items"] + second["items"]
        assert {i["provider_symbol"] for i in items} == {"OANDA:EURUSD", "FX:EURUSD"}
        assert {i["venue"] for i in items} == {"OANDA", "FX"}
        assert {i["broker"] for i in items} == {"OANDA", "FXCM"}
        assert len({i["instrument_id"] for i in items}) == 2
        recorded = next(i for i in items if i["provider_symbol"] == "OANDA:EURUSD")
        assert recorded["availability"] == "persisted"
        assert recorded["supported_historical_intervals"] == ["1m", "5m", "15m", "1h", "1d"]
        other = next(i for i in items if i["provider_symbol"] == "FX:EURUSD")
        assert other["availability"] == "configured_unverified"
        assert other["supported_historical_intervals"] == []
        assert (await client.get("/api/v1/instruments?asset_class=wrong")).status_code == 422


@pytest.mark.integration
async def test_bulk_latest_quote_candle_unavailable_and_source_independence(store, brokers):
    bucket = NOW.replace(second=0) - timedelta(minutes=1)
    candles = [await persist(store, p, [bucket]) for p in brokers]
    quote = normalize(
        brokers[0][0],
        brokers[0][1].model_copy(update={"realtime_status": DataStatus.REALTIME}),
        NOW - timedelta(seconds=2),
        event_timestamp=NOW - timedelta(seconds=3),
        last="1.123456789012345678901",
        bid="1.123",
        ask="1.124",
    )
    await store.write([quote])
    async with api_client(store) as (_, client):
        result = await client.get(
            "/api/v1/prices/latest", params=[("symbols", p[0].canonical_symbol) for p in brokers]
        )
        assert result.status_code == 200, result.text
        items = result.json()["items"]
        q = next(i for i in items if i["provider_symbol"] == "OANDA:EURUSD")
        close = next(i for i in items if i["provider_symbol"] == "FX:EURUSD")
        assert q["price_type"] == "quote" and q["last"] == str(quote.last)
        assert q["age_ms"] == 3000 and q["freshness"] == "fresh"
        assert q["quote_freshness"] == "fresh" and q["realtime_quote_available"]
        assert q["source_timestamp"] == quote.model_dump(mode="json")["event_timestamp"]
        assert close["price_type"] == close["price_basis"] == "candle_close"
        assert close["last"] == str(candles[1][0].close) and close["bid"] is None
        assert close["quote_freshness"] == "unavailable" and not close["realtime_quote_available"]
        missing = (await client.get("/api/v1/prices/latest?symbols=CRYPTO:BTCUSDT")).json()[
            "items"
        ][0]
        assert missing["price_type"] == missing["freshness"] == "unavailable"
        assert missing["last"] is None and missing["received_timestamp"] is None
        assert (await client.get("/api/v1/prices/latest?symbols=UNKNOWN")).status_code == 404


@pytest.mark.parametrize(
    "event_age,receipt_age,status,expected_type,freshness",
    [
        (3, 2, "realtime", "quote", "fresh"),
        (300, 2, "realtime", "candle_close", "fresh"),
        (3, 2, "delayed", "quote", "fresh"),
        (-1, 0, "realtime", "candle_close", "fresh"),
        (3, -1, "realtime", "candle_close", "fresh"),
    ],
)
@pytest.mark.integration
async def test_event_time_delay_and_clock_skew_semantics(
    store, brokers, event_age, receipt_age, status, expected_type, freshness
):
    pair = brokers[0]
    await persist(store, pair, [NOW.replace(second=0) - timedelta(minutes=1)])
    tick = normalize(
        pair[0],
        pair[1].model_copy(update={"realtime_status": DataStatus(status)}),
        NOW - timedelta(seconds=receipt_age),
        event_timestamp=NOW - timedelta(seconds=event_age),
        last="1.001",
    )
    await store.write([tick])
    async with api_client(store) as (_, client):
        item = (
            await client.get("/api/v1/prices/latest", params={"symbols": pair[0].canonical_symbol})
        ).json()["items"][0]
        assert item["price_type"] == expected_type and item["freshness"] == freshness
        assert item["realtime_quote_available"] is (
            status == "realtime" and event_age == 3 and receipt_age == 2
        )


def test_selection_stale_quote_receipt_only_and_future_candle(pair):
    tick = normalize(*pair, NOW - timedelta(seconds=100), bid="1", ask="2")
    c = make_candle(pair, NOW.replace(second=0), received_timestamp=NOW + timedelta(minutes=1))
    assert select_price(tick, c, NOW, Settings(_env_file=None)) == tick
    assert select_price(None, c, NOW, Settings(_env_file=None)) is None


@pytest.mark.integration
async def test_quote_without_last_or_event_time_is_not_invented(store, brokers):
    pair = brokers[0]
    await store.write([normalize(*pair, NOW, bid="1.01", ask="1.02")])
    async with api_client(store) as (_, client):
        item = (
            await client.get("/api/v1/prices/latest", params={"symbols": pair[0].canonical_symbol})
        ).json()["items"][0]
        assert item["price_basis"] == "bid_ask" and item["last"] is None
        assert item["source_timestamp"] is None and item["freshness_basis"] == "receipt"
        assert item["realtime_status"] == "unknown" and not item["realtime_quote_available"]


@pytest.mark.parametrize(
    "interval,minutes", [("1m", 1), ("5m", 5), ("15m", 15), ("1h", 60), ("1d", 1440)]
)
@pytest.mark.integration
async def test_all_native_intervals_exact_readback_null_volume(store, brokers, interval, minutes):
    pair = brokers[1]
    await persist(store, pair, [START + timedelta(minutes=i) for i in range(1440)])
    async with store.connection() as c:
        stored = await c.fetch(
            "SELECT * FROM market_candles WHERE instrument_id=$1 ORDER BY bucket_start", pair[0].id
        )
    async with api_client(store) as (_, client):
        response = await client.get(
            "/api/v1/candles",
            params={
                "symbol": pair[0].canonical_symbol,
                "provider": pair[1].provider,
                "provider_symbol": "FX:EURUSD",
                "interval": interval,
                "start": START.isoformat(),
                "end": (START + timedelta(days=1)).isoformat(),
                "limit": 1000,
            },
        )
        assert response.status_code == 200, response.text
        items = response.json()["items"]
        assert len(items) == min(1000, 1440 // minutes)
        for index, item in enumerate(items):
            rows = stored[index * minutes : (index + 1) * minutes]
            assert item["open"] == str(rows[0]["open"])
            assert Decimal(item["high"]) == max(r["high"] for r in rows)
            assert Decimal(item["low"]) == min(r["low"] for r in rows)
            assert item["close"] == str(rows[-1]["close"])
            assert item["volume"] is None and item["provider_symbol"] == "FX:EURUSD"
            assert item["complete"] and item["observations"] == minutes
            assert item["origin"] == ("native" if interval == "1m" else "derived")
            assert item["bucket_start"].endswith("Z") and item["bucket_end"].endswith("Z")


@pytest.mark.integration
async def test_history_cursor_no_missing_duplicates_mixed_status_and_forming_exclusion(
    store, brokers
):
    pair = brokers[0]
    current = NOW.replace(second=0)
    buckets = [current - timedelta(minutes=10 - i) for i in range(10)]
    bars = [
        candle(pair, b, realtime_status="realtime" if i % 2 else "unknown")
        for i, b in enumerate(buckets)
    ]
    # A future-closed row can exist in imported history; HTTP still excludes it at as_of.
    future = make_candle(pair, current, received_timestamp=NOW + timedelta(minutes=1))
    await store.write_candles([*bars, future], [])
    async with api_client(store) as (_, client):
        params = {
            "symbol": pair[0].canonical_symbol,
            "provider": pair[1].provider,
            "start": buckets[0].isoformat(),
            "end": (current + timedelta(minutes=5)).isoformat(),
            "interval": "5m",
            "limit": 1,
        }
        seen = []
        cursors = []
        while True:
            response = await client.get("/api/v1/candles", params=params)
            assert response.status_code == 200, response.text
            page = response.json()
            seen += page["items"]
            if not page["next_cursor"]:
                break
            cursors.append(page["next_cursor"])
            params["cursor"] = page["next_cursor"]
        assert len(seen) == 4
        assert len({(r["bucket_start"], r["realtime_status"]) for r in seen}) == 4
        assert sum(r["observations"] for r in seen) == 10
        assert all(not r["complete"] for r in seen)
        assert all(datetime.fromisoformat(r["bucket_end"]) <= NOW for r in seen)
        params["cursor"] = cursors[0]
        params["interval"] = "1m"
        assert (await client.get("/api/v1/candles", params=params)).status_code == 422
        params["cursor"] = "invalid"
        assert (await client.get("/api/v1/candles", params=params)).status_code == 422
        params["cursor"] = base64.urlsafe_b64encode(b"[]").decode()
        assert (await client.get("/api/v1/candles", params=params)).status_code == 422


@pytest.mark.parametrize(
    "change",
    [
        {"start": "2026-10-12T00:00:00"},
        {"end": "2026-10-12T00:00:00Z"},
        {"end": "2027-01-01T00:00:00Z"},
        {"start": "2026-10-12T00:00:01Z"},
        {"interval": "1s"},
        {"interval": "DROP TABLE instruments"},
        {"limit": 0},
        {"limit": 1001},
        {"provider": "tradingview_guest'; DROP TABLE instruments;--"},
        {"symbol": "'; DROP TABLE instruments;--"},
    ],
)
@pytest.mark.parametrize("endpoint", ["/api/v1/candles", "/api/v1/dashboard/candles"])
@pytest.mark.integration
async def test_invalid_history_utc_bounds_and_injection(store, brokers, change, endpoint):
    params = {
        "symbol": brokers[0][0].canonical_symbol,
        "provider": "tradingview_guest",
        "start": START.isoformat(),
        "end": (START + timedelta(days=1)).isoformat(),
    }
    params.update(change)
    async with api_client(store) as (_, client):
        response = await client.get(endpoint, params=params)
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "invalid_request"
        assert (await client.get("/api/v1/instruments")).status_code == 200


@pytest.mark.integration
async def test_source_filter_is_parameterized_and_unknown_mapping_404(store, brokers):
    async with api_client(store) as (_, client):
        response = await client.get(
            "/api/v1/candles",
            params={
                "symbol": brokers[0][0].canonical_symbol,
                "provider": "tradingview_guest",
                "provider_symbol": "FX:EURUSD",
            },
        )
        assert response.status_code == 404
        response = await client.get(
            "/api/v1/instruments", params={"provider_symbol": "x' OR TRUE --"}
        )
        assert response.status_code == 200 and response.json()["items"] == []
        assert (
            await client.get("/api/v1/prices/latest", params=[("symbols", "CRYPTO:BTCUSDT")] * 101)
        ).status_code == 422


@pytest.mark.integration
async def test_latest_completed_candle_does_not_substitute_quote(store, brokers):
    bar = (await persist(store, brokers[0], [START]))[0]
    await store.write([normalize(*brokers[1], NOW, last="999")])
    async with api_client(store) as (_, client):
        response = await client.get(
            "/api/v1/candles/latest", params=[("symbols", p[0].canonical_symbol) for p in brokers]
        )
        assert response.status_code == 200, response.text
        rows = response.json()["items"]
        present = next(r for r in rows if r["provider_symbol"] == "OANDA:EURUSD")
        missing = next(r for r in rows if r["provider_symbol"] == "FX:EURUSD")
        assert present["candle"]["close"] == str(bar.close)
        assert missing["candle"] is None and missing["availability"] == "unavailable"


@pytest.mark.integration
async def test_coverage_preserves_expected_unknown_not_expected_recovery_and_broker_gaps(
    store, brokers
):
    first = START
    end = first + timedelta(minutes=6)
    plans = [
        (i.id, m.provider, first, end, {"session": "24x7", "timezone": "Etc/UTC"})
        for i, m in brokers
    ]
    await store.write_candles([candle(brokers[0], first), candle(brokers[1], first)], plans)
    recovered = candle(brokers[0], first + timedelta(minutes=1)).model_copy(
        update={"metadata": {"backfilled": True}}
    )
    await store.write_candles([recovered], plans)
    async with store.connection() as c:
        await c.execute(
            """UPDATE candle_coverage SET
            not_expected=tstzmultirange(tstzrange($1,$2,'[)')),
            unknown=tstzmultirange(tstzrange($2,$3,'[)')),
            missing=missing-tstzmultirange(tstzrange($1,$3,'[)'))
            WHERE instrument_id=$4""",
            first + timedelta(minutes=3),
            first + timedelta(minutes=4),
            end,
            brokers[0][0].id,
        )
    async with api_client(store) as (_, client):
        params = [("symbols", p[0].canonical_symbol) for p in brokers] + [
            ("start", first.isoformat()),
            ("end", end.isoformat()),
        ]
        response = await client.get("/api/v1/coverage", params=params)
        assert response.status_code == 200, response.text
        rows = response.json()["items"]
        a = next(r for r in rows if r["provider_symbol"] == "OANDA:EURUSD")
        b = next(r for r in rows if r["provider_symbol"] == "FX:EURUSD")
        assert a["expected_minutes"] == 3 and a["persisted_completed_candles"] == 2
        assert a["unresolved_expected_gaps"] == 1 and a["recovered_candles"] == 1
        assert a["not_expected_minutes"] == 1 and a["unknown_minutes"] == 2
        assert a["initially_missing_candles"] is None and a["coverage_state"] == "gaps"
        assert b["unresolved_expected_gaps"] == 5 and b["recovered_candles"] == 0
        assert a["latest_completed_bucket"] == recovered.model_dump(mode="json")["bucket_start"]
        params = {"symbols": "CRYPTO:BTCUSDT", "start": first.isoformat(), "end": end.isoformat()}
        missing = (await client.get("/api/v1/coverage", params=params)).json()["items"][0]
        assert missing["coverage_state"] == "unknown" and missing["unknown_minutes"] == 6
        assert missing["expected_minutes"] == missing["persisted_completed_candles"] == 0
        assert missing["tracked_start"] is None


@pytest.mark.integration
async def test_coverage_untracked_time_and_gap_detail_bound(store, brokers):
    pair = brokers[0]
    buckets = [START + timedelta(minutes=i * 2) for i in range(105)]
    await persist(store, pair, buckets)
    async with api_client(store) as (_, client):
        result = (
            await client.get(
                "/api/v1/coverage",
                params={
                    "symbols": pair[0].canonical_symbol,
                    "start": (START - timedelta(minutes=1)).isoformat(),
                    "end": (buckets[-1] + timedelta(minutes=2)).isoformat(),
                },
            )
        ).json()["items"][0]
        assert result["unknown_minutes"] == 2
        assert result["unresolved_expected_gaps"] == 104
        assert len(result["missing_ranges"]) == 100 and result["missing_ranges_truncated"]


@pytest.mark.integration
async def test_offline_health_no_provider_initialization_and_no_data_mutation(
    store, brokers, monkeypatch
):
    await persist(store, brokers[0], [START])
    await store.publish_collector_status(
        uuid4(),
        False,
        True,
        0,
        [ProviderHealth(provider="tradingview_guest", state="disconnected")],
    )
    before = await snapshot(store)
    assert importlib.util.find_spec("marketdata.collector") is None
    monkeypatch.setattr(Store, "register", forbidden)
    monkeypatch.setattr(Registry, "load", forbidden)
    async with api_client(store, registry_file=Path("absent.json")) as (_, client):
        health = (await client.get("/api/v1/health")).json()
        assert health["api"] == health["database"] == "available"
        assert health["collector"] == "stopped" and health["status"] == "ok"
        assert health["streaming_mode"] == "database"
        response = await client.get(
            "/api/v1/candles",
            params={
                "symbol": brokers[0][0].canonical_symbol,
                "provider": "tradingview_guest",
                "start": START.isoformat(),
                "end": (START + timedelta(minutes=1)).isoformat(),
            },
        )
        assert response.status_code == 200 and len(response.json()["items"]) == 1
    assert await snapshot(store) == before


@pytest.mark.integration
async def test_database_error_and_query_timeout_are_503(store, brokers, monkeypatch):
    async with api_client(store, db_timeout_seconds=0.05) as (app, client):
        original = app.state.store.source_records

        async def broken(*args, **kwargs):
            raise asyncpg.CannotConnectNowError("test database offline")

        monkeypatch.setattr(app.state.store, "source_records", broken)
        response = await client.get("/api/v1/prices/latest")
        assert (
            response.status_code == 503
            and response.json()["error"]["code"] == "database_unavailable"
        )
        assert (await client.get("/api/v1/health")).status_code == 503

        async def slow(*args, **kwargs):
            await asyncio.sleep(0.1)
            return []

        monkeypatch.setattr(app.state.store, "source_records", slow)
        assert (await client.get("/api/v1/prices/latest")).status_code == 503
        monkeypatch.setattr(app.state.store, "source_records", original)
        assert (await client.get("/api/v1/prices/latest")).status_code == 200


@pytest.mark.integration
async def test_bulk_100_sources_constant_query_count(store, pair, monkeypatch):
    entries = [
        RegistryEntry(
            instrument=pair[0].model_copy(update={"canonical_symbol": f"CRYPTO:TEST{i:03}"}),
            mappings=[pair[1].model_copy(update={"provider_symbol": f"TEST{i:03}"})],
        )
        for i in range(100)
    ]
    registry = Registry(entries)
    await store.register(registry)
    await store.write(
        [
            normalize(i, m, NOW, last="123.123456789012345678901")
            for i, m in registry.subscriptions("mock")
        ]
    )
    async with api_client(store) as (app, client):
        queries = []
        original = app.state.store.connection

        @asynccontextmanager
        async def counted():
            async with original() as connection:
                connection.add_query_logger(queries.append)
                try:
                    yield connection
                finally:
                    await asyncio.sleep(0)
                    connection.remove_query_logger(queries.append)

        monkeypatch.setattr(app.state.store, "connection", counted)
        params = [("symbols", i.instrument.canonical_symbol) for i in entries]
        response = await client.get("/api/v1/prices/latest", params=params)
        assert response.status_code == 200 and len(response.json()["items"]) == 100
        selects = [q.query for q in queries if q.query.lstrip().startswith("SELECT")]
        assert len(selects) == 2  # One symbol validation + one joined latest-state query.
        assert all("market_ticks" not in sql for sql in selects)


@pytest.mark.integration
async def test_production_authentication_cors_and_openapi(store, readonly_database_url):
    async with api_client(
        store,
        database_url=readonly_database_url,
        api_environment="production",
        api_key=SecretStr(TOKEN),
        api_cors_origins=["https://frontend.example.com"],
    ) as (_, client):
        missing = await client.get(
            "/api/v1/instruments", headers={"Origin": "https://frontend.example.com"}
        )
        assert missing.status_code == 401 and missing.headers["www-authenticate"] == "Bearer"
        assert missing.headers["access-control-allow-origin"] == "https://frontend.example.com"
        assert (
            await client.get("/api/v1/instruments", headers={"Authorization": "Bearer wrong"})
        ).status_code == 403
        response = await client.get(
            "/api/v1/instruments", headers={"Authorization": "Bearer " + TOKEN}
        )
        assert response.status_code == 200
        headers = {
            "Origin": "https://frontend.example.com",
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization",
        }
        preflight = await client.options("/api/v1/instruments", headers=headers)
        assert (
            preflight.status_code == 200
            and "access-control-allow-credentials" not in preflight.headers
        )
        headers["Origin"] = "https://unapproved.example.com"
        rejected = await client.options("/api/v1/instruments", headers=headers)
        assert rejected.status_code == 400 and "access-control-allow-origin" not in rejected.headers
        schema = (await client.get("/openapi.json")).json()
        assert schema["components"]["securitySchemes"]["CompanyBearer"]["scheme"] == "bearer"
        for path, operations in schema["paths"].items():
            assert operations["get"]["security"] == (
                [] if path == "/health" else [{"CompanyBearer": []}]
            )
        assert (await client.get("/docs")).status_code == 200
        assert (await client.get("/health")).status_code == 200


@pytest.mark.parametrize(
    "kwargs",
    [
        {"api_environment": "production"},
        {"api_environment": "production", "api_key": "short"},
        {"api_key": ""},
        {"api_cors_origins": ["*"]},
        {"api_cors_origins": ["https://example.com/"]},
        {
            "api_environment": "production",
            "api_key": TOKEN,
            "api_cors_origins": ["http://example.com"],
        },
    ],
)
def test_insecure_settings_rejected(kwargs):
    with pytest.raises(ValidationError):
        Settings(_env_file=None, **kwargs)


@pytest.mark.integration
async def test_select_only_role_cannot_write_even_when_session_readonly_is_disabled(store, brokers):
    role = "batch2_read_" + uuid4().hex
    tables = (
        "instruments,provider_instruments,market_ticks,latest_prices,market_candles,"
        "latest_candles,candle_coverage,candle_revisions,collector_status,alembic_version"
    )
    async with store.connection() as admin:
        await admin.execute(
            f"CREATE ROLE {role} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
            "NOREPLICATION PASSWORD 'disposable_test'"
        )
        await admin.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
        await admin.execute(f"GRANT SELECT ON {tables} TO {role}")
        views = await admin.fetch(
            "SELECT viewname FROM pg_views WHERE schemaname='public' AND viewname LIKE 'candles_%'"
        )
        for v in views:
            await admin.execute(f'GRANT SELECT ON "{v["viewname"]}" TO {role}')
    parsed = urlsplit(store.settings.database_url.get_secret_value())
    url = urlunsplit(
        parsed._replace(netloc=f"{role}:disposable_test@{parsed.hostname}:{parsed.port}")
    )
    try:
        app = create_app(Settings(_env_file=None, database_url=url))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://test"
            ) as client:
                for path in (
                    "/api/v1/instruments",
                    "/api/v1/prices/latest",
                    "/api/v1/candles/latest",
                    "/api/v1/coverage",
                    "/api/v1/health",
                    "/candles/CRYPTO:BTCUSDT?provider=mock",
                ):
                    response = await client.get(path)
                    assert response.status_code == 200, response.text
            async with app.state.store.connection() as c:
                assert await c.fetchval("SHOW transaction_read_only") == "on"
                await c.execute("SET default_transaction_read_only=off")
                assert await c.fetchval("SHOW transaction_read_only") == "off"
                for statement in (
                    "UPDATE instruments SET active=active",
                    "DELETE FROM latest_prices",
                    "TRUNCATE market_ticks",
                    "CREATE TABLE public.forbidden_test (id int)",
                    "ALTER TABLE instruments ADD COLUMN forbidden_test int",
                ):
                    with pytest.raises(asyncpg.InsufficientPrivilegeError):
                        await c.execute(statement)
                await c.execute("SET default_transaction_read_only=on")
    finally:
        async with store.connection() as admin:
            await admin.execute(f"DROP OWNED BY {role}")
            await admin.execute(f"DROP ROLE {role}")


DASHBOARD = "/api/v1/dashboard/candles"


def assert_chart_types(data, length):
    assert set(data) == set("abcdefgh")
    assert all(isinstance(a, list) and len(a) == length for a in data.values())
    for key in "af":
        assert all(type(v) is int for v in data[key])
    for key in "bcdegh":
        assert all(type(v) is float for v in data[key])
    assert data["a"] == sorted(data["a"])


def test_chart_conversion_exact_mapping_and_unavailable_metrics(pair):
    bar = make_candle(
        pair,
        START,
        received_timestamp=NOW,
        open="1.123456789012345678901",
        close="2.25",
        high="3.75",
        low="0.125",
        volume="42.5",
    )
    row = {
        "instrument": pair[0],
        "provider": pair[1].provider,
        "provider_symbol": pair[1].provider_symbol,
    }
    data = bar.model_dump()
    data.update(bucket=START, observations=1)
    item = completed_candle(row, data, "1m")
    page = HistoricalCandlePage(
        items=[item], interval="1m", start=START, end=START + timedelta(minutes=1), as_of=NOW
    )
    actual = chart_data(page).model_dump(mode="json")
    assert_chart_types(actual, 1)
    assert actual == {
        "a": [int(START.timestamp())],
        "b": [float(bar.open)],
        "c": [2.25],
        "d": [3.75],
        "e": [0.125],
        "f": [0],
        "g": [42.5],
        "h": [0.0],
    }
    assert item.open == bar.open  # Existing decimal representation is untouched.
    page.items = [item.model_copy(update={"volume": None})]
    assert chart_data(page).g == [0.0]
    page.items = []
    assert chart_data(page).model_dump() == {k: [] for k in "abcdefgh"}


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
def test_chart_rejects_nonfinite_float64_without_replacing_with_zero(pair, field):
    from fastapi import HTTPException

    bar = make_candle(pair, START, received_timestamp=NOW)
    data = bar.model_dump()
    data.update(bucket=START, observations=1)
    item = completed_candle(
        {
            "instrument": pair[0],
            "provider": pair[1].provider,
            "provider_symbol": pair[1].provider_symbol,
        },
        data,
        "1m",
    )
    page = HistoricalCandlePage(
        items=[item.model_copy(update={field: Decimal("1e1000")})],
        interval="1m",
        start=START,
        end=START + timedelta(minutes=1),
        as_of=NOW,
    )
    with pytest.raises(HTTPException) as error:
        chart_data(page)
    assert error.value.status_code == 503


@pytest.mark.parametrize(
    "interval,minutes", [("1m", 1), ("5m", 5), ("15m", 15), ("1h", 60), ("1d", 1440)]
)
@pytest.mark.parametrize("additive", [True, False])
@pytest.mark.integration
async def test_dashboard_persisted_alignment_gaps_volume_and_source_isolation(
    store, brokers, interval, minutes, additive
):
    beginning = START - timedelta(days=7)
    end = beginning + timedelta(minutes=minutes * 4)
    for n, pair in enumerate(brokers):
        bars = []
        for i in [*range(minutes), minutes * 2, minutes * 3]:
            bars.append(
                make_candle(
                    pair,
                    beginning + timedelta(minutes=i),
                    received_timestamp=NOW,
                    open=str(10 + n + i),
                    close=str(11 + n + i),
                    high=str(12 + n + i),
                    low=str(9 + n + i),
                    volume="2.125" if i < minutes else None if i == minutes * 2 else "0",
                    metadata={"volume_type": "base", "volume_additive": additive},
                )
            )
        await store.write_candles(list(reversed(bars)), [])
    async with api_client(store) as (_, client):
        for pair in brokers:
            params = {
                "symbol": pair[0].canonical_symbol,
                "provider": pair[1].provider,
                "provider_symbol": pair[1].provider_symbol,
                "interval": interval,
                "start": beginning.isoformat(),
                "end": end.isoformat(),
            }
            original = (await client.get("/api/v1/candles", params=params)).json()
            response = await client.get(DASHBOARD, params=params)
            assert response.status_code == 200, response.text
            data = response.json()
            assert_chart_types(data, 3)
            assert data["a"] == [
                int((beginning + timedelta(minutes=i)).timestamp())
                for i in (0, minutes * 2, minutes * 3)
            ]
            for key, field in [("b", "open"), ("c", "close"), ("d", "high"), ("e", "low")]:
                assert data[key] == [float(c[field]) for c in original["items"]]
                assert all(isinstance(c[field], str) for c in original["items"])
            assert data["g"] == [minutes * 2.125 if additive or minutes == 1 else 0.0, 0.0, 0.0]
            assert data["f"] == [0, 0, 0] and data["h"] == [0.0, 0.0, 0.0]
            assert original["items"][1]["volume"] is None
            assert "X-Marketdata-Next-Cursor" not in response.headers
            params["limit"] = 1
            seen = {k: [] for k in "abcdefgh"}
            while True:
                part = await client.get(DASHBOARD, params=params)
                assert part.status_code == 200, part.text
                for k, values in part.json().items():
                    seen[k].extend(values)
                cursor = part.headers.get("X-Marketdata-Next-Cursor")
                if not cursor:
                    break
                params["cursor"] = cursor
            assert seen == data
            assert (
                await client.get(
                    "/api/v1/candles",
                    params={k: v for k, v in params.items() if k not in {"limit", "cursor"}},
                )
            ).json() == original


@pytest.mark.integration
async def test_dashboard_empty_auth_cors_limits_and_readonly(store, brokers, readonly_database_url):
    pair = brokers[0]
    params = {
        "symbol": pair[0].canonical_symbol,
        "provider": pair[1].provider,
        "start": START.isoformat(),
        "end": (START + timedelta(minutes=5)).isoformat(),
    }
    before = await snapshot(store)
    async with api_client(
        store,
        database_url=readonly_database_url,
        api_environment="production",
        api_key=SecretStr(TOKEN),
        api_max_rows=2,
        api_cors_origins=["https://frontend.example.com"],
    ) as (app, client):
        assert (await client.get(DASHBOARD, params=params)).status_code == 401
        assert (
            await client.get(DASHBOARD, params=params, headers={"Authorization": "Bearer wrong"})
        ).status_code == 403
        headers = {"Authorization": "Bearer " + TOKEN, "Origin": "https://frontend.example.com"}
        response = await client.get(DASHBOARD, params={**params, "limit": 2}, headers=headers)
        assert response.status_code == 200, response.text
        assert response.json() == {k: [] for k in "abcdefgh"}
        assert "X-Marketdata-Next-Cursor" in response.headers["access-control-expose-headers"]
        assert (
            await client.get(DASHBOARD, params={**params, "limit": 3}, headers=headers)
        ).status_code == 422
        assert (
            await client.get(
                DASHBOARD, params={**params, "limit": 1, "cursor": "invalid"}, headers=headers
            )
        ).status_code == 422
        assert (
            await client.get(
                DASHBOARD,
                params={**params, "limit": 1, "provider_symbol": "wrong"},
                headers=headers,
            )
        ).status_code == 404
        async with app.state.store.connection() as c:
            assert await c.fetchval("SHOW transaction_read_only") == "on"
    assert await snapshot(store) == before
