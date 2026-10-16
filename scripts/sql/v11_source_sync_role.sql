-- Administrator only, after base 0006 + sc0002 on an explicitly approved source.
-- No password here. Set the new LOGIN credential through private provisioning.
CREATE ROLE marketdata_v11_sync LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
DO $$ BEGIN
    EXECUTE format('REVOKE CREATE,TEMPORARY ON DATABASE %I FROM PUBLIC',current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO marketdata_v11_sync',current_database());
END $$;
GRANT USAGE ON SCHEMA public TO marketdata_v11_sync;
GRANT SELECT ON alembic_version,source_capture_version,instruments,provider_instruments,
    market_candles,latest_prices,candle_revisions,candle_coverage,collector_status,source_capture_clock,
    source_candle_outbox,source_candle_exports,source_quote_state,source_sync_owner,
    source_sync_batches,source_sync_archive TO marketdata_v11_sync;
GRANT EXECUTE ON FUNCTION source_sync_claim(UUID),source_sync_enter(UUID,BIGINT),
    source_sync_state(TEXT,JSONB),source_sync_capture(TEXT,JSONB),source_sync_storage()
    TO marketdata_v11_sync;
-- No financial INSERT/UPDATE/DELETE, no epoch/head/payload UPDATE, no policy or DDL.
-- SQL functions are owned by the separate schema administrator, never this LOGIN.
