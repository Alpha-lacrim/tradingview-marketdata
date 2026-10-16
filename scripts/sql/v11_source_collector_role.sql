-- Administrator only, after explicit source 0006 + sc0002 installation.
-- Writer for compatible collector binary, distinct from consumer and schema owner.
CREATE ROLE marketdata_v11_collect LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
DO $$ BEGIN
    EXECUTE format('GRANT CONNECT,TEMPORARY ON DATABASE %I TO marketdata_v11_collect',
        current_database());
END $$;
GRANT USAGE ON SCHEMA public TO marketdata_v11_collect;
GRANT SELECT ON alembic_version,source_capture_version,candle_revisions TO marketdata_v11_collect;
GRANT SELECT,INSERT ON market_ticks,source_candle_outbox TO marketdata_v11_collect;
GRANT SELECT,INSERT,UPDATE ON instruments,provider_instruments,latest_prices,
    market_candles,latest_candles,candle_coverage,collector_status,candle_observation_heads,
    source_candle_exports
    TO marketdata_v11_collect;
GRANT SELECT ON source_capture_clock TO marketdata_v11_collect;
GRANT UPDATE(head,pending_rows,pending_bytes) ON source_capture_clock TO marketdata_v11_collect;
GRANT SELECT,INSERT ON source_quote_state TO marketdata_v11_collect;
GRANT UPDATE(version,fields,record) ON source_quote_state TO marketdata_v11_collect;
GRANT INSERT ON candle_revisions TO marketdata_v11_collect;
GRANT SELECT,INSERT,DELETE ON marketdata_events TO marketdata_v11_collect;
GRANT SELECT,UPDATE ON marketdata_event_clock TO marketdata_v11_collect;
GRANT USAGE ON SEQUENCE candle_revisions_id_seq TO marketdata_v11_collect;
-- No source_sync_* state mutation, role management, schema DDL or history deletion.
