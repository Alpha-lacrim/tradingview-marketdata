-- Run once as the schema administrator in the dedicated company database AFTER 0010.
-- No passwords are supplied. Provision each LOGIN's private credential separately.
-- These fixed names intentionally fail if roles already exist; review rather than reuse.
CREATE ROLE marketdata_v11_read LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
CREATE ROLE marketdata_v11_ingest LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOREPLICATION NOBYPASSRLS;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
DO $$ BEGIN
    EXECUTE format('REVOKE CREATE,TEMPORARY ON DATABASE %I FROM PUBLIC',current_database());
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO marketdata_v11_read,marketdata_v11_ingest',
        current_database());
    EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO marketdata_v11_ingest',current_database());
END $$;
GRANT USAGE ON SCHEMA public TO marketdata_v11_read,marketdata_v11_ingest;
GRANT SELECT ON instruments,provider_instruments,market_ticks,latest_prices,market_candles,
    latest_candles,candle_coverage,candle_revisions,collector_status,marketdata_events,
    marketdata_event_clock,alembic_version,ingest_read_status TO marketdata_v11_read;
ALTER ROLE marketdata_v11_read SET default_transaction_read_only=on;
-- Only the known optional aggregate views; no blanket future-table/extension grants.
DO $$ DECLARE view_name TEXT; BEGIN
    FOREACH view_name IN ARRAY ARRAY[
        'candles_1s_last','candles_1s_mid','candles_1m_last','candles_1m_mid',
        'candles_5m_last','candles_5m_mid','candles_15m_last','candles_15m_mid',
        'candles_1h_last','candles_1h_mid','candles_1d_last','candles_1d_mid']
    LOOP
        IF to_regclass(format('public.%I',view_name)) IS NOT NULL THEN
            EXECUTE format('GRANT SELECT ON public.%I TO marketdata_v11_read',view_name);
        END IF;
    END LOOP;
END $$;

GRANT SELECT ON instruments,provider_instruments,alembic_version,ingest_producers,
    ingest_source_owners,ingest_batches,ingest_observations,market_ticks,candle_revisions
    TO marketdata_v11_ingest;
GRANT INSERT ON market_ticks,candle_revisions,ingest_batches,ingest_observations
    TO marketdata_v11_ingest;
GRANT SELECT,INSERT,UPDATE ON market_candles,latest_candles,latest_prices,candle_coverage,
    candle_observation_heads,ingest_candle_heads,ingest_coverage_heads,ingest_source_status,ingest_quote_heads
    TO marketdata_v11_ingest;
GRANT SELECT,INSERT,DELETE ON marketdata_events TO marketdata_v11_ingest;
GRANT SELECT,UPDATE ON marketdata_event_clock TO marketdata_v11_ingest;
GRANT USAGE ON SEQUENCE candle_revisions_id_seq TO marketdata_v11_ingest;
GRANT EXECUTE ON FUNCTION ingest_lock_producer(UUID),ingest_lock_owner(UUID,TEXT,TEXT)
    TO marketdata_v11_ingest;
GRANT EXECUTE ON FUNCTION ingest_admit_request(UUID,TEXT,INTEGER),
    ingest_renew_session(UUID,TEXT,UUID,BIGINT),ingest_recover_session(UUID,TEXT,JSONB)
    TO marketdata_v11_ingest;
-- Neither runtime owns tables, mutates policy/registry, deletes history, or runs DDL.

GRANT SELECT ON ingest_destination_state TO marketdata_v11_ingest;
