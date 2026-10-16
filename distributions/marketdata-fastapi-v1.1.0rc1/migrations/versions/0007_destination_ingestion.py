"""Destination receipts, fenced source ownership and observation ordering.

No source outbox or collector deployment changes. Existing market/event tables survive.
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ingest_producers (
            producer_id UUID PRIMARY KEY,
            label TEXT NOT NULL,
            credential_key_id TEXT NOT NULL UNIQUE,
            enabled BOOLEAN NOT NULL DEFAULT TRUE,
            session_id UUID NOT NULL,
            session_generation BIGINT NOT NULL CHECK (session_generation > 0),
            session_expires_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE ingest_source_owners (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            stream TEXT NOT NULL CHECK (stream IN ('candles','quotes','coverage','status')),
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            source_epoch UUID NOT NULL,
            ownership_generation BIGINT NOT NULL CHECK (ownership_generation > 0),
            allowed_statuses TEXT[] NOT NULL CHECK (cardinality(allowed_statuses) > 0 AND
                allowed_statuses <@ ARRAY['realtime','delayed','indicative','simulated',
                    'end_of_day','unknown']::text[]),
            PRIMARY KEY (instrument_id,provider,stream),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE INDEX ingest_source_owners_producer ON ingest_source_owners(producer_id);
        -- Row-locking SELECT requires UPDATE privilege. These narrow functions allow
        -- runtime fencing without granting mutation of producer/ownership policy.
        CREATE FUNCTION ingest_lock_producer(UUID) RETURNS SETOF public.ingest_producers
            LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
            SELECT * FROM public.ingest_producers WHERE producer_id=$1 FOR SHARE
        $$;
        CREATE FUNCTION ingest_lock_owner(UUID,TEXT,TEXT)
            RETURNS SETOF public.ingest_source_owners
            LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
            SELECT * FROM public.ingest_source_owners
                WHERE instrument_id=$1 AND provider=$2 AND stream=$3 FOR SHARE
        $$;
        REVOKE ALL ON FUNCTION ingest_lock_producer(UUID) FROM PUBLIC;
        REVOKE ALL ON FUNCTION ingest_lock_owner(UUID,TEXT,TEXT) FROM PUBLIC;
        CREATE FUNCTION enforce_ingest_fence() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_TABLE_NAME = 'ingest_producers' THEN
                IF NEW.session_generation < OLD.session_generation OR
                    (NEW.session_id <> OLD.session_id AND
                        NEW.session_generation <= OLD.session_generation) OR
                    (OLD.session_expires_at <= clock_timestamp() AND
                        NEW.session_expires_at > OLD.session_expires_at AND
                        NEW.session_generation <= OLD.session_generation) THEN
                    RAISE EXCEPTION 'producer session fence must increase' USING ERRCODE='23514';
                END IF;
            ELSE
                IF NEW.ownership_generation < OLD.ownership_generation OR
                    ((NEW.producer_id,NEW.source_epoch) IS DISTINCT FROM
                        (OLD.producer_id,OLD.source_epoch) AND
                        NEW.ownership_generation <= OLD.ownership_generation) THEN
                    RAISE EXCEPTION 'source ownership fence must increase' USING ERRCODE='23514';
                END IF;
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER ingest_producer_fence BEFORE UPDATE ON ingest_producers
            FOR EACH ROW EXECUTE FUNCTION enforce_ingest_fence();
        CREATE TRIGGER ingest_owner_fence BEFORE UPDATE ON ingest_source_owners
            FOR EACH ROW EXECUTE FUNCTION enforce_ingest_fence();
        CREATE TABLE ingest_batches (
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            batch_id UUID NOT NULL,
            source_epoch UUID NOT NULL,
            stream TEXT NOT NULL CHECK (stream IN ('candles','quotes','coverage','status')),
            payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256)=32),
            receipt JSONB NOT NULL CHECK (receipt->>'state'='committed'),
            server_ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (producer_id,batch_id)
        );
        CREATE TABLE ingest_observations (
            producer_id UUID NOT NULL,
            source_epoch UUID NOT NULL,
            observation_id UUID NOT NULL,
            batch_id UUID NOT NULL,
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            stream TEXT NOT NULL CHECK (stream IN ('candles','quotes','coverage','status')),
            source_position BIGINT NOT NULL CHECK (source_position > 0),
            ownership_generation BIGINT NOT NULL CHECK (ownership_generation > 0),
            observation_sha256 BYTEA NOT NULL CHECK (octet_length(observation_sha256)=32),
            payload JSONB NOT NULL,
            outcome TEXT NOT NULL CHECK (outcome IN ('applied','recorded_no_change','superseded')),
            server_ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (producer_id,source_epoch,observation_id),
            FOREIGN KEY (producer_id,batch_id) REFERENCES ingest_batches(producer_id,batch_id)
                DEFERRABLE INITIALLY DEFERRED,
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE INDEX ingest_observations_source ON ingest_observations
            (instrument_id,provider,stream,source_position);
        CREATE TABLE ingest_candle_heads (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            interval TEXT NOT NULL CHECK (interval='1m'),
            bucket_start TIMESTAMPTZ NOT NULL,
            price_basis TEXT NOT NULL CHECK (price_basis='native'),
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            source_epoch UUID NOT NULL,
            ownership_generation BIGINT NOT NULL CHECK (ownership_generation > 0),
            source_position BIGINT NOT NULL CHECK (source_position > 0),
            observation_sha256 BYTEA NOT NULL CHECK (octet_length(observation_sha256)=32),
            PRIMARY KEY (instrument_id,provider,interval,bucket_start,price_basis),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE ingest_coverage_heads (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            source_epoch UUID NOT NULL,
            ownership_generation BIGINT NOT NULL CHECK (ownership_generation > 0),
            source_position BIGINT NOT NULL CHECK (source_position > 0),
            observation_sha256 BYTEA NOT NULL CHECK (octet_length(observation_sha256)=32),
            PRIMARY KEY (instrument_id,provider),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE ingest_source_status (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            source_epoch UUID NOT NULL,
            session_id UUID NOT NULL,
            session_generation BIGINT NOT NULL CHECK (session_generation > 0),
            ownership_generation BIGINT NOT NULL CHECK (ownership_generation > 0),
            source_position BIGINT NOT NULL CHECK (source_position > 0),
            observation_sha256 BYTEA NOT NULL CHECK (octet_length(observation_sha256)=32),
            captured_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at > captured_at),
            payload JSONB NOT NULL,
            server_ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (instrument_id,provider),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE candle_observation_heads (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            interval TEXT NOT NULL CHECK (interval='1m'),
            bucket_start TIMESTAMPTZ NOT NULL,
            price_basis TEXT NOT NULL CHECK (price_basis='native'),
            received_timestamp TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (instrument_id,provider,interval,bucket_start,price_basis),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        INSERT INTO candle_observation_heads
            (instrument_id,provider,interval,bucket_start,price_basis,received_timestamp)
        SELECT instrument_id,provider,interval,bucket_start,price_basis,max(received_timestamp)
        FROM (
            SELECT instrument_id,provider,interval,bucket_start,price_basis,received_timestamp
                FROM market_candles
            UNION ALL
            SELECT instrument_id,provider,'1m',bucket_start,price_basis,received_timestamp
                FROM latest_candles
        ) observations GROUP BY instrument_id,provider,interval,bucket_start,price_basis;
    """)


def downgrade() -> None:
    # Market history, coverage, revisions, latest state and event cursors are untouched.
    op.execute("""
        DROP FUNCTION ingest_lock_producer(UUID), ingest_lock_owner(UUID,TEXT,TEXT);
        DROP TABLE candle_observation_heads, ingest_source_status, ingest_coverage_heads,
            ingest_candle_heads, ingest_observations, ingest_batches, ingest_source_owners,
            ingest_producers;
        DROP FUNCTION enforce_ingest_fence();
    """)
