"""Narrow session renewal, cross-process admission budgets and read status projection."""

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ingest_request_budgets (
            producer_id UUID PRIMARY KEY REFERENCES ingest_producers(producer_id),
            window_start TIMESTAMPTZ NOT NULL,
            requests INTEGER NOT NULL CHECK (requests > 0)
        );
        CREATE FUNCTION ingest_admit_request(UUID,TEXT,INTEGER) RETURNS BOOLEAN
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
        DECLARE used INTEGER; window_time TIMESTAMPTZ;
        BEGIN
            IF $3 < 1 OR $3 > 10000 THEN RAISE EXCEPTION 'invalid request budget'; END IF;
            PERFORM 1 FROM public.ingest_producers
                WHERE producer_id=$1 AND credential_key_id=$2 AND enabled FOR SHARE;
            IF NOT FOUND THEN RETURN NULL; END IF;
            window_time := date_trunc('minute',clock_timestamp());
            INSERT INTO public.ingest_request_budgets AS b VALUES ($1,window_time,1)
                ON CONFLICT (producer_id) DO UPDATE SET
                    window_start=EXCLUDED.window_start,
                    requests=CASE WHEN b.window_start=EXCLUDED.window_start
                        THEN b.requests+1 ELSE 1 END
                WHERE b.window_start<>EXCLUDED.window_start OR b.requests < $3
                RETURNING requests INTO used;
            RETURN used IS NOT NULL;
        END $$;
        CREATE FUNCTION ingest_renew_session(UUID,TEXT,UUID,BIGINT)
            RETURNS SETOF public.ingest_producers
            LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
        BEGIN
            -- Serialize renewal with uploads, revocation and administrator handover.
            PERFORM 1 FROM public.ingest_producers WHERE producer_id=$1 FOR UPDATE;
            RETURN QUERY UPDATE public.ingest_producers SET
                session_expires_at=GREATEST(session_expires_at,
                    clock_timestamp()+interval '120 seconds')
                WHERE producer_id=$1 AND credential_key_id=$2 AND enabled
                    AND session_id=$3 AND session_generation=$4
                    AND session_expires_at>clock_timestamp() RETURNING *;
        END $$;
        REVOKE ALL ON FUNCTION ingest_admit_request(UUID,TEXT,INTEGER) FROM PUBLIC;
        REVOKE ALL ON FUNCTION ingest_renew_session(UUID,TEXT,UUID,BIGINT) FROM PUBLIC;
        -- The reader sees captured health, not credentials or writable policy tables.
        CREATE VIEW ingest_read_status AS
            SELECT i.canonical_symbol,m.provider_symbol,o.provider,
                s.captured_at,s.expires_at,s.payload,
                COALESCE(s.expires_at>clock_timestamp() AND p.enabled
                    AND p.session_expires_at>clock_timestamp()
                    AND (s.session_id,s.session_generation)=(p.session_id,p.session_generation)
                    AND (s.producer_id,s.source_epoch,s.ownership_generation)=
                        (o.producer_id,o.source_epoch,o.ownership_generation),FALSE) AS fresh
            FROM public.ingest_source_owners o
            JOIN public.instruments i ON i.id=o.instrument_id
            JOIN public.provider_instruments m
                ON (m.instrument_id,m.provider)=(o.instrument_id,o.provider)
            LEFT JOIN public.ingest_source_status s
                ON (s.instrument_id,s.provider)=(o.instrument_id,o.provider)
            LEFT JOIN public.ingest_producers p ON p.producer_id=s.producer_id
            WHERE o.stream='status';
    """)


def downgrade() -> None:
    op.execute("""
        DROP VIEW ingest_read_status;
        DROP FUNCTION ingest_renew_session(UUID,TEXT,UUID,BIGINT);
        DROP FUNCTION ingest_admit_request(UUID,TEXT,INTEGER);
        DROP TABLE ingest_request_budgets;
    """)
