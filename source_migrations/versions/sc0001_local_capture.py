"""Opt-in local capture on base 0006. No destination migration is installed."""

from alembic import op
from sqlalchemy import text

revision = "sc0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    if op.get_bind().execute(text("SELECT version_num FROM alembic_version")).scalar() != "0006":
        raise RuntimeError("source capture requires base 0006; do not install on a destination")
    op.execute("""
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
        SELECT instrument_id,provider,interval,bucket_start,price_basis,max(received_timestamp)
        FROM (
            SELECT instrument_id,provider,interval,bucket_start,price_basis,received_timestamp
                FROM market_candles
            UNION ALL
            SELECT instrument_id,provider,'1m',bucket_start,price_basis,received_timestamp
                FROM latest_candles
        ) observations GROUP BY instrument_id,provider,interval,bucket_start,price_basis;
        CREATE TABLE source_capture_clock (
            singleton BOOLEAN PRIMARY KEY CHECK (singleton),
            source_epoch UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
            head BIGINT NOT NULL DEFAULT 0 CHECK (head>=0),
            candle_ack BIGINT NOT NULL DEFAULT 0 CHECK (candle_ack>=0 AND candle_ack<=head),
            pending_rows BIGINT NOT NULL DEFAULT 0 CHECK (pending_rows>=0),
            pending_bytes BIGINT NOT NULL DEFAULT 0 CHECK (pending_bytes>=0),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );
        INSERT INTO source_capture_clock(singleton) VALUES (true);
        CREATE TABLE source_candle_outbox (
            position BIGINT PRIMARY KEY CHECK (position>0),
            observation_id UUID NOT NULL UNIQUE,
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            record JSONB NOT NULL,
            byte_count INTEGER NOT NULL CHECK (byte_count>0),
            change_kind TEXT NOT NULL CHECK (change_kind IN
                ('candle_completed','candle_corrected','history_seed')),
            captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE source_candle_exports (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            bucket_start TIMESTAMPTZ NOT NULL,
            position BIGINT NOT NULL CHECK (position>0),
            PRIMARY KEY (instrument_id,provider,bucket_start),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE source_quote_state (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            version BIGINT NOT NULL CHECK (version>0),
            acked_version BIGINT NOT NULL DEFAULT 0 CHECK
                (acked_version>=0 AND acked_version<=version),
            fields JSONB NOT NULL,
            record JSONB NOT NULL,
            PRIMARY KEY (instrument_id,provider),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE INDEX source_quote_pending ON source_quote_state(version)
            WHERE acked_version<version;
    """)


def downgrade() -> None:
    raise RuntimeError(
        "capture downgrade discards delivery identities; restore/reconcile explicitly"
    )
