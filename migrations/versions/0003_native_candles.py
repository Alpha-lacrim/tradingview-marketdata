"""Direct completed candles, latest candle state and durable gap coverage.

Existing tick history and continuous aggregates are untouched.
"""

from alembic import context, op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE market_candles (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            provider_symbol TEXT NOT NULL,
            interval TEXT NOT NULL CHECK (interval = '1m'),
            bucket_start TIMESTAMPTZ NOT NULL,
            bucket_end TIMESTAMPTZ NOT NULL,
            open NUMERIC NOT NULL, high NUMERIC NOT NULL,
            low NUMERIC NOT NULL, close NUMERIC NOT NULL, volume NUMERIC,
            price_basis TEXT NOT NULL CHECK (price_basis = 'native'),
            realtime_status TEXT NOT NULL, source_status TEXT NOT NULL,
            received_timestamp TIMESTAMPTZ NOT NULL, metadata JSONB NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (instrument_id,provider,interval,bucket_start,price_basis),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider),
            CHECK (bucket_end = bucket_start + INTERVAL '1 minute'),
            CHECK (extract(epoch FROM bucket_start)::numeric % 60 = 0),
            CHECK (bucket_end <= received_timestamp),
            CHECK (low <= open AND low <= close AND high >= open AND high >= close),
            CHECK (volume IS NULL OR volume >= 0)
        );
        CREATE TABLE latest_candles (
            instrument_id UUID NOT NULL, provider TEXT NOT NULL,
            price_basis TEXT NOT NULL, bucket_start TIMESTAMPTZ NOT NULL,
            received_timestamp TIMESTAMPTZ NOT NULL, payload JSONB NOT NULL,
            PRIMARY KEY (instrument_id,provider,price_basis),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE candle_coverage (
            instrument_id UUID NOT NULL, provider TEXT NOT NULL,
            price_basis TEXT NOT NULL,
            coverage_start TIMESTAMPTZ NOT NULL, coverage_end TIMESTAMPTZ NOT NULL,
            missing TSTZMULTIRANGE NOT NULL DEFAULT '{}',
            PRIMARY KEY (instrument_id,provider,price_basis),
            FOREIGN KEY (instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
    """)
    if context.get_x_argument(as_dictionary=True).get("timescale", "false") == "true":
        op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        op.execute("""SELECT create_hypertable('market_candles','bucket_start',
            chunk_time_interval => INTERVAL '7 days', migrate_data => true)""")


def downgrade() -> None:
    # Only the explicitly requested downgrade removes NEW candle data.
    op.execute("DROP TABLE latest_candles, candle_coverage, market_candles")
