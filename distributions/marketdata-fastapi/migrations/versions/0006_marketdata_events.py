"""Bounded committed observation journal; market history is unchanged."""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE marketdata_event_clock (
            singleton BOOLEAN PRIMARY KEY CHECK (singleton),
            epoch UUID NOT NULL DEFAULT gen_random_uuid(),
            head BIGINT NOT NULL DEFAULT 0 CHECK (head >= 0),
            floor BIGINT NOT NULL DEFAULT 0 CHECK (floor >= 0 AND floor <= head)
        );
        INSERT INTO marketdata_event_clock(singleton) VALUES (true);
        CREATE TABLE marketdata_events (
            id BIGINT PRIMARY KEY,
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            canonical_symbol TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN
                ('price_update','candle_completed','candle_corrected')),
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            payload JSONB NOT NULL,
            FOREIGN KEY(instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE marketdata_events, marketdata_event_clock")
