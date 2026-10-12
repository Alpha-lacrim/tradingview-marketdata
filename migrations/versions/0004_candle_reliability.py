"""Session-aware coverage and durable correction provenance; preserves candles/ticks."""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE candle_coverage ADD COLUMN not_expected TSTZMULTIRANGE NOT NULL DEFAULT '{}';
        ALTER TABLE candle_coverage ADD COLUMN unknown TSTZMULTIRANGE NOT NULL DEFAULT '{}';
        ALTER TABLE candle_coverage ADD COLUMN session_info JSONB NOT NULL DEFAULT '{}';
        -- Previous gaps used a blanket calendar: uncertainty must not masquerade as loss.
        UPDATE candle_coverage SET unknown=missing,missing='{}';
        CREATE TABLE candle_revisions (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            instrument_id UUID NOT NULL, provider TEXT NOT NULL,
            bucket_start TIMESTAMPTZ NOT NULL, changed_fields TEXT[] NOT NULL,
            old_values JSONB NOT NULL, new_values JSONB NOT NULL,
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            FOREIGN KEY(instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE INDEX ON candle_revisions(instrument_id,provider,bucket_start);
    """)


def downgrade() -> None:
    op.execute("""
        UPDATE candle_coverage SET missing=missing+unknown;
        ALTER TABLE candle_coverage DROP COLUMN not_expected, DROP COLUMN unknown,
            DROP COLUMN session_info;
        DROP TABLE candle_revisions;
    """)
