"""Fenced coalesced quote ordering and bounded financial reconciliation."""

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ingest_quote_heads (
            instrument_id UUID NOT NULL,
            provider TEXT NOT NULL,
            producer_id UUID NOT NULL REFERENCES ingest_producers(producer_id),
            source_epoch UUID NOT NULL,
            ownership_generation BIGINT NOT NULL CHECK (ownership_generation>0),
            source_position BIGINT NOT NULL CHECK (source_position>0),
            observation_id UUID NOT NULL,
            observation_sha256 BYTEA NOT NULL CHECK (octet_length(observation_sha256)=32),
            payload JSONB NOT NULL,
            PRIMARY KEY(instrument_id,provider),
            FOREIGN KEY(instrument_id,provider)
                REFERENCES provider_instruments(instrument_id,provider)
        );
        CREATE TABLE ingest_destination_state (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
            destination_id UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        );
        INSERT INTO ingest_destination_state(singleton) VALUES(TRUE);
        CREATE INDEX ingest_observations_producer_position ON ingest_observations
            (producer_id,source_epoch,source_position DESC);
    """)


def downgrade() -> None:
    op.execute("""
        DROP INDEX ingest_observations_producer_position;
        DROP TABLE ingest_destination_state;
        DROP TABLE ingest_quote_heads;
    """)
