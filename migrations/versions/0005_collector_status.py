"""Separate process health; does not alter instruments, history or coverage."""

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE collector_status (
            singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            run_id UUID NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            expires_at TIMESTAMPTZ NOT NULL,
            running BOOLEAN NOT NULL,
            database_healthy BOOLEAN NOT NULL,
            queue_depth INTEGER NOT NULL CHECK (queue_depth >= 0),
            providers JSONB NOT NULL
        );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE collector_status")
