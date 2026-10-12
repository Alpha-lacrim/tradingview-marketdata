"""Canonical registry, idempotent tick history and provider-isolated latest state."""

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
    CREATE TABLE instruments (
        id UUID PRIMARY KEY,
        canonical_symbol TEXT NOT NULL UNIQUE,
        display_name TEXT NOT NULL,
        asset_class TEXT NOT NULL,
        instrument_type TEXT NOT NULL,
        exchange TEXT NOT NULL,
        currency TEXT NOT NULL,
        base_asset TEXT,
        quote_asset TEXT,
        root_symbol TEXT,
        contract_code TEXT,
        contract_month DATE,
        expiry TIMESTAMPTZ,
        roll_at TIMESTAMPTZ,
        tick_size NUMERIC,
        multiplier NUMERIC,
        active BOOLEAN NOT NULL,
        metadata JSONB NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        CHECK (instrument_type <> 'future' OR
            (root_symbol IS NOT NULL AND contract_code IS NOT NULL
             AND contract_month IS NOT NULL AND expiry IS NOT NULL))
    );
    CREATE TABLE provider_instruments (
        instrument_id UUID NOT NULL REFERENCES instruments(id),
        provider TEXT NOT NULL,
        provider_symbol TEXT NOT NULL,
        realtime_status TEXT NOT NULL,
        data_delay_seconds INTEGER CHECK(data_delay_seconds >= 0),
        metadata JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (instrument_id, provider),
        UNIQUE (provider, provider_symbol)
    );
    CREATE TABLE market_ticks (
        storage_timestamp TIMESTAMPTZ NOT NULL,
        tick_id UUID NOT NULL,
        instrument_id UUID NOT NULL,
        provider TEXT NOT NULL,
        provider_symbol TEXT NOT NULL,
        realtime_status TEXT NOT NULL,
        event_timestamp TIMESTAMPTZ,
        event_timestamp_ns BIGINT,
        received_timestamp TIMESTAMPTZ NOT NULL,
        order_ns BIGINT NOT NULL,
        bid NUMERIC,
        ask NUMERIC,
        last NUMERIC,
        bid_size NUMERIC,
        ask_size NUMERIC,
        last_size NUMERIC,
        volume NUMERIC,
        sequence_number BIGINT,
        metadata JSONB NOT NULL,
        payload JSONB NOT NULL,
        PRIMARY KEY (storage_timestamp, tick_id),
        FOREIGN KEY (instrument_id, provider)
            REFERENCES provider_instruments(instrument_id, provider),
        CHECK (bid IS NOT NULL OR ask IS NOT NULL OR last IS NOT NULL)
    );
    CREATE INDEX market_ticks_lookup ON market_ticks
        (instrument_id, provider, storage_timestamp DESC, tick_id DESC);
    CREATE TABLE latest_prices (
        instrument_id UUID NOT NULL,
        provider TEXT NOT NULL,
        order_ns BIGINT NOT NULL,
        received_timestamp TIMESTAMPTZ NOT NULL,
        tick_id UUID NOT NULL,
        payload JSONB NOT NULL,
        PRIMARY KEY (instrument_id, provider),
        FOREIGN KEY (instrument_id, provider)
            REFERENCES provider_instruments(instrument_id, provider)
    );
    """)


def downgrade() -> None:
    op.execute("DROP TABLE latest_prices, market_ticks, provider_instruments, instruments")
