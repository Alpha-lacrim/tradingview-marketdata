"""Optional TimescaleDB hypertable and real-time OHLC aggregates.

Opt in on first upgrade using alembic -x timescale=true upgrade head.
Plain PostgreSQL remains supported with bounded on-demand candles.
"""

from alembic import context, op
from sqlalchemy import text

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None

INTERVALS = {
    "1s": "1 second",
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
    "1d": "1 day",
}


def upgrade() -> None:
    if context.get_x_argument(as_dictionary=True).get("timescale", "false") != "true":
        return
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
    op.execute("""SELECT create_hypertable('market_ticks', 'storage_timestamp',
                  chunk_time_interval => INTERVAL '1 day', migrate_data => true)""")
    for suffix, interval in INTERVALS.items():
        for basis in ("last", "mid"):
            price = "last" if basis == "last" else "((bid + ask) / 2)"
            condition = (
                "last IS NOT NULL" if basis == "last" else "bid IS NOT NULL AND ask IS NOT NULL"
            )
            volume = "sum(last_size)" if basis == "last" else "NULL::numeric"
            view = f"candles_{suffix}_{basis}"
            # first/last require scalar ordering types. A fixed-width signed-offset
            # key preserves nanoseconds, local microseconds and deterministic UUID ties
            # without retaining every price in an array aggregate for a whole day.
            ordering = """lpad((order_ns::numeric+9223372036854775808)::text,20,'0') ||
                lpad(((extract(epoch FROM (received_timestamp AT TIME ZONE 'UTC'))*1000000)
                    ::bigint::numeric+9223372036854775808)::text,20,'0') || tick_id::text"""
            op.execute(f"""
                CREATE MATERIALIZED VIEW {view}
                WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
                SELECT time_bucket(INTERVAL '{interval}', storage_timestamp) AS bucket,
                    instrument_id, provider, realtime_status,
                    first({price}, {ordering}) AS open,
                    max({price}) AS high, min({price}) AS low,
                    last({price}, {ordering}) AS close,
                    {volume} AS volume, count(*) AS observations
                FROM market_ticks WHERE {condition}
                GROUP BY bucket, instrument_id, provider, realtime_status WITH NO DATA
            """)
            op.execute(f"""SELECT add_continuous_aggregate_policy('{view}',
                start_offset => INTERVAL '7 days', end_offset => INTERVAL '{interval}',
                schedule_interval => INTERVAL '1 minute')""")


def downgrade() -> None:
    connection = op.get_bind()
    for suffix in INTERVALS:
        for basis in ("last", "mid"):
            op.execute(f"DROP MATERIALIZED VIEW IF EXISTS candles_{suffix}_{basis} CASCADE")
    # A hypertable cannot be converted in place. Rebuild the ordinary table and indexes.
    if connection.execute(
        text("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')")
    ).scalar():
        is_hyper = connection.execute(
            text("""SELECT EXISTS(SELECT 1 FROM
            timescaledb_information.hypertables WHERE hypertable_name='market_ticks')""")
        ).scalar()
        if is_hyper:
            op.execute("""
                CREATE TABLE market_ticks_plain (LIKE market_ticks INCLUDING ALL);
                INSERT INTO market_ticks_plain SELECT * FROM market_ticks;
                DROP TABLE market_ticks;
                ALTER TABLE market_ticks_plain RENAME TO market_ticks;
                ALTER TABLE market_ticks ADD FOREIGN KEY (instrument_id, provider)
                    REFERENCES provider_instruments(instrument_id, provider);
            """)
