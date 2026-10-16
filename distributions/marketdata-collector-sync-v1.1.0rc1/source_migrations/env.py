"""Independent source extension; never traverses destination revisions 0007–0009."""

from alembic import context
from sqlalchemy import create_engine, pool

from marketdata.config import Settings

url = (
    Settings().database_url.get_secret_value().replace("postgresql://", "postgresql+psycopg://", 1)
)
url = url.replace("postgres://", "postgresql+psycopg://", 1)
if context.is_offline_mode():
    raise RuntimeError("source extension requires an online base-schema compatibility check")
engine = create_engine(url, poolclass=pool.NullPool)
with engine.connect() as connection:
    context.configure(connection=connection, version_table="source_capture_version")
    with context.begin_transaction():
        context.run_migrations()
