FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv pip install --no-cache-dir uv==0.11.27 && uv sync --frozen --no-dev --extra databento --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra databento
COPY migrations ./migrations
COPY alembic.ini ./
COPY config ./config
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8000
CMD [".venv/bin/uvicorn", "marketdata.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--ws", "websockets", "--ws-max-size", "65536", "--ws-max-queue", "16"]
