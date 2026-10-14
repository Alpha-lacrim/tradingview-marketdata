FROM python:3.12-slim AS api-base
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv pip install --no-cache-dir uv==0.11.27 && uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser
ENV APPLICATION_ROLE=api
EXPOSE 8000
CMD [".venv/bin/uvicorn", "marketdata.api:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--ws", "websockets", "--ws-max-size", "65536", "--ws-max-queue", "16"]

# Explicit administrative tooling; never used by the API service.
FROM api-base AS tools
USER root
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --extra migrations
COPY migrations ./migrations
COPY alembic.ini ./
RUN chown -R appuser:appuser /app
USER appuser

FROM api-base AS runtime
