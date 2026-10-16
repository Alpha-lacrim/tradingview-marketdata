ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
COPY requirements.lock ./
ARG PACKAGE_INDEX=https://pypi.org/simple
RUN pip install --no-cache-dir --require-hashes --index-url "${PACKAGE_INDEX}" --timeout 60 --retries 2 -r requirements.lock
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini RELEASE.json ./
COPY scripts ./scripts
RUN useradd --uid 10001 --create-home appuser && chown -R appuser:appuser /app
USER appuser
HEALTHCHECK --interval=15s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/livez',timeout=3)"
ENTRYPOINT ["python", "-m", "marketdata.release_runtime"]
CMD ["read"]
