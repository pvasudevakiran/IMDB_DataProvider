# Single-container: FastAPI API + APScheduler nightly ingest
# Performance-oriented volume layout: separate mounts for downloads, tmp, parquet, meta, logs

FROM python:3.12-slim

WORKDIR /app

# Non-root user (UID/GID 1000); host volumes should be writable by this user
RUN groupadd -r app --gid=1000 && useradd -r -g app --uid=1000 app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Entrypoint creates required dirs under mounted volumes
RUN chmod +x /app/docker/entrypoint.sh 2>/dev/null || true

ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/app/data
ENV LOG_DIR=/app/logs

# Default env (override in Compose)
ENV DOWNLOAD_DIR=/app/data/downloads
ENV TMP_DIR=/app/data/tmp
ENV PARQUET_DIR=/app/data/parquet_builds
ENV META_DIR=/app/data

EXPOSE 11100

USER app
ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["uvicorn", "imdb_service.api:app", "--host", "0.0.0.0", "--port", "11100", "--workers", "4"]
