# IMDb Data Provider

Production-ready Python project providing a **high-performance local API** over IMDb non-commercial datasets. Data is ingested from [IMDb datasets](https://datasets.imdbws.com/), converted to Parquet, and queried via DuckDB with a FastAPI layer and optional caching.

## Features

- **Single-container design**: One Docker image runs the FastAPI API and the nightly ingest scheduler; no host cron, systemd, or separate ingest container. The API starts immediately and keeps serving while ingestion runs in the background.
- **Nightly ingestion**: Download only changed TSV.gz files (ETag/Last-Modified), stream to Parquet with DuckDB. Scheduled in-app at 22:00 Asia/Kolkata via APScheduler (FastAPI lifespan).
- **Columnar storage**: Parquet for fast reads; serving tables (e.g. `title_details`) for common queries
- **DuckDB**: SQL views over Parquet, parameterized queries
- **FastAPI**: Health, search, title details, credits, person, series episodes
- **Caching**: In-process TTL + LRU (cachetools); optional Redis; version-based invalidation after ingest; multi-worker safe (version watcher clears caches when dataset changes).
- **Movies & TV**: Supports `movie`, `tvseries`, `tvepisode`, and other title types

## System requirements

- Python 3.10+
- ~10GB+ disk for TSV + Parquet (IMDb datasets are large)
- SSD/NVMe recommended for low-latency queries

---

## Setup

### 1. Clone and create venv

```bash
cd IMDB_DataProvider
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
# or editable install
pip install -e .
```

### 3. Configuration

```bash
cp .env.example .env
# Edit .env if needed (DATA_DIR, LOG_DIR, cache sizes, etc.)
```

### 4. Run ingest (first time)

Download datasets and build Parquet (idempotent; only downloads changed files):

```bash
python -m imdb_service.ingest
```

This will:

- Create `data/tsv/` and download `.tsv.gz` files from IMDb
- Create `data/parquet_builds/YYYYMMDD_HHMMSS/` with Parquet files
- Set `data/current` (symlink or `current_build.txt` on Windows) to the new build
- Write `data/dataset_version.txt` for cache invalidation

Allow 15–45+ minutes depending on network and disk. Subsequent runs only re-download changed files and rebuild Parquet.

### 5. Run the API

**Recommended (multi-worker, in-app scheduler):**

```bash
uvicorn imdb_service.api:app --host 0.0.0.0 --port 8000 --workers 4
```

Single worker (scheduler still runs):

```bash
python -m imdb_service
# or
uvicorn imdb_service.api:app --host 0.0.0.0 --port 8000
```

- Docs: http://localhost:8000/docs  
- Health: http://localhost:8000/health  

---

## Docker Compose deployment (recommended)

One service (`imdb-api`) runs the FastAPI API and the APScheduler-based nightly ingest. The layout uses **separate bind mounts** for performance and safety.

### Exact Compose commands

```bash
# Create volume directories (so they exist and can be chown'd)
mkdir -p volumes/downloads volumes/tmp volumes/parquet_builds volumes/meta volumes/logs

# Optional: run as non-root (container user is 1000:1000 by default)
chown -R 1000:1000 volumes

# Build and start
docker compose up -d --build

# View logs (follow)
docker compose logs -f imdb-api
```

### Where data is stored on the host

All persistent data lives under **`./volumes/`**:

| Host path | Container path | Purpose |
|-----------|----------------|---------|
| `./volumes/downloads` | `/app/data/downloads` | Downloaded `*.tsv.gz` files and `manifest.json` (etag/last-modified) |
| `./volumes/tmp` | `/app/data/tmp` | Staging: intermediate build; atomic promote to parquet_builds |
| `./volumes/parquet_builds` | `/app/data/parquet_builds` | Versioned Parquet builds (`YYYYMMDD_HHMMSS/`) |
| `./volumes/meta` | `/app/data` | Current pointer (`current_build.txt`), `dataset_version.json`, `ingest.lock` (survives restarts) |
| `./volumes/logs` | `/app/logs` | Daily API and ingest logs |
| `./volumes/duckdb` (optional) | `/app/data/duckdb` | Optional persistent DuckDB files; uncomment in `docker-compose.yml` if used |

### Why downloads / tmp / parquet are separate

- **Downloads**: Durability and reuse—downloaded `.tsv.gz` and manifest are kept so incremental ingest only fetches changed files. Isolating them avoids accidental deletion and allows a different mount/disk for large downloads.
- **Tmp (staging)**: Ingest builds Parquet into `tmp` first, then **atomically** moves the completed build to `parquet_builds`. If the process crashes mid-build, only `tmp` is affected; `parquet_builds` and the current pointer never point at half-written data.
- **Parquet builds**: Final output only. The API and `current` pointer reference builds here; separating from tmp and downloads avoids corruption and keeps I/O patterns clear (read-heavy parquet vs write-heavy ingest).

### Environment variables (Compose)

Compose passes (and you can override in `.env` or override file):

- `DATA_DIR=/app/data`
- `DOWNLOAD_DIR=/app/data/downloads`
- `TMP_DIR=/app/data/tmp`
- `PARQUET_DIR=/app/data/parquet_builds`
- `META_DIR=/app/data` (current pointer, version file, lock)
- `LOG_DIR=/app/logs`
- `UPDATE_TIME=22:00`
- `TZ=Asia/Kolkata`
- `ENABLE_AKAS=true` (or `false`)
- **Uvicorn workers**: Default is 4 (set in Dockerfile CMD). To change, override `command` in Compose, e.g. `command: ["uvicorn", "imdb_service.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]`.

### Permissions and ownership

- The image runs as user **1000:1000** (non-root). Ensure host directories under `./volumes/` are writable by that user:
  - `chown -R 1000:1000 ./volumes`
- Or set `PUID` and `PGID` in `.env` and use `user: "${PUID}:${PGID}"` in Compose so the container user matches your host user.

### Healthcheck and port

- The service exposes port **8000** (override with `PORT` in `.env`, e.g. `PORT=9000`).
- Compose configures a **healthcheck** against `GET /health` (interval 30s, 3 retries). Use `docker compose ps` to see health status.

### First run and manual ingest

The first time you start the stack, no Parquet data exists until the first ingest (scheduled at 22:00 Asia/Kolkata or run manually). To populate immediately:

```bash
docker compose run --rm imdb-api python -m imdb_service.ingest
```

Then (re)start the API: `docker compose up -d`.

### Optional: local dev override

`docker-compose.override.yml` is applied automatically when you run `docker compose up`. It can reduce workers, change the port, or mount source for live edits (see comments in the override file).

---

## In-app scheduling (APScheduler in FastAPI lifespan)

Nightly ingest is **scheduled inside the application**, not via host cron or systemd.

- **APScheduler** (AsyncIOScheduler) is started and stopped from FastAPI lifespan (startup/shutdown).
- **Timezone**: Asia/Kolkata.
- **Schedule**: 22:00 (10:00 PM IST) daily via `CronTrigger`.
- **Misfire**: `coalesce=True`, `max_instances=1`, `misfire_grace_time=3600` (1 hour).
- The scheduled job runs the ingest logic in a **thread pool** (`run_in_executor`) so the event loop is never blocked for long.

No separate cron or systemd timer is required when running the API (or the Docker container).

---

## File locking (multi-worker safety)

When running with **multiple Uvicorn workers** (e.g. `--workers 4`), each worker process starts its own scheduler. To ensure only **one** worker runs ingest at a time:

- **Lock file**: `{DATA_DIR}/ingest.lock` (e.g. `data/ingest.lock`).
- **Mechanism**: `filelock` library (cross-process, works across Uvicorn workers in the same container).
- **Acquisition**: **Non-blocking**. If the lock is held, the job logs *"ingest skipped due to lock (PID=…)"* and returns; it does not wait.
- **Stale lock**: If the lock file is older than `INGEST_LOCK_STALE_SECONDS` (default 6 hours), the process may attempt a takeover (remove stale file and acquire). If the lock is still held or takeover fails, it skips with a clear log. This avoids silent deadlock if a previous worker crashed without releasing.

Only the worker that acquires the lock runs the full ingest; others skip. All workers continue to serve API requests.

---

## Safe multi-worker mode: cache invalidation and dataset versioning

With multiple workers, each process has its own **in-process caches**. When one worker completes ingest and updates the dataset, other workers must invalidate their caches and use the new data.

- **Dataset version file**: After a successful ingest and **atomic** switch of `data/current`, the ingest worker writes `{DATA_DIR}/dataset_version.json` with:
  - `version_id`: monotonically increasing integer
  - `build_id`: parquet build folder name (e.g. `20260207_093159`)
  - `timestamp`: ISO build time
  - `source`: etag/last-modified metadata for the downloaded files

- **Version watcher** (per worker):
  - Each worker runs a lightweight background task that **polls** `dataset_version.json` every `VERSION_POLL_INTERVAL` seconds (default 15).
  - When the file’s content/mtime indicates a **version change**, the worker:
    - Clears its local caches
    - Reloads DuckDB view pointers (next request uses `get_connection()` which reads `data/current` from disk, so no persistent connection to “reload”; clearing caches is enough)
    - Logs: *"worker PID=… detected version change; caches cleared; views reloaded"*

- **Order of operations**: Ingest builds Parquet into `data/parquet_builds/<build_id>/`, then **atomically** switches `data/current` (symlink or `current_build.txt`), then writes `dataset_version.json` **last**. So other workers only see the new version after the new data is ready.

**Limitation**: Cache invalidation is driven by polling. After an ingest completes, other workers may continue serving from their cache for up to **VERSION_POLL_INTERVAL** seconds (e.g. 15s) until they detect the version change. This is acceptable for a home server.

---

## Optional: manual ingest and legacy cron/systemd

You can still run ingest manually (e.g. to backfill before first schedule):

```bash
python -m imdb_service.ingest
```

This uses the same file lock: if another process (or the scheduler) holds the lock, the CLI run will skip. For **host-based scheduling** (without using the in-app scheduler), see `examples/cron/` and `examples/systemd/` for legacy cron and systemd timer examples.

---

## Example API calls (curl)

**Health**

```bash
curl -s http://localhost:8000/health
```

**Search**

```bash
curl -s "http://localhost:8000/search?q=inception&type=movie&limit=5"
curl -s "http://localhost:8000/search?q=breaking+bad&type=tvseries&limit=10"
```

**Title by id**

```bash
curl -s "http://localhost:8000/title/tt1375666"
curl -s "http://localhost:8000/title/tt0903747?include_episodes=true"
```

**Credits**

```bash
curl -s "http://localhost:8000/title/tt1375666/credits?limit=20"
curl -s "http://localhost:8000/title/tt1375666/credits?category=director"
```

**Person**

```bash
curl -s "http://localhost:8000/person/nm0000138"
```

**Series episodes**

```bash
curl -s "http://localhost:8000/series/tt0903747/episodes?limit=50"
curl -s "http://localhost:8000/series/tt0903747/episodes?season=1&limit=20&offset=0"
```

---

## Project layout

```
imdb_service/
  __init__.py
  __main__.py      # run API: python -m imdb_service
  api.py           # FastAPI app + lifespan (scheduler, version watcher)
  cache.py         # Per-worker TTL+LRU cache; clear on version change
  config.py        # pydantic-settings, .env; DOWNLOAD_DIR, TMP_DIR, PARQUET_DIR, META_DIR
  db.py            # DuckDB views and parameterized queries
  ingest.py        # Ingest: build in TMP_DIR, promote to PARQUET_DIR, file lock
  models.py        # Pydantic response schemas
  scheduler.py     # APScheduler (AsyncIOScheduler), 22:00 Asia/Kolkata
  versioning.py    # dataset_version.json read/write, version watcher
  utils.py         # Logging, null/list parsing
docker/
  entrypoint.sh    # Creates dirs under mounted volumes; exec uvicorn
docker-compose.yml
docker-compose.override.yml   # Optional local dev
Dockerfile
volumes/           # Host bind mounts (create and chown 1000:1000)
  downloads/       # *.tsv.gz + manifest.json
  tmp/             # Staging; atomic promote to parquet_builds
  parquet_builds/  # Versioned Parquet builds
  meta/            # current_build.txt, dataset_version.json, ingest.lock (mounted as /app/data)
  logs/            # Daily logs
  duckdb/          # Optional
```

---

## Performance tips

### Why Parquet + DuckDB?

- **Parquet** is columnar: only needed columns are read, which reduces I/O and speeds up filters and aggregations. IMDb TSV is row-oriented and large; converting once to Parquet makes repeated queries much faster.
- **DuckDB** is an in-process analytical engine that can query Parquet directly with SQL, with predicate pushdown and efficient scans. No separate database server is required, which fits a home-server setup.

### SSD/NVMe recommendation

Parquet and DuckDB benefit from low-latency storage. Putting `DATA_DIR` (and if possible `LOG_DIR`) on an SSD or NVMe drive reduces query latency and ingest time compared to spinning disks.

### How caching works

- **In-process (default)**: cachetools `TTLCache` is used for title lookups (by `tconst`), person lookups (by `nconst`), and search results. Each cache has a TTL (e.g. 6h for title/person, 30m for search) and a max size (e.g. 50k / 20k entries). LRU eviction applies when the cache is full.
- **Invalidation**: The cache key includes the current **dataset version** (from `data/dataset_version.json`). When the version file changes (after a successful ingest), each worker’s **version watcher** clears that worker’s local caches so the next request uses the new data.
- **Optional Redis**: If `REDIS_URL` is set, the same keys (with prefix) and TTLs are used in Redis so that multiple API processes or machines can share the cache.

### Scaling concurrency (uvicorn workers) on a home server

- **Recommended**: `uvicorn imdb_service.api:app --host 0.0.0.0 --port 8000 --workers 4`. Each worker runs the scheduler and the version watcher; only one worker acquires the ingest lock and runs the nightly job. All workers serve requests; when ingest completes, the version watcher in each worker clears caches within about `VERSION_POLL_INTERVAL` seconds.
- **Single worker**: One process; scheduler and watcher still run. No lock contention.
- **Multiple workers**: DuckDB is in-process per request (no long-lived connection); each worker has its own in-memory caches. Caches are cleared on version change via the version watcher.

---

## Observability and safety

- **Logging**: Loguru logs to console and to daily files under `LOG_DIR`. Look for: *"scheduler started in PID=…"*, *"ingest lock acquired by PID=…"*, *"ingest skipped due to lock (PID=…)"*, *"dataset version updated to …"*, *"worker PID=… detected version change; caches cleared; views reloaded"*.
- **Read-consistency during ingest**: Ingest builds Parquet into a **new** versioned folder (`data/parquet_builds/<build_id>/`). Only after all files are written and validated does it **atomically** switch `data/current` (symlink or `current_build.txt`), then write `dataset_version.json`. If ingest fails at any step, the API continues serving the previous `data/current`; the new build is never pointed to until it is complete.
- **API**: If no data is loaded (e.g. ingest never run), `/health` reports `data_loaded: false` and other endpoints return 503 until data is available.

---

## License

Use of IMDb datasets is subject to [IMDb’s non-commercial licensing terms](https://www.imdb.com/conditions). This project is for non-commercial use only.
