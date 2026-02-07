"""
Nightly ingestion: download IMDb TSV.gz, convert to Parquet, atomic symlink.
Run: python -m imdb_service.ingest (CLI) or via scheduler (run_ingest_with_lock).
"""

import gzip
import json
import os
import shutil
import sys
import time
from pathlib import Path

import duckdb
import httpx
from filelock import FileLock, Timeout
from loguru import logger

from imdb_service.config import get_settings
from imdb_service.utils import setup_logging
from imdb_service.versioning import write_version_file

# Files to fetch (without .tsv.gz)
IMDB_FILES = [
    "title.basics",
    "title.ratings",
    "title.principals",
    "title.crew",
    "title.episode",
    "name.basics",
]
IMDB_AKAS = "title.akas"


def load_manifest(manifest_path: Path) -> dict:
    if manifest_path.exists():
        try:
            return json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"files": {}}


def save_manifest(manifest_path: Path, data: dict) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = manifest_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(manifest_path)


def head_metadata(client: httpx.Client, url: str) -> tuple[str | None, str | None]:
    try:
        r = client.head(url, follow_redirects=True, timeout=30)
        r.raise_for_status()
        etag = r.headers.get("etag") or r.headers.get("ETag")
        lm = r.headers.get("last-modified") or r.headers.get("Last-Modified")
        return (etag.strip('"') if etag else None, lm.strip() if lm else None)
    except Exception as e:
        logger.warning(f"HEAD {url} failed: {e}")
        return (None, None)


def need_download(manifest: dict, filename: str, etag: str | None, last_modified: str | None) -> bool:
    entry = manifest.get("files", {}).get(filename, {})
    if not entry:
        return True
    if etag and entry.get("etag") == etag:
        return False
    if last_modified and entry.get("last_modified") == last_modified:
        return False
    return True


def download_file(client: httpx.Client, url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    try:
        with client.stream("GET", url, follow_redirects=True, timeout=120) as resp:
            resp.raise_for_status()
            size = 0
            with open(tmp, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)
                    size += len(chunk)
        if size == 0:
            raise ValueError("Downloaded file size is 0")
        tmp.replace(dest)
        logger.info(f"Downloaded {dest.name}: {size / (1024*1024):.2f} MB")
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def verify_gzip(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as f:
            f.read(1)
        return True
    except Exception as e:
        logger.error(f"Gzip verify failed for {path}: {e}")
        return False


def run_downloads(settings) -> dict[str, dict]:
    base_url = settings.IMDB_BASE_URL.rstrip("/")
    manifest_path = settings.get_manifest_path()
    tsv_dir = settings.get_tsv_dir()
    manifest = load_manifest(manifest_path)
    updated = dict(manifest.get("files", {}))

    with httpx.Client() as client:
        for name in IMDB_FILES:
            fname = f"{name}.tsv.gz"
            url = f"{base_url}/{fname}"
            etag, last_modified = head_metadata(client, url)
            if need_download(manifest, fname, etag, last_modified):
                logger.info(f"Downloading {fname} ...")
                dest = tsv_dir / fname
                download_file(client, url, dest)
                if not verify_gzip(dest):
                    raise RuntimeError(f"Gzip integrity check failed: {dest}")
                updated[fname] = {"etag": etag, "last_modified": last_modified}
            else:
                logger.info(f"Skip (unchanged): {fname}")

        if settings.ENABLE_AKAS:
            fname = f"{IMDB_AKAS}.tsv.gz"
            url = f"{base_url}/{fname}"
            etag, last_modified = head_metadata(client, url)
            if need_download(manifest, fname, etag, last_modified):
                logger.info(f"Downloading {fname} ...")
                dest = tsv_dir / fname
                download_file(client, url, dest)
                if not verify_gzip(dest):
                    raise RuntimeError(f"Gzip integrity check failed: {dest}")
                updated[fname] = {"etag": etag, "last_modified": last_modified}
            else:
                logger.info(f"Skip (unchanged): {fname}")

    save_manifest(manifest_path, {"files": updated})
    return updated


def _tsv_path(tsv_dir: Path, base: str) -> str:
    return (tsv_dir / f"{base}.tsv.gz").as_posix()


def convert_to_parquet(settings, build_dir: Path) -> None:
    tsv_dir = settings.get_tsv_dir()
    conn = duckdb.connect(":memory:")

    # Common CSV read options for IMDb TSV
    opts = "delim='\\t', header=true, nullstr='\\\\N', compression='gzip'"

    def read_table(name: str) -> str:
        path = _tsv_path(tsv_dir, name)
        if not Path(path).exists():
            raise FileNotFoundError(path)
        return f"read_csv_auto('{path}', {opts})"

    # --- title.basics ---
    logger.info("Converting title.basics ...")
    conn.execute(f"""
        CREATE TABLE basics AS
        SELECT
            tconst,
            titleType,
            primaryTitle,
            originalTitle,
            CAST(COALESCE("isAdult", '0') AS VARCHAR) IN ('1', 'true', 'True') AS isAdult,
            try_cast("startYear" AS INTEGER) AS startYear,
            try_cast("endYear" AS INTEGER) AS endYear,
            try_cast("runtimeMinutes" AS INTEGER) AS runtimeMinutes,
            list_transform(
                list_filter(split(COALESCE(genres, ''), ','), x -> length(trim(x)) > 0),
                x -> trim(x)
            ) AS genres
        FROM {read_table('title.basics')}
    """)
    conn.execute(f"COPY basics TO '{(build_dir / "title_basics.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE basics")
    logger.info("title_basics.parquet written")

    # --- title.ratings ---
    logger.info("Converting title.ratings ...")
    conn.execute(f"""
        CREATE TABLE ratings AS
        SELECT
            tconst,
            try_cast(averageRating AS DOUBLE) AS averageRating,
            try_cast(numVotes AS INTEGER) AS numVotes
        FROM {read_table('title.ratings')}
    """)
    conn.execute(f"COPY ratings TO '{(build_dir / "title_ratings.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE ratings")
    logger.info("title_ratings.parquet written")

    # --- title.principals ---
    logger.info("Converting title.principals ...")
    conn.execute(f"""
        CREATE TABLE principals AS
        SELECT
            tconst,
            try_cast(ordering AS INTEGER) AS ordering,
            nconst,
            category,
            job,
            characters
        FROM {read_table('title.principals')}
    """)
    conn.execute(f"COPY principals TO '{(build_dir / "title_principals.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE principals")
    logger.info("title_principals.parquet written")

    # --- title.crew ---
    logger.info("Converting title.crew ...")
    conn.execute(f"""
        CREATE TABLE crew AS
        SELECT
            tconst,
            directors,
            writers
        FROM {read_table('title.crew')}
    """)
    conn.execute(f"COPY crew TO '{(build_dir / "title_crew.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE crew")
    logger.info("title_crew.parquet written")

    # --- title.episode ---
    logger.info("Converting title.episode ...")
    conn.execute(f"""
        CREATE TABLE episode AS
        SELECT
            tconst,
            parentTconst,
            try_cast("seasonNumber" AS INTEGER) AS "seasonNumber",
            try_cast("episodeNumber" AS INTEGER) AS "episodeNumber"
        FROM {read_table('title.episode')}
    """)
    conn.execute(f"COPY episode TO '{(build_dir / "title_episode.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE episode")
    logger.info("title_episode.parquet written")

    # --- name.basics ---
    logger.info("Converting name.basics ...")
    conn.execute(f"""
        CREATE TABLE names AS
        SELECT
            nconst,
            primaryName,
            try_cast("birthYear" AS INTEGER) AS "birthYear",
            try_cast("deathYear" AS INTEGER) AS "deathYear",
            list_transform(
                list_filter(split(COALESCE("primaryProfession", ''), ','), x -> length(trim(x)) > 0),
                x -> trim(x)
            ) AS "primaryProfession",
            "knownForTitles"
        FROM {read_table('name.basics')}
    """)
    conn.execute(f"COPY names TO '{(build_dir / "name_basics.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE names")
    logger.info("name_basics.parquet written")

    # --- title.akas (optional) ---
    if settings.ENABLE_AKAS and (tsv_dir / f"{IMDB_AKAS}.tsv.gz").exists():
        logger.info("Converting title.akas ...")
        conn.execute(f"""
            CREATE TABLE akas AS
        SELECT
            titleId,
            try_cast(ordering AS INTEGER) AS ordering,
                title,
                region,
                language,
                types,
                attributes,
                CAST(COALESCE("isOriginalTitle", '0') AS VARCHAR) IN ('1', 'true', 'True') AS isOriginalTitle
            FROM {read_table(IMDB_AKAS)}
        """)
        conn.execute(f"COPY akas TO '{(build_dir / "title_akas.parquet").as_posix()}' (FORMAT PARQUET)")
        conn.execute("DROP TABLE akas")
        logger.info("title_akas.parquet written")

    # --- Serving: title_details = title_basics + title_ratings ---
    logger.info("Building title_details.parquet ...")
    conn.execute(f"""
        CREATE TABLE title_details AS
        SELECT
            b.tconst,
            b.titleType,
            b.primaryTitle,
            b.originalTitle,
            b.isAdult,
            b.startYear,
            b.endYear,
            b.runtimeMinutes,
            b.genres,
            r.averageRating,
            r.numVotes
        FROM read_parquet('{(build_dir / "title_basics.parquet").as_posix()}') b
        LEFT JOIN read_parquet('{(build_dir / "title_ratings.parquet").as_posix()}') r ON b.tconst = r.tconst
    """)
    conn.execute(f"COPY title_details TO '{(build_dir / "title_details.parquet").as_posix()}' (FORMAT PARQUET)")
    conn.execute("DROP TABLE title_details")
    logger.info("title_details.parquet written")

    conn.close()


def atomic_symlink(settings, build_dir: Path) -> None:
    """Point current (symlink or current_build.txt) to the given build dir. Uses META_DIR when set."""
    current = settings.get_current_link()
    meta = settings._meta()
    meta.mkdir(parents=True, exist_ok=True)
    build_path = build_dir.absolute()
    temp_link = meta / "current_tmp"
    if temp_link.exists():
        temp_link.unlink()
    try:
        os.symlink(build_path, temp_link)
        temp_link.replace(current)
    except OSError:
        # Windows/some hosts: symlink not available; fallback: write current_build.txt
        current_file = settings.get_current_build_file()
        current_file.write_text(str(build_path), encoding="utf-8")
        if temp_link.exists():
            temp_link.unlink()


def run_ingest_core(settings):
    """
    Download, convert to Parquet in a staging folder, then atomically promote
    to parquet_builds. Does NOT switch current or write version file (caller does that).
    Returns (build_name: str, source_meta: dict).
    """
    settings.get_tsv_dir().mkdir(parents=True, exist_ok=True)
    settings.get_tmp_dir().mkdir(parents=True, exist_ok=True)
    settings.get_parquet_dir().mkdir(parents=True, exist_ok=True)
    settings._meta().mkdir(parents=True, exist_ok=True)

    source_meta = run_downloads(settings)

    build_name = time.strftime("%Y%m%d_%H%M%S")
    # Build in TMP_DIR so parquet_builds only ever sees complete builds (atomic promote)
    build_dir_staging = settings.get_tmp_dir() / build_name
    build_dir_staging.mkdir(parents=True, exist_ok=True)
    convert_to_parquet(settings, build_dir_staging)

    # Promote to parquet_builds (move is atomic on same filesystem; copy+rm on cross-mount)
    final_dir = settings.get_parquet_dir() / build_name
    shutil.move(str(build_dir_staging), str(final_dir))
    return build_name, {"files": source_meta}


def run_ingest_with_lock() -> None:
    """
    Run ingest guarded by cross-process file lock (DATA_DIR/ingest.lock).
    Non-blocking: if lock held, log and skip. Stale lock (older than
    INGEST_LOCK_STALE_SECONDS) may be taken over.
    On success: atomic switch data/current, then write dataset_version.json.
    """
    settings = get_settings()
    lock_path = settings.get_ingest_lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(lock_path, timeout=0)
    pid = os.getpid()
    stale_sec = settings.INGEST_LOCK_STALE_SECONDS

    def try_acquire() -> bool:
        try:
            lock.acquire()
            return True
        except Timeout:
            return False

    if not try_acquire():
        # Check stale: lock file (or lock file's sidecar) mtime
        try:
            mtime = lock_path.stat().st_mtime
            if (time.time() - mtime) > stale_sec:
                logger.warning(
                    f"ingest lock file older than {stale_sec}s; attempting takeover (PID={pid})"
                )
                try:
                    lock_path.unlink()
                except OSError:
                    pass
                if not try_acquire():
                    logger.info(f"ingest skipped due to lock after stale takeover attempt (PID={pid})")
                    return
            else:
                logger.info(f"ingest skipped due to lock (PID={pid})")
                return
        except OSError:
            logger.info(f"ingest skipped due to lock (PID={pid})")
            return

    try:
        logger.info(f"ingest lock acquired by PID={pid}")
        t0 = time.perf_counter()
        build_name, source_meta = run_ingest_core(settings)
        atomic_symlink(settings, settings.get_parquet_dir() / build_name)
        write_version_file(build_name, source_meta)
        elapsed = time.perf_counter() - t0
        logger.info(f"Ingest completed in {elapsed:.1f}s. Current -> {build_name}")
    except Exception as e:
        logger.exception(f"Ingest failed: {e}")
    finally:
        try:
            lock.release()
        except Exception:
            pass


def main() -> int:
    """CLI entrypoint: run ingest (with lock) and exit."""
    settings = get_settings()
    setup_logging(settings.LOG_DIR, console=True)
    try:
        run_ingest_with_lock()
        return 0
    except Exception as e:
        logger.exception(f"Ingest failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
