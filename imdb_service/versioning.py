"""
Dataset version file (dataset_version.json) and per-worker version watcher.
Workers poll the version file; on change they clear caches and reload (no persistent DuckDB).
"""

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Callable

from loguru import logger

from imdb_service.config import get_settings


def get_version_file_path() -> Path:
    return get_settings().get_version_file_path()


def read_version_file() -> dict[str, Any] | None:
    """Read dataset_version.json; return None if missing or invalid."""
    path = get_version_file_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def get_version_id() -> str:
    """Current dataset version id for cache keys (monotonic or build_id)."""
    data = read_version_file()
    if data:
        return str(data.get("version_id", data.get("build_id", "no_data")))
    # Legacy: dataset_version.txt (build_id only)
    settings = get_settings()
    legacy = settings.get_version_file_path().parent / "dataset_version.txt"
    if legacy.exists():
        return legacy.read_text(encoding="utf-8").strip() or "no_data"
    return "no_data"


def get_build_id() -> str | None:
    """Current build_id (parquet folder name) if available."""
    data = read_version_file()
    if not data:
        return None
    return data.get("build_id")


def write_version_file(build_id: str, source_meta: dict[str, Any]) -> None:
    """
    Write dataset_version.json after atomic switch of data/current.
    Increments version_id monotonically. Include build timestamp and source etag/last-modified.
    """
    path = get_version_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = read_version_file()
    version_id = 1
    if existing and isinstance(existing.get("version_id"), (int, float)):
        version_id = int(existing["version_id"]) + 1

    from datetime import datetime, timezone
    payload = {
        "version_id": version_id,
        "build_id": build_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source_meta,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)
    logger.info(f"dataset version updated to version_id={version_id} build_id={build_id}")


_watcher_task: asyncio.Task | None = None
_watcher_interval_sec: float = 15.0
_last_seen: str | None = None


def _version_fingerprint() -> str:
    """Fingerprint for change detection (mtime + version_id or legacy txt)."""
    path = get_version_file_path()
    if path.exists():
        try:
            stat = path.stat()
            data = read_version_file()
            vid = (data or {}).get("version_id", "")
            return f"{stat.st_mtime}:{vid}"
        except OSError:
            pass
    legacy = get_settings().get_version_file_path().parent / "dataset_version.txt"
    if legacy.exists():
        try:
            return f"{legacy.stat().st_mtime}:{legacy.read_text(encoding='utf-8').strip()}"
        except OSError:
            pass
    return ""


async def _version_watcher_loop(interval_sec: float, on_version_change: Callable[[], None]) -> None:
    """Background loop: poll version file; when it changes, call on_version_change."""
    global _last_seen
    pid = os.getpid()
    _last_seen = _version_fingerprint()
    while True:
        await asyncio.sleep(interval_sec)
        fp = _version_fingerprint()
        if fp and fp != _last_seen:
            _last_seen = fp
            logger.info(
                f"worker PID={pid} detected version change; caches cleared; views reloaded"
            )
            on_version_change()


def start_version_watcher(
    interval_sec: float | None = None,
    on_version_change: Callable[[], None] | None = None,
) -> asyncio.Task | None:
    """
    Start the version watcher in the current worker.
    on_version_change: callable that clears caches (and optionally reloads state).
    Returns the asyncio Task so caller can cancel on shutdown.
    """
    global _watcher_task, _watcher_interval_sec
    settings = get_settings()
    interval_sec = interval_sec or settings.VERSION_POLL_INTERVAL
    if on_version_change is None:
        def _clear():
            from imdb_service.cache import invalidate_caches
            invalidate_caches()
        on_version_change = _clear

    if _watcher_task is not None and not _watcher_task.done():
        return _watcher_task

    _watcher_interval_sec = float(interval_sec)
    _watcher_task = asyncio.create_task(
        _version_watcher_loop(_watcher_interval_sec, on_version_change)
    )
    return _watcher_task


async def stop_version_watcher() -> None:
    """Cancel the version watcher task (call from async context)."""
    global _watcher_task
    if _watcher_task is not None:
        _watcher_task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(_watcher_task), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        _watcher_task = None
