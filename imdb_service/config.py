"""Configuration via pydantic-settings with .env support."""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Paths (optional overrides for Docker volume layout; default derived from DATA_DIR)
    DATA_DIR: Path = Field(default=Path("./data"), description="Base data directory")
    LOG_DIR: Path = Field(default=Path("./logs"), description="Log directory")
    DOWNLOAD_DIR: Path | None = Field(default=None, description="Downloads and manifest; default DATA_DIR/downloads")
    TMP_DIR: Path | None = Field(default=None, description="Staging/temp for ingest; default DATA_DIR/tmp")
    PARQUET_DIR: Path | None = Field(default=None, description="Versioned parquet builds; default DATA_DIR/parquet_builds")
    META_DIR: Path | None = Field(default=None, description="Pointer, version, lock files; default DATA_DIR")

    # IMDb dataset
    IMDB_BASE_URL: str = Field(
        default="https://datasets.imdbws.com/",
        description="Base URL for IMDb TSV datasets",
    )
    ENABLE_AKAS: bool = Field(default=True, description="Include title.akas for alternate titles")

    # Scheduler (for documentation; actual schedule is cron/systemd)
    UPDATE_TIME: str = Field(
        default="22:00",
        description="Default update time (e.g. 22:00 for 10 PM)",
    )
    UPDATE_TZ: str = Field(default="Asia/Kolkata", description="Timezone for update time")

    # API
    MAX_SEARCH_LIMIT: int = Field(default=100, ge=1, le=500, description="Max results per search")

    # Cache - in-process (cachetools)
    CACHE_TITLE_TTL: int = Field(default=6 * 3600, description="Title cache TTL seconds (6h)")
    CACHE_TITLE_MAX: int = Field(default=50_000, description="Max title cache entries")
    CACHE_PERSON_TTL: int = Field(default=6 * 3600, description="Person cache TTL seconds (6h)")
    CACHE_PERSON_MAX: int = Field(default=50_000, description="Max person cache entries")
    CACHE_SEARCH_TTL: int = Field(default=30 * 60, description="Search cache TTL seconds (30m)")
    CACHE_SEARCH_MAX: int = Field(default=20_000, description="Max search cache entries")

    # Ingest lock (multi-worker safety)
    INGEST_LOCK_STALE_SECONDS: int = Field(
        default=6 * 3600,
        description="Lock file older than this may be taken over (stale safeguard)",
    )
    VERSION_POLL_INTERVAL: int = Field(
        default=15,
        description="Seconds between version file polls for cache invalidation",
    )

    # Optional Redis
    REDIS_URL: str | None = Field(default=None, description="Redis URL; if set, use Redis for cache")
    REDIS_KEY_PREFIX: str = Field(default="imdb:", description="Redis key prefix")

    def _meta(self) -> Path:
        """Directory for manifest, version, lock, current pointer."""
        return self.META_DIR if self.META_DIR is not None else self.DATA_DIR

    def get_parquet_dir(self) -> Path:
        """Resolved parquet builds directory."""
        return self.PARQUET_DIR if self.PARQUET_DIR is not None else self.DATA_DIR / "parquet_builds"

    def get_current_link(self) -> Path:
        """Path to current symlink (points to active parquet build)."""
        return self._meta() / "current"

    def get_manifest_path(self) -> Path:
        """Path to manifest.json for ingest metadata (etag/last-modified); stored with downloads."""
        return self.get_tsv_dir() / "manifest.json"

    def get_current_build_file(self) -> Path:
        """Path to current_build.txt (fallback when symlink not used)."""
        return self._meta() / "current_build.txt"

    def get_current_parquet_path(self) -> Path | None:
        """Resolve path to current parquet build (symlink or current_build.txt)."""
        current = self.get_current_link()
        if current.exists():
            if current.is_symlink():
                return current.resolve()
            return current
        current_file = self.get_current_build_file()
        if current_file.exists():
            return Path(current_file.read_text(encoding="utf-8").strip())
        return None

    def get_tsv_dir(self) -> Path:
        """Directory where .tsv.gz files are kept (downloads)."""
        if self.DOWNLOAD_DIR is not None:
            return self.DOWNLOAD_DIR
        return self.DATA_DIR / "tsv"

    def get_tmp_dir(self) -> Path:
        """Staging/temp directory for ingest (atomic promote to parquet_builds)."""
        return self.TMP_DIR if self.TMP_DIR is not None else self.DATA_DIR / "tmp"

    def get_ingest_lock_path(self) -> Path:
        """Cross-process ingest lock file (shared volume)."""
        return self._meta() / "ingest.lock"

    def get_version_file_path(self) -> Path:
        """Path to dataset_version.json for cache invalidation."""
        return self._meta() / "dataset_version.json"


@lru_cache
def get_settings() -> Settings:
    return Settings()
