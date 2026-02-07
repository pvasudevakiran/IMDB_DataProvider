"""
In-process TTL + LRU caching (cachetools) with optional Redis.
Dataset version is included in keys so caches invalidate after nightly update.
"""

import json
from typing import Any, Callable, TypeVar

from cachetools import TTLCache

from imdb_service.config import get_settings
from imdb_service.versioning import get_version_id

T = TypeVar("T")

_redis_client: Any = None


def _redis():
    """Lazy Redis client if REDIS_URL is set."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    url = get_settings().REDIS_URL
    if not url:
        return None
    try:
        import redis
        _redis_client = redis.from_url(url, decode_responses=True)
        return _redis_client
    except Exception:
        return None

# In-process caches (filled on first use with version in key)
_title_cache: TTLCache | None = None
_person_cache: TTLCache | None = None
_search_cache: TTLCache | None = None


def _version_prefix() -> str:
    return get_version_id()


def _get_title_cache() -> TTLCache:
    global _title_cache
    if _title_cache is None:
        s = get_settings()
        _title_cache = TTLCache(maxsize=s.CACHE_TITLE_MAX, ttl=s.CACHE_TITLE_TTL)
    return _title_cache


def _get_person_cache() -> TTLCache:
    global _person_cache
    if _person_cache is None:
        s = get_settings()
        _person_cache = TTLCache(maxsize=s.CACHE_PERSON_MAX, ttl=s.CACHE_PERSON_TTL)
    return _person_cache


def _get_search_cache() -> TTLCache:
    global _search_cache
    if _search_cache is None:
        s = get_settings()
        _search_cache = TTLCache(maxsize=s.CACHE_SEARCH_MAX, ttl=s.CACHE_SEARCH_TTL)
    return _search_cache


def _redis_key(kind: str, *parts: Any) -> str:
    prefix = get_settings().REDIS_KEY_PREFIX
    return f"{prefix}{_version_prefix()}:{kind}:{':'.join(str(p) for p in parts)}"


def cache_title_lookup(tconst: str, loader: Callable[[], T | None]) -> T | None:
    """Get title by tconst; use cache with version prefix."""
    key = f"{_version_prefix()}:title:{tconst}"
    r = _redis()
    if r:
        try:
            raw = r.get(_redis_key("title", tconst))
            if raw is not None:
                return json.loads(raw)
        except Exception:
            pass
    cache = _get_title_cache()
    if key in cache:
        return cache[key]
    val = loader()
    if val is not None:
        cache[key] = val
        if r:
            try:
                r.setex(
                    _redis_key("title", tconst),
                    get_settings().CACHE_TITLE_TTL,
                    json.dumps(val, default=str),
                )
            except Exception:
                pass
    return val


def cache_person_lookup(nconst: str, loader: Callable[[], T | None]) -> T | None:
    """Get person by nconst; use cache with version prefix."""
    key = f"{_version_prefix()}:person:{nconst}"
    r = _redis()
    if r:
        try:
            raw = r.get(_redis_key("person", nconst))
            if raw is not None:
                return json.loads(raw)
        except Exception:
            pass
    cache = _get_person_cache()
    if key in cache:
        return cache[key]
    val = loader()
    if val is not None:
        cache[key] = val
        if r:
            try:
                r.setex(
                    _redis_key("person", nconst),
                    get_settings().CACHE_PERSON_TTL,
                    json.dumps(val, default=str),
                )
            except Exception:
                pass
    return val


def cache_search(
    q: str,
    year: int | None,
    title_type: str | None,
    limit: int,
    include_akas: bool,
    loader: Callable[[], list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Search with cache key from normalized params."""
    key = f"{_version_prefix()}:search:{q}:{year}:{title_type}:{limit}:{include_akas}"
    r = _redis()
    if r:
        try:
            raw = r.get(_redis_key("search", q, year, title_type, limit, include_akas))
            if raw is not None:
                return json.loads(raw)
        except Exception:
            pass
    cache = _get_search_cache()
    if key in cache:
        return cache[key]
    val = loader()
    cache[key] = val
    if r:
        try:
            r.setex(
                _redis_key("search", q, year, title_type, limit, include_akas),
                get_settings().CACHE_SEARCH_TTL,
                json.dumps(val, default=str),
            )
        except Exception:
            pass
    return val


def invalidate_caches() -> None:
    """Clear in-process caches (e.g. on version change or admin)."""
    global _title_cache, _person_cache, _search_cache
    for c in (_title_cache, _person_cache, _search_cache):
        if c is not None:
            c.clear()
