"""Shared utilities: logging, null handling, list parsing."""

import gzip
import json
import re
from pathlib import Path
from typing import Any

from loguru import logger


# IMDb uses "\N" for missing values
NULL_STR = "\\N"


def null_safe_str(val: Any) -> str | None:
    """Return None if value is IMDb null or empty, else string."""
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s == NULL_STR:
        return None
    return s


def null_safe_int(val: Any) -> int | None:
    """Parse to int; return None for null or invalid."""
    s = null_safe_str(val)
    if s is None:
        return None
    try:
        return int(s)
    except (ValueError, TypeError):
        return None


def null_safe_float(val: Any) -> float | None:
    """Parse to float; return None for null or invalid."""
    s = null_safe_str(val)
    if s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_list_str(val: Any, sep: str = ",") -> list[str]:
    """Split comma-separated string; return empty list for null."""
    s = null_safe_str(val)
    if s is None:
        return []
    return [x.strip() for x in s.split(sep) if x.strip()]


def parse_characters(val: Any) -> list[str]:
    """Parse '["char1","char2"]' style characters field."""
    s = null_safe_str(val)
    if s is None:
        return []
    # IMDb format is JSON array of strings
    if s.startswith("[") and s.endswith("]"):
        try:
            out = json.loads(s)
            if isinstance(out, list):
                return [str(x) for x in out]
        except (json.JSONDecodeError, TypeError):
            pass
    return [s]


def setup_logging(log_dir: Path | None = None, console: bool = True) -> None:
    """Configure loguru: console + optional daily file in log_dir."""
    logger.remove()
    if console:
        logger.add(
            lambda msg: print(msg, end=""),
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="INFO",
        )
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(parents=True, exist_ok=True)
        logger.add(
            log_dir / "imdb_{time:YYYY-MM-DD}.log",
            rotation="00:00",
            retention=30,
            level="DEBUG",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
        )


def normalize_search_query(q: str) -> str:
    """Normalize search string: strip, collapse spaces."""
    if not q or not isinstance(q, str):
        return ""
    return " ".join(re.split(r"\s+", q.strip()))
