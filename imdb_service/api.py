"""
FastAPI service for low-latency querying over IMDb Parquet data.
Scheduler and version watcher run in lifespan (single container, multi-worker safe).
"""

from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query

from imdb_service.cache import (
    invalidate_caches,
    cache_person_lookup,
    cache_search,
    cache_title_lookup,
)
from imdb_service.config import get_settings
from imdb_service.db import (
    get_connection,
    get_credits,
    get_dataset_version,
    get_episodes_for_series,
    get_person,
    get_season_summary,
    get_title,
    search as db_search,
)
from imdb_service.models import (
    CreditItem,
    CreditsResponse,
    EpisodeItem,
    EpisodesResponse,
    KnownForTitle,
    PersonResponse,
    SearchResponse,
    SearchResultItem,
    TitleResponse,
)
from imdb_service.scheduler import start_scheduler, stop_scheduler
from imdb_service.utils import parse_characters, parse_list_str
from imdb_service.versioning import start_version_watcher, stop_version_watcher


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start API and background scheduler + version watcher; stop on shutdown."""
    settings = get_settings()
    settings.LOG_DIR.mkdir(parents=True, exist_ok=True)
    # Scheduler: nightly ingest at 22:00 Asia/Kolkata (only one worker runs it via file lock)
    start_scheduler()
    # Version watcher: each worker polls dataset_version.json and clears caches on change
    start_version_watcher(on_version_change=invalidate_caches)
    yield
    await stop_version_watcher()
    stop_scheduler()


app = FastAPI(title="IMDb Data API", version="1.0.0", lifespan=lifespan)


def _ensure_conn():
    conn = get_connection()
    if conn is None:
        raise HTTPException(status_code=503, detail="No data loaded. Run ingest first.")
    return conn


def _list_from_row(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val]
    return parse_list_str(val)


@app.get("/health")
def health():
    """Health check; reports data availability."""
    conn = get_connection()
    version = get_dataset_version()
    return {
        "status": "ok",
        "data_loaded": conn is not None,
        "dataset_version": version,
    }


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=1),
    year: int | None = Query(None),
    type: Literal["movie", "tvseries", "tvepisode", "any"] = Query("any"),
    limit: int = Query(20, ge=1, le=100),
    include_akas: bool = Query(False),
):
    """Search titles by name; optional year/type filter. Sorted by match score, votes, rating."""
    settings = get_settings()
    limit = min(limit, settings.MAX_SEARCH_LIMIT)
    conn = _ensure_conn()
    q_norm = q.strip()
    if not q_norm:
        return SearchResponse(results=[], total=0)

    def loader():
        return db_search(conn, q_norm, year, type if type != "any" else None, limit, include_akas)

    rows = cache_search(q_norm, year, type if type != "any" else None, limit, include_akas, loader)
    results = []
    for r in rows:
        genres = _list_from_row(r.get("genres"))
        results.append(
            SearchResultItem(
                tconst=r["tconst"],
                titleType=r["titleType"],
                primaryTitle=r["primaryTitle"],
                startYear=r.get("startYear"),
                genres=genres,
                averageRating=r.get("averageRating"),
                numVotes=r.get("numVotes"),
            )
        )
    return SearchResponse(results=results, total=len(results))


@app.get("/title/{tconst}", response_model=TitleResponse)
def get_title_by_id(
    tconst: str,
    include_episodes: bool = Query(False),
):
    """Title details: basics, ratings, crew. For tvseries, optional seasons + episodes."""
    def loader():
        conn = _ensure_conn()
        return get_title(conn, tconst)

    row = cache_title_lookup(tconst, loader)
    if not row:
        raise HTTPException(status_code=404, detail="Title not found")

    directors = _list_from_row(row.get("directors"))
    writers = _list_from_row(row.get("writers"))
    genres = _list_from_row(row.get("genres"))

    out = TitleResponse(
        tconst=row["tconst"],
        titleType=row["titleType"],
        primaryTitle=row["primaryTitle"],
        originalTitle=row.get("originalTitle"),
        isAdult=bool(row.get("isAdult")),
        startYear=row.get("startYear"),
        endYear=row.get("endYear"),
        runtimeMinutes=row.get("runtimeMinutes"),
        genres=genres,
        averageRating=row.get("averageRating"),
        numVotes=row.get("numVotes"),
        directors=directors,
        writers=writers,
    )

    if include_episodes and str(row.get("titleType", "")).lower() == "tvseries":
        conn = _ensure_conn()
        seasons = get_season_summary(conn, tconst)
        out.seasons_count = len(seasons)
        # First N episodes (e.g. first 20)
        eps, _ = get_episodes_for_series(conn, tconst, None, 50, 0)
        out.episodes = [
            {
                "tconst": e["tconst"],
                "seasonNumber": e.get("seasonNumber"),
                "episodeNumber": e.get("episodeNumber"),
                "primaryTitle": e.get("primaryTitle"),
            }
            for e in eps
        ]

    return out


@app.get("/title/{tconst}/credits", response_model=CreditsResponse)
def get_title_credits(
    tconst: str,
    category: str | None = Query(None, description="Filter by category: actor, actress, director, writer, etc."),
    limit: int = Query(50, ge=1, le=500),
):
    """Cast and crew for a title from title.principals + name.basics."""
    conn = _ensure_conn()
    rows = get_credits(conn, tconst, category, limit)
    cast_crew = []
    for r in rows:
        chars = r.get("characters")
        if isinstance(chars, list):
            characters = [str(c) for c in chars]
        elif chars:
            characters = parse_characters(chars)
        else:
            characters = []
        cast_crew.append(
            CreditItem(
                ordering=r["ordering"],
                nconst=r["nconst"],
                primaryName=r["primaryName"],
                category=r["category"],
                job=r.get("job"),
                characters=characters,
            )
        )
    return CreditsResponse(tconst=tconst, cast_crew=cast_crew)


@app.get("/person/{nconst}", response_model=PersonResponse)
def get_person_by_id(nconst: str):
    """Person details with knownForTitles expanded (basics + ratings)."""
    def loader():
        conn = _ensure_conn()
        return get_person(conn, nconst)

    row = cache_person_lookup(nconst, loader)
    if not row:
        raise HTTPException(status_code=404, detail="Person not found")

    professions = _list_from_row(row.get("primaryProfession"))
    expanded = row.get("knownForTitles_expanded") or []
    known_for = [
        KnownForTitle(
            tconst=x.get("tconst", ""),
            primaryTitle=x.get("primaryTitle", ""),
            titleType=x.get("titleType", ""),
            startYear=x.get("startYear"),
            averageRating=x.get("averageRating"),
            numVotes=x.get("numVotes"),
        )
        for x in expanded
    ]
    return PersonResponse(
        nconst=row["nconst"],
        primaryName=row["primaryName"],
        birthYear=row.get("birthYear"),
        deathYear=row.get("deathYear"),
        primaryProfession=professions,
        knownForTitles=known_for,
    )


@app.get("/series/{tconst}/episodes", response_model=EpisodesResponse)
def get_series_episodes(
    tconst: str,
    season: int | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Episodes for a TV series; optional season filter."""
    conn = _ensure_conn()
    # Resolve series title
    title_row = get_title(conn, tconst)
    series_title = title_row.get("primaryTitle") if title_row else None
    episodes, total = get_episodes_for_series(conn, tconst, season, limit, offset)
    return EpisodesResponse(
        tconst=tconst,
        series_title=series_title,
        episodes=[
            EpisodeItem(
                tconst=e["tconst"],
                seasonNumber=e.get("seasonNumber"),
                episodeNumber=e.get("episodeNumber"),
                primaryTitle=e.get("primaryTitle"),
                startYear=e.get("startYear"),
                runtimeMinutes=e.get("runtimeMinutes"),
            )
            for e in episodes
        ],
        total=total,
    )


def create_app() -> FastAPI:
    return app
