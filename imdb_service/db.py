"""
DuckDB read-only connection and views over Parquet under data/current.
All queries are parameterized.
"""

from pathlib import Path
from typing import Any

import duckdb

from imdb_service.config import get_settings


def _parquet_path(base: Path, name: str) -> str:
    return (base / name).as_posix()


def get_connection():
    """Return a DuckDB connection with views over the current parquet build."""
    settings = get_settings()
    base = settings.get_current_parquet_path()
    if not base or not base.exists():
        return None

    # In-memory DB cannot be read_only; we only attach read-only parquet views
    conn = duckdb.connect(":memory:")

    # Views over parquet files
    conn.execute(f"CREATE VIEW v_title_basics AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_basics.parquet')}')")
    conn.execute(f"CREATE VIEW v_title_ratings AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_ratings.parquet')}')")
    conn.execute(f"CREATE VIEW v_title_principals AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_principals.parquet')}')")
    conn.execute(f"CREATE VIEW v_title_crew AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_crew.parquet')}')")
    conn.execute(f"CREATE VIEW v_title_episode AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_episode.parquet')}')")
    conn.execute(f"CREATE VIEW v_name_basics AS SELECT * FROM read_parquet('{_parquet_path(base, 'name_basics.parquet')}')")
    conn.execute(f"CREATE VIEW v_title_details AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_details.parquet')}')")

    akas_path = base / "title_akas.parquet"
    if akas_path.exists():
        conn.execute(f"CREATE VIEW v_title_akas AS SELECT * FROM read_parquet('{_parquet_path(base, 'title_akas.parquet')}')")
    else:
        conn.execute("CREATE VIEW v_title_akas AS SELECT NULL::VARCHAR titleId, NULL::INT ordering, NULL::VARCHAR title, NULL::VARCHAR region, NULL::VARCHAR language, NULL::VARCHAR types, NULL::VARCHAR attributes, NULL::BOOLEAN isOriginalTitle WHERE FALSE")

    return conn


def get_dataset_version() -> str | None:
    """Return current dataset version id for cache invalidation (from dataset_version.json)."""
    from imdb_service.versioning import get_version_id
    v = get_version_id()
    return v if v != "no_data" else None


# --- Parameterized query helpers ---

def search(
    conn: duckdb.DuckDBPyConnection,
    q: str,
    year: int | None,
    title_type: str | None,
    limit: int,
    include_akas: bool,
) -> list[dict[str, Any]]:
    """Search titles; optional year/type filter; optional akas. Match: exact > prefix > contains; sort by score, numVotes, averageRating, startYear."""
    q_clean = q.strip().lower() if q else ""
    if not q_clean:
        return []

    # Build WHERE for type and year
    type_filter = ""
    if title_type and title_type != "any":
        type_filter = f" AND d.titleType = ?"
    year_filter = " AND d.startYear = ?" if year is not None else ""

    # Match condition: primary/original title or (when include_akas) aka title
    aka_cond = ""
    if include_akas:
        aka_cond = " OR d.tconst IN (SELECT titleId FROM v_title_akas WHERE lower(title) LIKE ?)"

    # Use v_title_details (basics + ratings)
    sql = f"""
        SELECT d.tconst, d.titleType, d.primaryTitle, d.startYear, d.genres, d.averageRating, d.numVotes,
               CASE
                 WHEN lower(d.primaryTitle) = ? OR lower(d.originalTitle) = ? THEN 3
                 WHEN lower(d.primaryTitle) LIKE ? OR lower(d.originalTitle) LIKE ? THEN 2
                 WHEN lower(d.primaryTitle) LIKE ? OR lower(d.originalTitle) LIKE ? THEN 1
                 ELSE 0
               END AS match_score
        FROM v_title_details d
        WHERE (
            lower(d.primaryTitle) LIKE ? OR lower(d.originalTitle) LIKE ? {{aka_cond}}
        )
        {{type_filter}}
        {{year_filter}}
        ORDER BY match_score DESC, d.numVotes DESC NULLS LAST, d.averageRating DESC NULLS LAST, d.startYear DESC NULLS LAST
        LIMIT ?
    """.replace("{aka_cond}", aka_cond).replace("{type_filter}", type_filter).replace("{year_filter}", year_filter)

    like_contains = f"%{q_clean}%"
    like_prefix = f"{q_clean}%"

    params: list[Any] = [q_clean, q_clean, like_prefix, like_prefix, like_contains, like_contains, like_contains, like_contains]
    if include_akas:
        params.append(like_contains)
    if title_type and title_type != "any":
        params.append(title_type)
    if year is not None:
        params.append(year)
    params.append(limit)

    cur = conn.execute(sql, params)
    columns = [d[0] for d in cur.description]
    result = cur.fetchall()
    return [dict(zip(columns, row)) for row in result]


def get_title(conn: duckdb.DuckDBPyConnection, tconst: str) -> dict[str, Any] | None:
    """Get title basics + ratings + crew (directors, writers)."""
    row = conn.execute(
        """
        SELECT d.tconst, d.titleType, d.primaryTitle, d.originalTitle, d.isAdult, d.startYear, d.endYear,
               d.runtimeMinutes, d.genres, d.averageRating, d.numVotes, c.directors, c.writers
        FROM v_title_details d
        LEFT JOIN v_title_crew c ON d.tconst = c.tconst
        WHERE d.tconst = ?
        """,
        [tconst],
    ).fetchone()
    if not row:
        return None
    cols = ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear",
            "runtimeMinutes", "genres", "averageRating", "numVotes", "directors", "writers"]
    return dict(zip(cols, row))


def get_episodes_for_series(conn: duckdb.DuckDBPyConnection, tconst: str, season: int | None, limit: int, offset: int) -> tuple[list[dict], int]:
    """Episodes for a series; optional season filter. Returns (rows, total)."""
    count_sql = """
        SELECT COUNT(*) FROM v_title_episode e
        WHERE e.parentTconst = ?
    """
    params_count: list[Any] = [tconst]
    if season is not None:
        count_sql = "SELECT COUNT(*) FROM v_title_episode e WHERE e.parentTconst = ? AND e.seasonNumber = ?"
        params_count.append(season)

    total = conn.execute(count_sql, params_count).fetchone()[0]

    sql = """
        SELECT e.tconst, e.seasonNumber, e.episodeNumber, b.primaryTitle, b.startYear, b.runtimeMinutes
        FROM v_title_episode e
        LEFT JOIN v_title_basics b ON e.tconst = b.tconst
        WHERE e.parentTconst = ?
    """
    params: list[Any] = [tconst]
    if season is not None:
        sql += " AND e.seasonNumber = ?"
        params.append(season)
    sql += " ORDER BY e.seasonNumber ASC NULLS LAST, e.episodeNumber ASC NULLS LAST LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    rows = conn.execute(sql, params).fetchall()
    cols = ["tconst", "seasonNumber", "episodeNumber", "primaryTitle", "startYear", "runtimeMinutes"]
    return [dict(zip(cols, r)) for r in rows], total


def get_credits(
    conn: duckdb.DuckDBPyConnection,
    tconst: str,
    category: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Cast/crew for title: principals joined with name_basics."""
    sql = """
        SELECT p.ordering, p.nconst, n.primaryName, p.category, p.job, p.characters
        FROM v_title_principals p
        JOIN v_name_basics n ON p.nconst = n.nconst
        WHERE p.tconst = ?
    """
    params: list[Any] = [tconst]
    if category:
        sql += " AND p.category = ?"
        params.append(category)
    sql += " ORDER BY p.ordering ASC LIMIT ?"
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    cols = ["ordering", "nconst", "primaryName", "category", "job", "characters"]
    return [dict(zip(cols, r)) for r in rows]


def get_person(conn: duckdb.DuckDBPyConnection, nconst: str) -> dict[str, Any] | None:
    """Person row plus knownForTitles expanded (basics+ratings)."""
    row = conn.execute(
        "SELECT nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles FROM v_name_basics WHERE nconst = ?",
        [nconst],
    ).fetchone()
    if not row:
        return None
    r = dict(zip(["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"], row))
    # Expand knownForTitles: can be comma-separated tconst list
    kft = r.get("knownForTitles")
    if not kft:
        r["knownForTitles_expanded"] = []
        return r
    if isinstance(kft, str):
        tconsts = [x.strip() for x in kft.split(",") if x.strip()]
    elif isinstance(kft, list):
        tconsts = list(kft)
    else:
        tconsts = []
    if not tconsts:
        r["knownForTitles_expanded"] = []
        return r
    placeholders = ",".join("?" * len(tconsts))
    details = conn.execute(
        f"SELECT tconst, primaryTitle, titleType, startYear, averageRating, numVotes FROM v_title_details WHERE tconst IN ({placeholders})",
        tconsts,
    ).fetchall()
    r["knownForTitles_expanded"] = [
        dict(zip(["tconst", "primaryTitle", "titleType", "startYear", "averageRating", "numVotes"], d))
        for d in details
    ]
    return r


def get_season_summary(conn: duckdb.DuckDBPyConnection, tconst: str) -> list[dict[str, Any]]:
    """Count of episodes per season for a series."""
    rows = conn.execute(
        """
        SELECT seasonNumber, COUNT(*) AS episode_count
        FROM v_title_episode
        WHERE parentTconst = ?
        GROUP BY seasonNumber
        ORDER BY seasonNumber
        """,
        [tconst],
    ).fetchall()
    return [{"seasonNumber": r[0], "episode_count": r[1]} for r in rows]
