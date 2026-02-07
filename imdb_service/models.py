"""Pydantic response schemas for the API."""

from typing import Any

from pydantic import BaseModel, Field


# --- Search ---
class SearchResultItem(BaseModel):
    tconst: str
    titleType: str
    primaryTitle: str
    startYear: int | None = None
    genres: list[str] = Field(default_factory=list)
    averageRating: float | None = None
    numVotes: int | None = None


class SearchResponse(BaseModel):
    results: list[SearchResultItem]
    total: int = 0


# --- Title details ---
class TitleResponse(BaseModel):
    tconst: str
    titleType: str
    primaryTitle: str
    originalTitle: str | None = None
    isAdult: bool = False
    startYear: int | None = None
    endYear: int | None = None
    runtimeMinutes: int | None = None
    genres: list[str] = Field(default_factory=list)
    averageRating: float | None = None
    numVotes: int | None = None
    directors: list[str] = Field(default_factory=list)
    writers: list[str] = Field(default_factory=list)
    seasons_count: int | None = None
    episodes: list[dict[str, Any]] = Field(default_factory=list)


# --- Credits ---
class CreditItem(BaseModel):
    ordering: int
    nconst: str
    primaryName: str
    category: str
    job: str | None = None
    characters: list[str] = Field(default_factory=list)


class CreditsResponse(BaseModel):
    tconst: str
    cast_crew: list[CreditItem]


# --- Person ---
class KnownForTitle(BaseModel):
    tconst: str
    primaryTitle: str
    titleType: str
    startYear: int | None = None
    averageRating: float | None = None
    numVotes: int | None = None


class PersonResponse(BaseModel):
    nconst: str
    primaryName: str
    birthYear: int | None = None
    deathYear: int | None = None
    primaryProfession: list[str] = Field(default_factory=list)
    knownForTitles: list[KnownForTitle] = Field(default_factory=list)


# --- Series episodes ---
class EpisodeItem(BaseModel):
    tconst: str
    seasonNumber: int | None = None
    episodeNumber: int | None = None
    primaryTitle: str | None = None
    startYear: int | None = None
    runtimeMinutes: int | None = None


class EpisodesResponse(BaseModel):
    tconst: str
    series_title: str | None = None
    episodes: list[EpisodeItem]
    total: int = 0
