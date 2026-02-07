#!/bin/sh
# Create all volume subfolders under /mnt/disk7/docker/imdb-data-import
# Run once before first 'docker compose up'. Then: chown -R 1000:1000 /mnt/disk7/docker/imdb-data-import

BASE="/mnt/disk7/docker/imdb-data-import"

mkdir -p "$BASE/downloads"
mkdir -p "$BASE/tmp"
mkdir -p "$BASE/parquet_builds"
mkdir -p "$BASE/meta"
mkdir -p "$BASE/logs"
mkdir -p "$BASE/duckdb"

echo "Created volume dirs under $BASE"
# Optional: make container user (1000:1000) owner
# chown -R 1000:1000 "$BASE"
