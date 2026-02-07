#!/bin/sh
# Create required directories on startup so mounted volumes are usable.
# Set PUID/PGID in Compose and use user: "${PUID}:${PGID}" so the process runs non-root;
# ensure host ./volumes/* are writable by that user (e.g. chown 1000:1000 ./volumes).

set -e
DATA="${DATA_DIR:-/app/data}"
for d in "$DATA" \
         "${DOWNLOAD_DIR:-$DATA/downloads}" \
         "${TMP_DIR:-$DATA/tmp}" \
         "${PARQUET_DIR:-$DATA/parquet_builds}" \
         "${META_DIR:-$DATA}" \
         "${LOG_DIR:-/app/logs}"; do
  [ -z "$d" ] && continue
  mkdir -p "$d"
done
exec "$@"
