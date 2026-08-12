#!/usr/bin/env bash
# scripts/sync_cloud.sh — local -> cloud sync (gold -> LakeStore, current
# state + predictions -> StateStore). No-op when STATE_BACKEND=postgres and
# LAKE_BACKEND=local (the defaults) — safe to run every cycle unconditionally,
# whether or not you've opted into cloud backends.
#
#   ./scripts/sync_cloud.sh                                  # one-shot, local defaults -> no-op
#   STATE_BACKEND=dynamodb LAKE_BACKEND=s3 \
#     AWS_PROFILE=aeroflux-local ./scripts/sync_cloud.sh      # real sync
#
# State/prediction writes are deduped against a local content-hash cache by
# default (SYNC_DEDUPE=1) — see aeroflux_ml/sync_cloud.py's module
# docstring. SYNC_DEDUPE=0 to write every row every cycle (old behavior);
# DYNAMODB_REFRESH_HOURS (default 12) bounds how long an unchanged item can
# go without a write before it's refreshed anyway (TTL safety).
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

: "${DSN:=postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux}"
: "${GOLD:=$ROOT/out/gold_features.parquet}"
: "${PREDICTIONS:=$ROOT/out/predictions.parquet}"
: "${STATE_HOURS:=48}"   # how far back in flight_instance.updated_at to sync; separate from DYNAMODB_TTL_HOURS (the destination TTL horizon)
: "${PYTHON:=python3}"

exec "$PYTHON" -m aeroflux_ml.sync_cloud --gold "$GOLD" --predictions "$PREDICTIONS" \
    --dsn "$DSN" --state-hours "$STATE_HOURS"
