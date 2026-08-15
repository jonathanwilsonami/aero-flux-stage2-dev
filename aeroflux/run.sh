#!/usr/bin/env bash
# AeroFlux — single-entry runner. From a fresh machine to gold, and a real-time
# streaming mode that keeps silver/gold fresh with a rolling 48h of raw data.
#
#   ./run.sh setup             # one-time: infra up (Kafka+Postgres), topic, tables
#   ./run.sh stream             # REAL-TIME: self-supervised bridge+consumer+poller
#                               #   + independent pipeline-refresh loop (optionally
#                               #   ./run.sh stream 3600 to override the bridge's
#                               #   recycle safety-valve, default INGEST_SECONDS=8h)
#   ./run.sh pipeline          # one-shot: raw -> silver -> load -> gold (+weather)
#   ./run.sh retention         # purge raw + adsb store older than 48h
#   ./run.sh sync              # export gold/silver to STORAGE_DEST (local or s3://)
#   ./run.sh status | stop | down
#
# Everything is env/.env-overridable, so the same script targets cloud. Set
# WEATHER=1 to add live METAR features; STORAGE_DEST=s3://bucket/path to sync.
set -euo pipefail

# A single malformed .env line (e.g. "KEY: value" instead of "KEY=value" --
# happened for real, 2026-08-14, two lines accidentally using YAML-style
# colons) used to kill this ENTIRE script instantly, every single
# invocation, via set -e -- and since e2e.sh's continuous-mode loop
# respawns `run.sh stream` with zero backoff on failure, that turned into
# an hours-long silent busy-loop (millions of repeated "command not
# found" lines, ingest never actually running) rather than a clean error.
# `if ! . ./.env` is exempt from errexit (its exit status is being
# tested), so a bad line now produces a loud warning and best-effort
# sourcing of whatever else is valid, instead of nuking the whole run.
if [ -f .env ]; then
  set -a
  if ! . ./.env; then
    echo "WARNING: .env did not source cleanly (a line likely uses ':' " >&2
    echo "instead of '=', or similar) -- continuing with whatever loaded " >&2
    echo "before the failure. Fix .env; this is not fatal but may mean " >&2
    echo "some expected config is missing." >&2
  fi
  set +a
fi
ROOT="$(cd "$(dirname "$0")" && pwd)"

PG_HOST="${POSTGRES_HOST:-localhost}"; PG_PORT="${POSTGRES_PORT:-5432}"
PG_USER="${POSTGRES_USER:-aeroflux}";  PG_DB="${POSTGRES_DB:-aeroflux}"
PG_PASS="${POSTGRES_PASSWORD:-aeroflux-db}"
DSN="${DSN:-postgresql://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${PG_DB}}"

COMPOSE_FILE="${COMPOSE_FILE:-compose.yaml}"
TOPIC="${KAFKA_TOPIC:-swim.raw.flight}"
RAW_TABLE="${RAW_TABLE:-swim.raw_messages}"; RAW_COLUMN="${RAW_COLUMN:-raw_xml}"
LIMIT="${LIMIT:-500000}"; LIVE="${LIVE:-100}"; OUT="${OUT:-$ROOT/out}"
# Recycle interval for the underlying SWIM bridge process -- a safety
# valve (memory/connection hygiene over very long runs), NOT the
# supervision mechanism (see cmd_stream): that's fully reactive now
# (PID + heartbeat), so this no longer needs to be short. Was 3600 (1h),
# purely because periodic recycling used to be the ONLY way anything
# noticed the bridge needed a fresh connection. swim_to_kafka.py's own
# in-process reconnect loop already treats any connection loss (network
# blip, or a hypothetical FAA-side session close) as retriable and
# recovers without a process restart -- see tests/test_swim_reconnect.py
# and the 2026-08-11 incident that added it. Nothing in this codebase or
# Solace's basic-auth (no expiring token) points to a hard session-
# lifetime cap, but there's also no precedent yet of an actually-uncapped
# multi-hour session (every run before this fix was externally cut short
# around 3600s by this exact value) -- so this stays a generous cap, not
# zero, until that's been observed live. 28800s = 8h.
INGEST_SECONDS="${INGEST_SECONDS:-28800}"
REFRESH_SECONDS="${REFRESH_SECONDS:-300}"
# How often cmd_stream's bridge-supervisor loop polls real state (PID +
# heartbeat) -- independent of REFRESH_SECONDS (pipeline refresh) on
# purpose, see cmd_stream's comment.
INGEST_POLL_SECONDS="${INGEST_POLL_SECONDS:-10}"
# Same name/meaning as e2e.sh cmd_health's INGEST_STALE_MINUTES (minutes
# of silence = dead) -- there it's a human-facing report; here the same
# signal is acted on automatically via the heartbeat file.
INGEST_STALE_MINUTES="${INGEST_STALE_MINUTES:-5}"
INGEST_BACKOFF_BASE_SECONDS="${INGEST_BACKOFF_BASE_SECONDS:-5}"
INGEST_BACKOFF_MAX_SECONDS="${INGEST_BACKOFF_MAX_SECONDS:-60}"
INGEST_HEARTBEAT_FILE="${INGEST_HEARTBEAT_FILE:-$OUT/.ingest_heartbeat}"
RETENTION_HOURS="${RETENTION_HOURS:-48}"
WEATHER="${WEATHER:-1}"
STORAGE_DEST="${STORAGE_DEST:-}"

log(){ printf "\n\033[1;36m==> %s | %s\033[0m\n" "$(date +%H:%M:%S)" "$*"; }
die(){ printf "\033[1;31mERROR:\033[0m %s\n" "$*" >&2; exit 1; }
need(){ command -v "$1" >/dev/null 2>&1; }

check_prereqs(){
  need docker || die "Docker not found: https://docs.docker.com/engine/install/"
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 not found."
  need python || die "Python 3.11+ not found."
  [ -f "$COMPOSE_FILE" ] || die "No $COMPOSE_FILE here. Run from the aeroflux/ root."
  [ -f .env ] || echo "  (warning: no .env — copy .env.example to .env and fill it)"
}

resolve_containers(){
  KAFKA_CONTAINER="${KAFKA_CONTAINER:-$(docker ps --format '{{.Names}} {{.Image}}' | grep -i kafka | awk '{print $1}' | head -1)}"
  PG_CONTAINER="${PG_CONTAINER:-$(docker ps --format '{{.Names}} {{.Image}}' | grep -i postgres | awk '{print $1}' | head -1)}"
  [ -n "${KAFKA_CONTAINER:-}" ] || die "No Kafka container. Run './run.sh setup' or set KAFKA_CONTAINER."
  [ -n "${PG_CONTAINER:-}" ] || die "No Postgres container. Run './run.sh setup' or set PG_CONTAINER."
}

pg(){ docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U "$PG_USER" -d "$PG_DB" "$@"; }

cmd_setup(){
  check_prereqs
  log "Starting infrastructure (Kafka + Postgres)"
  docker compose -f "$COMPOSE_FILE" up -d
  sleep 6; resolve_containers
  echo "  kafka=$KAFKA_CONTAINER  postgres=$PG_CONTAINER"
  for _ in $(seq 1 30); do docker exec "$PG_CONTAINER" pg_isready -U "$PG_USER" >/dev/null 2>&1 && break; sleep 2; done
  log "Creating Kafka topic '$TOPIC'"
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "$TOPIC" --partitions 1 --replication-factor 1 --if-not-exists || true
  log "Creating tables"
  [ -f schema.sql ] && pg < schema.sql || true
  pg <<'SQL'
CREATE TABLE IF NOT EXISTS flight_instance (
  schema_version text, flight_instance_id text PRIMARY KEY, gufi text, flight_ref text,
  callsign text, flight_number text, carrier_icao text, carrier_iata text, carrier_name text,
  resolution_status text, tail_number text, tail_source text, hex text, aircraft_type text,
  aircraft_category text, origin text, destination text,
  scheduled_gate_departure timestamptz, scheduled_gate_arrival timestamptz,
  estimated_arrival timestamptz, actual_off timestamptz, actual_on timestamptz,
  flight_status text, last_latitude double precision, last_longitude double precision,
  last_altitude_ft integer, last_ground_speed integer, last_position_time timestamptz,
  updated_at timestamptz DEFAULT now()
);
SQL
  log "Setup complete. Real-time: ./run.sh stream  |  one-shot: ./run.sh pipeline"
}

cmd_consume(){ resolve_containers; nohup python kafka_to_postgres.py > kafka_to_postgres.log 2>&1 & echo "consumer started (PID $!, kafka_to_postgres.log)"; }
cmd_adsb(){ resolve_containers; nohup python adsb_poller.py > adsb_poller.log 2>&1 & echo "adsb poller started (PID $!, adsb_poller.log)"; }
cmd_ingest(){
  local s="${1:-$INGEST_SECONDS}"
  # INGEST_HEARTBEAT_FILE scoped to just this command (bash's inline-
  # assignment-before-command idiom, same as e2e.sh cmd_sync_cloud's
  # `GOLD="$GOLD" ... "$ROOT/scripts/sync_cloud.sh"`) -- swim_to_kafka.py
  # reads it directly (no dotenv round-trip needed), no-ops if unset.
  INGEST_HEARTBEAT_FILE="$INGEST_HEARTBEAT_FILE" \
    nohup python swim_to_kafka.py --duration "$s" > swim_to_kafka.log 2>&1 &
  # Not `local` on purpose -- cmd_stream reads this right after calling
  # cmd_ingest, to supervise the bridge's actual liveness rather than just
  # assuming it survives for the full $s window (see cmd_stream's comment).
  INGEST_BRIDGE_PID=$!
  echo "bridge started ${s}s safety-valve (PID $INGEST_BRIDGE_PID, swim_to_kafka.log)"
}

cmd_pipeline(){
  resolve_containers
  log "Transform raw -> silver (ADS-B tails from the rolling store)"
  ( cd "$ROOT/scripts" && python build_dataset.py postgres --dsn "$DSN" \
      --table "$RAW_TABLE" --column "$RAW_COLUMN" --limit "$LIMIT" --live "$LIVE" \
      --adsb-store "$DSN" \
      --out-jsonl "$ROOT/dataset.jsonl" --out-csv "$ROOT/dataset.csv" )
  log "Load silver -> flight_instance"
  pg -c "TRUNCATE flight_instance;"
  pg -c "\copy flight_instance (schema_version, flight_instance_id, gufi, flight_ref, callsign, flight_number, carrier_icao, carrier_iata, carrier_name, resolution_status, tail_number, tail_source, hex, aircraft_type, aircraft_category, origin, destination, scheduled_gate_departure, scheduled_gate_arrival, estimated_arrival, actual_off, actual_on, flight_status, last_latitude, last_longitude, last_altitude_ft, last_ground_speed, last_position_time) FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')" < "$ROOT/dataset.csv"
  log "Features silver -> gold (weather=$WEATHER)"
  if [ "$WEATHER" = "1" ]; then
    python -m aeroflux_ml.run postgres --dsn "$DSN" --table public.flight_instance --out "$OUT" --weather live
  else
    python -m aeroflux_ml.run postgres --dsn "$DSN" --table public.flight_instance --out "$OUT"
  fi
  log "Gold ready: $OUT/gold_features.parquet (+ .csv)"
  [ -n "$STORAGE_DEST" ] && cmd_sync || true
}

cmd_retention(){
  resolve_containers
  log "Retention: keeping ${RETENTION_HOURS}h of raw + adsb store"
  pg -c "DELETE FROM $RAW_TABLE WHERE stored_at < now() - make_interval(hours => $RETENTION_HOURS);"
  pg -c "DELETE FROM adsb_airframe WHERE last_seen < now() - make_interval(hours => $RETENTION_HOURS);" 2>/dev/null || true
}

cmd_sync(){
  [ -n "$STORAGE_DEST" ] || die "Set STORAGE_DEST (e.g. s3://bucket/aeroflux or /mnt/archive)"
  local stamp; stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  log "Sync gold + silver -> $STORAGE_DEST (partition $stamp)"
  if [ "${STORAGE_DEST#s3://}" != "$STORAGE_DEST" ]; then
    need aws || die "aws CLI not found (needed for s3:// sync)"
    aws s3 cp "$OUT/gold_features.parquet" "$STORAGE_DEST/gold/dt=$stamp/gold_features.parquet"
    aws s3 cp "$ROOT/dataset.jsonl"         "$STORAGE_DEST/silver/dt=$stamp/dataset.jsonl"
  else
    mkdir -p "$STORAGE_DEST/gold/dt=$stamp" "$STORAGE_DEST/silver/dt=$stamp"
    cp "$OUT/gold_features.parquet" "$STORAGE_DEST/gold/dt=$stamp/"
    cp "$ROOT/dataset.jsonl"        "$STORAGE_DEST/silver/dt=$stamp/"
  fi
}

cmd_stream(){
  local s="${1:-$INGEST_SECONDS}"
  cmd_setup
  log "REAL-TIME mode: bridge (self-supervised) + consumer + poller, pipeline refresh every ${REFRESH_SECONDS}s"
  cmd_consume; cmd_adsb; cmd_ingest "$s"

  # Pipeline refresh is its OWN independent loop now, not interleaved with
  # bridge supervision below in one shared loop body. This is the actual
  # fix for the 2026-08-15 drift bug: cmd_pipeline's build_dataset.py takes
  # real minutes once raw_messages is in the millions, and none of that
  # time used to count toward the old loop's single "elapsed" bookkeeping
  # -- so a slow refresh could silently starve the bridge-liveness check
  # for hours. Two independent loops, sharing no timing state, can't do
  # that to each other again by construction, not by remembering to be
  # careful with one shared counter.
  ( while :; do
      sleep "$REFRESH_SECONDS"
      cmd_pipeline || log "pipeline refresh hiccup (continuing)"
      cmd_retention || true
      cmd_status || true
    done ) & local pipeline_loop_pid=$!

  _stream_cleanup(){
    kill "$pipeline_loop_pid" 2>/dev/null || true
    cmd_stop
  }
  trap 'echo; log "stopping stream"; _stream_cleanup; exit 0' INT TERM

  local fail_count=0 last_launch_ts; last_launch_ts=$(date +%s)
  while :; do
    # Orphan self-check: SUPERVISOR_PID is exported by e2e.sh's cmd_ingest
    # to its own PID before it calls `./run.sh stream`. If that process is
    # gone, nothing will EVER relaunch this one when it eventually dies --
    # self-terminate now rather than run un-supervised. Confirmed live
    # 2026-08-15: an orphaned `run.sh stream` from an already-exited
    # `e2e.sh ingest` sat running for hours with no supervisor left at
    # all, racing a fresh replacement on flight_instance's TRUNCATE
    # window. Skipped when SUPERVISOR_PID isn't set -- a person running
    # `./run.sh stream` directly IS the top-level entity; INT/TERM above
    # already covers that case.
    if [ -n "${SUPERVISOR_PID:-}" ] && ! kill -0 "$SUPERVISOR_PID" 2>/dev/null; then
      log "supervisor (PID $SUPERVISOR_PID) is gone -- this run.sh stream is orphaned, stopping"
      _stream_cleanup
      exit 0
    fi

    # Bridge liveness -- real observable state, not a timer. Two checks:
    # (a) is the PID even alive (covers a clean --duration/--max-messages
    #     exit, logged "Stopped after publishing N message(s)", AND any
    #     crash -- swim_to_kafka.py's own reconnect loop only covers
    #     in-process exceptions, not the process dying outright);
    # (b) if alive, has its heartbeat file gone stale (covers a wedged
    #     receiver call that never raises, so (a) alone would never
    #     trip -- nothing before this could detect that case at all).
    # Skip the heartbeat check for the first INGEST_STALE_MINUTES*60/2
    # (min 60s) after any launch -- otherwise a heartbeat file left over
    # from a PREVIOUS bridge instance can look "stale" for the few
    # seconds a fresh one needs to actually connect, causing an
    # immediate false-positive relaunch loop.
    local need_relaunch=0 reason=""
    if ! kill -0 "$INGEST_BRIDGE_PID" 2>/dev/null; then
      need_relaunch=1; reason="exited"
    else
      local since_launch=$(( $(date +%s) - last_launch_ts ))
      local grace=$(( INGEST_STALE_MINUTES * 30 )); [ "$grace" -lt 60 ] && grace=60
      if [ "$since_launch" -gt "$grace" ] && [ -f "$INGEST_HEARTBEAT_FILE" ]; then
        local hb_age=$(( $(date +%s) - $(stat -c %Y "$INGEST_HEARTBEAT_FILE" 2>/dev/null || echo 0) ))
        if [ "$hb_age" -gt $(( INGEST_STALE_MINUTES * 60 )) ]; then
          need_relaunch=1; reason="stalled (no heartbeat in ${hb_age}s)"
          log "SWIM bridge (PID $INGEST_BRIDGE_PID) $reason -- terminating it"
          kill "$INGEST_BRIDGE_PID" 2>/dev/null || true
          sleep 2
          kill -9 "$INGEST_BRIDGE_PID" 2>/dev/null || true
        fi
      fi
    fi

    if [ "$need_relaunch" = "1" ]; then
      # Bounded backoff, but only for RAPID repeat failures (< 30s alive)
      # -- a clean recycle after a full healthy 8h session must relaunch
      # immediately, same as today; only a crash-loop (e.g. the 2026-08-14
      # .env incident) should ever slow down. Mirrors the shape of
      # swim_to_kafka.py's own in-process backoff (2s doubling to 60s),
      # one layer up.
      local now; now=$(date +%s)
      if [ $(( now - last_launch_ts )) -lt 30 ]; then
        fail_count=$((fail_count + 1))
      else
        fail_count=0
      fi
      local backoff=$INGEST_BACKOFF_BASE_SECONDS i=1
      while [ "$i" -lt "$fail_count" ] && [ "$backoff" -lt "$INGEST_BACKOFF_MAX_SECONDS" ]; do
        backoff=$((backoff * 2)); i=$((i + 1))
      done
      [ "$backoff" -gt "$INGEST_BACKOFF_MAX_SECONDS" ] && backoff=$INGEST_BACKOFF_MAX_SECONDS
      log "SWIM bridge $reason -- relaunching (fail streak: $fail_count$( [ "$fail_count" -gt 0 ] && echo ", backoff ${backoff}s" ))"
      [ "$fail_count" -gt 0 ] && sleep "$backoff"
      cmd_ingest "$s"
      last_launch_ts=$(date +%s)
    fi

    sleep "$INGEST_POLL_SECONDS"
  done
}

cmd_all(){ local s="${1:-$INGEST_SECONDS}"; cmd_setup; cmd_consume; cmd_adsb; cmd_ingest "$s"; log "Collecting ${s}s..."; sleep "$s"; cmd_pipeline; }

cmd_status(){
  docker compose -f "$COMPOSE_FILE" ps || true
  echo "--- processes ---"
  ps aux | grep -v grep | egrep 'swim_to_kafka|kafka_to_postgres|adsb_poller' || echo "(none running)"
  resolve_containers 2>/dev/null && {
    echo "--- rows / coverage ---"
    pg -c "SELECT count(*) AS raw, round(extract(epoch from now()-min(stored_at))/3600,1) AS raw_age_h FROM $RAW_TABLE;" 2>/dev/null || true
    pg -c "SELECT count(*) AS silver, count(hex) AS with_hex, round(100.0*count(hex)/NULLIF(count(*),0),1) AS hex_pct FROM flight_instance;" 2>/dev/null || true
    pg -c "SELECT count(*) AS adsb_store FROM adsb_airframe;" 2>/dev/null || true
  } || true
}

cmd_stop(){ pkill -f swim_to_kafka.py 2>/dev/null || true; pkill -f kafka_to_postgres.py 2>/dev/null || true; pkill -f adsb_poller.py 2>/dev/null || true; echo "stopped bridge + consumer + poller"; }
cmd_down(){ docker compose -f "$COMPOSE_FILE" down; }

case "${1:-help}" in
  setup) cmd_setup ;; consume) cmd_consume ;; adsb) cmd_adsb ;; ingest) cmd_ingest "${2:-}" ;;
  pipeline) cmd_pipeline ;; stream) cmd_stream "${2:-}" ;; retention) cmd_retention ;;
  sync) cmd_sync ;; all) cmd_all "${2:-}" ;; status) cmd_status ;;
  stop) cmd_stop ;; down) cmd_down ;;
  *) sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//' ;;
esac