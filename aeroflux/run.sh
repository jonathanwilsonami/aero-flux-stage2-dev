#!/usr/bin/env bash
# AeroFlux — single-entry runner. From a fresh machine to gold, and a real-time
# streaming mode that keeps silver/gold fresh with a rolling 48h of raw data.
#
#   ./run.sh setup             # one-time: infra up (Kafka+Postgres), topic, tables
#   ./run.sh stream 3600       # REAL-TIME: bridge+consumer+poller + refresh loop
#   ./run.sh pipeline          # one-shot: raw -> silver -> load -> gold (+weather)
#   ./run.sh retention         # purge raw + adsb store older than 48h
#   ./run.sh sync              # export gold/silver to STORAGE_DEST (local or s3://)
#   ./run.sh status | stop | down
#
# Everything is env/.env-overridable, so the same script targets cloud. Set
# WEATHER=1 to add live METAR features; STORAGE_DEST=s3://bucket/path to sync.
set -euo pipefail

[ -f .env ] && { set -a; . ./.env; set +a; }
ROOT="$(cd "$(dirname "$0")" && pwd)"

PG_HOST="${POSTGRES_HOST:-localhost}"; PG_PORT="${POSTGRES_PORT:-5432}"
PG_USER="${POSTGRES_USER:-aeroflux}";  PG_DB="${POSTGRES_DB:-aeroflux}"
PG_PASS="${POSTGRES_PASSWORD:-aeroflux-db}"
DSN="${DSN:-postgresql://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${PG_DB}}"

COMPOSE_FILE="${COMPOSE_FILE:-compose.yaml}"
TOPIC="${KAFKA_TOPIC:-swim.raw.flight}"
RAW_TABLE="${RAW_TABLE:-swim.raw_messages}"; RAW_COLUMN="${RAW_COLUMN:-raw_xml}"
LIMIT="${LIMIT:-500000}"; LIVE="${LIVE:-100}"; OUT="${OUT:-$ROOT/out}"
INGEST_SECONDS="${INGEST_SECONDS:-3600}"
REFRESH_SECONDS="${REFRESH_SECONDS:-300}"
RETENTION_HOURS="${RETENTION_HOURS:-48}"
WEATHER="${WEATHER:-0}"
STORAGE_DEST="${STORAGE_DEST:-}"

log(){ printf "\n\033[1;36m==> %s\033[0m\n" "$*"; }
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
  log "Setup complete. Real-time: ./run.sh stream 3600  |  one-shot: ./run.sh pipeline"
}

cmd_consume(){ resolve_containers; nohup python kafka_to_postgres.py > kafka_to_postgres.log 2>&1 & echo "consumer started (PID $!, kafka_to_postgres.log)"; }
cmd_adsb(){ resolve_containers; nohup python adsb_poller.py > adsb_poller.log 2>&1 & echo "adsb poller started (PID $!, adsb_poller.log)"; }
cmd_ingest(){ local s="${1:-$INGEST_SECONDS}"; nohup python swim_to_kafka.py --duration "$s" > swim_to_kafka.log 2>&1 & echo "bridge started ${s}s (PID $!, swim_to_kafka.log)"; }

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
  local s="${1:-86400}"
  cmd_setup
  log "REAL-TIME mode: bridge + consumer + poller, refreshing every ${REFRESH_SECONDS}s"
  cmd_consume; cmd_adsb; cmd_ingest "$s"
  trap 'echo; log "stopping stream"; cmd_stop; exit 0' INT TERM
  local elapsed=0
  while [ "$elapsed" -lt "$s" ]; do
    sleep "$REFRESH_SECONDS"; elapsed=$((elapsed + REFRESH_SECONDS))
    cmd_pipeline || log "pipeline refresh hiccup (continuing)"
    cmd_retention || true
    cmd_status || true
  done
  cmd_stop
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