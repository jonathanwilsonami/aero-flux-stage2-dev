#!/usr/bin/env bash
# AeroFlux — one-time automated setup. Run once on a fresh machine or cloud VM.
# Portable: needs only Docker + Python 3.11. Idempotent — safe to re-run.
#
#   ./bootstrap.sh
#
# What it does: checks prereqs, installs Python deps, brings up infra
# (Kafka/Postgres/MinIO), creates the SWIM topic and DB tables. After this,
# use the Makefile for day-to-day runs (`make all`, `make ingest DURATION=86400`).

set -euo pipefail

DSN="${DSN:-postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux}"
KAFKA="${KAFKA:-aeroflux-kafka}"
TOPIC="${TOPIC:-swim.raw.flight}"

echo "==> checking prerequisites"
command -v docker >/dev/null || { echo "Docker is required"; exit 1; }
command -v python >/dev/null || { echo "Python is required"; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "Docker Compose v2 is required"; exit 1; }

echo "==> installing Python dependencies"
pip install -e . >/dev/null
pip install polars xgboost pyarrow pyyaml pandas psycopg2-binary confluent-kafka requests >/dev/null
# parser package too, if present alongside
[ -d ../aeroflux_parser ] && pip install -e ../aeroflux_parser >/dev/null || true

echo "==> bringing up infrastructure (Kafka, Postgres, MinIO, Spark)"
docker compose -f infra/docker-compose.yml up -d
echo "    waiting for Kafka to be ready..."; sleep 10

echo "==> creating Kafka topic '$TOPIC'"
docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 --if-not-exists || true

echo "==> creating database tables"
if [ -f schema.sql ]; then psql "$DSN" -f schema.sql; fi
psql "$DSN" <<'SQL'
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

echo "==> done. Next:"
echo "    make ingest DURATION=86400   # collect a full day (background)"
echo "    make consume                 # start the Kafka->Postgres consumer"
echo "    make all                     # raw -> silver -> load -> gold"
echo "    make status                  # check health"
