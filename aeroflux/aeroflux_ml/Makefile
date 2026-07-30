# AeroFlux — one-command orchestration. Runs the same on your laptop, another
# machine, or a cloud VM (anything with Docker + Python). Override any variable:
#   make ingest DURATION=86400
#   make all LIMIT=500000
#
# Prereqs: bootstrap.sh has been run once (installs deps, brings up infra).

DSN     ?= postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux
TABLE   ?= swim.raw_messages
COLUMN  ?= raw_xml
LIMIT   ?= 500000
DURATION?= 3600          # ingest seconds (86400 = full day)
LIVE    ?= 100           # ADS-B tail resolutions (0 to skip)
OUT     ?= ./out
KAFKA   ?= aeroflux-kafka
TOPIC   ?= swim.raw.flight

.PHONY: help up down topic ingest consume transform load features weather all status stop clean

help:
	@grep -E '^[a-z-]+:.*?#' $(MAKEFILE_LIST) | sed 's/:.*#/\t/'

up:            # start infra (Kafka, Postgres, MinIO, Spark)
	docker compose -f infra/docker-compose.yml up -d
	@echo "waiting for Kafka..."; sleep 8

topic:         # create the SWIM topic (idempotent)
	docker exec $(KAFKA) /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
	  --create --topic $(TOPIC) --partitions 1 --replication-factor 1 --if-not-exists

consume:       # start the Kafka->Postgres consumer (background)
	nohup python kafka_to_postgres.py > kafka_to_postgres.log 2>&1 &
	@echo "consumer started (kafka_to_postgres.log)"

ingest:        # run the SWIM->Kafka bridge for DURATION seconds (background)
	nohup python swim_to_kafka.py --duration $(DURATION) > swim_to_kafka.log 2>&1 &
	@echo "bridge started for $(DURATION)s (swim_to_kafka.log)"

transform:     # raw -> silver dataset.jsonl/csv (with ADS-B hex via --live)
	python build_dataset.py postgres --dsn "$(DSN)" --table $(TABLE) --column $(COLUMN) \
	  --limit $(LIMIT) --live $(LIVE) --out-jsonl dataset.jsonl --out-csv dataset.csv

load:          # load silver into Postgres flight_instance
	psql "$(DSN)" -c "TRUNCATE flight_instance;"
	psql "$(DSN)" -c "\copy flight_instance (schema_version, flight_instance_id, gufi, flight_ref, callsign, flight_number, carrier_icao, carrier_iata, carrier_name, resolution_status, tail_number, tail_source, hex, aircraft_type, aircraft_category, origin, destination, scheduled_gate_departure, scheduled_gate_arrival, estimated_arrival, actual_off, actual_on, flight_status, last_latitude, last_longitude, last_altitude_ft, last_ground_speed, last_position_time) FROM 'dataset.csv' WITH (FORMAT csv, HEADER true, NULL '')"

features:      # silver -> gold ML table
	python -m aeroflux_ml.run postgres --dsn "$(DSN)" --table public.flight_instance --out $(OUT)

weather:       # silver -> gold ML table WITH live METAR weather features
	python -m aeroflux_ml.run postgres --dsn "$(DSN)" --table public.flight_instance --out $(OUT) --weather live

all: transform load features   # full batch: raw -> silver -> load -> gold

status:        # health of the moving parts
	@docker compose -f infra/docker-compose.yml ps
	@ps aux | grep -v grep | egrep 'swim_to_kafka|kafka_to_postgres' || echo "(no bridge/consumer running)"
	@psql "$(DSN)" -c "SELECT count(*) AS raw FROM $(TABLE);" 2>/dev/null || true
	@psql "$(DSN)" -c "SELECT count(*) AS silver FROM flight_instance;" 2>/dev/null || true

stop:          # stop the app processes (leave infra up)
	-pkill -f swim_to_kafka.py; -pkill -f kafka_to_postgres.py

down:          # stop infra
	docker compose -f infra/docker-compose.yml down

clean:         # remove generated artifacts
	rm -f dataset.jsonl dataset.csv $(OUT)/gold_features.* $(OUT)/predictions.*
