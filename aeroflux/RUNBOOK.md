# AeroFlux — Run Workflow (cold start to gold data)

Complete, from-nothing runbook: no services running, no tables, no Kafka topic.
Follow top to bottom. Every stage has **run** commands and **check** commands.

> **Verify-these notes:** a few exact names live inside your own files
> (`compose.yaml`, `schema.sql`, `kafka_to_postgres.py`). Confirmed values used
> here: Kafka container `aeroflux-kafka`, topic `swim.raw.flight`, raw table
> `swim.raw_messages`, silver table `flight_instance`.

## The pipeline

```
swim_to_kafka.py  ->  Kafka topic swim.raw.flight  ->  kafka_to_postgres.py
   -> swim.raw_messages (raw XML)  ->  build_dataset.py  -> dataset.jsonl/csv (silver)
   -> \copy -> flight_instance (silver in Postgres)  ->  build_features.py -> gold.parquet (ML)
```

**Startup order matters.** The Kafka **topic must exist before the consumer
starts**, or the consumer dies with `UNKNOWN_TOPIC_OR_PART`. So the order is:
infra → tables → **create topic** → bridge → consumer. Two long-running
processes — the bridge (`swim_to_kafka.py`) and the consumer
(`kafka_to_postgres.py`) — starting one does NOT start the other.

---

## 0. One-time setup

```bash
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"
cd ~/Documents/grad_school/big_data/project/aeroflux2/aeroflux-swim-postgres_proto

conda activate or568_ml_project
pip install -e ./aeroflux_parser 2>/dev/null; pip install pydantic pandas pyarrow confluent-kafka psycopg2-binary python-dotenv
cat .env      # confirm SCDS_*, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DB settings
```

---

## 1. Start infrastructure (Kafka + Postgres)

```bash
docker compose up -d
```

**If you see a name conflict** (`container "/aeroflux-kafka" is already in use`)
— a stale container from a prior run is holding the name. Clear it and retry:

```bash
docker compose down --remove-orphans
docker compose up -d
# if it still conflicts, remove the stray container directly (safe; it recreates):
docker rm -f aeroflux-kafka && docker compose up -d
```

**Check** — the broker is actually running (an empty `docker compose ps` means
it is NOT; that is the usual root cause of downstream "unknown topic" errors):

```bash
docker ps                       # aeroflux-kafka + postgres present and "Up"
docker compose ps               # both services "running"
pg_isready -d "$DSN"            # Postgres accepting connections
```

---

## 2. Create the tables (from empty)

```bash
psql "$DSN" -f schema.sql
```

Ensure the **silver** table exists (add it if `schema.sql` doesn't):

```bash
psql "$DSN" <<'SQL'
CREATE TABLE IF NOT EXISTS flight_instance (
  flight_instance_id text PRIMARY KEY, gufi text, flight_ref text, callsign text,
  flight_number text, carrier_icao text, carrier_iata text, carrier_name text,
  resolution_status text, tail_number text, tail_source text, hex text,
  aircraft_type text, aircraft_category text, origin text, destination text,
  scheduled_gate_departure timestamptz, scheduled_gate_arrival timestamptz,
  estimated_arrival timestamptz, actual_off timestamptz, actual_on timestamptz,
  flight_status text, last_latitude double precision, last_longitude double precision,
  last_altitude_ft integer, last_ground_speed integer, last_position_time timestamptz,
  updated_at timestamptz DEFAULT now()
);
SQL
```

**Check** — tables exist, empty, and note the payload column:

```bash
psql "$DSN" -c "\dt swim.*"
psql "$DSN" -c "\d swim.raw_messages"      # <-- WRITE DOWN THE PAYLOAD COLUMN NAME
psql "$DSN" -c "SELECT count(*) FROM swim.raw_messages;"   # expect 0
```

---

## 3. Create the Kafka topic (removes the ordering trap)

The `kafka-topics` CLI is not on your host PATH, but it lives inside the broker
container. Create the topic explicitly and idempotently:

```bash
docker exec aeroflux-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic swim.raw.flight --partitions 1 --replication-factor 1 --if-not-exists
```

**Check** — the topic now exists:

```bash
docker exec aeroflux-kafka kafka-topics --bootstrap-server localhost:9092 --list
# expect: swim.raw.flight
```

> Why this stage exists: if the broker has `auto.create.topics.enable=false`, a
> topic appears only after a producer first writes to it — so a consumer that
> starts first fails with `UNKNOWN_TOPIC_OR_PART`. Creating it here makes start
> order irrelevant.

---

## 4. Start the bridge (SWIM -> Kafka)

Live feed:

```bash
nohup python swim_to_kafka.py --duration 3600 > swim_to_kafka.log 2>&1 &
echo "bridge PID $!"
```

Or test the whole pipe with NO live SWIM connection (synthetic XML):

```bash
python swim_to_kafka.py --mock --max-messages 50
```

**Check** — it is publishing:

```bash
tail -20 swim_to_kafka.log     # "Published message N (... bytes) to swim.raw.flight"
```

---

## 5. Start the consumer (Kafka -> Postgres)

```bash
nohup python kafka_to_postgres.py > kafka_to_postgres.log 2>&1 &
echo "consumer PID $!"
```

**Check** — it is alive and NOT erroring on the topic:

```bash
ps aux | grep -v grep | grep kafka_to_postgres      # process is running
tail -20 kafka_to_postgres.log
# healthy:  "Listening to Kafka topic swim.raw.flight" / "Writing to ... swim.raw_messages"
# BAD:      "UNKNOWN_TOPIC_OR_PART"  -> topic missing, redo Stage 3 (and check broker is up)
```

---

## 6. Verify data is flowing end-to-end

The key check — the raw table count should be **rising**:

```bash
watch -n 3 'psql "'"$DSN"'" -c "SELECT count(*) FROM swim.raw_messages;"'
```

Peek at the topic directly (no host CLI needed):

```bash
python - <<'PY'
from confluent_kafka import Consumer
c = Consumer({"bootstrap.servers":"localhost:9092","group.id":"peek",
              "auto.offset.reset":"earliest"})
c.subscribe(["swim.raw.flight"])
for _ in range(3):
    m = c.poll(5.0)
    print("no message" if m is None else f"{len(m.value())} bytes: {m.value()[:120]}")
c.close()
PY
```

Interpreting it:
- Topic has messages **and** table rising -> works, go to Stage 7.
- Topic has messages but table flat -> **consumer** issue (`tail kafka_to_postgres.log`).
- Topic empty but bridge logs "Published" -> topic-name mismatch, or broker restarted.
- Consumer logs `UNKNOWN_TOPIC_OR_PART` -> topic/broker (Stages 1 + 3).
- Bridge logs no "Published" -> SWIM feed / credentials (`tail swim_to_kafka.log`).

---

## 7. Transform raw -> silver (parse, fuse, validate)

Use the payload column you noted in Stage 2:

```bash
python build_dataset.py postgres \
  --dsn "$DSN" --table swim.raw_messages --column PAYLOAD_COLUMN \
  --limit 10000 --out-jsonl dataset.jsonl --out-csv dataset.csv
```

**Alternative — skip the raw table, read Kafka directly** (handy if the consumer
isn't working):

```bash
python build_dataset.py kafka --topic swim.raw.flight --limit 10000 \
  --out-jsonl dataset.jsonl --out-csv dataset.csv
```

**Check** — raw XML transformed into one clean flight:

```bash
wc -l dataset.jsonl
head -1 dataset.jsonl | python -m json.tool | head -30
head -3 dataset.csv
```

---

## 8. Load silver -> Postgres `flight_instance`

```bash
psql "$DSN" -c "TRUNCATE flight_instance;"

psql "$DSN" -c "\copy flight_instance (flight_instance_id, gufi, flight_ref, callsign, flight_number, carrier_icao, carrier_iata, carrier_name, resolution_status, tail_number, tail_source, hex, aircraft_type, aircraft_category, origin, destination, scheduled_gate_departure, scheduled_gate_arrival, estimated_arrival, actual_off, actual_on, flight_status, last_latitude, last_longitude, last_altitude_ft, last_ground_speed, last_position_time) FROM 'dataset.csv' WITH (FORMAT csv, HEADER true, NULL '')"
```

**Check** — rows loaded, ids unique, resolution mix sane:

```bash
psql "$DSN" -c "SELECT count(*) AS rows, count(DISTINCT flight_instance_id) AS ids FROM flight_instance;"
psql "$DSN" -c "SELECT resolution_status, count(*) FROM flight_instance GROUP BY 1 ORDER BY 2 DESC;"
psql "$DSN" -c "SELECT callsign, flight_number, origin, destination, flight_status FROM flight_instance LIMIT 10;"
```

`rows` should equal `ids`.

---

## 9. Build the gold ML table

```bash
python build_features.py --in dataset.jsonl
```

**Check** — readiness summary + the data itself:

```bash
head gold.csv
python -c "import pandas as pd; df=pd.read_parquet('gold.parquet'); print(df.shape); print(df.head())"
```

Reminder: from live SWIM the delay labels are proxies and label coverage is low
(few flights *complete* inside a 24-48h window). Clean features, sparse labels —
by design. Real training labels come from running the same transform over BTS.

---

## Consolidated check commands

```bash
docker ps                                                   # broker + db up
pg_isready -d "$DSN"
docker exec aeroflux-kafka kafka-topics --bootstrap-server localhost:9092 --list
ps aux | grep -v grep | egrep 'swim_to_kafka|kafka_to_postgres'
tail -5 swim_to_kafka.log ; tail -5 kafka_to_postgres.log
psql "$DSN" -c "SELECT count(*) FROM swim.raw_messages;"
psql "$DSN" -c "SELECT count(*) FROM flight_instance;"
wc -l dataset.jsonl gold.csv 2>/dev/null
```

---

## Reset / teardown

```bash
pkill -f swim_to_kafka.py ; pkill -f kafka_to_postgres.py
psql "$DSN" -c "TRUNCATE swim.raw_messages; TRUNCATE flight_instance;"
rm -f dataset.jsonl dataset.csv gold.csv gold.parquet dataset.invalid.jsonl
docker compose down --remove-orphans     # add -v to also wipe volumes + topics
```

---

## Troubleshooting

- **`container "/aeroflux-kafka" is already in use`** — stale container holds the
  name. `docker compose down --remove-orphans` then up; or `docker rm -f
  aeroflux-kafka` then up.
- **`docker compose ps` empty but containers exist** — project-name/compose-file
  mismatch; run from the folder with `compose.yaml`. Use `docker ps` to see the
  truth.
- **Consumer exits with `UNKNOWN_TOPIC_OR_PART`** — topic doesn't exist yet.
  Confirm broker is up (`docker ps`), then create the topic (Stage 3), then
  restart the consumer. Bridge-before-consumer also works because producing
  auto-creates the topic when enabled.
- **`kafka-console-consumer: command not found`** — CLI isn't on the host; use
  `docker exec aeroflux-kafka kafka-topics ...` or the Python peek in Stage 6.
- **`swim.raw_messages` empty but bridge logs "Published"** — consumer not
  draining; check it's running and reading `swim.raw.flight` -> `swim.raw_messages`.
  Fastest unblock: Stage 7 Kafka-direct path.
- **`flight_instance` empty after a rebuild** — `build_dataset.py` writes files;
  the Stage 8 `\copy` is the only thing that loads the table. Re-run Stage 8.
- **`No silver records read` from `build_features.py --dsn`** — table empty; run
  Stage 8, or use `build_features.py --in dataset.jsonl`.
