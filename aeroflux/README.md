# AeroFlux Minimal SWIM → Kafka → PostgreSQL Pipeline

## What connects to what

There are **two different message brokers**:

- FAA SWIM/SWIFT uses a remote **Solace** broker.
- Your computer runs a local **Kafka** broker.

They do not connect automatically. `swim_to_kafka.py` is the bridge. It opens
one connection to the FAA Solace queue and a second connection to local Kafka.
For each FAA message, it publishes the raw XML to the Kafka topic
`swim.raw.flight`.

`kafka_to_postgres.py` is a separate consumer. It reads that Kafka topic and
inserts each raw message into `swim.raw_messages` in PostgreSQL.

```text
FAA SWIM/SWIFT Solace broker
          |
          | SCDS_HOST + VPN + username + password + queue
          v
    swim_to_kafka.py
          |
          | localhost:9092 / topic swim.raw.flight
          v
    Local Kafka broker
          |
          v
  kafka_to_postgres.py
          |
          | PostgreSQL host/database/user/password
          v
 PostgreSQL swim.raw_messages
```

## 1. Enter the project and create Python environment

```bash
cd aeroflux-swim-postgres
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## 2. Create `.env`

```bash
cp .env.example .env
nano .env
```

Fill in your PostgreSQL values and the five FAA-provided `SCDS_*` values.
The `SCDS_QUEUE_FLIGHT` value must be the queue assigned to your subscription.

## 3. Make sure PostgreSQL is running

```bash
sudo systemctl start postgresql
sudo systemctl status postgresql --no-pager
```

Apply the schema if necessary:

```bash
PGPASSWORD='YOUR_POSTGRES_PASSWORD' psql \
  -h localhost -U aeroflux -d aeroflux -f schema.sql
```

## 4. Start Kafka

```bash
docker compose up -d
```

Check the broker:

```bash
docker compose ps
docker logs aeroflux-kafka --tail 50
```

Create the topic explicitly:

```bash
docker exec aeroflux-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic swim.raw.flight \
  --partitions 1 \
  --replication-factor 1
```

Describe it:

```bash
docker exec aeroflux-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic swim.raw.flight
```

## 5. Verify PostgreSQL and Kafka before using SWIM

```bash
source .venv/bin/activate
python check_setup.py
```

Both lines should say `OK`.

## 6. Test Kafka → PostgreSQL with mock SWIM data

Terminal 1:

```bash
source .venv/bin/activate
python kafka_to_postgres.py
```

Terminal 2:

```bash
source .venv/bin/activate
python swim_to_kafka.py --mock --max-messages 5
```

Terminal 1 should print five `Stored ...` messages.

Inspect PostgreSQL:

```bash
python inspect_postgres.py --limit 10
```

Or use SQL:

```bash
PGPASSWORD='YOUR_POSTGRES_PASSWORD' psql -h localhost -U aeroflux -d aeroflux
```

```sql
SELECT id, stored_at, message_types, payload_size_bytes
FROM swim.raw_messages
ORDER BY id DESC
LIMIT 10;
```

At this point you have proven:

```text
Python mock producer → Kafka → Python consumer → PostgreSQL
```

## 7. Test the live FAA SWIM connection

Keep `kafka_to_postgres.py` running in terminal 1.

In terminal 2, remove `--mock`:

```bash
source .venv/bin/activate
python swim_to_kafka.py --max-messages 10
```

The bridge will:

1. Connect to `SCDS_HOST` using your FAA credentials.
2. Bind to `SCDS_QUEUE_FLIGHT`.
3. Receive raw XML from Solace.
4. Publish the XML to Kafka.
5. Acknowledge the Solace message only after Kafka confirms the write.

To collect for five minutes instead:

```bash
python swim_to_kafka.py --duration 300
```

## 8. Optional: see Kafka messages directly

This bypasses PostgreSQL and proves that Kafka contains events:

```bash
docker exec -it aeroflux-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic swim.raw.flight \
  --from-beginning \
  --max-messages 1
```

## Start order for normal use

```text
1. PostgreSQL service
2. Kafka container
3. kafka_to_postgres.py
4. swim_to_kafka.py
```

## Stop

Stop the Python programs with Ctrl-C, then:

```bash
docker compose down

# docker start aeroflux-kafka
```

# Build Dataset 
```bash 
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"
python -m pytest -q 


# a) clear old output files
rm -f dataset.jsonl dataset.csv gold.csv gold.parquet dataset.invalid.jsonl

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
TRUNCATE flight_instance;
SQL 

# c) OPTIONAL: also wipe the raw landing table if you want a fully fresh ingest
# psql "$DSN" -c "TRUNCATE <swim_table>;" 


# make sure Kafka + Postgres are running, then run the bridge.
# live feed for ~1 hour:
python swim_to_kafka.py --duration 3600
# --- OR, to test with no live SWIM connection: ---
# python swim_to_kafka.py --mock --max-messages 200


# Transform: raw → silver (parse → fuse → validate)
python build_dataset.py postgres \
  --dsn "$DSN" --table swim.raw_messages --column raw_xml \
  --limit 6410 --out-jsonl dataset.jsonl --out-csv dataset.csv 


# (Alternative, skipping the raw table — read Kafka directly:)
python build_dataset.py kafka --topic swim.raw.flight --limit 6410 \
  --out-jsonl dataset.jsonl --out-csv dataset.csv


 ``` 