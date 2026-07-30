# AeroFlux

Real-time flight-delay prediction and cyber-physical intelligence platform.
Ingests FAA SWIM (+ ADS-B, weather), fuses it into a validated per-flight
state, and produces ML features and predictions — with train/serve parity so a
model trained on BTS history runs unchanged on live data.

## Structure

```
aeroflux/
├── run.sh                 # single entry point (setup / ingest / pipeline)
├── compose.yaml           # infra: Kafka + Postgres
├── .env.example           # config template (copy to .env, fill secrets)
├── schema.sql             # swim.raw_messages (bronze) DDL
├── swim_to_kafka.py       # SWIM -> Kafka bridge
├── kafka_to_postgres.py   # Kafka -> Postgres consumer
├── check_setup.py         # connectivity preflight
├── inspect_postgres.py    # DB inspection helper
├── aeroflux_parser/       # PACKAGE: parse -> fuse -> resolve -> validate (silver)
│   └── data/airlines.csv  #   bundled ICAO<->IATA airline crosswalk
├── aeroflux_ml/           # PACKAGE: feature engineering + inference (gold)
├── scripts/               # parser CLIs: build_dataset, build_features, ...
├── configs/pipeline.yaml  # ML feature/model config
├── spark/streaming_job.py # streaming wrapper (scaffold)
├── tests/                 # test_parse.py + test_ml.py (66 tests)
└── samples/               # sample SWIM XML
```

## Quickstart

See **RUNGUIDE.md** for the full walkthrough (team onboarding + cloud). Short version:

```bash
pip install -e .
cp .env.example .env        # fill in SWIM + Postgres values
./run.sh setup              # infra up, topic, tables
./run.sh all 3600           # ingest 1h, then raw -> silver -> load -> gold
```

## Data layers

- **bronze** — raw SWIM messages + lineage (`swim.raw_messages`)
- **silver** — fused, validated per-flight state (`flight_instance`)
- **gold** — ML feature/label table (`out/gold_features.parquet`)

## Tests

```bash
pip install -e ".[dev]" && pytest -q      # 66 tests
```
