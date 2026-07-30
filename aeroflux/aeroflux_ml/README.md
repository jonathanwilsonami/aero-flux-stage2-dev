# AeroFlux ML — feature engineering + inference

Modular, config-driven feature engineering and model-agnostic inference, built
so **the same feature definitions serve both BTS training and live SWIM/ADS-B
inference** — and so adding or changing a feature or swapping a model never
requires rewriting the pipeline.

## What is tested vs. scaffolded

**Tested core (runs today, 9 passing tests):**
- `schema.py` — parity adapters `from_bts` / `from_silver` into one canonical frame.
- `channels.py` — feature channels (flight, rotation, airport_state ready;
  flow, weather are seams).
- `engineer.py` — config-driven `FeatureEngineer`.
- `inference.py` — `InferenceEngine` (XGBoost), column alignment, versioning.
- `io.py` — Parquet writer + local state repositories.

**Scaffold (real templates; need a cluster / cloud to run):**
- `spark/streaming_job.py` — Structured Streaming wrapper (`foreachBatch`) that
  calls the tested core per micro-batch.
- `infra/docker-compose.yml` — Kafka + Spark + Postgres + MinIO local stack.

## Run the whole pipeline (one command)

Reads your streamed silver data, builds the gold table (the model's input), and
writes it where you can inspect it — no Spark/cloud needed.

```bash
# from your live Postgres flight_instance table:
python -m aeroflux_ml.run postgres --dsn "$DSN" --table public.flight_instance --out ./out

# or from a dataset.jsonl the parser produced:
python -m aeroflux_ml.run jsonl --in dataset.jsonl --out ./out

# add --model models/xgb.json to also score, --state-db state.db to store predictions
```

Outputs land in `./out`: `gold_features.parquet` + `.csv` (for analysis /
training), and `predictions.*` if a model is given. The run prints a coverage
bar per feature so you can see what's populated.

Installed as a console script too: `aeroflux-ml postgres --dsn ...`.

## The parity guarantee (the important part)

Both sources map into ONE canonical frame; features are computed once on it.

```python
from aeroflux_ml import from_bts, from_silver, FeatureEngineer, load_config

cfg = load_config("configs/pipeline.yaml")
eng = FeatureEngineer(cfg.features)

train = eng.build_matrix(from_bts(bts_df))       # historical, has labels
serve = eng.build_matrix(from_silver(silver_df)) # live, features only
assert train.columns == serve.columns            # guaranteed
```

## Inference (needs only features, not labels)

```python
from aeroflux_ml import InferenceEngine
engine = InferenceEngine(cfg.model, feature_version=cfg.features.feature_version)
preds = engine.predict(serve)   # delay_probability, predicted_delayed, versions, prediction_key
```

Missing channels (e.g. weather off) are passed to XGBoost as NaN, not errors.
Each prediction carries `model_version` + `feature_version` + a deterministic
`prediction_key` so re-scoring is idempotent (the state repo upserts on it).

## Adding a feature (no pipeline rewrite)

1. Write a channel in `channels.py`, decorate with `@channel("name")`, list its
   outputs in `CHANNEL_OUTPUTS`.
2. Toggle it in `configs/pipeline.yaml` under `features.channels`.
   Both BTS and live pick it up automatically.

## Local setup

```bash
pip install -e ".[dev]"
pytest -q
docker compose -f infra/docker-compose.yml up -d      # Kafka, Spark, Postgres, MinIO
```

## AWS configuration (later, no core rewrite)

- **Lake:** point `write_table` at `s3://bucket/...` (env AWS creds); MinIO ->
  S3 is a URL change. Iceberg tables via Spark's Iceberg catalog.
- **NoSQL:** implement `StateRepository` for DynamoDB or MongoDB; swap in config.
- **Compute:** run `spark/streaming_job.py` on a single EC2 (cheap) or later
  EMR/Glue. Query the lake with Athena. Avoid always-on MSK/EMR/SageMaker for
  the demo; the structure supports adding them without touching the core.

## Build order (what's next)

1. Fill `flow` channel once EDCT/TMI normalizers land in the parser.
2. Add the `weather` channel: METAR source + temporal/geographic as-of join.
3. Wire `spark/streaming_job.py` against the compose stack end to end.
4. Swap the local state repo for DynamoDB/MongoDB; lake to S3 + Iceberg.
5. Add a categorical-encoding step for carrier/origin/destination if the model
   needs them as features (currently numeric channels only).

## Weather (METAR)

The `weather` channel does a temporal + geographic as-of join: for each flight,
the most recent station observation at or before `sched_dep` (origin) and
`sched_arr` (destination), matched by airport, within a staleness tolerance (no
future leakage). It keys on airport + time, so it populates on BOTH BTS and
live — unlike rotation.

```bash
# fetch live METAR for the airports in your data and enable the channel:
python -m aeroflux_ml.run postgres --dsn "$DSN" --table public.flight_instance --weather live
```

Sources (both free, no key): live = Aviation Weather Center; historical (for
BTS-aligned training) = Iowa Environmental Mesonet ASOS archive
(`fetch_metar_history`). Both emit the same obs schema, so weather features
match across train/serve.

## Orchestration & portable setup

One-time setup on any machine or cloud VM (needs Docker + Python):

```bash
./bootstrap.sh          # installs deps, brings up infra, creates topic + tables
```

Then day-to-day with the Makefile (override any var):

```bash
make ingest DURATION=86400     # collect a full day (background)
make consume                   # start Kafka->Postgres consumer
make all                       # raw -> silver -> load -> gold  (one command)
make weather                   # gold WITH live weather features
make status                    # health of infra + processes + row counts
```
