# CLAUDE.md — AeroFlux Stage 2

Auto-loaded context for Claude Code. Read `PROJECT_CONTEXT.md` for the full story,
`AeroFlux_DataSchemas.md` for the data contract, and `AeroFlux_DataDictionary.md`
for the feature contract. Keep this file current as the project moves.

## What this is
AeroFlux is a real-time flight-delay prediction platform over FAA SWIM + ADS-B +
weather. Course: GMU **AIT 614** (Big Data). The thesis is **network-state delay
propagation**: aircraft rotation is *one nullable channel*; airport demand,
weather, and schedule are the always-available backbone. Train on historical BTS,
serve on live SWIM, with **train/serve parity by construction**.

## Golden rules (do not break these)
1. **Parity is sacred.** Training (BTS) and serving (live) must compute the *same*
   features. Both go through `aeroflux_ml/feature_prep.py`. If you add/change a
   feature, it changes there once, for both paths. Never fork the logic.
2. **No leakage.** Weather and any time-derived feature use the **score time =
   scheduled departure** (the prediction moment), never actual arrival. Never use
   `dep_delay_min`/`arr_delay_min` as model features (they're outcomes/labels).
3. **GUFI is the merge key, not `flight_ref`** (flight_ref changes on plan
   amendment). Dedup GUFI-fallback duplicates.
4. **Missing ≠ 0 unless it truly is.** `feature_prep` fills rotation/demand with 0
   (absence == 0) and leaves weather/recent-delay null (XGBoost routes NaN). Don't
   blanket-fill.
5. **Real data over fixtures. Validate each layer before advancing. Tests pass
   before moving on** (`python -m pytest`, currently 79 passing).

## Repo map (project dir: `aeroflux/`)
- `aeroflux_parser/` — SWIM parse → fuse → canonical silver. Key: `fusion`,
  `identity`, `adsb`/`adsb_store` (airframe store), `airports`/`airlines` dims.
- `aeroflux_ml/` — the ML side:
  - `schema.py` — `from_bts` / `from_silver` adapters (→ one canonical frame, tz-aware)
  - `channels.py`, `engineer.py` — feature channels + `FeatureEngineer`
  - `feature_prep.py` — **the feature contract** (fill policy, parity set, propagation)
  - `weather.py` — METAR live (AWC) + historical (IEM); NCEI fetch
  - `weather_cache.py` — cache-first NCEI loader + station→ICAO bridge
  - `bts_source.py` — BTS fetch/cache/discover-local-CSV
  - `pipeline.py` — silver → gold; `run.py` — pipeline CLI
  - `score_live.py` — score live gold → predictions (parquet + Postgres)
  - `training/` — configurable ML pipeline (see below)
- `scripts/build_bts_gold.py` — BTS → gold training table
- `configs/` — `pipeline.yaml`, `training.yaml`
- `streamlit_app/` — demo UI (Home, Live Map, Analyst, Live Inference)
- `run.sh` — live ingestion orchestrator · `e2e.sh` — full train→serve→UI
- `tests/` — `test_ml.py`, `test_parse.py`, `test_training.py`

## Key commands
```bash
./run.sh setup                                   # Kafka + Postgres + tables
./run.sh stream 3600                             # live ingest (bridge+consumer+poller+gold+retention)
python scripts/build_bts_gold.py --months 2015-01:2015-12 --cache data/bts \
    --weather-cache data/weather --station-bridge data/reference/airport_to_station_2019.csv
python -m aeroflux_ml.training.cli train --config configs/training.yaml --gold <gold.parquet>
python -m aeroflux_ml.score_live --run-dir <run> --gold out/gold_features.parquet --out out/predictions.parquet
./e2e.sh up | health | down                      # everything, with health checks
python -m pytest                                 # 79 tests
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"
```

## Training pipeline (`aeroflux_ml/training/`)
Config-driven (YAML). Components: `config`, `data` (time-aware split + CV),
`preprocess` (reuses feature_prep), `validation`, `models/` (XGBoost + logistic
now; Spark ML adapter + TF stub as extension points), `tune` (grid + time-aware
CV), `evaluate`, `compare`, `registry` (local runs + optional MLflow), `runner`,
`cli`. Add a model = add a class in `models/` + a registry entry + a YAML block.

## Gotchas that already bit us (don't rediscover)
- SWIM broker is **`ems2`** not ems1; feed is **`flight-data`** (TFMS) not flight-delay-tfms.
- ADS-B 403 = Cloudflare blocks default urllib UA → set a descriptive `ADSB_USER_AGENT`.
- Postgres `timestamptz` → Polars: read with `infer_schema_length=None`.
- METAR: chunk station requests (414) and retry/backoff (504); a live *batch* over
  48h of flight plans shows ~3% weather — that's a **time-window mismatch, not a
  bug** (plans are future-heavy). Real-time serving + historical training are fine.
- BTS `HHMM` come as floats when a column has nulls → cast via Int; BTS publishes
  ~2–3 months late (recent months 404).
- NCEI sentinels: wind `999.9 m/s`, ceiling `≥22000 m` → null.

## Current state / next
Milestones hit: live ingest, BTS↔live parity, cached-weather training, feature
contract, training pipeline, live scoring, E2E orchestration, demo UI. 79 tests
pass. See `PROJECT_CONTEXT.md` §Roadmap. **First: confirm `git main` reflects all
local work (it drifted during rapid iteration) so a fresh clone runs.**
