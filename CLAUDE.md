# CLAUDE.md — AeroFlux Stage 2

Auto-loaded context for Claude Code. Read `PROJECT_CONTEXT.md` for the full story,
`AeroFlux_DataSchemas.md` for the data contract, `AeroFlux_DataDictionary.md`
for the feature contract, and `DEPLOYMENT.md` for the cloud/Lightsail deploy
flow and gotchas. Keep this file current as the project moves.

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
   before moving on** (`python -m pytest`, currently 83 passing).
6. **The default model feature set is 18 features, weather included.**
   `feature_prep.feature_columns()` (default `include_gap_weather=False`) = 5
   structural + 5 rotation/propagation + 4 demand/recent-delay + 4 weather
   (`origin/dest_wx_wind_kt`, `origin/dest_wx_ifr` — wind + IFR only, both
   present in live METAR and BTS/NCEI). `temp_c`/`ceiling_ft`/`vis_mi` are the
   *actual* parity-gap weather (dense in BTS, null in live METAR) — those stay
   opt-in via `include_gap_weather=True`. The live pipeline's weather channel
   must be on (`WEATHER=1`, now the `run.sh` default) for the 4 wind/IFR
   features to be non-null live — this bit us once (see Gotchas).

## Secrets handling (hard rule)
**Never print, `cat`, `tail`, `head`, or `echo` the contents of `.env` files or
any file that may hold credentials** (AWS keys, DSNs with passwords, tokens,
API secrets) — locally, on the box, anywhere. This includes indirectly, e.g.
`tail -N` on a `.env` to check the last couple of lines you just added: it
prints everything else in that range too. This rule exists because it was
broken once — a `tail -5 .env` on the Lightsail box to confirm two new lines
landed also printed the real `AWS_SECRET_ACCESS_KEY` into the session
transcript.
- To inspect an env file: `grep -v -i 'secret\|password\|key\|token' file` to
  exclude secret-bearing lines, or `grep '^VAR_NAME='` to check one specific
  non-secret variable by name.
- To edit one: write the specific key(s) directly (e.g. a `grep -q ... ||
  echo "VAR=val" >> .env` guard, matching what `sync_cloud`/deploy tooling
  already does), or use an interactive editor (`nano`/`vim`) — never a
  command that echoes the whole file to append/modify it.
- To confirm a variable is *set* (not its value): test its effect — does the
  AWS call succeed, does the DB connect — rather than printing it.
- If a secret is ever exposed in a transcript or log despite this, say so
  plainly and recommend rotation; don't just quietly move on.

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
  - `io.py` — **StateRepository/LakeStore abstraction** (the cloud storage
    seam): `PostgresStateRepository`/`DynamoDBStateRepository`,
    `LocalLakeStore`/`S3LakeStore`, selected via `STATE_BACKEND`/
    `LAKE_BACKEND` env vars through `state_backend_from_env()`/
    `lake_backend_from_env()` — see `DEPLOYMENT.md`
  - `sync_cloud.py` — local → cloud sync step (gold → LakeStore, current
    state + predictions → StateStore); no-op unless a cloud backend is
    selected
  - `training/` — configurable ML pipeline (see below)
- `scripts/build_bts_gold.py` — BTS → gold training table
- `scripts/sync_cloud.sh`, `scripts/smoke_cloud_backends.py` — cloud sync
  wrapper + a standalone DynamoDB/S3 round-trip smoke test (run before
  trusting any cloud-backend change)
- `configs/` — `pipeline.yaml`, `training.yaml`
- `aeroflux_ui/streamlit_app/` — demo UI (Home, Live Map, Analyst, Live
  Inference); reads through `data_access.py`, which reads through the same
  `state_backend_from_env()`/`lake_backend_from_env()` factories as
  everything else — Postgres/local by default, DynamoDB/S3 when deployed.
  Also: `Dockerfile`, `docker-compose.lightsail.yml`, `Caddyfile` (deploy
  target — see `DEPLOYMENT.md`)
- `run.sh` — live ingestion orchestrator · `e2e.sh` — full train→serve→UI
  (now also: sync-to-cloud stage, duplicate-stack guard — `FORCE=1` to
  override)
- `deploy.sh` (repo root) — manual build/push/deploy/rollback/status for the
  Lightsail app; `.github/workflows/deploy-ui.yml` — CI build+push (+ gated
  auto-deploy)
- `tests/` — `test_ml.py`, `test_parse.py`, `test_training.py`

## Key commands
```bash
./run.sh setup                                   # Kafka + Postgres + tables
./run.sh stream 3600                             # live ingest (bridge+consumer+poller+gold+retention)
python scripts/build_bts_gold.py --months 2015-01:2015-12 --cache data/bts \
    --weather-cache data/weather --station-bridge data/reference/airport_to_station_2019.csv
python -m aeroflux_ml.training.cli train --config configs/training.yaml --gold <gold.parquet>
python -m aeroflux_ml.score_live --run-dir <run> --gold out/gold_features.parquet --out out/predictions.parquet
./e2e.sh up | health | down                      # everything, with health checks (FORCE=1 up to replace a running stack)
python -m pytest                                 # 83 tests
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"

# Cloud backends (local Postgres/filesystem by default — opt in via env, fully reversible):
export STATE_BACKEND=dynamodb LAKE_BACKEND=s3 AWS_PROFILE=aeroflux-local \
       AWS_REGION=us-east-1 S3_BUCKET=<bucket> DYNAMODB_TABLE=aeroflux-current-state
python scripts/smoke_cloud_backends.py                    # verify creds/perms before trusting anything else
./scripts/sync_cloud.sh                                    # one local->cloud sync cycle
./e2e.sh up                                                 # same env vars -> live stack syncs to cloud too
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
- **`interval '%s hours'` in psycopg silently mis-parses** — a bind parameter
  inside a quoted SQL string literal is not real substitution; it always came
  out as exactly 1 hour regardless of the value passed. Use
  `make_interval(hours => %s)` (a real function call) instead. Bit
  `PostgresStateRepository.recent_flight_states`.
- **boto3 rejects native `float` and `datetime`** for DynamoDB items — convert
  `float` → `Decimal(str(v))` (not `Decimal(v)`, avoids binary-float
  artifacts) and `datetime`/`date` → `.isoformat()`. See
  `DynamoDBStateRepository._dynamo_value`.
- **DynamoDB `Scan` with no `Limit` reads the whole table** — a 32-second,
  full-table scan every 30s page-load is a real cost/latency problem, not a
  local-test quirk. `recent_flight_states(limit=N)` does a single-page
  `Scan(Limit=N)` (no `LastEvaluatedKey` pagination) to hard-bound it —
  demo-scale choice; a GSI + `Query` is the real scale path.
- **A `while true; do sync; sleep N; done` loop: `VAR="${VAR:-}"` on the
  subprocess command line is NOT the same as leaving `VAR` unset.** It sets
  an explicit empty string, which silently defeats every
  `os.getenv(NAME, default)` fallback downstream (broke
  `int(os.getenv("DYNAMODB_TTL_HOURS", "48"))` outright). Export
  conditionally (`[ -n "${!v:-}" ] && export "$v"`) instead.
- **Two un-torn-down `e2e.sh up` stacks running concurrently** looks exactly
  like "intermittent" bugs — same log file, interleaved output, doubled
  chance of hitting real races (e.g. a sync landing inside
  `flight_instance`'s `TRUNCATE`+reload window). `e2e.sh up` now refuses to
  start a second stack by default (`FORCE=1` to override) — see
  `DEPLOYMENT.md`.
- **Nginx from an old deploy left running on 80/443** blocks Caddy from
  binding those ports at all (`address already in use`) — `docker compose
  up` doesn't error loudly enough to make this obvious; check `sudo ss
  -tlnp` on the box first.
- **A container that failed to bind ports once (e.g. from the nginx
  conflict) needs `--force-recreate`, not just another `up -d`** — compose
  will happily restart the same broken network config otherwise.
- **GHCR images pushed via the built-in `GITHUB_TOKEN` are private by
  default**, even in a public repo — the deploy box's anonymous `docker pull`
  gets 401 until you flip the package to public (or give the box a
  `read:packages` token).
- **`python:3.11-slim` has no `curl`** — `HEALTHCHECK CMD curl ...` reports
  "unhealthy" forever despite the app genuinely serving traffic. Use stdlib
  `python -c "import urllib.request; ..."` instead.

## Current state / next
Milestones hit: live ingest, BTS↔live parity, cached-weather training, feature
contract, training pipeline, live scoring, E2E orchestration, demo UI, cloud
storage backends (S3/DynamoDB) with `data_access.py` reading through them,
Lightsail deployment (Docker + Caddy + GHCR CI). 83 tests pass. See
`PROJECT_CONTEXT.md` §Roadmap and `DEPLOYMENT.md` for the deploy flow. Next:
evaluation work; the Spark batch analytics job and `AGENT_INTEGRATION.md` are
still open from the original AWS-storage plan.
