# AeroFlux Stage 2 — Project Context

Full context for anyone (human or Claude) picking up the project. Pairs with
`CLAUDE.md` (operational quick-reference), `AeroFlux_DataSchemas.md` (data
contract for the reasoning/agent team), and `AeroFlux_DataDictionary.md` (feature
dictionaries + train/live parity table).

---

## 1. Mission & thesis

AeroFlux predicts U.S. flight delays in real time and is a proof-of-concept for
**propagation-aware modeling of cascading state in tightly-coupled physical
systems** — flights are the first domain, not the last. Delays rarely occur in
isolation: a late arrival propagates through the same aircraft's rotation and
through shared airport queues, so a local disturbance becomes network-wide.

**Core thesis — network-state delay propagation:** aircraft rotation is *one
nullable channel*; airport demand, weather, and schedule form an always-available
backbone. The model degrades gracefully when rotation can't be resolved rather
than depending on it. Framing borrows the Swiss Cheese Model (Reason, 1997).

**Course:** GMU AIT 614 (Big Data), Dr. Duoduo Liao. **Team:** Jonathan Wilson
(platform, data, ML) and Ryan Sollom (intelligence/reasoning layer — RAG/agent).
~2.5-week build window.

---

## 2. Architecture

**Layered data flow (bronze → silver → gold):**
- **Bronze** — raw SWIM messages in Postgres `swim.raw_messages` (48h retention).
- **Silver** — fused, validated per-flight canonical state in `flight_instance`
  (Postgres, 48h). Keyed on **GUFI**; `flight_ref` is a reconciliation bridge only.
- **Gold** — ML feature/label table as partitioned Parquet (local MinIO / cloud S3).

**Two sources, one feature frame (train/serve parity by construction):**
- **Training** = BTS On-Time Performance (true gate times + tail numbers + dense
  weather). **Serving** = live SWIM + ADS-B (sparse, real-time).
- Both map through adapters (`schema.from_bts` / `schema.from_silver`) into one
  canonical frame, then through **one** `FeatureEngineer` and **one**
  `feature_prep` contract. Training and serving therefore compute identical
  features — parity is structural, not a convention that can drift.

**Compute:** Polars locally (data fits in memory); Spark Structured Streaming is
the scale-out target (the feature/inference core runs in `foreachBatch`
unchanged). Container-first, cloud-agnostic; local Postgres/MinIO swap to managed
RDS/S3/DynamoDB by config.

**Serving seam:** the live pipeline writes gold; `score_live.py` scores it with
the trained model → `predictions` (parquet + Postgres). The Streamlit UI and the
agent read predictions + current state. FastAPI is the intended front door
(planned). The agent (Ryan) reads via the data contract in
`AeroFlux_DataSchemas.md`.

**Cloud deployment (built, live):** all streaming/ML stays on the local
machine — nothing about the pipeline moved. A `sync_cloud.py` step (no-op
unless opted into) pushes gold to **S3** and current flight state +
predictions to **DynamoDB**, through the same `StateRepository`/`LakeStore`
abstraction (`aeroflux_ml/io.py`) that local Postgres/filesystem already used
— selected by `STATE_BACKEND`/`LAKE_BACKEND` env vars, so local dev is
unaffected unless you opt in. An always-on Streamlit container on a Lightsail
VM (Docker + Caddy for TLS, image built/pushed by GitHub Actions to GHCR)
reads *only* from S3 + DynamoDB via read-only `aeroflux-app` credentials —
`data_access.py` now reads through the same factories, not a hardcoded
`psycopg` connection. Full flow, gotchas, and required secrets: `DEPLOYMENT.md`.

---

## 3. Decision log (what we chose and why)

1. **GUFI, not `flight_ref`, is the merge key.** `flight_ref` changes on plan
   amendment (proven on real data); GUFI is stable. ~38–49% of live flights carry
   a GUFI; the rest fall back to `flight_ref` with dedup to collapse
   amendment-created duplicates.
2. **Airframe identity comes from ADS-B, not SWIM.** SWIM flight data omits the
   tail. A **bulk ADS-B poller** (≤1 req/s, full-NAS sweeps) builds a rolling 48h
   `callsign → hex → registration` store. Coverage climbed 0% → ~38% hex, ~52%
   tail as the poller ran — this is what makes the rotation channel come alive.
3. **Train on BTS, serve on live.** BTS has true gate actuals + tails for labels;
   live SWIM is features/state with sparse actuals.
4. **Weather = METAR for both sides, by default,** for maximal parity (same
   sensor): live from AWS/AWC, historical from the IEM ASOS archive — ICAO-keyed,
   UTC, no station bridge. **Exception (in use):** for the **10-year historical**
   training we use **cached NCEI** weather (already fetched — avoids re-pulling a
   decade), mapped to the same obs schema via `weather_cache.py` + a
   station→ICAO bridge (`airport_to_station_2019.csv`). Parity holds at the
   feature-schema level; only the historical *source* differs. **Note this in the
   proposal.**
5. **Weather score-time = scheduled departure (the prediction moment), for BOTH
   origin and destination — never actual arrival.** Using arrival-time weather is
   leakage that breaks live serving (the flight hasn't landed). Configurable via
   `cfg['weather_score_time']`. This was a real bug in the OR568 join.
6. **Missing-value policy (`feature_prep`):** *fill 0* where absence truly is zero
   (rotation features, demand counts — `inbound_resolved`/`legs_into_day` flag
   real-0 vs unknown); *leave null* where missing ≠ 0 (weather, recent-delay
   means — XGBoost routes NaN natively); *drop* structurally-unscoreable rows (no
   scheduled departure). Default model feature set is **parity-safe** — excludes
   `temp_c`/`ceiling_ft`/`vis_mi` (dense in BTS, null in live METAR); opt back in
   with `include_gap_weather=True` (adds missingness indicators).
7. **Propagation feature (2-week scope):** `propagation_pressure_min =
   max(0, prev_leg_arr_delay_min − turnaround_buffer_min)` — inbound delay that
   eats the turnaround. Simple, interpretable, parity-clean. Advanced options
   (airframe-graph cascade sim, GNN) are later.
8. **Stores:** Postgres (local hot silver + current state), DynamoDB (cloud
   current state), MinIO/S3 (lake). MongoDB was dropped (it created a 3-way
   inconsistency across text/table/diagram — settle on Postgres + DynamoDB).
9. **Storage abstraction reuses the existing seam, doesn't fork it.**
   `aeroflux_ml/io.py` already had a `StateRepository` Protocol
   (`InMemory`/`Sqlite` implementations) — extended with
   `PostgresStateRepository` (formalizes what `score_live.py` did inline)
   and `DynamoDBStateRepository`, plus a matching new `LakeStore` Protocol
   (`LocalLakeStore`/`S3LakeStore`). Every caller resolves through
   `state_backend_from_env()`/`lake_backend_from_env()` — no caller
   branches on which backend is active.
10. **DynamoDB: one item per `flight_key`, state and prediction as disjoint
    attribute groups on that item, not separate items.** The table has a
    single HASH key (no sort key) — `upsert_flight_state`/`upsert_prediction`
    each `UpdateItem` only their own named attributes, never a full-item
    `put_item`, so neither can clobber the other regardless of write order.
    `expires_at` (epoch-seconds `N` — DynamoDB TTL silently ignores any
    other type) is refreshed by whichever call writes last.
11. **`recent_flight_states` on DynamoDB is `Scan`+`FilterExpression`, not a
    GSI `Query` — an explicit demo-scale choice, not the permanent design.**
    An unbounded Scan took ~32s against the live table; capped with a
    single-page `Scan(Limit=N)` it's ~1s. The documented scale path is a GSI
    on `updated_at` if item counts ever justify it.
12. **Deployment: containers + GitHub Actions, not the old systemd+gunicorn
    Lightsail setup.** `docker-compose.lightsail.yml` (app + Caddy, app
    published directly on `:8501` so Caddy/TLS issues can never take the
    demo down), `.github/workflows/deploy-ui.yml` (build+push to GHCR on
    every push touching `aeroflux_ui/**`, using the built-in `GITHUB_TOKEN`
    — no PAT), `deploy.sh` for manual build/push/deploy/rollback. Full
    gotchas in `DEPLOYMENT.md`.

---

## 4. What's built (module inventory)

**`aeroflux_parser/`** — SWIM XML → canonical silver: `parse`/`parsers`,
`normalizers`, `fusion` (GUFI-keyed, source-priority), `identity` (carrier/tail
resolution), `adsb` + `adsb_store` (rolling airframe store), `enrich`,
`airports`/`airlines` dims (+ `data/*.csv`), `sinks`, `schema`, `result`.

**`aeroflux_ml/`** — `schema` (BTS/silver adapters + tz), `channels` +
`engineer` (feature channels + `FeatureEngineer`), `feature_prep` (fill policy +
parity set + propagation), `weather` (METAR live/historical + NCEI), `weather_cache`
(cache-first NCEI + bridge), `bts_source` (BTS fetch/cache/local-discovery),
`pipeline` + `run` (silver→gold + CLI), `inference`, `config`,
`score_live` (live scoring), `io` (**StateRepository/LakeStore** — Postgres/
DynamoDB, Local/S3, the cloud storage abstraction), `sync_cloud` (local→cloud
sync step), and **`training/`** (config, data, preprocess, validation,
models/{base,xgboost,logistic,sparkml,tensorflow-stub}, tune, evaluate,
compare, registry, runner, cli).

**Root:** `run.sh` (live ingest), `e2e.sh` (full train→serve→UI; sync-to-cloud
stage; refuses to start a duplicate stack), `deploy.sh` (manual Lightsail
build/push/deploy/rollback), `compose.yaml` (Kafka+Postgres), `schema.sql`,
`scripts/` (build_bts_gold, build_dataset, sync_cloud.sh,
smoke_cloud_backends.py, …), `configs/` (pipeline.yaml, training.yaml),
`aeroflux_ui/streamlit_app/` (Dockerfile, docker-compose.lightsail.yml,
Caddyfile), `.github/workflows/deploy-ui.yml`, `tests/` (97 pass).

**Docs:** `CLAUDE.md`, this file, `AeroFlux_DataSchemas.md`,
`AeroFlux_DataDictionary.md`, `DEPLOYMENT.md`, `E2E_RUNBOOK.md`, `RUNGUIDE.md`,
`CLOUD.md`.

---

## 5. Current state

- **97 tests passing** across parse, ML, and training.
- **Live coverage** (grows with poller uptime): hex ~38%, GUFI ~49%, tail ~52%,
  aircraft_type ~72%, airline-resolved ~78%. Rotation features live.
- **Weather** validated both ways: live METAR (thousands of obs/run) and cached
  NCEI historical (97–100% feature coverage on training gold). Live pipeline's
  weather channel is on by default (`WEATHER=1` in `run.sh`) — the deployed
  model's 18 default features include `wind_kt`/`ifr` (present both sides);
  `temp_c`/`ceiling_ft`/`vis_mi` stay opt-in (`include_gap_weather=True`,
  null in live METAR regardless).
- **BTS↔live parity** proven: one BTS gold + one live gold, same schema, features
  computed by the same core.
- **Training** works: baseline logistic vs XGBoost, time-aware split/CV, grid
  tuning, run registry with metrics/plots/artifacts.
- **Live scoring + E2E orchestration + demo UI** built and wired.
- **Cloud deployment live**: gold → S3, current state + predictions →
  DynamoDB (`sync_cloud.py`, no-op unless opted in via
  `STATE_BACKEND`/`LAKE_BACKEND`), `data_access.py` reads through the same
  factories the local path uses. Always-on Streamlit app on Lightsail
  (Docker + Caddy, GHCR via GitHub Actions) confirmed serving real synced
  data at `https://aeroflux.duckdns.org` — `mode: LIVE`, `~1s` read latency
  after the DynamoDB Scan-cost fix (was ~32s unbounded). Full flow and every
  gotcha hit getting there: `DEPLOYMENT.md`.

### Known limitations (state honestly; don't try to "fix" the inherent ones)
- Live rotation is bounded by ADS-B hex coverage (~38%) — not fixable in code;
  the design's graceful degradation is the answer.
- Live *batch* weather over a 48h plan-heavy window is inherently sparse (~3%) —
  expected; real-time serving and historical training are the real use cases.
- `airport_to_station_2019.csv` is a single-year bridge (~358 airports) — covers
  majors; unmapped stations' obs are dropped (graceful).
- **Live predictions skew pessimistic** (~80% flagged at-risk vs. the BTS
  training base rate of ~18–22%, i.e. `positive_rate` ≈0.29 in `run.json`).
  Confirmed NOT a feature-parity bug: with the live weather channel on
  (`WEATHER=1`), all 18 model features are genuinely present with real values
  (verified 2026-08-05) — the skew persists unchanged. Root cause is
  **train/serve distribution shift in missingness**: `prev_leg_arr_delay_min`,
  `turnaround_buffer_min`, `legs_into_day`, and the `origin/dest_recent_*_delay`
  features are dense in BTS training but mostly null live
  (`inbound_resolved = 0` for ~56% of live scoreable flights vs. rare in BTS,
  since BTS rotation is near-fully resolved). XGBoost's learned
  missing-value routing, tuned on BTS's rotation-mostly-known distribution,
  appears to route rotation-unresolved live flights toward higher predicted
  risk. Not a bug — a calibration issue. Candidate fixes (not yet done):
  train on a live-like missingness distribution (e.g. synthetically null out
  rotation/recent-delay for a fraction of BTS training rows to match live's
  ~44–56% unresolved rate), calibrate probabilities post-hoc (Platt scaling
  or isotonic regression against a live-labeled holdout), or add explicit
  missingness indicators (similar to the `include_gap_weather` indicator
  pattern) so the model can distinguish "genuinely low risk" from
  "features unknown" instead of conflating them.
- **Live-prediction evaluation is capped by a structural ground-truth
  coverage gap, not right-censoring — it will NOT self-correct by
  waiting.** (Revises the earlier "right-censoring, self-corrects over the
  coming days" framing — traced deeper on 2026-08-13 and that framing was
  wrong; this replaces it.) Live `arr_delay_min` is computed only from
  `actual_on` (`schema.py`'s `from_silver()`:
  `arr_delay_min = actual_arr − sched_arr`, `actual_arr` = `actual_on`),
  and `actual_on` is populated from exactly **one** source: SWIM's
  `arrivalInformation` message (`fusion.py`'s source-priority map —
  `"actual_on": ["arrivalInformation"]`, no ADS-B fallback;
  `normalizers.py`'s `normalize_arrival()` already flags it
  `"best-effort; structure varies"`). Traced live: of flights currently
  `flight_status = 'COMPLETED'` in Postgres, only **13.6–19.8%** have
  `actual_on` populated at all — the FAA feed mostly never sends this
  message, a property of the source data, not of our retention window.
  Across all 109 `gold_live` snapshots taken so far, only 6,272 of 365,330
  distinct flight_keys ever show `arr_delay_min` populated anywhere — a
  **1.72% lifetime completion-capture rate**. Worse, the flights that do
  resolve are a biased-easy subsample, not a fair one: 88.6% land early
  (negative `arr_delay_min`), median is −5min, p90 is 0min, and only 2.26%
  show ≥15min delay (BTS base rate is ~18–22%). Predicted-delayed flights
  resolve at roughly 63% the rate of predicted-not-delayed ones (2.31% of
  82,918 ever-flagged flight_keys vs. 3.68% of 12,432), consistent with
  delayed/irregular-ops flights being less likely to get a clean SWIM
  close-out. The 48h retention window is a secondary, minor contributor,
  not the primary cause: for flights that *do* resolve, capture happens
  quickly regardless of delay severity (median 2.3h from first prediction
  to captured outcome; only one case >24h, none beyond 40.4h) — the
  ceiling is set by whether `arrivalInformation` is ever sent at all, not
  by how long we retain data waiting for it. **Practical effect: today's
  live-eval numbers (~2% delay rate, ~0.59 AUC on 5,626 pairs) reflect a
  biased-easy subsample, not the model's real performance, and letting it
  run longer grows the pair *count* without correcting the skew.**
  Candidate fixes (not yet done, roughly in order of leverage): (1)
  **derive touchdown from ADS-B position data** (ground speed/altitude
  near the destination) instead of waiting on `arrivalInformation` — ADS-B
  coverage (~38–88% depending on flight phase) is already far better than
  the ~14–20% `arrivalInformation` rate; (2) **persist a separate,
  un-TTL'd flight-outcome ledger** (piggyback on `sync_cloud`'s cadence, a
  longer DynamoDB/S3 TTL than the operational 48h state) so a flight aging
  out of the serving window can still be reconciled later; (3) **extend
  `RETENTION_HOURS`** (48→96+) as a cheap, low-risk mitigation — recovers
  only the small tail near the current edge (~0.04% of resolved flights),
  not the structural gap. Tracked live on the app's Model Performance page
  (pending vs. resolved counts, per-lag-bucket table).
- **Live `arr_delay_min`, when captured, uses wheels-on touchdown, not
  gate arrival — a structural ~5–15min early bias.** `schema.py`'s
  `from_silver()` maps `actual_arr` to `actual_on`, sourced from SWIM's
  `arrivalInformation` message (corrected 2026-08-13 — earlier text here
  said "ADS-B"; it isn't, see the coverage-gap bullet above for the actual
  source and why that message is rare in the first place), while
  `from_bts()`'s `actual_arr` is BTS's `ARR_TIME` (actual *gate* arrival).
  These are different physical events — touchdown precedes gate arrival by
  taxi-in time — and live silver has no gate-arrival timestamp at all
  (`_FLIGHT_INSTANCE_COLS` only carries `actual_off`/`actual_on`, both
  runway events). Real but secondary to the coverage gap above: it nudges
  every *captured* live delay measurement slightly early, but doesn't
  explain why so few flights get measured at all. Not fixable without live
  gate-arrival data, which SWIM/ADS-B doesn't provide.

---

## 6. Roadmap (next work — good for Claude Code)

**Done** (was the roadmap; now history): `git main` synced; cloud storage
abstraction (`io.py` StateRepository/LakeStore, Postgres/DynamoDB +
Local/S3); `sync_cloud.py` (+ write-volume dedup, `SYNC_DEDUPE`); write-
ahead cost cut ~18x on DynamoDB (content-hash change detection — a GSI
was evaluated and deliberately not built, see Gotchas); `data_access.py`
reading through the same factories; ingest self-healing (SWIM
auto-reconnect + a real liveness health check, not just PID); live-
prediction evaluation (`evaluate_live.py`, multi-lag-bucket reconciliation)
+ the Model Performance analyst page, both local and cloud-aware (eval
outputs now sync to S3 too); Lightsail deployment (Docker + Caddy + GHCR
CI, `deploy.sh`, duplicate-stack guard) **with fully automated, hands-off
CI deploy now verified working** (fixed a `DEPLOY_ENABLED` secret-vs-
variable mixup, a wrong remote path, and a corrupted SSH key secret — see
`DEPLOYMENT.md` §6); `AGENT_INTEGRATION.md` written (wire contract,
production S3/DynamoDB read path, credentials, boundary — flags that the
UI doesn't render `citations` yet, only `answer`). See `DEPLOYMENT.md` for
the deploy flow and every gotcha hit getting there.

**Open:**
1. **Evaluation work** (current focus) — the live-eval skew is a
   structural ground-truth-coverage gap, not right-censoring (see Known
   Limitations); the pending backlog will keep growing pair *count*
   without correcting the skew. Next concrete step is one of the
   candidate fixes there (ADS-B-derived touchdown is the highest-leverage
   option), not just letting it run longer.
2. **Spark batch analytics job** — the original AWS-storage plan's item 4
   (containerized PySpark reading gold from the LakeStore, delay-rate/mean-
   risk aggregations by route/carrier/hour, written back as an `analytics/`
   table) was never built. `training/models/sparkml_model.py` and
   `spark/streaming_job.py` (scaffold) exist but neither is this.
3. **Distribution-shift skew** (see Known Limitations above) — none of the
   three candidate fixes implemented.
4. **Train + tune the real model** (`training.cli tune`) on the full 10-year
   gold if not already done; confirm XGBoost beats the logistic baseline on
   real data.
5. **Agent integration** (Ryan): wire the RAG/LangGraph analyst against the
   data contract now that `AGENT_INTEGRATION.md` exists — needs a
   dedicated read-only IAM identity provisioned (policies already exist,
   see `AGENT_INTEGRATION.md` §3) and, if citations matter, a small
   `2_Analyst.py` change to actually render them.
6. **Proposal polish:** note the NCEI-historical/METAR-live source split;
   settle the Postgres+DynamoDB story (drop MongoDB); condense Methodology
   to the big-data architecture; add data-source citations.
7. **Later (out of 2-week scope):** RDS/MSK/EMR if the local pipeline itself
   needs to move off this machine (currently only storage/serving moved —
   streaming/ML stays local by design); TAF forecasts for destination
   weather; TensorFlow model; full Spark streaming path; airframe-graph
   cascade simulation.

---

## 7. Conventions

Incremental and validation-first (test each layer before advancing). Code-first
over design docs. Real data over fixtures (bugs hide in fixtures). Modular and
portable (laptop → cloud with no re-architecting). Config over hardcoding
(YAML). Keep prose/docs concise. Update `CLAUDE.md` when structure or invariants
change.
