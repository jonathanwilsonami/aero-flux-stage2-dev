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
`pipeline` + `run` (silver→gold + CLI), `inference`, `io`, `config`,
`score_live` (live scoring), and **`training/`** (config, data, preprocess,
validation, models/{base,xgboost,logistic,sparkml,tensorflow-stub}, tune,
evaluate, compare, registry, runner, cli).

**Root:** `run.sh` (live ingest), `e2e.sh` (full train→serve→UI), `compose.yaml`
(Kafka+Postgres), `schema.sql`, `scripts/` (build_bts_gold, build_dataset, …),
`configs/` (pipeline.yaml, training.yaml), `streamlit_app/`, `tests/` (79 pass).

**Docs:** `CLAUDE.md`, this file, `AeroFlux_DataSchemas.md`,
`AeroFlux_DataDictionary.md`, `E2E_RUNBOOK.md`, `RUNGUIDE.md`, `CLOUD.md`.

---

## 5. Current state

- **79 tests passing** across parse, ML, and training.
- **Live coverage** (grows with poller uptime): hex ~38%, GUFI ~49%, tail ~52%,
  aircraft_type ~72%, airline-resolved ~78%. Rotation features live.
- **Weather** validated both ways: live METAR (thousands of obs/run) and cached
  NCEI historical (97–100% feature coverage on training gold).
- **BTS↔live parity** proven: one BTS gold + one live gold, same schema, features
  computed by the same core.
- **Training** works: baseline logistic vs XGBoost, time-aware split/CV, grid
  tuning, run registry with metrics/plots/artifacts.
- **Live scoring + E2E orchestration + demo UI** built and wired.

### Known limitations (state honestly; don't try to "fix" the inherent ones)
- Live rotation is bounded by ADS-B hex coverage (~38%) — not fixable in code;
  the design's graceful degradation is the answer.
- Live *batch* weather over a 48h plan-heavy window is inherently sparse (~3%) —
  expected; real-time serving and historical training are the real use cases.
- `airport_to_station_2019.csv` is a single-year bridge (~358 airports) — covers
  majors; unmapped stations' obs are dropped (graceful).

---

## 6. Roadmap (next work — good for Claude Code)

1. **Sync `git main`.** During rapid iteration, deliverables were applied locally
   from zips faster than they were pushed. Before anything else, confirm main
   contains: `airports.py`/`data/airports.csv`, tz-aware `schema.py`, weather
   parity + `weather_cache.py`, `bts_source.py` (with local-CSV discovery),
   `feature_prep.py`, `score_live.py`, the whole `training/` subpackage,
   `configs/training.yaml`, `e2e.sh`, and the docs. A fresh clone must run.
2. **Build the real 10-year gold** from cached BTS + NCEI weather; produce
   `bts_out/bts_2015_2025.parquet`.
3. **Train + tune** the real model (`training.cli tune`); confirm XGBoost beats
   the logistic baseline on real data (expect realistic AUC ~0.65–0.75, not the
   ~0.99 seen on synthetic).
4. **Run `./e2e.sh up`** on the real box; validate with `./e2e.sh health`; soak 48h.
5. **Agent integration** (Ryan): wire the RAG/LangGraph analyst against the data
   contract; connect the Streamlit Analyst page via `AEROFLUX_AGENT_URL`.
6. **Proposal polish:** note the NCEI-historical/METAR-live source split; settle
   the Postgres+DynamoDB story (drop MongoDB); condense Methodology to the
   big-data architecture; add data-source citations.
7. **Later (out of 2-week scope):** cloud migration (RDS/S3/DynamoDB/MSK/EMR),
   TAF forecasts for destination weather, TensorFlow model, full Spark path,
   airframe-graph cascade simulation.

---

## 7. Conventions

Incremental and validation-first (test each layer before advancing). Code-first
over design docs. Real data over fixtures (bugs hide in fixtures). Modular and
portable (laptop → cloud with no re-architecting). Config over hardcoding
(YAML). Keep prose/docs concise. Update `CLAUDE.md` when structure or invariants
change.
