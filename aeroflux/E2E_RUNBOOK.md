# AeroFlux — End-to-End Integration Runbook

One operable system: **train → serve (live ingest + score) → UI**. The
`e2e.sh` script automates it; this runbook gives the ordered steps, what to
expect, and a checklist to confirm the whole thing works.

## Prerequisites (once)

```bash
cd aeroflux
pip install -e .                                   # aeroflux_ml + training
pip install scikit-learn matplotlib pyyaml         # training deps
pip install -r streamlit_app/requirements.txt      # UI deps
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"
./run.sh setup                                     # Kafka + Postgres up, tables created
```

You need the 10-year training file at `bts_out/bts_2015_2025.parquet`. Build it
from your cached BTS + weather if you haven't:

```bash
python scripts/build_bts_gold.py --months 2015-01:2025-12 \
  --cache data/bts --out bts_out \
  --weather-cache data/weather --station-bridge data/reference/airport_to_station_2019.csv
# (rename/point the produced gold to bts_out/bts_2015_2025.parquet)
```

## The one-command path

```bash
./e2e.sh up          # trains if needed, starts ingest + scoring + gold archive + UI
./e2e.sh health      # validate every stage (run a few times over ~10 min)
./e2e.sh down        # stop everything
```

For a run that only stops when you stop it: `DURATION=continuous ./e2e.sh up`.
For the assignment's 48-hour soak, the default `DURATION=172800` is exactly 48h.

## Ordered steps (what `up` does, and how to run them by hand)

### 1. Train + register the model
```bash
./e2e.sh train
# = python -m aeroflux_ml.training.cli train --config configs/training.yaml \
#       --gold bts_out/bts_2015_2025.parquet --name live_xgb
```
**Expected:** a run dir `model_outputs/runs/live_xgb_<ts>/` with `models/`,
`plots/`, `tables/comparison.md`, `metrics/`, `run.json`; the best model copied to
`out/current_model.joblib` and `streamlit_app/models/xgb_classifier_live.joblib`.
The console prints the baseline-vs-XGBoost ranking.

### 2. Start live ingestion
```bash
./e2e.sh ingest      # wraps ./run.sh stream (bridge + consumer + ADS-B poller + gold refresh + 48h retention)
```
**Expected (within ~1–2 refresh cycles):** `swim.raw_messages` growing, tens of
thousands of `flight_instance` rows, `out/gold_features.parquet` written, hex
coverage climbing. Watch `logs/ingest.log`.

### 3. Score live gold with the trained model
```bash
./e2e.sh score       # loops every 60s: reads gold -> feature_prep -> model -> predictions
```
**Expected:** `out/predictions.parquet` (flight_key, delay_probability,
predicted_delayed, model_version, scored_at) and a Postgres `predictions` table,
both refreshing each cycle. Watch `logs/score.log`.

### 4. Archive gold for the team
Automatic under `up`: hourly snapshots to `out/gold_live/gold_<ts>.parquet` — the
cleaned live dataset for later analysis.

### 5. Launch the UI against live data + the new model
```bash
./e2e.sh ui          # AEROFLUX_DSN + AEROFLUX_PREDICTIONS set -> Streamlit on :8501
```
**Expected:** Home shows 🟢 LIVE, real counts; Live Map shows recent flights as
arcs colored by the model's risk; Live Inference scores a picked flight with the
new model.

## Validation — data is moving through every stage

`./e2e.sh health` checks all of these at once:

| Stage | Check | Healthy looks like |
|---|---|---|
| Source→Kafka→raw | `swim.raw_messages` count | grows between checks |
| Fusion (silver) | `flight_instance` count | tens of thousands |
| Features (gold) | `out/gold_features.parquet` rows | matches silver scale |
| Scoring | `out/predictions.parquet` + `predictions` table rows | grows each cycle |
| Model | `out/current_model.joblib` | present |
| UI | `http://localhost:8501/_stcore/health` | UP |
| Processes | score / sync / archive / ui | "running" (PID alive) |
| Ingest | PID alive **and** `raw_messages` grew in the last `INGEST_STALE_MINUTES` (default 5) | "running (PID) — N raw message(s) in the last 5m" |

Ingest gets a stronger check than the others on purpose: a PID can stay
alive for its full `--duration` while the underlying SWIM bridge is dead
(the wrapper doesn't supervise the backgrounded process) — that let ingest
sit dead for ~2 days once while the PID check alone still said "running."
If you see `INGEST STALLED`, the PID is up but nothing is arriving —
restart it (`./e2e.sh ingest`) rather than trusting the process list.

Run it a few times over ten minutes; the counts should move.

## Restart guidance

- A stage died? `./e2e.sh health` shows which. Restart just that one:
  `./e2e.sh ingest` / `./e2e.sh score` / `./e2e.sh ui`.
- Ingestion hiccups are non-fatal (the stream loop continues; weather/METAR 504s
  fall back). Check `logs/ingest.log`.
- Scoring hiccups are caught and retried next cycle. Check `logs/score.log`.
- Full reset: `./e2e.sh down && ./run.sh down && ./run.sh setup && ./e2e.sh up`.

## Final checklist — full system works end to end

- [ ] `./run.sh setup` completed; Kafka + Postgres healthy
- [ ] `bts_out/bts_2015_2025.parquet` exists
- [ ] `./e2e.sh train` produced a run dir + `out/current_model.joblib`
- [ ] `comparison.md` shows XGBoost beating the logistic baseline
- [ ] `./e2e.sh ingest` → `raw_messages` and `flight_instance` growing
- [ ] `out/gold_features.parquet` present and refreshing
- [ ] `./e2e.sh score` → `predictions.parquet` + Postgres `predictions` growing
- [ ] `out/gold_live/` accumulating hourly snapshots
- [ ] UI up on :8501, banner 🟢 LIVE
- [ ] Live Map shows recent flights; colors reflect model risk
- [ ] Live Inference scores a selected flight with the live model
- [ ] `./e2e.sh health` shows every stage green
- [ ] left running 48h (or `continuous`) without manual intervention
