#!/usr/bin/env bash
# AeroFlux end-to-end orchestration: train -> serve (live ingest + score) -> UI.
# Keep this at the PROJECT ROOT (sibling of run.sh). It orchestrates run.sh.
#
#   ./e2e.sh up      # (re)use a trained model + ingest + score + sync + archive + UI
#   ./e2e.sh health  # validate every stage
#   ./e2e.sh down    # stop everything
# Stages: train | link | ingest | score | sync_cloud | archive | ui
#
# sync_cloud is a no-op unless you set STATE_BACKEND=dynamodb and/or
# LAKE_BACKEND=s3 (see aeroflux_ml/io.py / scripts/sync_cloud.sh) — with the
# defaults (postgres/local) it still runs on schedule but does nothing each
# cycle, so `./e2e.sh up` behaves exactly as before if you never opt in.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"     # absolute project root, wherever invoked
cd "$ROOT"

# ---- config (override via env). All paths absolute so cd-into-UI can't break them.
: "${DSN:=postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux}"
: "${BTS_PARQUET:=$ROOT/bts_out/bts_2015_2025.parquet}"
: "${TRAIN_CONFIG:=$ROOT/configs/training.yaml}"
: "${GOLD:=$ROOT/out/gold_features.parquet}"
: "${PREDICTIONS:=$ROOT/out/predictions.parquet}"
: "${GOLD_ARCHIVE:=$ROOT/out/gold_live}"
: "${DURATION:=172800}"                   # 48h; 'continuous' loops forever
: "${SCORE_EVERY:=60}"
: "${SYNC_EVERY:=300}"                    # matches run.sh's REFRESH_SECONDS default —
                                           # no point syncing more often than gold refreshes
: "${UI_PORT:=8501}"
: "${RUN_DIR_FILE:=$ROOT/out/.current_run_dir}"
: "${MODEL_LINK:=$ROOT/out/current_model.joblib}"
export DSN
# auto-detect the Streamlit app dir (yours is aeroflux_ui/streamlit_app)
if [ -z "${UI_DIR:-}" ]; then
  for d in "$ROOT/aeroflux_ui/streamlit_app" "$ROOT/streamlit_app"; do
    [ -f "$d/app.py" ] && UI_DIR="$d" && break
  done
fi
UI_DIR="${UI_DIR:-$ROOT/aeroflux_ui/streamlit_app}"
LOGS="$ROOT/logs"; mkdir -p "$LOGS" "$ROOT/out" "$GOLD_ARCHIVE"
log(){ echo "$(date +%H:%M:%S) | e2e | $*"; }

_newest_run(){ ls -dt "$ROOT"/model_outputs/runs/*/ 2>/dev/null | head -1; }
_best_of(){ python -c "import csv;r=sorted(csv.DictReader(open('$1/tables/comparison.csv')),key=lambda x:int(x['rank']));print(r[0]['name'])" 2>/dev/null; }

# ---- 1. TRAIN (only if you want a fresh model) -----------------------------
cmd_train(){
  [ -f "$BTS_PARQUET" ] || { log "ERROR: $BTS_PARQUET not found"; exit 1; }
  log "training XGBoost on $BTS_PARQUET"
  python -m aeroflux_ml.training.cli train --config "$TRAIN_CONFIG" \
      --gold "$BTS_PARQUET" --name live_xgb 2>&1 | tee "$LOGS/train.log"
  cmd_link
}

# ---- link an EXISTING trained run's best model (no retraining) -------------
cmd_link(){
  RUN="$(_newest_run)"; RUN="${RUN%/}"
  [ -n "$RUN" ] || { log "no run in model_outputs/runs; run ./e2e.sh train"; return 1; }
  BEST="$(_best_of "$RUN")"; [ -n "$BEST" ] || BEST=xgb_full
  echo "$RUN" > "$RUN_DIR_FILE"
  cp "$RUN/models/$BEST.joblib" "$MODEL_LINK"
  mkdir -p "$UI_DIR/models" && cp "$MODEL_LINK" "$UI_DIR/models/xgb_classifier_live.joblib"
  log "model linked: run=$(basename "$RUN") best=$BEST -> $MODEL_LINK and $UI_DIR/models/"
}

# ---- 2. INGEST (live pipeline via run.sh) ----------------------------------
cmd_ingest(){
  log "starting live ingestion (duration=$DURATION) -> $LOGS/ingest.log"
  if [ "$DURATION" = "continuous" ]; then
    ( while true; do ./run.sh stream 3600 >>"$LOGS/ingest.log" 2>&1; done ) & echo $! > "$ROOT/out/.ingest_pid"
  else
    ( ./run.sh stream "$DURATION" >>"$LOGS/ingest.log" 2>&1 ) & echo $! > "$ROOT/out/.ingest_pid"
  fi
  log "ingest pid $(cat "$ROOT/out/.ingest_pid")"
}

# ---- 3. SCORE live gold ----------------------------------------------------
cmd_score(){
  RUN="$(cat "$RUN_DIR_FILE" 2>/dev/null || true)"
  [ -n "$RUN" ] || { cmd_link || { log "ERROR: no model to score with"; exit 1; }; RUN="$(cat "$RUN_DIR_FILE")"; }
  log "scoring loop every ${SCORE_EVERY}s -> $PREDICTIONS (+ Postgres)"
  ( python -m aeroflux_ml.score_live --run-dir "$RUN" --gold "$GOLD" \
      --out "$PREDICTIONS" --dsn "$DSN" --loop "$SCORE_EVERY" >>"$LOGS/score.log" 2>&1 ) & echo $! > "$ROOT/out/.score_pid"
}

# ---- 4. SYNC to cloud backends (no-op unless STATE_BACKEND/LAKE_BACKEND ----
#         are set to something other than the local defaults) ---------------
cmd_sync_cloud(){
  log "cloud sync loop every ${SYNC_EVERY}s -> STATE_BACKEND=${STATE_BACKEND:-postgres} LAKE_BACKEND=${LAKE_BACKEND:-local} ($LOGS/sync_cloud.log)"
  ( while true; do
      GOLD="$GOLD" PREDICTIONS="$PREDICTIONS" DSN="$DSN" "$ROOT/scripts/sync_cloud.sh" >>"$LOGS/sync_cloud.log" 2>&1
      sleep "$SYNC_EVERY"
    done ) & echo $! > "$ROOT/out/.sync_pid"
  log "sync pid $(cat "$ROOT/out/.sync_pid")"
}

# ---- 5. ARCHIVE gold snapshots (hourly) ------------------------------------
cmd_archive(){
  ( while true; do [ -f "$GOLD" ] && cp "$GOLD" "$GOLD_ARCHIVE/gold_$(date +%Y%m%d_%H%M).parquet"; sleep 3600; done ) & echo $! > "$ROOT/out/.archive_pid"
  log "gold archiver -> $GOLD_ARCHIVE (hourly)"
}

# ---- 6. UI -----------------------------------------------------------------
cmd_ui(){
  [ -f "$UI_DIR/app.py" ] || { log "ERROR: no app.py under $UI_DIR (set UI_DIR=...)"; return 1; }
  command -v streamlit >/dev/null || { log "streamlit not installed"; return 1; }
  log "launching Streamlit from $UI_DIR on :$UI_PORT"
  ( cd "$UI_DIR" && AEROFLUX_DSN="$DSN" AEROFLUX_PREDICTIONS="$PREDICTIONS" \
      streamlit run app.py --server.port "$UI_PORT" --server.address 0.0.0.0 \
      >>"$LOGS/ui.log" 2>&1 ) & echo $! > "$ROOT/out/.ui_pid"
  log "ui pid $(cat "$ROOT/out/.ui_pid") -> http://localhost:$UI_PORT"
}

# ---- HEALTH ----------------------------------------------------------------
cmd_health(){
  echo "===== AeroFlux end-to-end health ====="
  echo "UI_DIR=$UI_DIR"
  psql "$DSN" -tAc "SELECT 'raw_messages: '||count(*) FROM swim.raw_messages;" 2>/dev/null || echo "raw_messages: DB UNREACHABLE"
  psql "$DSN" -tAc "SELECT 'flight_instance(silver): '||count(*) FROM flight_instance;" 2>/dev/null
  psql "$DSN" -tAc "SELECT 'predictions(db): '||count(*) FROM predictions;" 2>/dev/null || echo "predictions(db): (none yet)"
  for f in "$GOLD" "$PREDICTIONS" "$MODEL_LINK"; do
    if [ -f "$f" ]; then
      case "$f" in *.parquet) echo "$(basename "$f"): $(python -c "import polars as pl;print(pl.read_parquet('$f').height)") rows";; *) echo "$(basename "$f"): present";; esac
    else echo "$(basename "$f"): MISSING"; fi
  done
  curl -sf "http://localhost:$UI_PORT/_stcore/health" >/dev/null 2>&1 && echo "ui: UP (:$UI_PORT)" || echo "ui: down"
  echo "--- processes ---"
  for p in ingest score sync archive ui; do
    pid=$(cat "$ROOT/out/.${p}_pid" 2>/dev/null || true)
    if [ -n "${pid:-}" ] && kill -0 "$pid" 2>/dev/null; then echo "$p: running ($pid)"; else echo "$p: STOPPED"; fi
  done
}

# ---- UP / DOWN -------------------------------------------------------------
cmd_up(){
  [ -f "$MODEL_LINK" ] || cmd_link || cmd_train      # reuse existing model; only train if none
  cmd_ingest; sleep 5; cmd_score; cmd_sync_cloud; cmd_archive; cmd_ui
  log "ALL UP. validate: ./e2e.sh health   stop: ./e2e.sh down"
}
cmd_down(){
  for p in ui archive sync score ingest; do
    pid=$(cat "$ROOT/out/.${p}_pid" 2>/dev/null || true)
    [ -n "${pid:-}" ] && kill "$pid" 2>/dev/null && echo "stopped $p ($pid)"
    rm -f "$ROOT/out/.${p}_pid"
  done
  ./run.sh stop 2>/dev/null || true
  log "all stopped"
}

case "${1:-}" in
  train) cmd_train;; link) cmd_link;; ingest) cmd_ingest;; score) cmd_score;;
  sync_cloud|sync) cmd_sync_cloud;;
  archive) cmd_archive;; ui) cmd_ui;; health|status) cmd_health;;
  up|all) cmd_up;; down|stop) cmd_down;;
  *) echo "usage: ./e2e.sh {up|health|down | train|link|ingest|score|sync_cloud|archive|ui}"; exit 1;;
esac
