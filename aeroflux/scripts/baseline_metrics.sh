#!/usr/bin/env bash
# scripts/baseline_metrics.sh — one-command, point-in-time performance snapshot.
#
# Run it once now (label "local") and again after the AWS migration (label
# "aws") to get two directly comparable Markdown reports — same metrics, same
# definitions, different environment. Read-only: never modifies any data.
#
#   ./scripts/baseline_metrics.sh [env_label]     # default env_label = local
#
# Writes: out/metrics/baseline_<env_label>_<UTC timestamp>.md
# Also echoes a short summary to stdout.
#
# Paths/DSN are read the same way e2e.sh does (same var names, same defaults),
# so this works unmodified in both the local-docker and post-migration layouts
# — just override DSN/GOLD/PREDICTIONS/PYTHON in the environment if AWS uses
# different ones.
#
# Also captures cloud storage + records (S3 + DynamoDB, via the
# aeroflux-local AWS profile) alongside local storage in one table —
# queried directly, independent of this host's own STATE_BACKEND/
# LAKE_BACKEND, so it's backend-aware: works run from the local dev box or
# the Lightsail app box. Override AWS_PROFILE/AWS_REGION/S3_BUCKET/
# DYNAMODB_TABLE/RECENT_HOURS in the environment if yours differ.
#
# DynamoDB's exact item count (a full Select=COUNT scan) is opt-in
# (DYNAMODB_EXACT_COUNT=1) — it costs ~1 RCU per item evaluated, which is
# 90-170k RCU on this table every time it runs at default. The free
# describe-table ItemCount estimate is used by default instead.
#
# Resilience: one metric failing (DB unreachable, file missing, etc.) never
# aborts the run — that section notes what it couldn't capture and the script
# continues to the next one.

set -uo pipefail

ENV_LABEL="${1:-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

# ---- config (override via env, same names/defaults as e2e.sh) --------------
: "${DSN:=postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux}"
: "${GOLD:=$ROOT/out/gold_features.parquet}"
: "${PREDICTIONS:=$ROOT/out/predictions.parquet}"
: "${LOGS:=$ROOT/logs}"
: "${COMPOSE_FILE:=$ROOT/compose.yaml}"
: "${PYTHON:=python3}"

# ---- cloud config — queried directly regardless of this host's own
# STATE_BACKEND/LAKE_BACKEND, so the section works run from either the local
# dev box or the Lightsail app box (same names/defaults as sync_cloud.sh) ----
: "${AWS_PROFILE:=aeroflux-local}"
: "${AWS_REGION:=us-east-1}"
: "${S3_BUCKET:=aeroflux-lake-411750981882-us-east-1-an}"
: "${DYNAMODB_TABLE:=aeroflux-current-state}"
: "${RECENT_HOURS:=2}"   # same default as aeroflux_ui/streamlit_app/data_access.py

TS_UTC="$(date -u +%Y%m%dT%H%M%SZ)"
TS_HUMAN="$(date -u +"%Y-%m-%d %H:%M:%S UTC")"
OUT_DIR="$ROOT/out/metrics"
mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/baseline_${ENV_LABEL}_${TS_UTC}.md"
GIT_SHA="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo "unknown (no git commit found)")"

# ---- capability probes (once, reused by every section) ---------------------
pg_ok=0
if command -v psql >/dev/null 2>&1 && psql "$DSN" -tAc "SELECT 1;" >/dev/null 2>&1; then
  pg_ok=1
fi
py_ok=0
if command -v "$PYTHON" >/dev/null 2>&1 && "$PYTHON" -c "import polars" >/dev/null 2>&1; then
  py_ok=1
fi
aws_ok=0
if command -v aws >/dev/null 2>&1 && AWS_PROFILE="$AWS_PROFILE" aws sts get-caller-identity >/dev/null 2>&1; then
  aws_ok=1
fi
psql1(){ psql "$DSN" -tAc "$1" 2>/dev/null; }

# ---- small helpers -----------------------------------------------------
write(){ printf '%s\n' "$1" >> "$REPORT"; }
blank(){ printf '\n' >> "$REPORT"; }
hr(){ blank; write "---"; blank; }

# stdout-summary vars — defaulted so the summary never prints "unbound"
SUMMARY_HEX="N/A"; SUMMARY_TAIL="N/A"; SUMMARY_ACTYPE="N/A"; SUMMARY_RATE="N/A"; SUMMARY_ATRISK="N/A"

# ============================================================================
section_header(){
  : > "$REPORT"
  write "# AeroFlux Baseline Metrics — ${ENV_LABEL}"
  blank
  write "**Captured:** ${TS_HUMAN}"
  write "**Environment label:** \`${ENV_LABEL}\`"
  write "**Git commit:** \`${GIT_SHA}\`"
  blank
  write "> This is a **point-in-time snapshot** — raw message volume, coverage,"
  write "> and prediction mix all vary by time of day and by how long the live"
  write "> pipeline has been running, so note the capture time above when"
  write "> reading these numbers."
  blank
  write "> **How to compare:** run \`./scripts/baseline_metrics.sh <env_label>\`"
  write "> again with a different label (e.g. \`aws\` after migration) — it"
  write "> produces a report with the identical structure and metric"
  write "> definitions, so the two files can be diffed directly."
  hr
}

# ============================================================================
section_airframe(){
  write "## Airframe-Resolution Coverage — headline metric"
  blank
  if [ "$pg_ok" -eq 1 ]; then
    cov_sql='count(*)||'"'"'|'"'"'||count(hex)||'"'"'|'"'"'||round(100.0*count(hex)/NULLIF(count(*),0),1)||'"'"'|'"'"'||count(tail_number)||'"'"'|'"'"'||round(100.0*count(tail_number)/NULLIF(count(*),0),1)||'"'"'|'"'"'||count(aircraft_type)||'"'"'|'"'"'||round(100.0*count(aircraft_type)/NULLIF(count(*),0),1)'
    all_row="$(psql1 "SELECT ${cov_sql} FROM flight_instance;")"
    active_row="$(psql1 "SELECT ${cov_sql} FROM flight_instance WHERE flight_status = 'ACTIVE' OR last_position_time > now() - interval '2 hours';")"
    if [ -n "$all_row" ]; then
      write "Coverage depends heavily on the denominator — a table full of"
      write "not-yet-departed scheduled flights dilutes the rate, since ADS-B"
      write "can't see an aircraft that hasn't started moving yet. Both are"
      write "reported below; the second row is the more meaningful \"can we"
      write "actually resolve the airframe when it matters\" number."
      blank
      write "| Denominator | n | hex % | tail_number % | aircraft_type % |"
      write "|---|---|---|---|---|"
      IFS='|' read -r total hex_n hex_pct tail_n tail_pct at_n at_pct <<< "$all_row"
      write "| **All flight_instance rows** | ${total:-0} | ${hex_pct:-0.0}% | ${tail_pct:-0.0}% | ${at_pct:-0.0}% |"
      if [ -n "$active_row" ]; then
        IFS='|' read -r a_total a_hex_n a_hex_pct a_tail_n a_tail_pct a_at_n a_at_pct <<< "$active_row"
        write "| **Active/airborne only** (\`flight_status='ACTIVE'\` OR \`last_position_time\` within 2h) | ${a_total:-0} | ${a_hex_pct:-0.0}% | ${a_tail_pct:-0.0}% | ${a_at_pct:-0.0}% |"
        SUMMARY_HEX="${a_hex_pct:-N/A}"; SUMMARY_TAIL="${a_tail_pct:-N/A}"; SUMMARY_ACTYPE="${a_at_pct:-N/A}"
      else
        write "| **Active/airborne only** | _query returned no data_ | | | |"
        SUMMARY_HEX="${hex_pct:-N/A}"; SUMMARY_TAIL="${tail_pct:-N/A}"; SUMMARY_ACTYPE="${at_pct:-N/A}"
      fi
    else
      write "_Query returned no data — flight_instance may be empty or not yet created._"
    fi
  else
    write "_Postgres unreachable at \`${DSN}\` — coverage not captured this run._"
  fi
  blank
  write "_What this means: the share of flights where ADS-B resolved the"
  write "airframe's hex code / tail / type. This bounds how often the rotation"
  write "and propagation features can activate (\`inbound_resolved=1\`) — the"
  write "single biggest lever on live prediction quality. Use the"
  write "active/airborne row as the headline number; the all-rows number is"
  write "diluted by flights ADS-B hasn't had a chance to see yet._"
  hr
}

# ============================================================================
section_throughput(){
  write "## Throughput"
  blank
  if [ "$pg_ok" -eq 1 ]; then
    # Rate is computed over the actual min->max stored_at span of whatever is
    # currently retained (up to the 48h retention window), not a fixed "last
    # hour" bucket — a fixed window reads as a false 0 whenever ingest was
    # paused/stalled during that exact hour even though earlier data is fine.
    row="$(psql1 "SELECT (SELECT count(*) FROM swim.raw_messages)||'|'||(SELECT COALESCE(extract(epoch FROM (max(stored_at)-min(stored_at)))::bigint,0) FROM swim.raw_messages)||'|'||(SELECT COALESCE(extract(epoch FROM (now()-max(stored_at)))::bigint,0) FROM swim.raw_messages)||'|'||(SELECT count(*) FROM flight_instance);")"
    if [ -n "$row" ]; then
      IFS='|' read -r total_raw span_s staleness_s total_inst <<< "$row"
      total_raw="${total_raw:-0}"; span_s="${span_s:-0}"
      staleness_s="${staleness_s:-0}"; total_inst="${total_inst:-0}"
      if [ "${span_s:-0}" -gt 0 ] 2>/dev/null; then
        rate="$(awk -v c="$total_raw" -v s="$span_s" 'BEGIN{printf "%.3f", c/s}')"
        span_h="$(awk -v s="$span_s" 'BEGIN{printf "%.1f", s/3600}')"
        write "- **Raw message rate:** ${rate} msg/sec  (${total_raw} messages over ${span_h}h — full min→max \`stored_at\` span currently in the table, not a fixed window)"
      else
        rate="N/A"
        write "- **Raw message rate:** _N/A — fewer than two distinct \`stored_at\` timestamps in the table (not enough data to compute a span)._"
      fi
      if [ "${total_raw:-0}" -gt 0 ] 2>/dev/null; then
        yield="$(awk -v a="$total_inst" -v b="$total_raw" 'BEGIN{printf "%.3f", a/b}')"
      else
        yield="N/A"
      fi
      write "- **Total raw messages (swim.raw_messages):** ${total_raw}"
      write "- **Total flight_instance rows (fused silver):** ${total_inst}"
      write "- **Fusion yield (instances / messages):** ${yield}"
      if [ "${staleness_s:-0}" -gt 300 ] 2>/dev/null; then
        staleness_min="$(awk -v s="$staleness_s" 'BEGIN{printf "%.1f", s/60}')"
        blank
        write "  ⚠️ **No messages have landed recently** — the newest \`stored_at\` is"
        write "  ${staleness_min} min old. The rate above reflects historical throughput,"
        write "  not current activity; this usually means the SWIM bridge"
        write "  (\`swim_to_kafka.py\`) isn't running or lost its connection right"
        write "  now — check \`swim_to_kafka.log\`."
      fi
      SUMMARY_RATE="$rate"
    else
      write "_Query returned no data._"
    fi
  else
    write "_Postgres unreachable at \`${DSN}\` — throughput not captured this run._"
  fi
  blank
  write "_What this means: how fast raw SWIM messages are landing, and how many"
  write "distinct fused flights they collapse into (each flight typically"
  write "generates several track/plan-amendment messages)._"
  hr
}

# ============================================================================
section_latency(){
  write "## Latency — Silver → Gold Pipeline Cycle Time"
  blank
  latency_found=0
  if [ -f "$LOGS/ingest.log" ] && [ "$py_ok" -eq 1 ]; then
    pair="$("$PYTHON" - "$LOGS/ingest.log" <<'PYEOF'
import re, sys
path = sys.argv[1]
ts_re = re.compile(r'(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}|\d{2}:\d{2}:\d{2})')
try:
    lines = open(path, errors="replace").readlines()
except Exception:
    print("")
    sys.exit()
start = end = None
for i, line in enumerate(lines):
    if "Transform raw -> silver" in line:
        m = ts_re.search(line)
        if m:
            start = (i, m.group(1))
    if "Gold ready" in line:
        m = ts_re.search(line)
        if m and start is not None:
            end = (i, m.group(1))
print(f"{start[1]}|{end[1]}" if start and end else "")
PYEOF
)"
    if [ -n "$pair" ]; then
      IFS='|' read -r start_ts end_ts <<< "$pair"
      write "- **Most recent cycle:** ${start_ts} → ${end_ts} (parsed from \`logs/ingest.log\`)"
      latency_found=1
    fi
  fi
  if [ "$latency_found" -eq 0 ]; then
    write "- **Silver→Gold cycle time:** _measured manually: ~___ min_ (placeholder — fill in)"
    write ""
    write "  \`run.sh\`'s \`log()\` doesn't currently write timestamps into"
    write "  \`logs/ingest.log\` (only \`e2e.sh\`'s own log lines are timestamped,"
    write "  and those aren't the ones redirected into that file), so this"
    write "  can't be parsed automatically today. Time a manual \`./run.sh"
    write "  pipeline\` run with a stopwatch and fill in the blank, or add a"
    write "  \`date +%H:%M:%S\` prefix to \`run.sh\`'s \`log()\` to make this"
    write "  auto-parseable in future baselines."
  fi
  blank
  write "_What this means: the end-to-end freshness lag — how long between a"
  write "raw SWIM message landing and it being reflected in scoreable gold"
  write "features._"
  hr
}

# ============================================================================
section_fusion(){
  write "## Fusion Completeness — per-field fill rate"
  blank
  if [ "$pg_ok" -eq 1 ]; then
    row="$(psql1 "SELECT count(*)||'|'||round(100.0*count(gufi)/NULLIF(count(*),0),1)||'|'||round(100.0*count(callsign)/NULLIF(count(*),0),1)||'|'||round(100.0*count(carrier_name)/NULLIF(count(*),0),1)||'|'||round(100.0*count(scheduled_gate_departure)/NULLIF(count(*),0),1)||'|'||round(100.0*count(origin)/NULLIF(count(*),0),1)||'|'||round(100.0*count(destination)/NULLIF(count(*),0),1) FROM flight_instance;")"
    if [ -n "$row" ]; then
      IFS='|' read -r total gufi_pct callsign_pct carrier_pct sched_pct origin_pct dest_pct <<< "$row"
      write "n = ${total:-0} flight_instance rows"
      blank
      write "| Field | Fill rate |"
      write "|---|---|"
      write "| \`gufi\` | ${gufi_pct:-0.0}% |"
      write "| \`callsign\` | ${callsign_pct:-0.0}% |"
      write "| \`carrier_name\` | ${carrier_pct:-0.0}% |"
      write "| \`scheduled_gate_departure\` | ${sched_pct:-0.0}% |"
      write "| \`origin\` | ${origin_pct:-0.0}% |"
      write "| \`destination\` | ${dest_pct:-0.0}% |"
    else
      write "_Query returned no data._"
    fi
  else
    write "_Postgres unreachable at \`${DSN}\` — fusion completeness not captured this run._"
  fi
  blank
  write "_What this means: how often each core identity/schedule field survived"
  write "the SWIM parse+fuse step. Low \`gufi\` fill (~38–49% expected) is why"
  write "the \`flight_ref\`-fallback + dedup path exists; a drop elsewhere flags"
  write "a parser regression, not a data-sparsity issue._"
  hr
}

# ============================================================================
section_predictions(){
  write "## Prediction Distribution"
  blank
  if [ ! -f "$PREDICTIONS" ]; then
    write "_No predictions file at \`${PREDICTIONS}\` — the scoring loop may not have run yet._"
  elif [ "$py_ok" -ne 1 ]; then
    write "_\`${PYTHON}\` + polars unavailable — predictions not captured this run._"
  else
    block="$("$PYTHON" - "$PREDICTIONS" <<'PYEOF'
import sys
import polars as pl
path = sys.argv[1]
try:
    df = pl.read_parquet(path)
except Exception as e:
    print(f"_Could not read {path} ({type(e).__name__}: {e})._")
    sys.exit()
if df.height == 0 or "delay_probability" not in df.columns:
    print("_predictions file has no scored rows yet._")
    sys.exit()
p = df["delay_probability"]
n = df.height
mean = p.mean()
atrisk = 100 * (p >= 0.5).mean()
qs = {q: p.quantile(q) for q in (0.10, 0.25, 0.50, 0.75, 0.90)}
print(f"- **n (scored flights):** {n}")
print(f"- **mean probability:** {mean:.3f}")
print(f"- **at-risk rate (probability >= 0.5):** {atrisk:.1f}%")
print(f"- **quantiles:** p10={qs[0.10]:.3f}  p25={qs[0.25]:.3f}  p50={qs[0.50]:.3f}  p75={qs[0.75]:.3f}  p90={qs[0.90]:.3f}")
print(f"__SUMMARY_ATRISK__{atrisk:.1f}")
PYEOF
)"
    summary_line="$(printf '%s\n' "$block" | grep '^__SUMMARY_ATRISK__' || true)"
    if [ -n "$summary_line" ]; then
      SUMMARY_ATRISK="${summary_line#__SUMMARY_ATRISK__}"
    fi
    printf '%s\n' "$block" | grep -v '^__SUMMARY_ATRISK__' >> "$REPORT"
  fi
  blank
  write "_What this means: the model's predicted delay-risk distribution over"
  write "currently-scoreable live flights. A high at-risk rate here reflects"
  write "the **documented train/serve distribution shift** (rotation and"
  write "recent-delay features are dense in BTS training but mostly null"
  write "live — see \`PROJECT_CONTEXT.md\` § Known Limitations), not a bug and"
  write "not a re-introduction of the weather feature-parity crash._"
  hr
}

# ============================================================================
section_resources(){
  write "## Resource Footprint"
  blank
  write "**Container stats** (\`docker stats --no-stream\`):"
  blank
  write '```text'
  if command -v docker >/dev/null 2>&1; then
    cids="$(docker compose -f "$COMPOSE_FILE" ps -q 2>/dev/null)"
    if [ -n "$cids" ]; then
      # shellcheck disable=SC2086
      docker stats --no-stream $cids >> "$REPORT" 2>&1 || write "(docker stats failed)"
    else
      write "(no running containers for ${COMPOSE_FILE})"
    fi
  else
    write "(docker not available on this host)"
  fi
  write '```'
  blank
  write "**Host memory** (\`free -h\`):"
  blank
  write '```text'
  if command -v free >/dev/null 2>&1; then
    free -h >> "$REPORT" 2>&1 || write "(free failed)"
  else
    write "(free not available on this platform)"
  fi
  write '```'
  blank
  write "**Gold + predictions data size** (\`du -h\`):"
  blank
  write '```text'
  shopt -s nullglob
  files=("$ROOT"/out/*.parquet "$ROOT"/out/gold_live/*.parquet)
  shopt -u nullglob
  if [ "${#files[@]}" -gt 0 ]; then
    du -h "${files[@]}" >> "$REPORT" 2>&1
    du -ch "${files[@]}" 2>/dev/null | tail -1 >> "$REPORT"
  else
    write "(no parquet files found under out/)"
  fi
  write '```'
  blank
  write "_What this means: container CPU/memory, host free memory, and on-disk"
  write "size of gold + predictions data — the resource baseline to size"
  write "AWS instances against after migration._"
  hr
}

# ============================================================================
section_cloud_storage(){
  write "## Cloud Storage + Records"
  blank
  write "Queried directly against S3/DynamoDB via the \`${AWS_PROFILE}\` AWS"
  write "profile, independent of this host's own \`STATE_BACKEND\`/"
  write "\`LAKE_BACKEND\` — so this section captures the same cloud numbers"
  write "whether the script is run from the local dev box or the Lightsail"
  write "app box."
  blank
  write "| Metric | Value | Unit | Source | Notes |"
  write "|---|---|---|---|---|"

  # ---- S3 -------------------------------------------------------------
  if [ "$aws_ok" -eq 1 ]; then
    s3_total="$(AWS_PROFILE="$AWS_PROFILE" aws s3 ls "s3://${S3_BUCKET}/" --recursive --summarize 2>/dev/null | tail -2)"
    if [ -n "$s3_total" ]; then
      obj_n="$(printf '%s\n' "$s3_total" | awk -F': ' '/Total Objects/{print $2}')"
      size_b="$(printf '%s\n' "$s3_total" | awk -F': ' '/Total Size/{print $2}')"
      size_mb="$(awk -v b="${size_b:-0}" 'BEGIN{printf "%.2f", b/1048576}')"
      write "| S3 total size | ${size_mb} | MB | cloud | bucket \`${S3_BUCKET}\` |"
      write "| S3 total objects | ${obj_n:-0} | count | cloud | bucket \`${S3_BUCKET}\` |"

      # Per-prefix breakdown — discovered from the bucket, not assumed. As of
      # this writing the lake only actually has gold/ and meta/ (LakeStore
      # syncs the gold feature table + a sync-status marker); no bronze/
      # silver/analytics prefixes exist yet, so this reports what's real
      # rather than a guessed layout.
      prefixes="$(AWS_PROFILE="$AWS_PROFILE" aws s3api list-objects-v2 --bucket "$S3_BUCKET" --delimiter / --query 'CommonPrefixes[].Prefix' --output text 2>/dev/null)"
      if [ -n "$prefixes" ]; then
        for p in $prefixes; do
          p_stats="$(AWS_PROFILE="$AWS_PROFILE" aws s3 ls "s3://${S3_BUCKET}/${p}" --recursive --summarize 2>/dev/null | tail -2)"
          p_obj="$(printf '%s\n' "$p_stats" | awk -F': ' '/Total Objects/{print $2}')"
          p_size="$(printf '%s\n' "$p_stats" | awk -F': ' '/Total Size/{print $2}')"
          p_size_mb="$(awk -v b="${p_size:-0}" 'BEGIN{printf "%.2f", b/1048576}')"
          write "| S3 \`${p}\` prefix | ${p_size_mb} | MB | cloud | ${p_obj:-0} objects |"
        done
      fi
    else
      write "| S3 | _unreachable or empty_ | | cloud | bucket \`${S3_BUCKET}\` |"
    fi
  else
    write "| S3 | _not captured_ | | cloud | \`aws\` CLI unavailable or \`${AWS_PROFILE}\` profile/credentials didn't authenticate |"
  fi

  # ---- DynamoDB ---------------------------------------------------------
  if [ "$aws_ok" -eq 1 ]; then
    dd_desc="$(AWS_PROFILE="$AWS_PROFILE" aws dynamodb describe-table --table-name "$DYNAMODB_TABLE" --query 'Table.[ItemCount,TableSizeBytes]' --output text 2>/dev/null)"
    if [ -n "$dd_desc" ]; then
      read -r dd_items dd_bytes <<< "$dd_desc"
      dd_mb="$(awk -v b="${dd_bytes:-0}" 'BEGIN{printf "%.2f", b/1048576}')"
      write "| DynamoDB ItemCount (approx) | ${dd_items:-0} | count | cloud | \`describe-table\`'s cached estimate — AWS only refreshes this ~every 6h |"
      write "| DynamoDB TableSizeBytes (approx) | ${dd_mb} | MB | cloud | same caveat |"
    else
      write "| DynamoDB describe-table | _unreachable_ | | cloud | table \`${DYNAMODB_TABLE}\` |"
    fi
    if [ "$py_ok" -eq 1 ]; then
      dd_block="$(AWS_PROFILE="$AWS_PROFILE" AWS_REGION="$AWS_REGION" DYNAMODB_TABLE="$DYNAMODB_TABLE" \
                  STATE_BACKEND=dynamodb RECENT_HOURS="$RECENT_HOURS" DYNAMODB_EXACT_COUNT="${DYNAMODB_EXACT_COUNT:-0}" "$PYTHON" - <<'PYEOF'
import os, time
from datetime import datetime, timezone, timedelta
import boto3

table_name = os.environ["DYNAMODB_TABLE"]
region = os.environ.get("AWS_REGION", "us-east-1")
recent_hours = float(os.environ.get("RECENT_HOURS", "2"))

# A full Select=COUNT scan still costs one RCU per item EVALUATED (it only
# skips the network transfer of attributes, not the read charge) — for a
# 90-170k item table that's 90-170k RCU on every single run of this script.
# Confirmed as a real, live contributor to a ~$29 DynamoDB bill, not a
# theoretical concern. describe-table's ItemCount above is free (AWS
# already tracks it) and is a fine estimate for a baseline snapshot, so
# the exact count is now opt-in only: DYNAMODB_EXACT_COUNT=1.
if os.environ.get("DYNAMODB_EXACT_COUNT") == "1":
    client = boto3.client("dynamodb", region_name=region)
    t0 = time.time()
    exact = 0
    for page in client.get_paginator("scan").paginate(TableName=table_name, Select="COUNT"):
        exact += page["Count"]
    print(f"EXACT|{exact}|{time.time()-t0:.1f}")

# Active-flight count — mirrors aeroflux_ui/streamlit_app/data_access.py's
# current_flights()/_current_mask(): flight_status == ACTIVE (ground truth)
# OR within recent_hours of scheduled/actual departure or arrival. Keep in
# sync if that logic changes — duplicated here (not imported) because that
# module pulls in streamlit, which this script's python env has no need for.
from aeroflux_ml import state_backend_from_env
rows = state_backend_from_env().recent_flight_states(hours=48, limit=5000)

def parse(ts):
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts) if isinstance(ts, str) else ts
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

now = datetime.now(timezone.utc)
window = timedelta(hours=recent_hours)
active = 0
for r in rows:
    if r.get("flight_status") == "ACTIVE":
        active += 1; continue
    sd = parse(r.get("scheduled_gate_departure"))
    if sd is not None and abs(now - sd) <= window:
        active += 1; continue
    ao = parse(r.get("actual_off"))
    if ao is not None and timedelta(0) <= (now - ao) <= window:
        active += 1; continue
    an = parse(r.get("actual_on"))
    if an is not None and timedelta(0) <= (now - an) <= window:
        active += 1; continue
print(f"TRACKED|{len(rows)}")
print(f"ACTIVE|{active}")
PYEOF
)"
      exact_line="$(printf '%s\n' "$dd_block" | grep '^EXACT|' || true)"
      tracked_line="$(printf '%s\n' "$dd_block" | grep '^TRACKED|' || true)"
      active_line="$(printf '%s\n' "$dd_block" | grep '^ACTIVE|' || true)"
      if [ -n "$exact_line" ]; then
        IFS='|' read -r _ exact_n exact_s <<< "$exact_line"
        write "| DynamoDB exact item count | ${exact_n:-0} | count | cloud | full \`Select=COUNT\` paginated scan, took ${exact_s:-?}s — costs ~1 RCU/item evaluated, opt-in only |"
      else
        write "| DynamoDB exact item count | _skipped_ | | cloud | costs ~1 RCU per item evaluated (90-170k on this table) — set \`DYNAMODB_EXACT_COUNT=1\` if you need it; the approx ItemCount above is free |"
      fi
      if [ -n "$tracked_line" ] && [ -n "$active_line" ]; then
        tracked_n="${tracked_line#TRACKED|}"
        active_n="${active_line#ACTIVE|}"
        pct="$(awk -v a="${active_n:-0}" -v b="${tracked_n:-1}" 'BEGIN{printf "%.0f", (b>0)?100*a/b:0}')"
        write "| DynamoDB tracked (48h window, capped) | ${tracked_n:-0} | count | cloud | matches the app's \`FLIGHTS_LIMIT\` cap |"
        write "| DynamoDB active-now (status/recency filtered) | ${active_n:-0} | count | cloud | ${pct}% of tracked — \`RECENT_HOURS=${RECENT_HOURS}\`, mirrors \`current_flights()\` |"
      fi
    else
      write "| DynamoDB exact/active-now counts | _not captured_ | | cloud | \`${PYTHON}\` or its deps unavailable |"
    fi
  else
    write "| DynamoDB | _not captured_ | | cloud | \`aws\` CLI unavailable or \`${AWS_PROFILE}\` profile/credentials didn't authenticate |"
  fi

  # ---- Local storage ------------------------------------------------------
  out_size="$(du -sh "$ROOT/out" 2>/dev/null | awk '{print $1}')"
  write "| Local \`out/\` directory | ${out_size:-N/A} | du -h | local | includes gold, predictions, gold_live, eval |"
  if [ -d "$ROOT/out/gold_live" ]; then
    gl_size="$(du -sh "$ROOT/out/gold_live" 2>/dev/null | awk '{print $1}')"
    write "| Local \`out/gold_live/\` | ${gl_size:-N/A} | du -h | local | hourly gold snapshots |"
  fi
  if [ -d "$ROOT/out/predictions" ]; then
    pr_size="$(du -sh "$ROOT/out/predictions" 2>/dev/null | awk '{print $1}')"
    write "| Local \`out/predictions/\` | ${pr_size:-N/A} | du -h | local | hourly prediction snapshots |"
  fi
  if [ "$pg_ok" -eq 1 ]; then
    db_bytes="$(psql1 "SELECT pg_database_size(current_database());")"
    db_mb="$(awk -v b="${db_bytes:-0}" 'BEGIN{printf "%.2f", b/1048576}')"
    write "| Postgres database size | ${db_mb} | MB | local | \`pg_database_size(current_database())\` |"
  else
    write "| Postgres database size | _not captured_ | | local | Postgres unreachable at \`${DSN}\` |"
  fi
  if command -v docker >/dev/null 2>&1; then
    while IFS='|' read -r dtype dsize dreclaim; do
      [ -n "$dtype" ] || continue
      write "| Docker ${dtype} | ${dsize} | docker system df | local | reclaimable: ${dreclaim} |"
    done < <(docker system df --format '{{.Type}}|{{.Size}}|{{.Reclaimable}}' 2>/dev/null)
  else
    write "| Docker system df | _not captured_ | | local | docker unavailable |"
  fi

  blank
  write "_What this means: cloud storage volume (S3 lake size, DynamoDB item"
  write "count/size) alongside local disk footprint, in one table so growth"
  write "can be tracked over time regardless of which backend a given run is"
  write "against. \"DynamoDB active-now\" is the number that actually matters"
  write "for \"is the map showing something reasonable\" — it should be a"
  write "meaningful fraction of \"tracked,\" not a token few percent; see"
  write "\`aeroflux_ui/streamlit_app/data_access.py\`'s \`current_flights()\`._"
  hr
}

# ============================================================================
main(){
  section_header
  section_airframe
  section_throughput
  section_latency
  section_fusion
  section_predictions
  section_resources
  section_cloud_storage
  write "_Report generated by \`scripts/baseline_metrics.sh ${ENV_LABEL}\` at ${TS_HUMAN}._"

  echo "Baseline report written: $REPORT"
  echo
  echo "----- summary -----"
  echo "env:            $ENV_LABEL"
  echo "captured:       $TS_HUMAN"
  echo "git commit:     $GIT_SHA"
  echo "hex coverage (active flights):   ${SUMMARY_HEX}%"
  echo "tail coverage (active flights):  ${SUMMARY_TAIL}%"
  echo "aircraft_type (active flights):  ${SUMMARY_ACTYPE}%"
  echo "raw msg rate:   ${SUMMARY_RATE} msg/sec"
  echo "at-risk rate:   ${SUMMARY_ATRISK}%"
  echo "report:         $REPORT"
}

main
