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
main(){
  section_header
  section_airframe
  section_throughput
  section_latency
  section_fusion
  section_predictions
  section_resources
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
