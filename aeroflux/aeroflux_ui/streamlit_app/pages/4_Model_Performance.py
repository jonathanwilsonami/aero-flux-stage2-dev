"""Model Performance & Data Quality — analyst-facing, read-only.

Read-only and additive by design: reads out/eval/live_metrics_latest.json
(written by `python -m aeroflux_ml.evaluate_live`), the local model
artifact, the latest gold_live snapshot, and — for DynamoDB record count —
the FREE `describe-table` ItemCount only. Never issues a DynamoDB Scan,
never touches sync_cloud.py, never writes anything. Every read is cached
(30 min) so this page adds no meaningful load to the running pipeline.

Live-evaluation metrics here are early and still stabilizing as more
snapshots reconcile — see the banner below before treating any number here
as final.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="AeroFlux · Model Performance", page_icon="📐", layout="wide")
st.title("📐 Model Performance & Data Quality")
st.caption("Live model evaluation, data quality, and feature reference for analysts.")

st.warning(
    "🚧 **Evaluation in development — metrics are still stabilizing.** "
    "The reconciliation loop is young; some lag buckets have very few pairs "
    "(check **n** before trusting a number) and the numbers below will move "
    "as more live outcomes accumulate. Not final results.",
    icon="🚧",
)

# ---- shared path resolution — same convention as data_access.py's
# AEROFLUX_PREDICTIONS (out/predictions.parquet); everything else here lives
# alongside it under the same out/ directory. -------------------------------
_OUT_DIR = Path(os.getenv("AEROFLUX_PREDICTIONS", "out/predictions.parquet")).resolve().parent
_EVAL_DIR = _OUT_DIR / "eval"
_GOLD_LIVE_DIR = _OUT_DIR / "gold_live"
_RUN_DIR_FILE = _OUT_DIR / ".current_run_dir"


# ============================================================================
# Section 1 — Live model evaluation
# ============================================================================

@st.cache_data(ttl=1800)
def load_live_metrics() -> dict | None:
    p = _EVAL_DIR / "live_metrics_latest.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _fmt(x) -> str:
    if x is None:
        return "N/A"
    try:
        if x != x:  # NaN
            return "N/A"
        return f"{x:.4f}"
    except TypeError:
        return str(x)


st.header("Live model evaluation")

metrics = load_live_metrics()
if metrics is None:
    st.info(
        "No live evaluation data yet. Run `python -m aeroflux_ml.evaluate_live` "
        "to reconcile live predictions against realized outcomes and generate "
        "`out/eval/live_metrics_latest.json`."
    )
else:
    captured = metrics.get("captured_at_utc", "unknown")
    st.caption(f"Last evaluated: **{captured}** · reconciled pairs: "
               f"**{metrics.get('n_pairs', 'N/A'):,}** · prediction date range: "
               f"{metrics.get('scored_at_min', 'N/A')} → {metrics.get('scored_at_max', 'N/A')}")

    overall = metrics.get("overall", {})
    bts = metrics.get("bts_reference")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("ROC-AUC", _fmt(overall.get("roc_auc")))
    c2.metric("PR-AUC", _fmt(overall.get("pr_auc")))
    c3.metric("F1", _fmt(overall.get("f1")))
    c4.metric("Brier", _fmt(overall.get("brier")))
    c5.metric("Actual delay rate", _fmt(overall.get("actual_delay_rate")))

    if bts:
        st.caption(
            f"BTS training reference (`{bts.get('run_id')}` / `{bts.get('name')}`, "
            f"held-out test, n={bts.get('n')}): ROC-AUC={_fmt(float(bts.get('roc_auc', 0)))}  "
            f"PR-AUC={_fmt(float(bts.get('pr_auc', 0)))}  F1={_fmt(float(bts.get('f1', 0)))}  "
            f"Brier={_fmt(float(bts.get('brier', 0)))}"
        )

    col_a, col_b = st.columns([0.45, 0.55])

    with col_a:
        st.subheader("Confusion matrix (threshold 0.5)")
        cm = overall.get("confusion_matrix", {})
        cm_df = pd.DataFrame(
            [[cm.get("tn", 0), cm.get("fp", 0)], [cm.get("fn", 0), cm.get("tp", 0)]],
            index=["Actual on-time", "Actual delayed"],
            columns=["Predicted on-time", "Predicted delayed"],
        )
        fig_cm = go.Figure(go.Heatmap(
            z=cm_df.values, x=list(cm_df.columns), y=list(cm_df.index),
            text=cm_df.values, texttemplate="%{text}", colorscale="Blues", showscale=False))
        fig_cm.update_layout(template="plotly_dark", height=280,
                             paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig_cm, use_container_width=True)

    with col_b:
        st.subheader("Calibration — predicted vs. observed")
        cal = overall.get("calibration")
        if cal and cal.get("mean_predicted"):
            fig_cal = go.Figure()
            fig_cal.add_trace(go.Scatter(x=[0, 1], y=[0, 1], mode="lines",
                                          line=dict(dash="dash", color="#64748b"),
                                          name="perfect calibration"))
            fig_cal.add_trace(go.Scatter(
                x=cal["mean_predicted"], y=cal["observed_rate"], mode="lines+markers",
                line=dict(color="#22d3ee"), marker=dict(size=8), name="live"))
            fig_cal.update_layout(
                template="plotly_dark", height=280,
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=10, b=10),
                xaxis_title="mean predicted P(delay)", yaxis_title="observed delay rate",
                xaxis_range=[0, 1], yaxis_range=[0, 1])
            st.plotly_chart(fig_cal, use_container_width=True)
        else:
            st.caption("Not enough pairs (or only one outcome class) yet to bin a calibration curve.")

    st.subheader("Accuracy by lag bucket — how far ahead did we predict?")
    st.caption(
        "Each bucket is how long before the outcome the kept prediction was made "
        "(most lead-time-generous sample available in that band). This answers "
        "\"how well do we predict N hours out,\" not just \"how good was our last "
        "guess before landing.\""
    )
    bucket_order = metrics.get("bucket_order", [])
    buckets = metrics.get("buckets", {})
    rows = []
    for label in bucket_order:
        bm = buckets.get(label, {})
        cm = bm.get("confusion_matrix", {})
        rows.append({
            "Lag bucket": label, "n": bm.get("n", 0),
            "ROC-AUC": _fmt(bm.get("roc_auc")), "PR-AUC": _fmt(bm.get("pr_auc")),
            "F1": _fmt(bm.get("f1")), "Brier": _fmt(bm.get("brier")),
            "Actual delay rate": _fmt(bm.get("actual_delay_rate")),
            "TN": cm.get("tn", 0), "FP": cm.get("fp", 0),
            "FN": cm.get("fn", 0), "TP": cm.get("tp", 0),
        })
    bucket_df = pd.DataFrame(rows)
    if not bucket_df.empty:
        n_fig = go.Figure(go.Bar(x=bucket_df["Lag bucket"], y=bucket_df["n"],
                                  marker_color="#22c55e"))
        n_fig.update_layout(template="plotly_dark", height=220,
                            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            margin=dict(l=10, r=10, t=10, b=10),
                            yaxis_title="reconciled pairs (n)")
        st.plotly_chart(n_fig, use_container_width=True)
        st.dataframe(bucket_df, use_container_width=True, hide_index=True)
    st.caption(
        "⚠️ Ingest was down Aug 9–11 2026; some of this data comes from that "
        "thinner window. Small-n buckets are noisy — treat n<50 as directional, "
        "not conclusive."
    )

st.divider()

# ============================================================================
# Section 2 — Data quality
# ============================================================================

st.header("Data quality")


@st.cache_data(ttl=1800)
def load_dynamodb_item_count() -> dict | None:
    """FREE describe-table call only — never a Scan. Returns None if not on
    the DynamoDB backend or the call fails for any reason (never crashes the
    page over this)."""
    if os.getenv("STATE_BACKEND", "postgres").lower() != "dynamodb":
        return None
    try:
        from aeroflux_ml.io import _boto3_client
        table = os.getenv("DYNAMODB_TABLE", "aeroflux-current-state")
        region = os.getenv("AWS_REGION", "us-east-1")
        client = _boto3_client("dynamodb", region)
        resp = client.describe_table(TableName=table)
        t = resp["Table"]
        return {"item_count": t.get("ItemCount"), "size_bytes": t.get("TableSizeBytes"),
                "table": table}
    except Exception as e:
        return {"error": str(e)}


@st.cache_data(ttl=1800)
def load_latest_gold_snapshot_stats() -> dict | None:
    """ADS-B/hex-coverage proxy + today's actual delay rate, from files
    already on disk — no new DB reads of any kind."""
    if not _GOLD_LIVE_DIR.exists():
        return None
    snaps = sorted(_GOLD_LIVE_DIR.glob("gold_*.parquet"))
    if not snaps:
        return None
    import polars as pl
    latest = snaps[-1]
    try:
        df = pl.read_parquet(latest, columns=["inbound_resolved"])
    except Exception:
        return None
    return {
        "snapshot": latest.name,
        "n": df.height,
        "inbound_resolved_rate": float(df["inbound_resolved"].mean()) if df.height else None,
    }


@st.cache_data(ttl=1800)
def load_today_delay_rate() -> dict | None:
    pairs_path = _EVAL_DIR / "reconciled_pairs.parquet"
    if not pairs_path.exists():
        return None
    import polars as pl
    try:
        df = pl.read_parquet(pairs_path, columns=["outcome_observed_at_utc", "actual_delayed"])
    except Exception:
        return None
    if df.height == 0:
        return None
    today = datetime.now(timezone.utc).date()
    today_df = df.filter(pl.col("outcome_observed_at_utc").dt.date() == today)
    return {
        "n_today": today_df.height,
        "delay_rate_today": float(today_df["actual_delayed"].mean()) if today_df.height else None,
        "n_overall": df.height,
        "delay_rate_overall": float(df["actual_delayed"].mean()),
    }


dq1, dq2, dq3 = st.columns(3)

with dq1:
    st.subheader("DynamoDB records")
    dd = load_dynamodb_item_count()
    if dd is None:
        st.caption("STATE_BACKEND is not `dynamodb` on this host — not applicable "
                   "(local Postgres mode).")
    elif "error" in dd:
        st.caption(f"Unavailable: {dd['error']}")
    else:
        st.metric("Total items (approx.)", f"{dd['item_count']:,}" if dd["item_count"] is not None else "N/A")
        if dd.get("size_bytes"):
            st.caption(f"Table size: {dd['size_bytes']/1_048_576:.1f} MB · "
                       f"`{dd['table']}` · free `describe-table` call, "
                       f"AWS refreshes this estimate ~every 6h — never a Scan.")

with dq2:
    st.subheader("ADS-B / hex coverage")
    g = load_latest_gold_snapshot_stats()
    if g is None:
        st.caption("No gold_live snapshot found yet.")
    else:
        rate = g["inbound_resolved_rate"]
        st.metric("Rotation resolution rate", f"{rate*100:.1f}%" if rate is not None else "N/A")
        st.caption(f"`inbound_resolved` mean over {g['n']:,} rows in the latest gold_live "
                   f"snapshot (`{g['snapshot']}`) — a proxy for ADS-B hex resolution "
                   f"(rotation features can only activate when the airframe's hex "
                   f"resolved to a prior leg). Not a direct hex-fill-rate query.")

with dq3:
    st.subheader("Actual delay rate")
    td = load_today_delay_rate()
    if td is None:
        st.caption("No reconciled outcomes yet.")
    else:
        st.metric("Today (UTC)", f"{td['delay_rate_today']*100:.1f}%" if td["delay_rate_today"] is not None
                   else "no outcomes today yet", f"n={td['n_today']}")
        st.caption(f"All-time reconciled: {td['delay_rate_overall']*100:.1f}% "
                   f"(n={td['n_overall']:,}).")

st.divider()

# ============================================================================
# Section 3 — Feature list + XGBoost importances
# ============================================================================

st.header("Model features")


@st.cache_resource
def load_model():
    """Same lookup chain as pages/3_Live_Inference.py's load_model() — the
    app's own models/ dir, not out/current_model.joblib, so this reflects
    what's actually scoring live flights right now."""
    import joblib
    here = Path(__file__).resolve().parent.parent  # aeroflux_ui/streamlit_app/
    for name in ("xgb_classifier_live.joblib",
                 "xgb_classifier_xgb_full_aircraft.joblib",
                 "xgb_classifier_xgb_full.joblib"):
        p = here / "models" / name
        if p.exists():
            try:
                return joblib.load(p)
            except Exception:
                pass
    return None


@st.cache_data(ttl=1800)
def load_feature_columns() -> tuple[list[str], bool]:
    """The exact feature order/set the model was trained+scored with —
    feature_prep.feature_columns() is the single source of truth (parity
    rule), not something this page re-derives. include_gap_weather comes
    from the linked run's own config where available."""
    from aeroflux_ml import feature_prep as fp
    include_gap = False
    if _RUN_DIR_FILE.exists():
        try:
            run_dir = Path(_RUN_DIR_FILE.read_text().strip())
            meta = json.loads((run_dir / "run.json").read_text())
            include_gap = bool(meta.get("config", {}).get("preprocess", {}).get("include_gap_weather", False))
        except Exception:
            pass
    return fp.feature_columns(include_gap_weather=include_gap), include_gap


_FEATURE_DESCRIPTIONS = {
    "sched_dep_hour": "Scheduled departure hour (0–23 UTC)",
    "sched_dep_dow": "Day of week (1=Mon…7=Sun)",
    "sched_dep_month": "Month (1–12)",
    "is_weekend": "Weekend flag (dow ∈ {6,7})",
    "sched_block_min": "Scheduled gate-to-gate time (minutes)",
    "prev_leg_arr_delay_min": "Inbound aircraft's arrival delay (rotation)",
    "turnaround_buffer_min": "Scheduled ground time before this leg",
    "legs_into_day": "0-based leg index for the airframe that day",
    "inbound_resolved": "Was the inbound leg found? (rotation link known)",
    "origin_dep_demand": "Departures from origin airport in the rolling window",
    "origin_recent_dep_delay": "Mean recent departure delay at origin",
    "dest_arr_demand": "Arrivals into destination airport in the rolling window",
    "dest_recent_arr_delay": "Mean recent arrival delay at destination",
    "origin_wx_wind_kt": "Origin wind speed (knots)",
    "origin_wx_ifr": "Origin IFR conditions (ceiling < 1000ft)",
    "dest_wx_wind_kt": "Destination wind speed (knots)",
    "dest_wx_ifr": "Destination IFR conditions (ceiling < 1000ft)",
    "origin_wx_temp_c": "Origin temperature (°C) — opt-in parity-gap weather",
    "origin_wx_ceiling_ft": "Origin cloud ceiling (feet) — opt-in parity-gap weather",
    "origin_wx_vis_mi": "Origin visibility (miles) — opt-in parity-gap weather",
    "dest_wx_temp_c": "Destination temperature (°C) — opt-in parity-gap weather",
    "dest_wx_ceiling_ft": "Destination cloud ceiling (feet) — opt-in parity-gap weather",
    "dest_wx_vis_mi": "Destination visibility (miles) — opt-in parity-gap weather",
}

MODEL = load_model()
feat_cols, include_gap = load_feature_columns()

if MODEL is None:
    st.info("No model artifact found under the app's `models/` directory.")
else:
    try:
        importances = MODEL.feature_importances_
    except AttributeError:
        importances = None
    if importances is None or len(importances) != len(feat_cols):
        st.warning(f"Model has {len(importances) if importances is not None else '?'} "
                   f"importances but feature_prep reports {len(feat_cols)} features "
                   f"(include_gap_weather={include_gap}) — can't align them reliably. "
                   f"Showing the feature list only.")
        imp_df = pd.DataFrame({"feature": feat_cols,
                               "description": [_FEATURE_DESCRIPTIONS.get(f, "") for f in feat_cols]})
        st.dataframe(imp_df, use_container_width=True, hide_index=True)
    else:
        imp_df = pd.DataFrame({
            "feature": feat_cols, "importance": importances,
            "description": [_FEATURE_DESCRIPTIONS.get(f, "") for f in feat_cols],
        }).sort_values("importance", ascending=False).reset_index(drop=True)

        fig_imp = go.Figure(go.Bar(
            x=imp_df["importance"][::-1], y=imp_df["feature"][::-1], orientation="h",
            marker_color="#a855f7"))
        fig_imp.update_layout(template="plotly_dark", height=max(320, 22 * len(imp_df)),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              margin=dict(l=10, r=10, t=10, b=10),
                              xaxis_title="XGBoost feature importance (gain-normalized)")
        st.plotly_chart(fig_imp, use_container_width=True)
        st.dataframe(imp_df, use_container_width=True, hide_index=True)
        st.caption(f"{len(feat_cols)} features · include_gap_weather={include_gap} "
                   f"(see CLAUDE.md golden rule #6) · order matches "
                   f"`feature_prep.feature_columns()`, the single feature contract "
                   f"used by both training and live scoring.")

st.divider()

# ============================================================================
# Section 4 — Glossary
# ============================================================================

st.header("Glossary")
st.markdown(
    """
**Delay (label)** — a flight is counted as *delayed* if `arr_delay_min ≥ 15`
(arrival at least 15 minutes late). This is the model's training label and
the definition used everywhere on this page (`actual_delayed`). Departure
delay (`dep_delay_min`) is tracked as a diagnostic only — it is never a
model feature or the label.

**Propagation pressure** — `propagation_pressure_min = max(0, prev_leg_arr_delay_min − turnaround_buffer_min)`:
the inbound aircraft's delay that eats into (and exceeds) the scheduled
turnaround time. A simple, interpretable stand-in for real queuing/rotation
propagation — 0 when there's no resolved inbound leg or the inbound wasn't
late enough to matter.

**Origin recent departure delay** (`origin_recent_dep_delay`) — the rolling
mean departure delay (minutes) among recent flights out of the origin
airport, as of the scheduled-departure scoring moment. A same-airport
demand/congestion signal, not specific to this flight's own aircraft.

**Rotation / inbound resolution** (`inbound_resolved`) — whether this
flight's incoming aircraft (by tail/ADS-B hex) could be linked to its prior
leg. When unresolved, `prev_leg_arr_delay_min`, `turnaround_buffer_min`, and
`legs_into_day` are filled 0 (absence, not "no delay") — see CLAUDE.md
golden rule #4.

**IFR** (`origin_wx_ifr` / `dest_wx_ifr`) — Instrument Flight Rules
conditions, flagged when cloud ceiling is below 1000ft. One of the two
weather signals present in both live METAR and training data (the other is
wind speed); temperature/ceiling/visibility are training-dense but
live-sparse ("parity-gap weather") and stay opt-in.

**Unusual event** — an operationally abnormal condition (e.g. a ground
stop, severe weather program, ATC-imposed delay program) outside normal
day-to-day variation. AeroFlux does not currently model this as a distinct
feature — flagged here for analyst context, not as a column in the feature
table above.

**Lag bucket** (this page) — how long before the outcome (landing) the
prediction being evaluated was made: `24h+`, `6-24h`, `2-6h`, `0.5-2h`,
`<30min`. Introduced 2026-08-12 to replace a single "last guess before
landing" number that was silently discarding genuine multi-hour-lead-time
evidence.

**BTS / training vs. live** — BTS is the historical DOT On-Time Performance
dataset the model is trained on; live is real-time FAA SWIM + ADS-B +
METAR. Both go through the exact same `feature_prep.py` contract
(train/serve parity) — see CLAUDE.md golden rule #1.
"""
)

st.caption(
    "Read-only page: reads out/eval/live_metrics_latest.json, the local model "
    "artifact, the latest gold_live snapshot, and DynamoDB's free describe-table "
    "ItemCount only. No Scans, no writes, no effect on the running pipeline."
)
