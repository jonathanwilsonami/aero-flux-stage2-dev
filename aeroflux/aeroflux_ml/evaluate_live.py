"""Reconcile forward-captured live predictions against realized outcomes,
then score them with the same metric family used for BTS training — broken
out per lag bucket (how far ahead of the outcome the kept prediction was
made), not flattened to one number.

Predictions are scored at scheduled-departure time (the forecast moment);
ground truth (actual arrival delay) only becomes known hours later, once the
flight lands. score_live.py's hourly prediction snapshots (out/predictions/)
and e2e.sh's hourly gold snapshots (out/gold_live/ — arr_delay_min populated
once a flight completes) are the two halves; this module pairs EARLIER
predictions with a LATER-observed outcome for the same flight_key — never
the reverse (that would be hindsight, not evaluation).

    python -m aeroflux_ml.evaluate_live              # reconcile, then report
    python -m aeroflux_ml.evaluate_live reconcile    # stage 1 only
    python -m aeroflux_ml.evaluate_live report       # stage 2 only

Stage 1 (reconcile) is incremental / append-only: out/eval/reconciled_pairs.parquet
only ever grows, and out/eval/reconcile_state.json tracks which snapshot
files have already been folded in, so a rerun only reads whatever's new.

REVISION (2026-08-12): the original version kept only the single LATEST
pre-outcome prediction per flight_key. Verified live that this silently
discarded genuine multi-hour-lag evidence — flight_key KS786757QQ was
predicted hourly for 36 straight hours (2026-08-09 10:55 -> 2026-08-10
23:00) before landing, and the old dedupe kept only the last (~17min lag)
prediction, throwing away the other 30+. An outage-window vs. clean-
overnight split showed statistically identical lag profiles (both ~88%
under 30min), which is what exposed this as a dedupe-rule artifact, not an
outage artifact. Now every flight can contribute UP TO one pair per lag
bucket (its most lead-time-generous prediction within that bucket), so
accuracy can be reported as a function of how far ahead the prediction was
made — directly answering "how well do we predict 2+ hours out," not just
"how well do we predict right before landing."

Guardrails (unchanged in spirit):
  - earlier-prediction-to-later-outcome ONLY: a prediction's `scored_at`
    (UTC, per-row, the real forecast moment) must be strictly before the
    gold snapshot's own capture time. Gold snapshot filenames are stamped
    in the host's LOCAL time (bash `date +%Y%m%d_%H%M`), not UTC — verified
    empirically (2026-08-11) that predictions_20260809_0655's filename
    (local ~06:55) holds rows with scored_at=10:55:26 (a 4h/EDT offset).
    _local_to_utc() below fixes this.
  - only flights with arr_delay_min actually populated in that gold
    snapshot count as a resolved outcome.
  - dedupe by (flight_key, lag_bucket): within a bucket, keep the
    LARGEST-lag candidate (the earliest prediction that still falls in
    that bucket's range — the most lead-time-generous sample for that
    band), verified via sort(descending) + group_by().first().
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl

PRED_DIR = Path("out/predictions")
GOLD_DIR = Path("out/gold_live")
OUT_DIR = Path("out/eval")
PAIRS_PATH = OUT_DIR / "reconciled_pairs.parquet"
STATE_PATH = OUT_DIR / "reconcile_state.json"
PENDING_PATH = OUT_DIR / "_pending_predictions.parquet"

DELAY_THRESHOLD_MIN = 15  # matches the training label: arr_delay_min >= 15
_TS_RE = re.compile(r"(\d{8})_(\d{4})")

# Lag buckets: hours between the kept prediction's scored_at and the outcome
# becoming observed. Ordered widest-lead-time first — this order is reused
# for report/table ordering everywhere below, so it's the single source of
# truth for both bucket assignment and display order.
_LAG_BUCKETS = [
    (24.0, float("inf"), "24h+"),
    (6.0, 24.0, "6-24h"),
    (2.0, 6.0, "2-6h"),
    (0.5, 2.0, "0.5-2h"),
    (0.0, 0.5, "<30min"),
]
_LAG_BUCKET_LABELS = [label for _, _, label in _LAG_BUCKETS]


def _bucket_expr(lag_col: str = "lag_h") -> pl.Expr:
    lag = pl.col(lag_col)
    expr = None
    for lo, hi, label in _LAG_BUCKETS:
        cond = (lag >= lo) if hi == float("inf") else ((lag >= lo) & (lag < hi))
        expr = pl.when(cond).then(pl.lit(label)) if expr is None else expr.when(cond).then(pl.lit(label))
    return expr.otherwise(None)


def _parse_snapshot_local_dt(path: Path) -> datetime | None:
    m = _TS_RE.search(path.name)
    if not m:
        return None
    return datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M")


def _local_to_utc(dt_local: datetime) -> datetime:
    """See module docstring — filenames are local time, scored_at is UTC."""
    offset = datetime.now().astimezone().utcoffset() or timedelta(0)
    return dt_local - offset


def _list_snapshots(dir_: Path, prefix: str) -> list[Path]:
    return sorted(dir_.glob(f"{prefix}_*.parquet"))


def _load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"processed_predictions": [], "processed_gold": [], "runs": []}


def _save_state(state: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2, default=str))


# Pending predictions accumulate — one row per (flight_key, scored_at) seen
# so far, not collapsed to one-per-flight — because which bucket a
# prediction belongs to can't be known until the outcome's timestamp is
# known, so nothing can be discarded before then. This is the actual fix:
# the old version collapsed to "latest wins" at THIS stage, before the
# outcome was even known, which is what silently threw away long-lag
# evidence.
_PENDING_SCHEMA = {
    "flight_key": pl.Utf8, "scored_at": pl.Datetime,
    "delay_probability": pl.Float64, "predicted_delayed": pl.Int64,
    "model_version": pl.Utf8,
}


def _load_pending() -> pl.DataFrame:
    if PENDING_PATH.exists():
        return pl.read_parquet(PENDING_PATH)
    return pl.DataFrame(schema=_PENDING_SCHEMA)


def _save_pending(df: pl.DataFrame) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if df.height == 0:
        if PENDING_PATH.exists():
            PENDING_PATH.unlink()
        return
    df.write_parquet(PENDING_PATH)


def reconcile(pred_dir: Path = PRED_DIR, gold_dir: Path = GOLD_DIR, *, quiet: bool = False) -> dict:
    """Stage 1. Returns a summary dict — new_pairs, total_pairs, per-bucket
    counts, date range, still_pending."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    state = _load_state()
    processed_pred = set(state["processed_predictions"])
    processed_gold = set(state["processed_gold"])

    # A flight_key is "resolved" once its FIRST completed-outcome gold
    # snapshot has been processed — every applicable bucket gets emitted at
    # that moment, and it's never reconsidered (arr_delay_min, once
    # populated, stays populated in every later gold snapshot too — we only
    # want the first, truest observation time).
    resolved: set[str] = set()
    if PAIRS_PATH.exists():
        resolved = set(pl.read_parquet(PAIRS_PATH, columns=["flight_key"])["flight_key"].to_list())

    pending = _load_pending()
    if resolved and pending.height:
        pending = pending.filter(~pl.col("flight_key").is_in(list(resolved)))

    # ---- fold in new prediction snapshots: accumulate, never collapse -----
    pred_files = _list_snapshots(pred_dir, "predictions")
    new_pred_files = [p for p in pred_files if p.name not in processed_pred]
    new_pred_rows = []
    for p in new_pred_files:
        try:
            df = pl.read_parquet(p, columns=list(_PENDING_SCHEMA)).select(list(_PENDING_SCHEMA))
        except Exception as e:
            if not quiet:
                print(f"  skip (unreadable) {p.name}: {e}")
            continue
        if resolved:
            df = df.filter(~pl.col("flight_key").is_in(list(resolved)))
        new_pred_rows.append(df)
        processed_pred.add(p.name)

    if new_pred_rows:
        pending = pl.concat([pending, *new_pred_rows], how="diagonal_relaxed")
        # Exact (flight_key, scored_at) duplicates only (e.g. a snapshot
        # read twice) — NOT a flight_key-only collapse, which would be the
        # same bug all over again.
        pending = pending.unique(subset=["flight_key", "scored_at"], keep="first")

    # ---- check new gold snapshots; bucket + emit pairs on first completion ----
    gold_files = _list_snapshots(gold_dir, "gold")
    new_gold_files = [g for g in gold_files if g.name not in processed_gold]
    new_pair_rows: list[dict] = []

    for g in new_gold_files:
        g_local = _parse_snapshot_local_dt(g)
        if g_local is None:
            if not quiet:
                print(f"  skip (unparseable filename) {g.name}")
            continue
        g_utc = _local_to_utc(g_local)
        try:
            gdf = pl.read_parquet(g, columns=["flight_key", "arr_delay_min"])
        except Exception as e:
            if not quiet:
                print(f"  skip (unreadable) {g.name}: {e}")
            processed_gold.add(g.name)
            continue
        completed = gdf.filter(pl.col("arr_delay_min").is_not_null())
        if completed.height and pending.height:
            newly_completed = [k for k in completed["flight_key"].to_list() if k not in resolved]
            if newly_completed:
                arr_by_key = dict(zip(completed["flight_key"].to_list(), completed["arr_delay_min"].to_list()))
                cand = pending.filter(pl.col("flight_key").is_in(newly_completed))
                cand = cand.filter(pl.col("scored_at") < g_utc)
                if cand.height:
                    lag_h = (g_utc - cand["scored_at"]).dt.total_minutes() / 60.0
                    cand = cand.with_columns(lag_h.alias("lag_h"))
                    cand = cand.with_columns(_bucket_expr().alias("lag_bucket"))
                    cand = cand.filter(pl.col("lag_bucket").is_not_null())
                    if cand.height:
                        # Within each (flight_key, lag_bucket), keep the
                        # LARGEST lag — the earliest prediction that still
                        # falls in that bucket, i.e. the most lead-time-
                        # generous sample available for that band. Verified
                        # (module tests) that sort(descending) +
                        # group_by().first() picks the max, not an
                        # arbitrary row.
                        best = (cand.sort("lag_h", descending=True)
                                .group_by(["flight_key", "lag_bucket"], maintain_order=False)
                                .first())
                        for row in best.iter_rows(named=True):
                            adm = arr_by_key[row["flight_key"]]
                            new_pair_rows.append({
                                "flight_key": row["flight_key"],
                                "scored_at": row["scored_at"],
                                "delay_probability": row["delay_probability"],
                                "predicted_delayed": row["predicted_delayed"],
                                "model_version": row["model_version"],
                                "lag_bucket": row["lag_bucket"],
                                "lag_hours": row["lag_h"],
                                "arr_delay_min": adm,
                                "actual_delayed": int(adm >= DELAY_THRESHOLD_MIN),
                                "outcome_snapshot": g.name,
                                "outcome_observed_at_utc": g_utc,
                            })
                # Resolved whether or not it had a valid preceding
                # prediction to pair (e.g. first ever seen already-landed)
                # — must never be reconsidered against a later gold snapshot.
                resolved.update(newly_completed)
                pending = pending.filter(~pl.col("flight_key").is_in(newly_completed))
        processed_gold.add(g.name)

    new_pair_count = 0
    bucket_counts_this_run: dict[str, int] = {}
    if new_pair_rows:
        pairs_df = pl.DataFrame(new_pair_rows).select([
            "flight_key", "scored_at", "delay_probability", "predicted_delayed",
            "model_version", "lag_bucket", "lag_hours", "arr_delay_min", "actual_delayed",
            "outcome_snapshot", "outcome_observed_at_utc",
        ])
        # Defensive: (flight_key, lag_bucket) should never repeat within a
        # single pass (each flight_key is resolved exactly once, above) —
        # assert rather than silently double-count if that's ever violated.
        assert pairs_df.select(["flight_key", "lag_bucket"]).n_unique() == pairs_df.height, \
            "duplicate (flight_key, lag_bucket) within a single reconcile pass — dedupe logic bug"
        new_pair_count = pairs_df.height
        bucket_counts_this_run = dict(
            pairs_df.group_by("lag_bucket").agg(pl.len().alias("n")).iter_rows())
        if PAIRS_PATH.exists():
            existing = pl.read_parquet(PAIRS_PATH)
            pairs_df = pl.concat([existing, pairs_df], how="diagonal_relaxed")
        pairs_df.write_parquet(PAIRS_PATH)

    _save_pending(pending)
    state["processed_predictions"] = sorted(processed_pred)
    state["processed_gold"] = sorted(processed_gold)
    state.setdefault("runs", []).append({
        "at": datetime.now(timezone.utc).isoformat(),
        "new_prediction_snapshots": len(new_pred_files),
        "new_gold_snapshots": len(new_gold_files),
        "new_pairs": new_pair_count,
        "new_pairs_by_bucket": bucket_counts_this_run,
        "still_pending": pending.height,
    })
    _save_state(state)

    total = pl.read_parquet(PAIRS_PATH) if PAIRS_PATH.exists() else pl.DataFrame(schema={"scored_at": pl.Datetime, "lag_bucket": pl.Utf8})
    result = {"new_pairs": new_pair_count, "total_pairs": total.height,
              "new_prediction_snapshots": len(new_pred_files), "new_gold_snapshots": len(new_gold_files),
              "still_pending": pending.height, "new_pairs_by_bucket": bucket_counts_this_run}
    if total.height:
        result["scored_at_min"] = total["scored_at"].min()
        result["scored_at_max"] = total["scored_at"].max()
        result["total_by_bucket"] = dict(total.group_by("lag_bucket").agg(pl.len().alias("n")).iter_rows())
    return result


# ============================================================================
# Stage 2 — metrics, same family as BTS training + calibration, per bucket
# ============================================================================

def _bts_reference_row() -> dict | None:
    """Pull the winning BTS model's held-out test metrics from the currently
    deployed training run, for a direct side-by-side comparison — same
    metrics() function, same columns, just a different (BTS test split vs.
    live) dataset."""
    run_dir_file = Path("out/.current_run_dir")
    if not run_dir_file.exists():
        return None
    run_dir = Path(run_dir_file.read_text().strip())
    comp = run_dir / "tables" / "comparison.csv"
    if not comp.exists():
        return None
    import csv
    rows = list(csv.DictReader(open(comp)))
    if not rows:
        return None
    rows.sort(key=lambda r: int(r.get("rank", 9)))
    row = rows[0]
    row["run_id"] = run_dir.name
    return row


def compute_metrics_for(df: pl.DataFrame) -> dict:
    """Metrics for one subset (overall, or one lag bucket). Guards small/
    degenerate subsets rather than letting sklearn raise."""
    n = df.height
    if n == 0:
        return {"n": 0, "roc_auc": None, "pr_auc": None, "f1": None, "accuracy": None,
                "brier": None, "positive_rate": None, "actual_delay_rate": None,
                "confusion_matrix": {"tn": 0, "fp": 0, "fn": 0, "tp": 0}, "calibration": None}

    from sklearn.metrics import confusion_matrix
    from sklearn.calibration import calibration_curve
    from aeroflux_ml.training.evaluate import metrics as shared_metrics

    y_true = df["actual_delayed"].to_numpy().astype(int)
    p = df["delay_probability"].to_numpy().astype(float)

    m = shared_metrics(y_true, p)  # roc_auc, pr_auc, f1, accuracy, brier, positive_rate, n
    m["actual_delay_rate"] = float(y_true.mean())

    yhat = (p >= 0.5).astype(int)
    cm = confusion_matrix(y_true, yhat, labels=[0, 1])
    m["confusion_matrix"] = {"tn": int(cm[0, 0]), "fp": int(cm[0, 1]),
                              "fn": int(cm[1, 0]), "tp": int(cm[1, 1])}

    n_classes = len(set(y_true.tolist()))
    if n_classes > 1 and n >= 10:
        n_bins = min(10, max(2, n // 5))
        try:
            frac_pos, mean_pred = calibration_curve(y_true, p, n_bins=n_bins, strategy="quantile")
        except ValueError:
            frac_pos, mean_pred = calibration_curve(y_true, p, n_bins=n_bins, strategy="uniform")
        m["calibration"] = {"mean_predicted": [float(x) for x in mean_pred],
                             "observed_rate": [float(x) for x in frac_pos]}
    else:
        m["calibration"] = None

    return m


def compute_metrics(pairs: pl.DataFrame) -> dict:
    """Overall metrics + one entry per lag bucket."""
    overall = compute_metrics_for(pairs)
    buckets = {}
    for label in _LAG_BUCKET_LABELS:
        buckets[label] = compute_metrics_for(pairs.filter(pl.col("lag_bucket") == label))
    return {"overall": overall, "buckets": buckets}


def _fmt(x) -> str:
    if x is None:
        return "N/A"
    try:
        if x != x:  # NaN
            return "N/A"
        return f"{x:.4f}"
    except TypeError:
        return str(x)


def _metrics_table_rows(m: dict, bts: dict | None) -> list[str]:
    lines = []
    if bts:
        lines.append("| Metric | Live | BTS training (held-out test) |")
        lines.append("|---|---|---|")
        lines.append(f"| n | {m['n']} | {bts.get('n', 'N/A')} |")
        lines.append(f"| ROC-AUC | {_fmt(m['roc_auc'])} | {_fmt(float(bts['roc_auc']))} |")
        lines.append(f"| PR-AUC | {_fmt(m['pr_auc'])} | {_fmt(float(bts['pr_auc']))} |")
        lines.append(f"| F1 | {_fmt(m['f1'])} | {_fmt(float(bts['f1']))} |")
        lines.append(f"| Accuracy | {_fmt(m['accuracy'])} | {_fmt(float(bts['accuracy']))} |")
        lines.append(f"| Brier | {_fmt(m['brier'])} | {_fmt(float(bts['brier']))} |")
        lines.append(f"| Positive rate (predicted) | {_fmt(m['positive_rate'])} | {_fmt(float(bts['positive_rate']))} |")
        lines.append(f"| Actual delay rate | {_fmt(m['actual_delay_rate'])} | (BTS base rate) |")
    else:
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        for key, label in [("n", "n"), ("roc_auc", "ROC-AUC"), ("pr_auc", "PR-AUC"), ("f1", "F1"),
                            ("accuracy", "Accuracy"), ("brier", "Brier"),
                            ("positive_rate", "Positive rate (predicted)"),
                            ("actual_delay_rate", "Actual delay rate")]:
            lines.append(f"| {label} | {m[key] if key == 'n' else _fmt(m[key])} |")
    return lines


def write_report(pairs: pl.DataFrame, metrics: dict, recon_summary: dict) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y%m%dT%H%M%SZ")
    md_path = OUT_DIR / f"live_metrics_{ts_str}.md"
    json_path = OUT_DIR / "live_metrics_latest.json"

    date_min = pairs["scored_at"].min()
    date_max = pairs["scored_at"].max()
    bts = _bts_reference_row()
    overall = metrics["overall"]
    buckets = metrics["buckets"]

    lines: list[str] = []
    lines.append("# AeroFlux Live-Prediction Evaluation")
    lines.append("")
    lines.append(f"**Captured:** {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"**Reconciled pairs:** {pairs.height}")
    lines.append(f"**Prediction date range (scored_at, UTC):** {date_min} → {date_max}")
    lines.append("")
    lines.append("> Each flight can contribute UP TO one pair per lag bucket — how far"
                  " ahead of the outcome the kept prediction was made — not just a single"
                  " last-guess-before-landing number. Within a bucket, the kept prediction"
                  " is the one with the MOST lead time in that band (earliest prediction"
                  " still inside the bucket range). `actual_delayed = arr_delay_min >= "
                  f"{DELAY_THRESHOLD_MIN}`, matching the training label definition.")
    lines.append(">")
    lines.append("> ⚠️ Ingest was down Aug 9–11; prediction/gold snapshots from that"
                  " window are thinner. Reported honestly, not excluded or reweighted.")
    lines.append("")
    lines.append("## Overall metrics — same family as BTS training (`aeroflux_ml/training/evaluate.py`)")
    lines.append("")
    lines.extend(_metrics_table_rows(overall, bts))
    lines.append("")
    lines.append("## Metrics by lag bucket — accuracy vs. how far ahead we predicted")
    lines.append("")
    lines.append("| Lag bucket | n | ROC-AUC | PR-AUC | F1 | Brier | Actual delay rate | TN | FP | FN | TP |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for label in _LAG_BUCKET_LABELS:
        bm = buckets[label]
        cm = bm["confusion_matrix"]
        lines.append(f"| {label} | {bm['n']} | {_fmt(bm['roc_auc'])} | {_fmt(bm['pr_auc'])} | "
                      f"{_fmt(bm['f1'])} | {_fmt(bm['brier'])} | {_fmt(bm['actual_delay_rate'])} | "
                      f"{cm['tn']} | {cm['fp']} | {cm['fn']} | {cm['tp']} |")
    lines.append("")
    lines.append("_Read this as: how much does accuracy degrade the further ahead of the"
                  " outcome the prediction was made? Buckets with very small n are noisy —"
                  " check n before trusting a bucket's numbers._")
    lines.append("")
    lines.append("## Overall confusion matrix (threshold = 0.5)")
    lines.append("")
    cm = overall["confusion_matrix"]
    lines.append("| | Predicted on-time | Predicted delayed |")
    lines.append("|---|---|---|")
    lines.append(f"| **Actual on-time** | {cm['tn']} (TN) | {cm['fp']} (FP) |")
    lines.append(f"| **Actual delayed** | {cm['fn']} (FN) | {cm['tp']} (TP) |")
    lines.append("")
    lines.append("## Overall calibration — predicted probability vs. observed delay rate")
    lines.append("")
    if overall["calibration"]:
        lines.append("| Bin mean predicted P(delay) | Observed delay rate |")
        lines.append("|---|---|")
        for mp, obs in zip(overall["calibration"]["mean_predicted"], overall["calibration"]["observed_rate"]):
            lines.append(f"| {mp:.3f} | {obs:.3f} |")
    else:
        lines.append("_Not enough reconciled pairs yet (or only one outcome class present)"
                      " to bin a calibration curve._")
    lines.append("")
    lines.append("## Reconciliation summary (this run)")
    lines.append("")
    lines.append(f"- New prediction snapshots folded in: {recon_summary.get('new_prediction_snapshots', 'N/A')}")
    lines.append(f"- New gold snapshots checked: {recon_summary.get('new_gold_snapshots', 'N/A')}")
    lines.append(f"- New pairs reconciled this run: {recon_summary.get('new_pairs', 'N/A')}")
    lines.append(f"- Predictions still pending an outcome: {recon_summary.get('still_pending', 'N/A')}")
    lines.append("")
    lines.append(f"_Report generated by `python -m aeroflux_ml.evaluate_live` at {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}._")

    md_path.write_text("\n".join(lines) + "\n")

    json_out = {
        "captured_at_utc": ts.isoformat(),
        "n_pairs": pairs.height,
        "scored_at_min": str(date_min),
        "scored_at_max": str(date_max),
        "bts_reference": bts,
        "overall": overall,
        "buckets": buckets,
        "bucket_order": _LAG_BUCKET_LABELS,
        "reconciliation": recon_summary,
    }
    json_path.write_text(json.dumps(json_out, indent=2, default=str))

    return md_path, json_path


def report() -> dict:
    if not PAIRS_PATH.exists():
        raise SystemExit(f"No reconciled pairs at {PAIRS_PATH} yet — run `reconcile` first.")
    pairs = pl.read_parquet(PAIRS_PATH)
    if pairs.height == 0:
        raise SystemExit("reconciled_pairs.parquet exists but is empty — nothing to report.")
    metrics = compute_metrics(pairs)
    state = _load_state()
    last_run = state.get("runs", [{}])[-1] if state.get("runs") else {}
    md_path, json_path = write_report(pairs, metrics, last_run)
    result = {"metrics": metrics, "md_path": md_path, "json_path": json_path, "n_pairs": pairs.height}

    # Best-effort cloud sync of the small eval outputs, so the deployed
    # app's Model Performance page has real data — never allowed to break
    # local report generation (must keep working with zero cloud creds).
    try:
        from .sync_cloud import sync_eval_outputs
        result["cloud_sync"] = sync_eval_outputs(eval_dir=str(OUT_DIR))
    except Exception as e:
        result["cloud_sync"] = {"synced": False, "error": str(e)}

    return result


def main(argv=None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    action = args[0] if args else "all"

    if action in ("reconcile", "all"):
        print("=" * 70)
        print("Stage 1 — reconciling (prediction, actual_delayed) pairs, per lag bucket")
        print("=" * 70)
        summary = reconcile()
        print(f"new prediction snapshots folded in: {summary['new_prediction_snapshots']}")
        print(f"new gold snapshots checked:          {summary['new_gold_snapshots']}")
        print(f"new pairs reconciled this run:       {summary['new_pairs']}  {summary['new_pairs_by_bucket']}")
        print(f"predictions still pending an outcome: {summary['still_pending']}")
        print()
        print(f"TOTAL RECONCILED PAIRS: {summary['total_pairs']}")
        if summary["total_pairs"]:
            print(f"BY BUCKET: {summary.get('total_by_bucket', {})}")
            print(f"DATE RANGE (scored_at, UTC): {summary['scored_at_min']} -> {summary['scored_at_max']}")
        print()

    if action in ("report", "all"):
        print("=" * 70)
        print("Stage 2 — metrics (same family as BTS training) + calibration, per bucket")
        print("=" * 70)
        out = report()
        m = out["metrics"]["overall"]
        print(f"OVERALL  n={out['n_pairs']}  ROC-AUC={_fmt(m['roc_auc'])}  PR-AUC={_fmt(m['pr_auc'])}  "
              f"F1={_fmt(m['f1'])}  Brier={_fmt(m['brier'])}  actual_delay_rate={_fmt(m['actual_delay_rate'])}")
        print(f"  confusion matrix: {m['confusion_matrix']}")
        for label in _LAG_BUCKET_LABELS:
            bm = out["metrics"]["buckets"][label]
            print(f"[{label:>7}] n={bm['n']:<5} ROC-AUC={_fmt(bm['roc_auc'])}  PR-AUC={_fmt(bm['pr_auc'])}  "
                  f"F1={_fmt(bm['f1'])}  Brier={_fmt(bm['brier'])}  actual_delay_rate={_fmt(bm['actual_delay_rate'])}")
        print()
        print(f"Report written: {out['md_path']}")
        print(f"JSON written:   {out['json_path']}")
        cs = out.get("cloud_sync", {})
        if cs.get("synced"):
            print(f"Cloud sync:     {cs.get('files')}")
        elif cs.get("reason") == "local-only":
            print("Cloud sync:     skipped (STATE_BACKEND=postgres, LAKE_BACKEND=local)")
        elif "error" in cs:
            print(f"Cloud sync:     FAILED ({cs['error']}) — local report unaffected")

    if action not in ("reconcile", "report", "all"):
        print("usage: python -m aeroflux_ml.evaluate_live [reconcile|report|all]", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
