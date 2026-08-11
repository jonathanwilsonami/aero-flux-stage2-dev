"""Reconcile forward-captured live predictions against realized outcomes,
then score them with the same metric family used for BTS training.

Predictions are scored at scheduled-departure time (the forecast moment);
ground truth (actual arrival delay) only becomes known hours later, once the
flight lands. score_live.py's hourly prediction snapshots (out/predictions/)
and e2e.sh's hourly gold snapshots (out/gold_live/ — arr_delay_min populated
once a flight completes) are the two halves; this module pairs an EARLIER
prediction with a LATER-observed outcome for the same flight_key — never the
reverse (that would be hindsight, not evaluation).

    python -m aeroflux_ml.evaluate_live              # reconcile, then report
    python -m aeroflux_ml.evaluate_live reconcile    # stage 1 only
    python -m aeroflux_ml.evaluate_live report       # stage 2 only

Stage 1 (reconcile) is incremental / append-only: out/eval/reconciled_pairs.parquet
only ever grows (one row per flight_key, forever — see the dedupe rule
below), and out/eval/reconcile_state.json tracks which snapshot files have
already been folded in, so a rerun only reads whatever's new.

Guardrails:
  - earlier-prediction-to-later-outcome ONLY: a prediction's `scored_at`
    (UTC, per-row, the real forecast moment) must be strictly before the
    gold snapshot's own capture time. Gold snapshot filenames are stamped
    in the host's LOCAL time (bash `date +%Y%m%d_%H%M`), not UTC — verified
    empirically (2026-08-11) that predictions_20260809_0655's filename
    (local ~06:55) holds rows with scored_at=10:55:26 (a 4h/EDT offset).
    Comparing the two without converting would silently mis-order every
    match by the host's UTC offset; _local_to_utc() below fixes this.
  - only flights with arr_delay_min actually populated in that gold
    snapshot count as a resolved outcome.
  - dedupe by flight_key: if multiple predictions preceded the outcome,
    keep the LATEST one (closest foresight to the actual event, not
    whatever the first guess hours earlier happened to be).
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
    df.write_parquet(PENDING_PATH)


def reconcile(pred_dir: Path = PRED_DIR, gold_dir: Path = GOLD_DIR, *, quiet: bool = False) -> dict:
    """Stage 1. Returns {"new_pairs": int, "total_pairs": int, "scored_at_min": ..., "scored_at_max": ...}."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    state = _load_state()
    processed_pred = set(state["processed_predictions"])
    processed_gold = set(state["processed_gold"])

    already_reconciled: set[str] = set()
    if PAIRS_PATH.exists():
        already_reconciled = set(pl.read_parquet(PAIRS_PATH, columns=["flight_key"])["flight_key"].to_list())

    pending = _load_pending()
    if already_reconciled and pending.height:
        pending = pending.filter(~pl.col("flight_key").is_in(list(already_reconciled)))

    # ---- fold in any new prediction snapshots -------------------------------
    pred_files = _list_snapshots(pred_dir, "predictions")
    new_pred_files = [p for p in pred_files if p.name not in processed_pred]
    new_pred_rows = []
    for p in new_pred_files:
        try:
            # Explicit column order (matches _PENDING_SCHEMA) — vertical
            # concat below matches positionally, not by name; a plain
            # `columns=[...]` read that doesn't match pending's column
            # order broke this on the very first real run.
            df = pl.read_parquet(p, columns=list(_PENDING_SCHEMA)).select(list(_PENDING_SCHEMA))
        except Exception as e:
            if not quiet:
                print(f"  skip (unreadable) {p.name}: {e}")
            continue
        if already_reconciled:
            df = df.filter(~pl.col("flight_key").is_in(list(already_reconciled)))
        new_pred_rows.append(df)
        processed_pred.add(p.name)

    if new_pred_rows:
        incoming = pl.concat(new_pred_rows, how="diagonal_relaxed")
        combined = pl.concat([pending, incoming], how="diagonal_relaxed")
        # Keep the LATEST prediction per flight_key (max scored_at) — verified
        # (see module tests) that sort-ascending + group_by().last() picks the
        # max, not an arbitrary row.
        pending = combined.sort("scored_at").group_by("flight_key", maintain_order=False).last()

    # ---- check new gold snapshots against the pending predictions ----------
    gold_files = _list_snapshots(gold_dir, "gold")
    new_gold_files = [g for g in gold_files if g.name not in processed_gold]
    new_pairs: list[pl.DataFrame] = []

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
            matched = (
                pending.join(completed, on="flight_key", how="inner")
                .filter(pl.col("scored_at") < g_utc)
            )
            if matched.height:
                matched = matched.with_columns([
                    pl.lit(g.name).alias("outcome_snapshot"),
                    pl.lit(g_utc).alias("outcome_observed_at_utc"),
                    (pl.col("arr_delay_min") >= DELAY_THRESHOLD_MIN).cast(pl.Int64).alias("actual_delayed"),
                ])
                new_pairs.append(matched)
                newly_reconciled = matched["flight_key"].to_list()
                pending = pending.filter(~pl.col("flight_key").is_in(newly_reconciled))
                already_reconciled.update(newly_reconciled)
        processed_gold.add(g.name)

    new_pair_count = 0
    if new_pairs:
        pairs_df = pl.concat(new_pairs, how="diagonal_relaxed").select([
            "flight_key", "scored_at", "delay_probability", "predicted_delayed",
            "model_version", "arr_delay_min", "actual_delayed",
            "outcome_snapshot", "outcome_observed_at_utc",
        ])
        # Defensive: a flight_key should never appear twice across runs (the
        # already_reconciled filter above prevents it) — assert rather than
        # silently double-count if that guarantee is ever violated.
        assert pairs_df["flight_key"].n_unique() == pairs_df.height, \
            "duplicate flight_key within a single reconcile pass — dedupe logic bug"
        new_pair_count = pairs_df.height
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
        "still_pending": pending.height,
    })
    _save_state(state)

    total = pl.read_parquet(PAIRS_PATH) if PAIRS_PATH.exists() else pl.DataFrame(schema={"scored_at": pl.Datetime})
    result = {"new_pairs": new_pair_count, "total_pairs": total.height,
               "new_prediction_snapshots": len(new_pred_files), "new_gold_snapshots": len(new_gold_files),
               "still_pending": pending.height}
    if total.height:
        result["scored_at_min"] = total["scored_at"].min()
        result["scored_at_max"] = total["scored_at"].max()
    return result


# ============================================================================
# Stage 2 — metrics, same family as BTS training + calibration
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


def compute_metrics(pairs: pl.DataFrame) -> dict:
    import numpy as np
    from sklearn.metrics import confusion_matrix
    from sklearn.calibration import calibration_curve

    from aeroflux_ml.training.evaluate import metrics as shared_metrics

    y_true = pairs["actual_delayed"].to_numpy().astype(int)
    p = pairs["delay_probability"].to_numpy().astype(float)

    m = shared_metrics(y_true, p)  # roc_auc, pr_auc, f1, accuracy, brier, positive_rate, n
    m["actual_delay_rate"] = float(y_true.mean())

    yhat = (p >= 0.5).astype(int)
    cm = confusion_matrix(y_true, yhat, labels=[0, 1])
    m["confusion_matrix"] = {"tn": int(cm[0, 0]), "fp": int(cm[0, 1]),
                              "fn": int(cm[1, 0]), "tp": int(cm[1, 1])}

    n_classes = len(set(y_true.tolist()))
    if n_classes > 1 and len(y_true) >= 10:
        n_bins = min(10, max(2, len(y_true) // 5))
        try:
            frac_pos, mean_pred = calibration_curve(y_true, p, n_bins=n_bins, strategy="quantile")
        except ValueError:
            frac_pos, mean_pred = calibration_curve(y_true, p, n_bins=n_bins, strategy="uniform")
        m["calibration"] = {"mean_predicted": [float(x) for x in mean_pred],
                             "observed_rate": [float(x) for x in frac_pos]}
    else:
        m["calibration"] = None

    return m


def _fmt(x) -> str:
    if x is None:
        return "N/A"
    try:
        if x != x:  # NaN
            return "N/A"
        return f"{x:.4f}"
    except TypeError:
        return str(x)


def write_report(pairs: pl.DataFrame, m: dict, recon_summary: dict) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc)
    ts_str = ts.strftime("%Y%m%dT%H%M%SZ")
    md_path = OUT_DIR / f"live_metrics_{ts_str}.md"
    json_path = OUT_DIR / "live_metrics_latest.json"

    date_min = pairs["scored_at"].min()
    date_max = pairs["scored_at"].max()
    bts = _bts_reference_row()

    lines: list[str] = []
    lines.append("# AeroFlux Live-Prediction Evaluation")
    lines.append("")
    lines.append(f"**Captured:** {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append(f"**Reconciled pairs:** {pairs.height}")
    lines.append(f"**Prediction date range (scored_at, UTC):** {date_min} → {date_max}")
    lines.append("")
    lines.append("> Reconciled by pairing each live prediction's `delay_probability` /"
                  " `predicted_delayed` (scored at the flight's scheduled-departure"
                  " moment) with the REALIZED outcome once `arr_delay_min` becomes"
                  " populated in a later gold_live snapshot — earlier-prediction-to-"
                  "later-outcome only, never hindsight. `actual_delayed = "
                  f"arr_delay_min >= {DELAY_THRESHOLD_MIN}`, matching the training"
                  " label definition.")
    lines.append(">")
    lines.append("> ⚠️ Ingest was down Aug 9–11; prediction/gold snapshots from that"
                  " window are progressively thinner (fewer flights tracked each"
                  " cycle as 48h retention aged out stale data with nothing new"
                  " backfilling it — see CLAUDE.md Gotchas). Reported honestly below,"
                  " not excluded or reweighted.")
    lines.append("")
    lines.append("## Metrics — same family as BTS training (`aeroflux_ml/training/evaluate.py`)")
    lines.append("")
    if bts:
        lines.append(f"Side by side against the currently-deployed training run's held-out"
                      f" test metrics (`{bts.get('run_id')}` / `{bts.get('name')}`,"
                      f" n={bts.get('n')}):")
        lines.append("")
        lines.append("| Metric | Live (reconciled) | BTS training (held-out test) |")
        lines.append("|---|---|---|")
        lines.append(f"| n | {m['n']} | {bts.get('n', 'N/A')} |")
        lines.append(f"| ROC-AUC | {_fmt(m['roc_auc'])} | {_fmt(float(bts['roc_auc']))} |")
        lines.append(f"| PR-AUC | {_fmt(m['pr_auc'])} | {_fmt(float(bts['pr_auc']))} |")
        lines.append(f"| F1 | {_fmt(m['f1'])} | {_fmt(float(bts['f1']))} |")
        lines.append(f"| Accuracy | {_fmt(m['accuracy'])} | {_fmt(float(bts['accuracy']))} |")
        lines.append(f"| Brier | {_fmt(m['brier'])} | {_fmt(float(bts['brier']))} |")
        lines.append(f"| Positive rate (predicted) | {_fmt(m['positive_rate'])} | {_fmt(float(bts['positive_rate']))} |")
        lines.append(f"| Actual delay rate | {_fmt(m['actual_delay_rate'])} | (BTS base rate, see PROJECT_CONTEXT.md) |")
    else:
        lines.append("_No BTS training run found at `out/.current_run_dir` for the side-by-side — showing live metrics only._")
        lines.append("")
        lines.append("| Metric | Value |")
        lines.append("|---|---|")
        lines.append(f"| n | {m['n']} |")
        lines.append(f"| ROC-AUC | {_fmt(m['roc_auc'])} |")
        lines.append(f"| PR-AUC | {_fmt(m['pr_auc'])} |")
        lines.append(f"| F1 | {_fmt(m['f1'])} |")
        lines.append(f"| Accuracy | {_fmt(m['accuracy'])} |")
        lines.append(f"| Brier | {_fmt(m['brier'])} |")
        lines.append(f"| Positive rate (predicted) | {_fmt(m['positive_rate'])} |")
        lines.append(f"| Actual delay rate | {_fmt(m['actual_delay_rate'])} |")
    lines.append("")
    lines.append("## Confusion matrix (threshold = 0.5)")
    lines.append("")
    cm = m["confusion_matrix"]
    lines.append("| | Predicted on-time | Predicted delayed |")
    lines.append("|---|---|---|")
    lines.append(f"| **Actual on-time** | {cm['tn']} (TN) | {cm['fp']} (FP) |")
    lines.append(f"| **Actual delayed** | {cm['fn']} (FN) | {cm['tp']} (TP) |")
    lines.append("")
    lines.append("## Calibration — predicted probability vs. observed delay rate")
    lines.append("")
    if m["calibration"]:
        lines.append("| Bin mean predicted P(delay) | Observed delay rate |")
        lines.append("|---|---|")
        for mp, obs in zip(m["calibration"]["mean_predicted"], m["calibration"]["observed_rate"]):
            lines.append(f"| {mp:.3f} | {obs:.3f} |")
        lines.append("")
        lines.append("_Well-calibrated means these two columns track each other closely._")
    else:
        lines.append("_Not enough reconciled pairs yet (or only one outcome class present)"
                      " to bin a calibration curve — check back once more snapshots accumulate._")
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

    json_out = dict(m)
    json_out["captured_at_utc"] = ts.isoformat()
    json_out["n_pairs"] = pairs.height
    json_out["scored_at_min"] = str(date_min)
    json_out["scored_at_max"] = str(date_max)
    json_out["bts_reference"] = bts
    json_out["reconciliation"] = recon_summary
    json_path.write_text(json.dumps(json_out, indent=2, default=str))

    return md_path, json_path


def report() -> dict:
    if not PAIRS_PATH.exists():
        raise SystemExit(f"No reconciled pairs at {PAIRS_PATH} yet — run `reconcile` first.")
    pairs = pl.read_parquet(PAIRS_PATH)
    if pairs.height == 0:
        raise SystemExit("reconciled_pairs.parquet exists but is empty — nothing to report.")
    m = compute_metrics(pairs)
    state = _load_state()
    last_run = state.get("runs", [{}])[-1] if state.get("runs") else {}
    md_path, json_path = write_report(pairs, m, last_run)
    return {"metrics": m, "md_path": md_path, "json_path": json_path, "n_pairs": pairs.height}


def main(argv=None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    action = args[0] if args else "all"

    if action in ("reconcile", "all"):
        print("=" * 70)
        print("Stage 1 — reconciling (prediction, actual_delayed) pairs")
        print("=" * 70)
        summary = reconcile()
        print(f"new prediction snapshots folded in: {summary['new_prediction_snapshots']}")
        print(f"new gold snapshots checked:          {summary['new_gold_snapshots']}")
        print(f"new pairs reconciled this run:       {summary['new_pairs']}")
        print(f"predictions still pending an outcome: {summary['still_pending']}")
        print()
        print(f"TOTAL RECONCILED PAIRS: {summary['total_pairs']}")
        if summary["total_pairs"]:
            print(f"DATE RANGE (scored_at, UTC): {summary['scored_at_min']} -> {summary['scored_at_max']}")
        print()

    if action in ("report", "all"):
        print("=" * 70)
        print("Stage 2 — metrics (same family as BTS training) + calibration")
        print("=" * 70)
        out = report()
        m = out["metrics"]
        print(f"n = {out['n_pairs']}")
        print(f"ROC-AUC:  {_fmt(m['roc_auc'])}")
        print(f"PR-AUC:   {_fmt(m['pr_auc'])}")
        print(f"F1:       {_fmt(m['f1'])}")
        print(f"Accuracy: {_fmt(m['accuracy'])}")
        print(f"Brier:    {_fmt(m['brier'])}")
        print(f"Actual delay rate: {_fmt(m['actual_delay_rate'])}")
        print(f"Confusion matrix: {m['confusion_matrix']}")
        print()
        print(f"Report written: {out['md_path']}")
        print(f"JSON written:   {out['json_path']}")

    if action not in ("reconcile", "report", "all"):
        print(f"usage: python -m aeroflux_ml.evaluate_live [reconcile|report|all]", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
