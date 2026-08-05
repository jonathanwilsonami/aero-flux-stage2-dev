"""Score live gold with a trained model → predictions.

The link between training and serving. Reads the live gold feature table, applies
the SAME feature_prep contract used in training, runs the registered model, and
writes predictions as parquet (for the Streamlit app / agent) and optionally
upserts them into the Postgres `predictions` table (current-state serving).

    # one-shot
    python -m aeroflux_ml.score_live --model model_outputs/runs/<run>/models/xgb_full.joblib \
        --gold out/gold_features.parquet --out out/predictions.parquet

    # continuous (score every 60s as the live pipeline refreshes gold)
    python -m aeroflux_ml.score_live --run-dir model_outputs/runs/<run> \
        --gold out/gold_features.parquet --out out/predictions.parquet \
        --dsn "$DSN" --loop 60
"""

from __future__ import annotations

import argparse
import glob
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl

from aeroflux_ml import feature_prep as fp


def _resolve_model(model: str | None, run_dir: str | None) -> tuple[str, str, bool]:
    """Return (model_path, model_version, include_gap_weather). If a run dir is
    given, pick the top-ranked model from run.json."""
    include_gap = False
    if run_dir:
        meta = json.loads((Path(run_dir) / "run.json").read_text())
        include_gap = meta["config"]["preprocess"]["include_gap_weather"]
        # best model = the one whose comparison rank is 1, else first saved
        best = None
        comp = Path(run_dir) / "tables" / "comparison.csv"
        if comp.exists():
            import csv
            rows = list(csv.DictReader(open(comp)))
            rows.sort(key=lambda r: int(r.get("rank", 9)))
            best = rows[0]["name"] if rows else None
        best = best or next(iter(meta["models"]))
        return str(Path(run_dir) / "models" / f"{best}.joblib"), f"{meta['run_id']}:{best}", include_gap
    if not model:
        raise SystemExit("provide --model or --run-dir")
    return model, Path(model).stem, include_gap


def score_once(gold_path: str, model_path: str, model_version: str,
               out_path: str, *, include_gap: bool = False, threshold: float = 0.5,
               dsn: str | None = None) -> int:
    import joblib
    df = pl.read_parquet(gold_path)
    prepped = fp.prepare(df, include_gap_weather=include_gap)
    if prepped.height == 0:
        print("  no scoreable rows this cycle"); return 0
    # No `if c in prepped.columns` filter here on purpose: silently dropping a
    # missing feature just shifts the failure downstream into an opaque XGBoost
    # "Feature shape mismatch" (or worse, a silent width match by coincidence).
    # If live gold is ever missing a column the model expects (e.g. the weather
    # channel gets disabled again), fail loud here with a clear polars
    # "column not found" instead.
    feats = fp.feature_columns(include_gap_weather=include_gap)
    X = prepped.select(feats).to_numpy().astype("float32")
    model = joblib.load(model_path)
    proba = model.predict_proba(X)[:, 1]

    preds = prepped.select("flight_key").with_columns(
        pl.Series("delay_probability", proba).round(4),
        (pl.Series("delay_probability", proba) >= threshold).cast(pl.Int8).alias("predicted_delayed"),
        pl.lit(model_version).alias("model_version"),
        pl.lit(datetime.now(timezone.utc).replace(tzinfo=None)).alias("scored_at"),
    ).rename({"flight_key": "flight_key"})

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    preds.write_parquet(out_path)
    if dsn:
        _upsert_postgres(preds, dsn)
    n_risk = int((preds["predicted_delayed"] == 1).sum())
    print(f"  scored {preds.height} flights → {out_path} "
          f"({n_risk} at risk, {100*n_risk/preds.height:.0f}%)")
    return preds.height


def _upsert_postgres(preds: pl.DataFrame, dsn: str) -> None:
    """Idempotent upsert into a predictions table (created if absent)."""
    import psycopg
    ddl = """
    CREATE TABLE IF NOT EXISTS predictions (
        flight_key TEXT PRIMARY KEY,
        delay_probability DOUBLE PRECISION,
        predicted_delayed SMALLINT,
        model_version TEXT,
        scored_at TIMESTAMP
    );"""
    rows = preds.rows()
    with psycopg.connect(dsn) as c:
        c.execute(ddl)
        c.cursor().executemany(
            """INSERT INTO predictions
               (flight_key, delay_probability, predicted_delayed, model_version, scored_at)
               VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT (flight_key) DO UPDATE SET
                 delay_probability=EXCLUDED.delay_probability,
                 predicted_delayed=EXCLUDED.predicted_delayed,
                 model_version=EXCLUDED.model_version,
                 scored_at=EXCLUDED.scored_at""", rows)
        c.commit()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Score live gold → predictions.")
    ap.add_argument("--gold", default="out/gold_features.parquet")
    ap.add_argument("--out", default="out/predictions.parquet")
    ap.add_argument("--model", help="path to a .joblib model")
    ap.add_argument("--run-dir", help="a training run dir (uses its best model)")
    ap.add_argument("--dsn", help="Postgres DSN to also upsert the predictions table")
    ap.add_argument("--include-gap-weather", action="store_true")
    ap.add_argument("--threshold", type=float, default=0.5)
    ap.add_argument("--loop", type=int, default=0, help="seconds between scoring cycles (0=once)")
    args = ap.parse_args(argv)

    model_path, model_version, include_gap = _resolve_model(args.model, args.run_dir)
    include_gap = include_gap or args.include_gap_weather
    print(f"model: {model_path} (version {model_version})")

    def cycle():
        return score_once(args.gold, model_path, model_version, args.out,
                          include_gap=include_gap, threshold=args.threshold, dsn=args.dsn)

    if args.loop:
        print(f"scoring loop every {args.loop}s (Ctrl-C to stop)")
        while True:
            try:
                cycle()
            except Exception as e:
                print(f"  scoring hiccup (continuing): {e}")
            time.sleep(args.loop)
    else:
        cycle()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
