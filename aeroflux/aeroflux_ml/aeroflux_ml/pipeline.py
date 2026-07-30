"""Local pipeline orchestrator — run the whole ML feature pipeline with one call.

Reads your streamed silver data (Postgres flight_instance or a dataset.jsonl),
runs the config-driven feature engineering, writes the GOLD table (what feeds
the model) plus an optional predictions table, and returns a readable summary.
No Spark/cloud needed — this is the local batch path over data the stream has
already fused.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import polars as pl

from .config import Config, ModelConfig
from .engineer import FeatureEngineer
from .inference import InferenceEngine
from .io import write_table, SqliteStateRepository
from .schema import from_silver

# columns from_silver needs off the silver source
_SILVER_COLS = [
    "flight_instance_id", "hex", "tail_number", "carrier_icao", "origin",
    "destination", "scheduled_gate_departure", "scheduled_gate_arrival",
    "actual_off", "actual_on",
]


def read_silver_postgres(dsn: str, table: str = "public.flight_instance",
                         limit: int = 20000) -> pl.DataFrame:
    try:
        import psycopg
        conn = psycopg.connect(dsn)
    except ImportError:
        import psycopg2 as psycopg
        conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table} LIMIT %s", (limit,))
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()
    return _ensure_cols(pl.DataFrame(rows))


def read_silver_jsonl(path: str) -> pl.DataFrame:
    recs = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    recs.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return _ensure_cols(pl.DataFrame(recs))


def _ensure_cols(df: pl.DataFrame) -> pl.DataFrame:
    # tolerate a source missing an optional column (e.g. hex not populated)
    add = [pl.lit(None).alias(c) for c in _SILVER_COLS if c not in df.columns]
    return df.with_columns(add) if add else df


def _coverage(df: pl.DataFrame, cols: list[str]) -> list[tuple[str, float]]:
    n = len(df) or 1
    return [(c, 100.0 * df[c].is_not_null().sum() / n) for c in cols if c in df.columns]


def run(config: Config, silver: pl.DataFrame, out_dir: str,
        model_path: Optional[str] = None, state_db: Optional[str] = None) -> dict:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    canonical = from_silver(silver, airframe_key=config.features.airframe_key)
    eng = FeatureEngineer(config.features)
    gold = eng.build_matrix(canonical)

    gold_pq = str(out / "gold_features.parquet")
    write_table(gold, gold_pq)
    gold.write_csv(str(out / "gold_features.csv"))

    summary = {
        "silver_rows": len(silver),
        "gold_rows": len(gold),
        "feature_columns": eng.feature_columns(),
        "feature_coverage": _coverage(gold, eng.feature_columns()),
        "outputs": [gold_pq, str(out / "gold_features.csv")],
        "predictions": None,
    }

    model_path = model_path or (config.model.path or None)
    if model_path and Path(model_path).exists():
        mc = config.model if config.model.path else ModelConfig(path=model_path)
        engine = InferenceEngine(mc, feature_version=config.features.feature_version)
        preds = engine.predict(gold)
        pred_pq = str(out / "predictions.parquet")
        write_table(preds, pred_pq)
        preds.write_csv(str(out / "predictions.csv"))
        summary["predictions"] = pred_pq
        if state_db:
            repo = SqliteStateRepository(state_db)
            for p in preds.to_dicts():
                repo.upsert_prediction(p)
    return summary
