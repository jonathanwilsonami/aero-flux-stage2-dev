"""Inference engine.

Config-driven and model-agnostic (XGBoost today). It needs ONLY features — no
labels — matching how serving works: extract features, score, done. Labels are
retained separately for future continuous training.

Guarantees that matter here:
  * Column alignment: the model is fed exactly the feature columns it was
    trained on, in order; missing columns (e.g. a disabled/unsourced channel
    like weather) are passed as NaN, which XGBoost handles natively. So the
    nullable channels never break scoring.
  * Versioning / dedup: every prediction carries model_version + feature_version
    and a deterministic prediction_key = flight_key:feature_version:model_version,
    so re-scoring the same flight with the same versions is idempotent (the state
    repo upserts on that key) — no duplicate scoring.
"""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import polars as pl
import xgboost as xgb

from .config import ModelConfig


class InferenceEngine:
    def __init__(self, model_config: ModelConfig, feature_version: str = "1.0") -> None:
        if model_config.kind != "xgboost":
            raise ValueError(f"unsupported model kind: {model_config.kind}")
        self.config = model_config
        self.feature_version = feature_version
        self.model_version = model_config.version

        self.booster = xgb.Booster()
        self.booster.load_model(model_config.path)
        # Expected features: explicit config list wins, else read from the model.
        self.expected_features = (
            model_config.features or self.booster.feature_names or []
        )
        if not self.expected_features:
            raise ValueError("could not determine expected feature names; set "
                             "model.features in config or train with named columns")

    def _matrix(self, feature_df: pl.DataFrame) -> xgb.DMatrix:
        # add any missing expected columns as nulls, then order + cast to float
        cols = feature_df.columns
        additions = [
            pl.lit(None, dtype=pl.Float64).alias(f)
            for f in self.expected_features if f not in cols
        ]
        aligned = feature_df.with_columns(additions).select(
            [pl.col(f).cast(pl.Float64, strict=False) for f in self.expected_features]
        )
        arr = aligned.to_numpy()  # polars nulls -> NaN in float array
        return xgb.DMatrix(arr, feature_names=self.expected_features, missing=np.nan)

    def predict(self, feature_df: pl.DataFrame, threshold: float = 0.5) -> pl.DataFrame:
        """Score a feature frame (must include 'flight_key'). Returns one row per
        flight with probability, label, and full versioning."""
        if "flight_key" not in feature_df.columns:
            raise ValueError("feature_df must include 'flight_key'")

        proba = self.booster.predict(self._matrix(feature_df))
        scored_at = datetime.now(timezone.utc).isoformat()

        return feature_df.select("flight_key").with_columns(
            pl.Series("delay_probability", proba).cast(pl.Float64),
            (pl.Series("delay_probability", proba) >= threshold)
                .cast(pl.Int8).alias("predicted_delayed"),
            pl.lit(self.model_version).alias("model_version"),
            pl.lit(self.feature_version).alias("feature_version"),
            pl.lit(scored_at).alias("scored_at"),
            (pl.col("flight_key").cast(pl.Utf8) + ":" + self.feature_version
             + ":" + self.model_version).alias("prediction_key"),
        )
