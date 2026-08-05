"""Preprocessing = the shared feature contract. Reuses aeroflux_ml.feature_prep
so training uses the EXACT features (and null policy) that live serving will,
then hands numeric matrices to the models."""
from __future__ import annotations

import logging

import numpy as np
import polars as pl

from aeroflux_ml import feature_prep as fp

log = logging.getLogger("aeroflux.training.preprocess")


def prepare_xy(df: pl.DataFrame, target: str, *, include_gap_weather: bool = False):
    """Apply the shared feature prep, then return (X, y, feature_names).
    X is a float32 numpy matrix with NaN preserved (XGBoost routes NaN)."""
    prepped = fp.prepare(df, include_gap_weather=include_gap_weather)
    feats = [c for c in fp.feature_columns(include_gap_weather=include_gap_weather)
             if c in prepped.columns]
    y = None
    if target in prepped.columns:
        prepped = prepped.filter(pl.col(target).is_not_null())   # need a label to train
        y = prepped[target].to_numpy().astype(np.int8)
    X = prepped.select(feats).to_numpy().astype(np.float32)
    log.info("prepared X=%s (%d features), y=%s", X.shape, len(feats),
             None if y is None else y.shape)
    return X, y, feats
