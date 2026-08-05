"""Tuning — grid search with time-aware cross-validation."""
from __future__ import annotations

import itertools
import logging

import numpy as np

from .data import time_folds
from .evaluate import metrics
from .models import build_model
from .preprocess import prepare_xy

log = logging.getLogger("aeroflux.training.tune")


def _grid(grid: dict):
    keys = list(grid)
    for combo in itertools.product(*[grid[k] for k in keys]):
        yield dict(zip(keys, combo))


def cross_validate(model_spec, train_df, cfg, feature_target) -> float:
    """Mean ROC-AUC across time-aware folds for one model spec."""
    target, include_gap = feature_target
    aucs = []
    for tr, va in time_folds(train_df, cfg["cv"]["n_splits"], cfg["data"]["time_column"]):
        Xtr, ytr, feats = prepare_xy(tr, target, include_gap_weather=include_gap)
        Xva, yva, _ = prepare_xy(va, target, include_gap_weather=include_gap)
        if ytr is None or yva is None or len(set(yva)) < 2:
            continue
        m = build_model(model_spec, cfg["run"]["seed"]).fit(Xtr, ytr, feats)
        aucs.append(metrics(yva, m.predict_proba(Xva))["roc_auc"])
    return float(np.nanmean(aucs)) if aucs else float("nan")


def grid_search(model_spec, train_df, cfg, feature_target):
    """Search the config grid with time-aware CV; return (best_params, best_auc, trials)."""
    grid = cfg["tuning"]["grid"]
    best, best_auc, trials = None, -1.0, []
    for override in _grid(grid):
        spec = {**model_spec, "params": {**model_spec.get("params", {}), **override}}
        auc = cross_validate(spec, train_df, cfg, feature_target)
        trials.append({"params": override, "cv_roc_auc": auc})
        log.info("grid trial %s -> CV AUC %.4f", override, auc)
        if auc == auc and auc > best_auc:      # not NaN and better
            best, best_auc = override, auc
    return best, best_auc, trials
