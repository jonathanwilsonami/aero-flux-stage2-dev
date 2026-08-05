"""Training configuration — YAML-driven, with sensible defaults.

Replaces OR568's Python config dataclasses with a single YAML file so data paths,
features, targets, model params, compute backend, and outputs are all editable
without touching code. `load_config` deep-merges the YAML over the defaults, so a
minimal YAML still runs.
"""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

DEFAULTS: dict[str, Any] = {
    "run": {"name": "xgb_baseline", "seed": 42,
            "output_dir": "model_outputs/runs"},
    "data": {
        "gold_path": "out_bts/bts_gold.parquet",
        "target": "label_delayed",
        "time_column": "flight_key",        # date-prefixed -> chronological sort
        "sample_fraction": None,
    },
    "preprocess": {"include_gap_weather": False},
    "split": {"strategy": "time", "test_size": 0.2},   # time | random
    "validation": {"min_rows": 100, "max_null_fraction": 0.99,
                   "require_columns": []},
    "cv": {"enabled": True, "strategy": "time", "n_splits": 3},
    "tuning": {"enabled": False, "grid": {
        "max_depth": [4, 6, 8],
        "learning_rate": [0.05, 0.1],
        "n_estimators": [200, 400]}},
    "models": [
        {"name": "xgb_full", "type": "xgboost",
         "params": {"max_depth": 6, "learning_rate": 0.1,
                    "n_estimators": 300, "subsample": 0.8,
                    "colsample_bytree": 0.8}},
    ],
    "compute": {"backend": "local", "n_jobs": -1, "use_gpu": False},
    "registry": {"backend": "local", "mlflow_uri": None},
    "outputs": {"save_model": True, "save_plots": True, "save_metrics": True},
}


def _deep_merge(base: dict, over: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def load_config(path: str | Path | None) -> dict:
    """Load a YAML config merged over defaults. `path=None` returns defaults."""
    user = {}
    if path:
        with open(path) as fh:
            user = yaml.safe_load(fh) or {}
    cfg = _deep_merge(DEFAULTS, user)
    _validate(cfg)
    return cfg


def _validate(cfg: dict) -> None:
    if cfg["split"]["strategy"] not in ("time", "random"):
        raise ValueError("split.strategy must be 'time' or 'random'")
    if not 0.0 < cfg["split"]["test_size"] < 1.0:
        raise ValueError("split.test_size must be in (0, 1)")
    if cfg["compute"]["backend"] not in ("local", "spark"):
        raise ValueError("compute.backend must be 'local' or 'spark'")
    if cfg["registry"]["backend"] not in ("local", "mlflow"):
        raise ValueError("registry.backend must be 'local' or 'mlflow'")
    if not cfg["models"]:
        raise ValueError("config must define at least one model")
    for m in cfg["models"]:
        if "name" not in m or "type" not in m:
            raise ValueError("each model needs 'name' and 'type'")
