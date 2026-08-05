"""Model registry — maps config `type` to an implementation."""
from __future__ import annotations

from .base import BaseModel
from .xgboost_model import XGBoostModel, LogisticModel
from .sparkml_model import SparkMLModel, TensorFlowModel

_REGISTRY = {m.kind: m for m in
             (XGBoostModel, LogisticModel, SparkMLModel, TensorFlowModel)}


def build_model(spec: dict, seed: int = 42) -> BaseModel:
    kind = spec["type"]
    if kind not in _REGISTRY:
        raise ValueError(f"unknown model type '{kind}'; have {sorted(_REGISTRY)}")
    return _REGISTRY[kind](name=spec["name"], params=spec.get("params", {}), seed=seed)


def available_types() -> list[str]:
    return sorted(_REGISTRY)
