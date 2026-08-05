"""Model interface — every model (XGBoost, Spark ML, TF, logistic) implements
this, so training/tuning/eval/registry code is model-agnostic."""
from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np


class BaseModel(ABC):
    #: short type key used in config (`type: xgboost`)
    kind: str = "base"

    def __init__(self, name: str, params: dict | None = None, seed: int = 42):
        self.name = name
        self.params = dict(params or {})
        self.seed = seed
        self.model = None
        self.feature_names: list[str] = []

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray, feature_names: list[str]) -> "BaseModel":
        ...

    @abstractmethod
    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Return P(delayed) as a 1-D array."""

    @abstractmethod
    def save(self, path: str) -> None:
        ...

    def feature_importance(self) -> dict[str, float] | None:
        return None
