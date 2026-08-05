"""XGBoost classifier — the primary Stage 2 model. Handles NaN natively, so the
null policy from feature_prep flows straight through."""
from __future__ import annotations

import logging

import numpy as np

from .base import BaseModel

log = logging.getLogger("aeroflux.training.models.xgb")

_DEFAULTS = dict(max_depth=6, learning_rate=0.1, n_estimators=300,
                 subsample=0.8, colsample_bytree=0.8,
                 objective="binary:logistic", eval_metric="auc")


class XGBoostModel(BaseModel):
    kind = "xgboost"

    def fit(self, X, y, feature_names):
        from xgboost import XGBClassifier
        self.feature_names = feature_names
        params = {**_DEFAULTS, **self.params, "random_state": self.seed}
        # class imbalance: scale_pos_weight = neg/pos
        pos = float((y == 1).sum()); neg = float((y == 0).sum())
        params.setdefault("scale_pos_weight", (neg / pos) if pos else 1.0)
        self.model = XGBClassifier(**params)
        self.model.fit(X, y)
        log.info("fit XGBoost '%s' on %d rows (%d pos / %d neg)",
                 self.name, len(y), int(pos), int(neg))
        return self

    def predict_proba(self, X):
        return self.model.predict_proba(X)[:, 1]

    def feature_importance(self):
        imp = self.model.feature_importances_
        return dict(sorted(zip(self.feature_names, map(float, imp)),
                           key=lambda kv: kv[1], reverse=True))

    def save(self, path):
        import joblib
        joblib.dump(self.model, path)


class LogisticModel(BaseModel):
    """Baseline: logistic regression (needs imputation since it can't take NaN)."""
    kind = "logistic"

    def fit(self, X, y, feature_names):
        from sklearn.linear_model import LogisticRegression
        from sklearn.impute import SimpleImputer
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import make_pipeline
        self.feature_names = feature_names
        self.model = make_pipeline(
            SimpleImputer(strategy="median"), StandardScaler(),
            LogisticRegression(max_iter=self.params.get("max_iter", 200),
                               class_weight="balanced", random_state=self.seed))
        self.model.fit(X, y)
        log.info("fit Logistic '%s' on %d rows", self.name, len(y))
        return self

    def predict_proba(self, X):
        return self.model.predict_proba(X)[:, 1]

    def save(self, path):
        import joblib
        joblib.dump(self.model, path)
