"""Evaluation — classification metrics + plots (ROC, feature importance)."""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

log = logging.getLogger("aeroflux.training.evaluate")


def metrics(y_true: np.ndarray, p: np.ndarray, threshold: float = 0.5) -> dict:
    from sklearn.metrics import (roc_auc_score, average_precision_score,
                                 f1_score, brier_score_loss, accuracy_score)
    yhat = (p >= threshold).astype(int)
    out = {
        "roc_auc": float(roc_auc_score(y_true, p)) if len(set(y_true)) > 1 else float("nan"),
        "pr_auc": float(average_precision_score(y_true, p)) if len(set(y_true)) > 1 else float("nan"),
        "f1": float(f1_score(y_true, yhat, zero_division=0)),
        "accuracy": float(accuracy_score(y_true, yhat)),
        "brier": float(brier_score_loss(y_true, p)),
        "positive_rate": float(yhat.mean()),
        "n": int(len(y_true)),
    }
    return out


def plot_roc(y_true, p, path: Path, label: str = "model"):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from sklearn.metrics import roc_curve, roc_auc_score
    fpr, tpr, _ = roc_curve(y_true, p)
    auc = roc_auc_score(y_true, p)
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot(fpr, tpr, label=f"{label} (AUC={auc:.3f})", color="#2563eb")
    ax.plot([0, 1], [0, 1], "--", color="#94a3b8")
    ax.set_xlabel("False positive rate"); ax.set_ylabel("True positive rate")
    ax.set_title("ROC"); ax.legend()
    fig.tight_layout(); fig.savefig(path, dpi=120); plt.close(fig)


def plot_importance(importance: dict, path: Path, top: int = 15):
    if not importance:
        return
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    items = list(importance.items())[:top][::-1]
    names = [k for k, _ in items]; vals = [v for _, v in items]
    fig, ax = plt.subplots(figsize=(6, max(3, 0.35 * len(items))))
    ax.barh(names, vals, color="#22c55e")
    ax.set_title("Feature importance"); fig.tight_layout()
    fig.savefig(path, dpi=120); plt.close(fig)
