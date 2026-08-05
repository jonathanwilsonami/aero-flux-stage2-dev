"""Model comparison — rank trained models by ROC-AUC (then PR-AUC)."""
from __future__ import annotations


def rank(results: list[dict]) -> list[dict]:
    rows = [{"name": r["name"], **{k: round(v, 4) if isinstance(v, float) else v
                                   for k, v in r["metrics"].items()}}
            for r in results]
    rows.sort(key=lambda r: (r.get("roc_auc") or 0, r.get("pr_auc") or 0), reverse=True)
    for i, r in enumerate(rows):
        r["rank"] = i + 1
    return rows
