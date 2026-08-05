"""Runner — orchestrates the whole pipeline from a config dict:
load → validate → split → (tune) → train each model → evaluate → compare → save.

The verbs (train/tune/evaluate/compare) are thin wrappers over this so the CLI
and any notebook share one code path.
"""

from __future__ import annotations

import logging

from .compare import rank
from .data import load_gold, split
from .evaluate import metrics, plot_importance, plot_roc
from .models import build_model
from .preprocess import prepare_xy
from .registry import RunRegistry
from .tune import grid_search
from .validation import validate_gold

log = logging.getLogger("aeroflux.training.runner")


def run(cfg: dict) -> dict:
    """Full run. Returns a summary dict (ranked comparison + run dir)."""
    seed = cfg["run"]["seed"]
    target = cfg["data"]["target"]
    include_gap = cfg["preprocess"]["include_gap_weather"]
    feature_target = (target, include_gap)

    reg = RunRegistry(cfg)
    from .logging_setup import setup_logging
    setup_logging(reg.dirs["logs"], run_name=reg.run_id)

    df = load_gold(cfg["data"]["gold_path"], cfg["data"]["sample_fraction"], seed)
    report = validate_gold(df, cfg)
    reg.meta["validation"] = report

    train_df, test_df = split(df, strategy=cfg["split"]["strategy"],
                              test_size=cfg["split"]["test_size"],
                              time_column=cfg["data"]["time_column"], seed=seed)

    Xtr, ytr, feats = prepare_xy(train_df, target, include_gap_weather=include_gap)
    Xte, yte, _ = prepare_xy(test_df, target, include_gap_weather=include_gap)

    results = []
    for spec in cfg["models"]:
        log.info("=== model: %s (%s) ===", spec["name"], spec["type"])
        params = dict(spec.get("params", {}))

        if cfg["tuning"]["enabled"] and spec["type"] in ("xgboost",):
            best, best_auc, trials = grid_search(spec, train_df, cfg, feature_target)
            if best:
                params.update(best)
                log.info("tuned %s -> %s (CV AUC %.4f)", spec["name"], best, best_auc)
            reg.meta.setdefault("tuning", {})[spec["name"]] = {
                "best": best, "cv_roc_auc": best_auc, "trials": trials}

        model = build_model({**spec, "params": params}, seed).fit(Xtr, ytr, feats)
        p = model.predict_proba(Xte)
        m = metrics(yte, p)
        log.info("%s test: AUC=%.4f PR-AUC=%.4f F1=%.4f Brier=%.4f",
                 spec["name"], m["roc_auc"], m["pr_auc"], m["f1"], m["brier"])

        model_path = None
        if cfg["outputs"]["save_model"]:
            model_path = str(reg.dirs["models"] / f"{spec['name']}.joblib")
            model.save(model_path)
        if cfg["outputs"]["save_plots"] and len(set(yte)) > 1:
            plot_roc(yte, p, reg.dirs["plots"] / f"roc_{spec['name']}.png", spec["name"])
            plot_importance(model.feature_importance() or {},
                            reg.dirs["plots"] / f"importance_{spec['name']}.png")

        reg.record_model(spec["name"], params, m, feats, model_path)
        results.append({"name": spec["name"], "metrics": m})

    ranked = rank(results)
    reg.write_comparison(ranked)
    run_dir = reg.finalize()
    log.info("BEST: %s", ranked[0]["name"] if ranked else "(none)")
    return {"run_dir": str(run_dir), "ranking": ranked}
