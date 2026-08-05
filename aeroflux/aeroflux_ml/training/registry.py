"""Model repository — a structured local run registry (default), with an optional
MLflow hook. Each run gets its own directory:

  <output_dir>/<run_name>_<timestamp>/
    models/   <name>.joblib
    plots/    roc_<name>.png, importance_<name>.png
    tables/   comparison.csv, comparison.md
    metrics/  <name>.json
    logs/     <run>.log
    run.json  (metadata: config, seed, git-less version, feature list, metrics)

This is lightweight and migrates cleanly to cloud (copy the dir to S3) or MLflow
(set registry.backend=mlflow)."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("aeroflux.training.registry")


class RunRegistry:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.run_id = f"{cfg['run']['name']}_{ts}"
        self.root = Path(cfg["run"]["output_dir"]) / self.run_id
        self.dirs = {d: self.root / d for d in
                     ("models", "plots", "tables", "metrics", "logs")}
        for p in self.dirs.values():
            p.mkdir(parents=True, exist_ok=True)
        self.meta = {"run_id": self.run_id, "created_utc": ts,
                     "config": cfg, "seed": cfg["run"]["seed"], "models": {}}
        self._mlflow = None
        if cfg["registry"]["backend"] == "mlflow":
            self._init_mlflow()

    def _init_mlflow(self):
        try:
            import mlflow
            if self.cfg["registry"].get("mlflow_uri"):
                mlflow.set_tracking_uri(self.cfg["registry"]["mlflow_uri"])
            mlflow.set_experiment(self.cfg["run"]["name"])
            self._mlflow = mlflow
            log.info("MLflow tracking enabled")
        except Exception as e:
            log.warning("MLflow requested but unavailable (%s); using local registry", e)

    def record_model(self, name: str, params: dict, metrics: dict,
                     feature_names: list[str], model_path: str | None):
        self.meta["models"][name] = {"params": params, "metrics": metrics,
                                     "features": feature_names, "model_path": model_path}
        (self.dirs["metrics"] / f"{name}.json").write_text(json.dumps(metrics, indent=2))
        if self._mlflow:
            with self._mlflow.start_run(run_name=name):
                self._mlflow.log_params(params)
                self._mlflow.log_metrics(metrics)
                if model_path:
                    self._mlflow.log_artifact(model_path)

    def write_comparison(self, rows: list[dict]):
        import csv
        cols = ["name"] + [k for k in rows[0] if k != "name"] if rows else ["name"]
        with open(self.dirs["tables"] / "comparison.csv", "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(rows)
        md = ["| " + " | ".join(cols) + " |", "|" + "|".join(["---"] * len(cols)) + "|"]
        for r in rows:
            md.append("| " + " | ".join(f"{r.get(c,'')}" for c in cols) + " |")
        (self.dirs["tables"] / "comparison.md").write_text("\n".join(md))

    def finalize(self):
        (self.root / "run.json").write_text(json.dumps(self.meta, indent=2, default=str))
        log.info("run saved -> %s", self.root)
        return self.root
