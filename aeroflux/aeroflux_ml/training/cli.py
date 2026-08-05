"""CLI — train / tune / evaluate / compare, all driven by the YAML config.

    python -m aeroflux_ml.training.cli train   --config configs/training.yaml
    python -m aeroflux_ml.training.cli tune    --config configs/training.yaml
    python -m aeroflux_ml.training.cli compare --config configs/training.yaml

`tune` just runs with tuning forced on; `compare` runs all configured models and
prints the ranking (the default `train` already trains + evaluates + compares).
"""
from __future__ import annotations

import argparse
import json

from .config import load_config
from .logging_setup import setup_logging
from .runner import run


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="aeroflux-train")
    ap.add_argument("verb", choices=["train", "tune", "evaluate", "compare"])
    ap.add_argument("--config", default="configs/training.yaml")
    ap.add_argument("--gold", help="override data.gold_path")
    ap.add_argument("--name", help="override run.name")
    args = ap.parse_args(argv)

    setup_logging()
    cfg = load_config(args.config)
    if args.gold:
        cfg["data"]["gold_path"] = args.gold
    if args.name:
        cfg["run"]["name"] = args.name
    if args.verb == "tune":
        cfg["tuning"]["enabled"] = True

    summary = run(cfg)
    print(json.dumps(summary["ranking"], indent=2))
    print(f"\nrun dir: {summary['run_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
