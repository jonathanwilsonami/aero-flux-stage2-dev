"""Configurable logging — console + per-run file, like OR568 but centralized."""
from __future__ import annotations

import logging
from pathlib import Path

_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def setup_logging(logs_dir: Path | None = None, level: int = logging.INFO,
                  run_name: str = "run") -> logging.Logger:
    root = logging.getLogger("aeroflux.training")
    root.setLevel(level)
    root.handlers.clear()
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(_FMT))
    root.addHandler(ch)
    if logs_dir is not None:
        logs_dir = Path(logs_dir)
        logs_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(logs_dir / f"{run_name}.log")
        fh.setFormatter(logging.Formatter(_FMT))
        root.addHandler(fh)
    root.propagate = False
    return root
