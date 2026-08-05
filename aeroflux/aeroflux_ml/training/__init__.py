"""AeroFlux training pipeline — configurable, model-agnostic ML over gold data.

Consumes the gold feature table (from the feature pipeline) and produces trained,
evaluated, versioned models. XGBoost is implemented first; Spark ML and
TensorFlow are extension points. Entry point: `runner.run(config_dict)` or the
CLI `python -m aeroflux_ml.training.cli`.
"""
from .config import load_config
from .runner import run

__all__ = ["load_config", "run"]
