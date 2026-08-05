"""Feature validation — fail fast before training on bad data."""
from __future__ import annotations

import logging

import numpy as np
import polars as pl

log = logging.getLogger("aeroflux.training.validation")


class ValidationError(Exception):
    pass


def validate_gold(df: pl.DataFrame, cfg: dict) -> dict:
    """Structural checks on the gold frame + reporting. Raises on hard failures,
    warns on soft ones. Returns a small report dict for the run metadata."""
    v = cfg["validation"]
    report = {"rows": df.height, "cols": df.width, "warnings": []}

    if df.height < v["min_rows"]:
        raise ValidationError(f"too few rows: {df.height} < {v['min_rows']}")

    target = cfg["data"]["target"]
    if target not in df.columns:
        raise ValidationError(f"target '{target}' not in gold columns")

    for c in v.get("require_columns", []):
        if c not in df.columns:
            raise ValidationError(f"required column '{c}' missing")

    # per-feature null fraction (soft): warn if a feature is almost entirely null
    from aeroflux_ml import feature_prep as fp
    feats = [c for c in fp.feature_columns(
        include_gap_weather=cfg["preprocess"]["include_gap_weather"]) if c in df.columns]
    nulls = {}
    for c in feats:
        frac = float(df[c].is_null().mean())
        nulls[c] = round(frac, 3)
        if frac > v["max_null_fraction"]:
            report["warnings"].append(f"{c} is {frac*100:.0f}% null")

    labelled = int(df[target].is_not_null().sum())
    if labelled < v["min_rows"]:
        raise ValidationError(f"too few labelled rows: {labelled}")
    report["labelled"] = labelled
    report["null_fraction"] = nulls
    if report["warnings"]:
        for w in report["warnings"]:
            log.warning("validation: %s", w)
    log.info("validation ok: %d rows, %d labelled, %d features",
             df.height, labelled, len(feats))
    return report
