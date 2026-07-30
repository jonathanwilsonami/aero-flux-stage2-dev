"""The feature engineer — the one place features get built, for both sources.

    eng = FeatureEngineer(config.features)
    feats = eng.build(canonical_df)     # canonical_df from schema.from_bts OR from_silver

Because it runs on the canonical parity frame, the SAME call produces the SAME
feature columns whether the input came from BTS or live SWIM/ADS-B. That is the
train/serve guarantee, mechanized.
"""

from __future__ import annotations

import polars as pl

from .channels import CHANNELS, CHANNEL_OUTPUTS
from .config import FeatureConfig
from .schema import CANONICAL_COLUMNS, add_base_delays


class FeatureEngineer:
    def __init__(self, config: FeatureConfig) -> None:
        self.config = config

    def feature_columns(self) -> list[str]:
        """The exact feature columns this config produces, in order. Persist
        this alongside a trained model so serving asks for the same set."""
        cols: list[str] = []
        for name in self.config.enabled_channels():
            cols.extend(CHANNEL_OUTPUTS.get(name, []))
        return cols

    def build(self, canonical: pl.DataFrame) -> pl.DataFrame:
        missing = [c for c in CANONICAL_COLUMNS if c not in canonical.columns]
        if missing:
            raise ValueError(f"canonical frame missing columns: {missing} "
                             f"(use schema.from_bts / schema.from_silver)")

        df = add_base_delays(canonical)
        cfg = {"window_minutes": self.config.window_minutes}
        for name in self.config.enabled_channels():
            fn = CHANNELS.get(name)
            if fn is None:
                raise ValueError(f"unknown channel: {name}")
            df = fn(df, cfg)
        return df

    def build_matrix(self, canonical: pl.DataFrame) -> pl.DataFrame:
        """Just the id + feature columns (+ labels if present), ready to model."""
        df = self.build(canonical)
        keep = ["flight_key"] + self.feature_columns()
        for label in ("dep_delay_min", "arr_delay_min"):
            if label in df.columns:
                keep.append(label)
        return df.select([c for c in keep if c in df.columns])
