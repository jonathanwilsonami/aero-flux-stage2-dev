"""Data loading + train/test split (time-aware by default to avoid leakage)."""
from __future__ import annotations

import logging

import polars as pl

log = logging.getLogger("aeroflux.training.data")


def load_gold(path: str, sample_fraction: float | None = None,
              seed: int = 42) -> pl.DataFrame:
    df = pl.read_parquet(path)
    log.info("loaded gold: %d rows, %d cols from %s", df.height, df.width, path)
    if sample_fraction:
        df = df.sample(fraction=sample_fraction, seed=seed)
        log.info("sampled to %d rows (fraction=%.3f)", df.height, sample_fraction)
    return df


def split(df: pl.DataFrame, *, strategy: str, test_size: float,
          time_column: str, seed: int = 42) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Time-aware split (default): sort by `time_column` and take the last
    `test_size` fraction as test — the model trains on the past, is tested on the
    future, exactly as it will serve. `random` is available for ablation."""
    if strategy == "time":
        df = df.sort(time_column)
        cut = int(df.height * (1 - test_size))
        train, test = df.head(cut), df.tail(df.height - cut)
        log.info("time split on '%s': train=%d test=%d", time_column, train.height, test.height)
    else:
        df = df.sample(fraction=1.0, shuffle=True, seed=seed)
        cut = int(df.height * (1 - test_size))
        train, test = df.head(cut), df.tail(df.height - cut)
        log.info("random split: train=%d test=%d", train.height, test.height)
    return train, test


def time_folds(df: pl.DataFrame, n_splits: int, time_column: str):
    """Expanding-window time-aware CV folds: each fold trains on a growing prefix
    and validates on the next block. Yields (train_idx_df, valid_idx_df)."""
    df = df.sort(time_column).with_row_index("_i")
    n = df.height
    block = n // (n_splits + 1)
    for k in range(1, n_splits + 1):
        tr = df.slice(0, block * k)
        va = df.slice(block * k, block)
        if va.height:
            yield tr.drop("_i"), va.drop("_i")
