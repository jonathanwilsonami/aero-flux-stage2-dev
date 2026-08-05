"""Feature preparation — the single place that turns a raw gold frame into the
model's input, applied IDENTICALLY to training (BTS) and serving (live) so the
two can't drift.

It encodes three things:
  1. a missing-value policy (what to fill with 0, what to leave null),
  2. a simple, parity-safe propagation feature,
  3. the feature set the model actually consumes (parity-safe by default).

Why the policy matters: a blank does not always mean zero. `turnaround_buffer_min`
blank means "no inbound leg" (0 is right). `origin_wx_temp_c` blank means "no
observation" (0 °C would be a lie). XGBoost handles NaN natively — it learns a
default direction per split — so the correct move for genuinely-unknown values is
to LEAVE them null, and only fill where absence truly equals zero.
"""

from __future__ import annotations

import polars as pl

# Absence genuinely == 0. Rotation features are null exactly when there is no
# inbound leg (first leg of the day, or rotation unresolved); demand counts are
# 0 when nothing is scheduled in the window. `inbound_resolved` + `legs_into_day`
# stay as flags so the model can tell "real 0" from "unknown".
FILL_ZERO = [
    "prev_leg_arr_delay_min", "turnaround_buffer_min", "legs_into_day",
    "origin_dep_demand", "dest_arr_demand",
]

# Missing != 0 — leave null, let XGBoost route it. Filling these with 0 would
# inject false readings (0 °C, 0 kt, 0 ft ceiling, 0-minute recent delay).
LEAVE_NULL = [
    "origin_recent_dep_delay", "dest_recent_arr_delay",
    "origin_wx_wind_kt", "origin_wx_vis_mi", "origin_wx_ifr",
    "origin_wx_temp_c", "origin_wx_ceiling_ft",
    "dest_wx_wind_kt", "dest_wx_vis_mi", "dest_wx_ifr",
    "dest_wx_temp_c", "dest_wx_ceiling_ft",
]

# If these are null the flight has no schedule and can't be scored — filter it.
STRUCTURAL = ["sched_dep_hour", "sched_dep_dow", "sched_dep_month",
              "is_weekend", "sched_block_min"]

# Not model inputs: id, the label, and outcomes unknown at prediction time.
# dep_delay_min is excluded from features on purpose — for a pre-departure
# prediction it isn't known live, so using it would be train/serve leakage.
NON_FEATURES = ["flight_key", "dep_delay_min", "arr_delay_min", "label_delayed"]

# Weather that is dense in training (NCEI cache) but null in live METAR serving.
# Including these lets the model lean on a feature it won't have live — a parity
# trap — so they're OFF by default and only added with a missingness indicator.
PARITY_GAP_WEATHER = ["origin_wx_temp_c", "origin_wx_ceiling_ft",
                      "dest_wx_temp_c", "dest_wx_ceiling_ft",
                      "origin_wx_vis_mi", "dest_wx_vis_mi"]


def add_propagation(df: pl.DataFrame) -> pl.DataFrame:
    """Simple, interpretable propagation feature: the inbound delay that eats
    into the scheduled turnaround. If the aircraft arrived 40 min late and only
    had 25 min of buffer, ~15 min of pressure pushes into this departure. Zero
    when there's slack or no inbound leg. Computable in training and live alike."""
    prev = pl.col("prev_leg_arr_delay_min").fill_null(0)
    buf = pl.col("turnaround_buffer_min").fill_null(0)
    return df.with_columns(
        pl.max_horizontal(pl.lit(0, dtype=pl.Int64), prev - buf)
        .alias("propagation_pressure_min"))


def apply_fill_policy(df: pl.DataFrame, *, add_indicators: bool = False) -> pl.DataFrame:
    """Fill only the absence==0 columns; leave the unknown-valued ones null.
    Optionally add missingness indicators for the parity-gap weather features."""
    df = df.with_columns([pl.col(c).fill_null(0) for c in FILL_ZERO if c in df.columns])
    if add_indicators:
        df = df.with_columns([
            pl.col(c).is_null().cast(pl.Int8).alias(f"{c}_missing")
            for c in PARITY_GAP_WEATHER if c in df.columns])
    return df


def scoreable(df: pl.DataFrame) -> pl.DataFrame:
    """Keep only flights that can actually be scored: they have a scheduled
    departure. A flight that's just an id + position can't get a delay prediction
    — dropping it here (in BOTH training and serving) keeps the two aligned."""
    return df.filter(pl.col("sched_dep_hour").is_not_null())


def feature_columns(*, include_gap_weather: bool = False) -> list[str]:
    """The columns the model consumes. Default is parity-safe: only weather that
    exists in both training and live (wind + IFR). Set include_gap_weather=True
    to add temp/ceiling/vis (denser signal, but null in live serving)."""
    cols = STRUCTURAL + [
        "prev_leg_arr_delay_min", "turnaround_buffer_min", "legs_into_day",
        "inbound_resolved", "propagation_pressure_min",
        "origin_dep_demand", "origin_recent_dep_delay",
        "dest_arr_demand", "dest_recent_arr_delay",
        "origin_wx_wind_kt", "origin_wx_ifr",
        "dest_wx_wind_kt", "dest_wx_ifr",
    ]
    if include_gap_weather:
        cols += PARITY_GAP_WEATHER + [f"{c}_missing" for c in PARITY_GAP_WEATHER]
    return cols


def prepare(df: pl.DataFrame, *, include_gap_weather: bool = False,
            add_indicators: bool | None = None) -> pl.DataFrame:
    """Full prep, identical for training and live. Returns the frame with the
    propagation feature added, the fill policy applied, non-scoreable rows
    dropped, and columns limited to features (+ flight_key and any labels present)."""
    if add_indicators is None:
        add_indicators = include_gap_weather
    df = add_propagation(df)
    df = apply_fill_policy(df, add_indicators=add_indicators)
    df = scoreable(df)
    keep = ["flight_key"] + feature_columns(include_gap_weather=include_gap_weather)
    keep += [c for c in ("label_delayed", "arr_delay_min") if c in df.columns]  # training only
    return df.select([c for c in keep if c in df.columns])