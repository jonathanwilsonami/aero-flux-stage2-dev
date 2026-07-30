"""Feature channels, as a registry.

Each channel is a function `(df, cfg) -> df` that ADDS columns. Adding or
changing a feature means editing/adding a channel function and toggling it in
config — never rewriting the pipeline. Channels run on the canonical parity
frame, so they compute identically for BTS and live.

Ready today: flight, rotation, airport_state (channels 0/1/2-3).
Seams in place, not yet sourced: flow (EDCT/TMI), weather (METAR as-of join).
Adding those later = fill the stub; the engineer and inference don't change.
"""

from __future__ import annotations

from typing import Callable

import polars as pl

Channel = Callable[[pl.DataFrame, dict], pl.DataFrame]
CHANNELS: dict[str, Channel] = {}


def channel(name: str) -> Callable[[Channel], Channel]:
    def deco(fn: Channel) -> Channel:
        CHANNELS[name] = fn
        return fn
    return deco


# --- Channel 0: flight-level ------------------------------------------------

@channel("flight")
def flight_features(df: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    return df.with_columns(
        pl.col("sched_dep").dt.hour().alias("sched_dep_hour"),
        pl.col("sched_dep").dt.weekday().alias("sched_dep_dow"),   # 1=Mon..7=Sun
        pl.col("sched_dep").dt.month().alias("sched_dep_month"),
        (pl.col("sched_dep").dt.weekday() >= 6).cast(pl.Int8).alias("is_weekend"),
        ((pl.col("sched_arr") - pl.col("sched_dep")).dt.total_minutes()).alias("sched_block_min"),
    )


# --- Channel 1: aircraft rotation (the two-hop, now nullable) ---------------

@channel("rotation")
def rotation_features(df: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Previous-leg delay + turnaround, keyed on airframe_key (tail or hex).
    Rows with no resolved airframe get null features + inbound_resolved=0."""
    df = df.sort(["airframe_key", "sched_dep"])
    over = pl.col("airframe_key")
    prev_arr_delay = pl.col("arr_delay_min").shift(1).over(over)
    prev_sched_arr = pl.col("sched_arr").shift(1).over(over)
    turnaround = (pl.col("sched_dep") - prev_sched_arr).dt.total_minutes()

    df = df.with_columns(
        prev_arr_delay.alias("prev_leg_arr_delay_min"),
        turnaround.alias("turnaround_buffer_min"),
        pl.int_range(pl.len()).over(over).alias("legs_into_day"),  # 0-based leg index
        pl.col("airframe_key").is_not_null().cast(pl.Int8).alias("inbound_resolved"),
    )
    # a flight cannot inherit from a "previous" leg if it is the first of the day
    return df.with_columns(
        pl.when(pl.col("legs_into_day") == 0)
        .then(None)
        .otherwise(pl.col("prev_leg_arr_delay_min"))
        .alias("prev_leg_arr_delay_min"),
    )


# --- Channels 2 & 3: origin/destination airport state -----------------------

@channel("airport_state")
def airport_state_features(df: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Rolling demand + recent delay momentum at origin and destination, over a
    trailing time window. No airframe key needed -> always available."""
    window = f"{int(cfg.get('window_minutes', 60))}m"

    # Origin departure state: count + mean recent departure delay per origin.
    o = (
        df.sort("sched_dep")
        .rolling(index_column="sched_dep", period=window, group_by="origin")
        .agg(
            pl.len().alias("origin_dep_demand"),
            pl.col("dep_delay_min").mean().alias("origin_recent_dep_delay"),
        )
    )
    # rolling() returns one row per input row (aligned); attach back by position.
    df = df.sort(["origin", "sched_dep"]).with_columns(
        o.sort(["origin", "sched_dep"]).select("origin_dep_demand", "origin_recent_dep_delay")
    )

    # Destination arrival state.
    d = (
        df.sort("sched_arr")
        .rolling(index_column="sched_arr", period=window, group_by="destination")
        .agg(
            pl.len().alias("dest_arr_demand"),
            pl.col("arr_delay_min").mean().alias("dest_recent_arr_delay"),
        )
    )
    df = df.sort(["destination", "sched_arr"]).with_columns(
        d.sort(["destination", "sched_arr"]).select("dest_arr_demand", "dest_recent_arr_delay")
    )
    return df


# --- Channel 4: flow / airspace constraints (SEAM — needs SWIM extraction) --

@channel("flow")
def flow_features(df: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Placeholder columns until EDCT / TMI normalizers land in the parser.
    Emitting them now keeps the feature schema stable so the model's column set
    doesn't change when this is filled in."""
    return df.with_columns(
        pl.lit(None, dtype=pl.Int8).alias("has_edct"),
        pl.lit(None, dtype=pl.Int8).alias("tmi_on_route"),
        pl.lit(None, dtype=pl.Float64).alias("system_delay_index"),
    )


# --- Channel 5: weather (SEAM — needs METAR source + as-of join) ------------

@channel("weather")
def weather_features(df: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Placeholder for origin/destination METAR features. Real implementation
    is a temporal+geographic as-of join to the nearest station observation at
    or before sched_dep/sched_arr (the pattern proven in the prior project)."""
    return df.with_columns(
        pl.lit(None, dtype=pl.Float64).alias("origin_wx_wind_kt"),
        pl.lit(None, dtype=pl.Float64).alias("origin_wx_vis_mi"),
        pl.lit(None, dtype=pl.Int8).alias("origin_wx_ifr"),
        pl.lit(None, dtype=pl.Float64).alias("dest_wx_wind_kt"),
        pl.lit(None, dtype=pl.Float64).alias("dest_wx_vis_mi"),
        pl.lit(None, dtype=pl.Int8).alias("dest_wx_ifr"),
    )


# Feature columns each channel contributes (for the engineer's manifest).
CHANNEL_OUTPUTS: dict[str, list[str]] = {
    "flight": ["sched_dep_hour", "sched_dep_dow", "sched_dep_month",
               "is_weekend", "sched_block_min"],
    "rotation": ["prev_leg_arr_delay_min", "turnaround_buffer_min",
                 "legs_into_day", "inbound_resolved"],
    "airport_state": ["origin_dep_demand", "origin_recent_dep_delay",
                      "dest_arr_demand", "dest_recent_arr_delay"],
    "flow": ["has_edct", "tmi_on_route", "system_delay_index"],
    "weather": ["origin_wx_wind_kt", "origin_wx_vis_mi", "origin_wx_ifr",
                "dest_wx_wind_kt", "dest_wx_vis_mi", "dest_wx_ifr"],
}
