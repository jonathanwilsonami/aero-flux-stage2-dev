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

    Rows with NO resolved airframe (null key) must not be chained — Polars would
    otherwise lump every null-key row into one bogus 'rotation'. So all rotation
    features are null for those rows, with inbound_resolved=0. First leg of a
    real airframe also has no inbound."""
    df = df.sort(["airframe_key", "sched_dep"])
    over = pl.col("airframe_key")
    has_af = pl.col("airframe_key").is_not_null()
    leg_idx = pl.int_range(pl.len()).over(over)
    prev_arr_delay = pl.col("arr_delay_min").shift(1).over(over)
    turnaround = (pl.col("sched_dep") - pl.col("sched_arr").shift(1).over(over)).dt.total_minutes()

    inbound = has_af & (leg_idx > 0)
    return df.with_columns(
        pl.when(inbound).then(prev_arr_delay).alias("prev_leg_arr_delay_min"),
        pl.when(inbound).then(turnaround).alias("turnaround_buffer_min"),
        pl.when(has_af).then(leg_idx).alias("legs_into_day"),
        has_af.cast(pl.Int8).alias("inbound_resolved"),
    )


# --- Channels 2 & 3: origin/destination airport state -----------------------

@channel("airport_state")
def airport_state_features(df: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    """Rolling demand + recent delay momentum at origin and destination, over a
    trailing time window. No airframe key needed -> always available.

    Robust to real data: rows whose time index is null (a flight with no
    scheduled time yet) are excluded from the rolling window and rejoin with
    null features, rather than filling a fake timestamp."""
    window = f"{int(cfg.get('window_minutes', 60))}m"
    df = df.with_row_index("_row")

    def _rolling(frame: pl.DataFrame, time_col: str, key_col: str,
                 delay_col: str, count_name: str, delay_name: str) -> pl.DataFrame:
        valid = frame.filter(pl.col(time_col).is_not_null()).sort([key_col, time_col])
        if valid.height == 0:
            return frame.select("_row").with_columns(
                pl.lit(None, dtype=pl.UInt32).alias(count_name),
                pl.lit(None, dtype=pl.Float64).alias(delay_name),
            )
        rolled = valid.rolling(index_column=time_col, period=window, group_by=key_col).agg(
            pl.len().alias(count_name),
            pl.col(delay_col).mean().alias(delay_name),
        )
        # valid is [key,time]-sorted == rolling's output order -> align positionally
        out = valid.select("_row").with_columns(
            rolled.select(count_name, delay_name)
        )
        return frame.select("_row").join(out, on="_row", how="left")

    o = _rolling(df, "sched_dep", "origin", "dep_delay_min",
                 "origin_dep_demand", "origin_recent_dep_delay")
    d = _rolling(df, "sched_arr", "destination", "arr_delay_min",
                 "dest_arr_demand", "dest_recent_arr_delay")

    return (df.join(o, on="_row", how="left")
              .join(d, on="_row", how="left")
              .drop("_row"))


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
