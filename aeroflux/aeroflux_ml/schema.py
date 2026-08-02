"""The parity contract.

Train/serve parity is guaranteed *by construction*: both BTS (historical) and
live (silver from SWIM/ADS-B) are mapped into ONE canonical intermediate frame
with identical columns, and every feature is computed only on that frame. There
is no second code path to drift, so a model trained on BTS features consumes
live features unchanged.

The canonical frame's columns are the union of what both sources can provide:

    flight_key      unique per flight instance
    airframe_key    rotation join key: TAIL_NUM (BTS) or hex (live)
    carrier         ICAO airline code  (normalized on both sides)
    origin, destination   ICAO airport code (normalized on both sides)
    sched_dep, sched_arr  scheduled gate times (datetime, UTC)
    actual_dep, actual_arr  best-available actual times (datetime, UTC)

IMPORTANT PARITY NUANCES (surfaced here so they can't silently break a model):
  * Code systems differ. BTS uses IATA (AA, ATL); SWIM uses ICAO (AAL, KATL).
    Adapters normalize BOTH to ICAO via injected maps. For US airports the
    default is 'K'+IATA; Alaska/Hawaii/international need a real airport
    dimension table (the "Airport Reference Record" — a known follow-up).
  * Actual times differ. BTS actual_dep/arr are true GATE times; live actual_*
    are wheels-off/on (runway) proxies. Same column, slightly different meaning;
    documented, and the reason training labels come from BTS.
"""

from __future__ import annotations

from typing import Callable, Optional

import polars as pl

CANONICAL_COLUMNS = [
    "flight_key", "airframe_key", "carrier", "origin", "destination",
    "sched_dep", "sched_arr", "actual_dep", "actual_arr",
]

# Optional normalization hooks. Default airport map = US 'K'+IATA convention.
AirlineMap = Callable[[str], str]
AirportMap = Callable[[str], str]


def _default_airport_to_icao(code: Optional[str]) -> Optional[str]:
    if not code:
        return code
    code = code.strip().upper()
    if len(code) == 4 and code[0] in "KPC":  # already ICAO-ish
        return code
    if len(code) == 3:  # US IATA -> ICAO (CONUS approximation)
        return "K" + code
    return code


def _to_datetime(df: pl.DataFrame, col: str) -> pl.Expr:
    """Coerce a column to naive-UTC Datetime, whether it arrives as an ISO
    string (JSONL/CSV) or an already-typed datetime (Postgres timestamptz)."""
    dt = df.schema.get(col)
    if dt == pl.Utf8:
        return (pl.col(col).str.replace("Z", "", literal=True)
                .str.to_datetime(strict=False, time_unit="us"))
    if isinstance(dt, pl.Datetime):
        return pl.col(col).dt.replace_time_zone(None).cast(pl.Datetime("us"))
    return pl.col(col).cast(pl.Datetime("us"), strict=False)  # Null/other


def from_silver(
    df: pl.DataFrame,
    *,
    airframe_key: str = "hex",
) -> pl.DataFrame:
    """Map a live canonical (silver) frame to the parity schema.

    airframe_key selects which resolved airframe id to use for rotation: 'hex'
    (from ADS-B) in production, or 'tail_number' where present (GA)."""
    key_col = airframe_key if airframe_key in df.columns else "tail_number"
    return df.select(
        pl.col("flight_instance_id").alias("flight_key"),
        pl.col(key_col).alias("airframe_key"),
        pl.col("carrier_icao").alias("carrier"),
        pl.col("origin"),
        pl.col("destination"),
        _to_datetime(df, "scheduled_gate_departure").alias("sched_dep"),
        _to_datetime(df, "scheduled_gate_arrival").alias("sched_arr"),
        _to_datetime(df, "actual_off").alias("actual_dep"),   # runway proxy (documented)
        _to_datetime(df, "actual_on").alias("actual_arr"),
    )


def localize_local_to_utc(df: pl.DataFrame, time_col: str, airport_col: str,
                          tz_lookup: dict[str, str]) -> pl.DataFrame:
    """Convert a naive local-time column to naive UTC, using each row's airport
    timezone. Order-preserving. Rows whose airport has no known tz are left as-is
    (documented gap) rather than silently shifted."""
    if time_col not in df.columns or airport_col not in df.columns:
        return df
    tzdf = pl.DataFrame({airport_col: list(tz_lookup.keys()),
                         "_tz": list(tz_lookup.values())})
    d = df.with_row_index("_i").join(tzdf, on=airport_col, how="left")
    parts = []
    for tz in d["_tz"].unique().to_list():
        sub = d.filter(pl.col("_tz").is_null() if tz is None else pl.col("_tz") == tz)
        if tz is not None:
            sub = sub.with_columns(
                pl.col(time_col)
                .dt.replace_time_zone(tz, ambiguous="earliest", non_existent="null")
                .dt.convert_time_zone("UTC").dt.replace_time_zone(None).alias(time_col))
        parts.append(sub.select("_i", time_col))
    conv = pl.concat(parts)
    return (df.with_row_index("_i").drop(time_col)
              .join(conv, on="_i", how="left").sort("_i").drop("_i"))


def from_bts(
    df: pl.DataFrame,
    *,
    airline_map: Optional[AirlineMap] = None,
    airport_map: Optional[AirportMap] = None,
    airport_tz: Optional[dict[str, str]] = None,
) -> pl.DataFrame:
    """Map a BTS On-Time Performance frame to the parity schema.

    Expects BTS-style columns (FL_DATE, OP_UNIQUE_CARRIER/OP_CARRIER, TAIL_NUM,
    ORIGIN, DEST, CRS_DEP_TIME, DEP_TIME, CRS_ARR_TIME, ARR_TIME as HHMM ints or
    parsed datetimes). Carrier/airport codes are normalized to ICAO.

    BTS times are LOCAL wall-clock. Pass `airport_tz` (an ICAO->IANA-tz map, e.g.
    from AirportTable) to convert them to UTC so they line up with live SWIM
    (already UTC) and with the UTC weather join. Departures use the origin tz,
    arrivals the destination tz."""
    apt = airport_map or _default_airport_to_icao
    air = airline_map or (lambda c: c)  # supply an IATA->ICAO map for real parity

    def _bts_time(date_col: str, hhmm_col: str) -> pl.Expr:
        # BTS times are local HHMM; combine FL_DATE + HHMM into a naive datetime,
        # then localize to UTC below if airport_tz is supplied.
        hhmm = pl.col(hhmm_col).cast(pl.Utf8).str.zfill(4)
        return (
            pl.col(date_col).cast(pl.Utf8) + "T" + hhmm.str.slice(0, 2) + ":" + hhmm.str.slice(2, 2)
        ).str.to_datetime(strict=False, time_unit="us")

    carrier_col = "OP_UNIQUE_CARRIER" if "OP_UNIQUE_CARRIER" in df.columns else "OP_CARRIER"
    out = df.select(
        (pl.col("FL_DATE").cast(pl.Utf8) + "_" + pl.col(carrier_col).cast(pl.Utf8)
         + pl.col("OP_CARRIER_FL_NUM").cast(pl.Utf8)).alias("flight_key"),
        pl.col("TAIL_NUM").alias("airframe_key"),
        pl.col(carrier_col).map_elements(air, return_dtype=pl.Utf8).alias("carrier"),
        pl.col("ORIGIN").map_elements(apt, return_dtype=pl.Utf8).alias("origin"),
        pl.col("DEST").map_elements(apt, return_dtype=pl.Utf8).alias("destination"),
        _bts_time("FL_DATE", "CRS_DEP_TIME").alias("sched_dep"),
        _bts_time("FL_DATE", "CRS_ARR_TIME").alias("sched_arr"),
        _bts_time("FL_DATE", "DEP_TIME").alias("actual_dep"),   # true gate time
        _bts_time("FL_DATE", "ARR_TIME").alias("actual_arr"),
    )
    if airport_tz:
        out = localize_local_to_utc(out, "sched_dep", "origin", airport_tz)
        out = localize_local_to_utc(out, "actual_dep", "origin", airport_tz)
        out = localize_local_to_utc(out, "sched_arr", "destination", airport_tz)
        out = localize_local_to_utc(out, "actual_arr", "destination", airport_tz)
    return out


def add_base_delays(df: pl.DataFrame) -> pl.DataFrame:
    """Delay columns needed as both labels and as context for later legs.
    Computed identically regardless of source."""
    return df.with_columns(
        ((pl.col("actual_dep") - pl.col("sched_dep")).dt.total_minutes()).alias("dep_delay_min"),
        ((pl.col("actual_arr") - pl.col("sched_arr")).dt.total_minutes()).alias("arr_delay_min"),
    )