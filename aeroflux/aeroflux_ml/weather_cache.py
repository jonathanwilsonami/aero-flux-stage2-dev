"""Cache-first weather loading + NCEI-cache adapter.

You already fetched years of NCEI weather (the expensive part). This reads those
cached `weather_clean_YYYY_MM.parquet` files and converts them to the common obs
schema the weather channel joins — so the pipeline never re-downloads weather it
already has.

Your cache is NCEI format (numeric station ids, m/s wind, metre ceilings). The
weather channel joins on airport ICAO, so this needs a station->ICAO bridge
(from your OR568 reference build). Pass it as a dict or a CSV/Parquet with
columns [station, icao].

    from aeroflux_ml.weather_cache import load_weather_cache, load_station_bridge
    bridge = load_station_bridge("data/reference/station_icao.csv")
    obs = load_weather_cache("data/weather", [(2015,1),(2015,2)], bridge)
    # obs -> context={"weather_obs": obs} for the same FeatureEngineer
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

_OBS_COLS = ["station", "obs_time", "wind_kt", "vis_mi", "ifr", "temp_c", "ceiling_ft"]

# NCEI cleaned-cache column names (from the OR568 weather export)
_NCEI_COLS = {"station", "valid_ts", "temp_c", "wind_speed_m_s",
              "wind_dir_deg", "ceiling_height_m"}


def from_ncei_cache(df: pl.DataFrame, station_to_icao: dict[str, str]) -> pl.DataFrame:
    """Convert an NCEI cleaned-cache frame to the common obs schema.

    Units: wind m/s -> kt, ceiling m -> ft, temp already °C. Sentinels dropped:
    wind 999.9 m/s -> null; ceiling >= 22000 m (unlimited/missing) -> null (clear).
    IFR derived from ceiling < 1000 ft. Rows whose station has no ICAO mapping
    are dropped (they can't join to a flight airport)."""
    bridge = pl.DataFrame({"station": list(station_to_icao.keys()),
                           "icao": list(station_to_icao.values())})
    wind = pl.col("wind_speed_m_s").cast(pl.Float64)
    ceil_m = pl.col("ceiling_height_m").cast(pl.Float64)
    return (
        df.join(bridge, on="station", how="left")
        .select(
            pl.col("icao").alias("station"),
            pl.col("valid_ts").alias("obs_time"),
            pl.when(wind < 200).then(wind * 1.9438445).otherwise(None).alias("wind_kt"),
            pl.lit(None, dtype=pl.Float64).alias("vis_mi"),            # NCEI TMP/WND/CIG has no vis
            pl.col("temp_c").cast(pl.Float64),
            pl.when(ceil_m < 22000).then(ceil_m * 3.2808399).otherwise(None).alias("ceiling_ft"),
        )
        .with_columns(
            pl.when(pl.col("ceiling_ft") < 1000).then(1)
              .when(pl.col("ceiling_ft").is_not_null()).then(0)
              .otherwise(0).cast(pl.Int8).alias("ifr"),               # clear/high -> VFR
        )
        .drop_nulls("station")
        .drop_nulls("obs_time")
        .select(_OBS_COLS)
        .sort("obs_time")
    )


def load_station_bridge(path: str) -> dict[str, str]:
    """Load a station->ICAO map from CSV/Parquet with columns [station, icao]."""
    p = Path(path)
    df = pl.read_parquet(p) if p.suffix == ".parquet" else pl.read_csv(p)
    # tolerate a few common column namings
    scol = next(c for c in df.columns if c.lower() in ("station", "ncei_station", "usaf_wban"))
    icol = next(c for c in df.columns if c.lower() in ("icao", "icao_id", "ident"))
    df = df.select(pl.col(scol).cast(pl.Utf8), pl.col(icol).cast(pl.Utf8)).drop_nulls()
    return dict(zip(df[scol].to_list(), df[icol].to_list()))


def load_weather_cache(cache_dir: str, months: list[tuple[int, int]],
                       station_to_icao: dict[str, str],
                       pattern: str = "weather_clean_{year}_{month:02d}.parquet") -> pl.DataFrame:
    """Load + convert cached NCEI weather for the given months. Cache-first:
    reads local parquet, never hits the network. Missing months are skipped."""
    cache = Path(cache_dir)
    frames = []
    for year, month in months:
        f = cache / pattern.format(year=year, month=month)
        if not f.exists():
            print(f"  (weather cache miss: {f.name})")
            continue
        raw = pl.read_parquet(f)
        missing = _NCEI_COLS - set(raw.columns)
        if missing:
            raise ValueError(f"{f.name} missing NCEI columns: {missing}")
        frames.append(from_ncei_cache(raw, station_to_icao))
    if not frames:
        return pl.DataFrame(schema={c: (pl.Datetime("us") if c == "obs_time"
                                        else pl.Utf8 if c == "station"
                                        else pl.Int8 if c == "ifr" else pl.Float64)
                                    for c in _OBS_COLS})
    return pl.concat(frames).sort("obs_time")