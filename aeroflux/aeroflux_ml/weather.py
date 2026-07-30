"""Weather (METAR) fetchers.

Produce the common observation frame the `weather` channel joins:
    station | obs_time | wind_kt | vis_mi | ifr

Two sources, same output schema — so live serving and BTS training use the same
weather features:
  * live      -> Aviation Weather Center JSON API (current/recent METAR)
  * historical-> Iowa Environmental Mesonet ASOS archive (free bulk history)

Network calls (not exercised in unit tests). Both are free and need no key.
Airport codes are ICAO (KATL); the AWC API takes ICAO directly, IEM takes the
3-letter id, so `_iem_id` strips a leading K for CONUS.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone

import polars as pl

_AWC_URL = "https://aviationweather.gov/api/data/metar"
_IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"


def _flight_category_to_ifr(cat: str | None) -> int | None:
    if cat is None:
        return None
    return 1 if str(cat).upper() in ("IFR", "LIFR") else 0


def _iem_id(icao: str) -> str:
    icao = (icao or "").upper()
    return icao[1:] if len(icao) == 4 and icao.startswith("K") else icao


def fetch_metar_live(stations: list[str], hours: int = 6) -> pl.DataFrame:
    """Recent METAR for ICAO stations from the Aviation Weather Center."""
    import requests
    ids = ",".join(sorted(set(stations)))
    resp = requests.get(_AWC_URL, params={"ids": ids, "format": "json", "hours": hours}, timeout=30)
    resp.raise_for_status()
    rows = []
    for m in resp.json():
        rows.append({
            "station": m.get("icaoId"),
            "obs_time": datetime.fromtimestamp(m["obsTime"], tz=timezone.utc).replace(tzinfo=None)
                        if m.get("obsTime") else None,
            "wind_kt": m.get("wspd"),
            "vis_mi": m.get("visib"),
            "ifr": _flight_category_to_ifr(m.get("fltCat")),
        })
    return _normalize(pl.DataFrame(rows))


def fetch_metar_history(stations: list[str], start: datetime, end: datetime) -> pl.DataFrame:
    """Historical ASOS observations from IEM (for BTS-aligned training weather)."""
    import requests
    params = [("data", "sknt"), ("data", "vsby"), ("tz", "UTC"), ("format", "onlycomma"),
              ("latlon", "no"), ("report_type", "3"),
              ("year1", start.year), ("month1", start.month), ("day1", start.day),
              ("year2", end.year), ("month2", end.month), ("day2", end.day)]
    params += [("station", _iem_id(s)) for s in sorted(set(stations))]
    resp = requests.get(_IEM_URL, params=params, timeout=120)
    resp.raise_for_status()
    raw = pl.read_csv(io.StringIO(resp.text), null_values=["M", ""])
    # IEM columns: station, valid (UTC), sknt (wind kt), vsby (statute miles)
    return _normalize(raw.select(
        ("K" + pl.col("station")).alias("station"),
        pl.col("valid").str.to_datetime(strict=False).alias("obs_time"),
        pl.col("sknt").cast(pl.Float64, strict=False).alias("wind_kt"),
        pl.col("vsby").cast(pl.Float64, strict=False).alias("vis_mi"),
        (pl.col("vsby").cast(pl.Float64, strict=False) < 3.0).cast(pl.Int8).alias("ifr"),
    ))


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    if df.height == 0:
        return pl.DataFrame(schema={
            "station": pl.Utf8, "obs_time": pl.Datetime("us"),
            "wind_kt": pl.Float64, "vis_mi": pl.Float64, "ifr": pl.Int8})
    return df.with_columns(
        pl.col("wind_kt").cast(pl.Float64, strict=False),
        pl.col("vis_mi").cast(pl.Float64, strict=False),
        pl.col("ifr").cast(pl.Int8, strict=False),
    ).drop_nulls("obs_time").sort("obs_time")
