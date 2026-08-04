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


def fetch_metar_live(stations: list[str], hours: int = 6, chunk_size: int = 100,
                     retries: int = 3) -> pl.DataFrame:
    """Recent METAR for ICAO stations from the Aviation Weather Center.

    Chunked (default 100) to stay under the URL length limit, with retry/backoff
    per chunk. A failing chunk is skipped rather than discarding the whole fetch,
    so a single 504 doesn't wipe out weather for every airport."""
    import requests, time
    uniq = sorted({s for s in stations if s})
    rows, failed = [], 0
    for i in range(0, len(uniq), chunk_size):
        ids = ",".join(uniq[i:i + chunk_size])
        for attempt in range(retries):
            try:
                resp = requests.get(_AWC_URL,
                                    params={"ids": ids, "format": "json", "hours": hours},
                                    timeout=45)
                resp.raise_for_status()
                for m in resp.json():
                    rows.append({
                        "station": m.get("icaoId"),
                        "obs_time": datetime.fromtimestamp(m["obsTime"], tz=timezone.utc).replace(tzinfo=None)
                                    if m.get("obsTime") else None,
                        "wind_kt": m.get("wspd"),
                        "vis_mi": m.get("visib"),
                        "ifr": _flight_category_to_ifr(m.get("fltCat")),
                    })
                break
            except requests.exceptions.RequestException:
                if attempt == retries - 1:
                    failed += 1
                else:
                    time.sleep(2 ** attempt)      # 1s, 2s, 4s backoff
    if failed:
        print(f"  (weather: {failed} chunk(s) failed after retries; kept the rest)")
    return _normalize(pl.DataFrame(rows))


def fetch_metar_history(stations: list[str], start: datetime, end: datetime) -> pl.DataFrame:
    """Historical (archived) METAR from the Iowa ASOS archive — the training-side
    twin of live METAR, so features align by construction. Same obs schema:
    wind/vis/ifr/temp/ceiling, keyed by airport ICAO, in UTC (no station bridge,
    no local->UTC conversion)."""
    import requests
    params = [("data", "sknt"), ("data", "vsby"), ("data", "tmpf"),
              ("data", "skyc1"), ("data", "skyl1"),
              ("tz", "UTC"), ("format", "onlycomma"), ("latlon", "no"),
              ("report_type", "3"),
              ("year1", start.year), ("month1", start.month), ("day1", start.day),
              ("year2", end.year), ("month2", end.month), ("day2", end.day)]
    params += [("station", _iem_id(s)) for s in sorted(set(stations))]
    resp = requests.get(_IEM_URL, params=params, timeout=120)
    resp.raise_for_status()
    raw = pl.read_csv(io.StringIO(resp.text), null_values=["M", ""])
    return _normalize(_parse_iem(raw))


def _parse_iem(raw: pl.DataFrame) -> pl.DataFrame:
    """IEM ASOS -> common obs schema. Columns: station, valid(UTC), sknt(kt),
    vsby(mi), tmpf(F), skyc1(cover), skyl1(ft). Ceiling = lowest broken/overcast
    layer; IFR from ceiling < 1000 ft or visibility < 3 mi."""
    f = lambda c: pl.col(c).cast(pl.Float64, strict=False)
    vis = f("vsby")
    ceiling = (pl.when(pl.col("skyc1").is_in(["BKN", "OVC", "VV"]))
               .then(f("skyl1")).otherwise(None))
    return raw.select(
        ("K" + pl.col("station")).alias("station"),
        pl.col("valid").str.to_datetime(strict=False).alias("obs_time"),
        f("sknt").alias("wind_kt"),
        vis.alias("vis_mi"),
        ((f("tmpf") - 32.0) * 5.0 / 9.0).alias("temp_c"),
        ceiling.alias("ceiling_ft"),
    ).with_columns(
        pl.when(pl.col("ceiling_ft").is_not_null() | pl.col("vis_mi").is_not_null())
        .then(((pl.col("ceiling_ft") < 1000).fill_null(False)
               | (pl.col("vis_mi") < 3.0).fill_null(False)).cast(pl.Int8))
        .otherwise(None).alias("ifr"),
    ).select("station", "obs_time", "wind_kt", "vis_mi", "ifr", "temp_c", "ceiling_ft")


def _normalize(df: pl.DataFrame) -> pl.DataFrame:
    cols = ["station", "obs_time", "wind_kt", "vis_mi", "ifr", "temp_c", "ceiling_ft"]
    if df.height == 0:
        return pl.DataFrame(schema={
            "station": pl.Utf8, "obs_time": pl.Datetime("us"),
            "wind_kt": pl.Float64, "vis_mi": pl.Float64, "ifr": pl.Int8,
            "temp_c": pl.Float64, "ceiling_ft": pl.Float64})
    # pad any wx column a given source doesn't provide, so the schema is uniform
    add = [pl.lit(None, dtype=(pl.Int8 if c == "ifr" else pl.Float64)).alias(c)
           for c in ("wind_kt", "vis_mi", "ifr", "temp_c", "ceiling_ft") if c not in df.columns]
    df = df.with_columns(add) if add else df
    return df.with_columns(
        pl.col("wind_kt").cast(pl.Float64, strict=False),
        pl.col("vis_mi").cast(pl.Float64, strict=False),
        pl.col("ifr").cast(pl.Int8, strict=False),
        pl.col("temp_c").cast(pl.Float64, strict=False),
        pl.col("ceiling_ft").cast(pl.Float64, strict=False),
    ).drop_nulls("obs_time").select(cols).sort("obs_time")


# --- NOAA NCEI global-hourly (ISD) — historical, for BTS-aligned training ----
# Matches the OR568 approach: dataset=global-hourly, dataTypes=TMP,WND,CIG.

_NCEI_URL = "https://www.ncei.noaa.gov/access/services/data/v1"


def _ncei_field(col: str, idx: int) -> pl.Expr:
    """Pull comma-separated sub-field `idx` from an ISD coded column as Int64."""
    return (pl.col(col).cast(pl.Utf8).str.split(",")
            .list.get(idx, null_on_oob=True).cast(pl.Int64, strict=False))


def parse_ncei_records(records: list[dict]) -> pl.DataFrame:
    """Parse NCEI global-hourly rows (STATION, DATE, TMP, WND, CIG) into the
    common obs schema. ISD sentinels (+9999 temp, 9999 wind, 99999 ceiling) ->
    null. DATE is UTC for global-hourly. Units: TMP tenths degC, WND speed
    tenths m/s (field 3) -> knots, CIG meters -> feet."""
    if not records:
        return _empty_ncei()
    df = pl.DataFrame(records)
    ren = {c: c.upper() for c in df.columns if c.upper() in ("STATION", "DATE", "TMP", "WND", "CIG")}
    df = df.rename(ren)

    temp = (_ncei_field("TMP", 0).truediv(10.0) if "TMP" in df.columns
            else pl.lit(None, dtype=pl.Float64))
    wind = ((_ncei_field("WND", 3).truediv(10.0) * 1.9438445) if "WND" in df.columns
            else pl.lit(None, dtype=pl.Float64))
    ceil = ((_ncei_field("CIG", 0).cast(pl.Float64) * 3.2808399) if "CIG" in df.columns
            else pl.lit(None, dtype=pl.Float64))

    out = df.select(
        pl.col("STATION").cast(pl.Utf8).str.strip_chars().alias("station"),
        pl.col("DATE").cast(pl.Utf8).str.to_datetime(strict=False, time_unit="us").alias("obs_time"),
        temp.alias("temp_c"), wind.alias("wind_kt"), ceil.alias("ceiling_ft"),
    ).with_columns(
        # ISD sentinels -> null (temp +999.9 after /10, wind ~999 kt, ceiling 99999 m)
        pl.when(pl.col("temp_c") > 900).then(None).otherwise(pl.col("temp_c")).alias("temp_c"),
        pl.when(pl.col("wind_kt") > 900).then(None).otherwise(pl.col("wind_kt")).alias("wind_kt"),
        pl.when(pl.col("ceiling_ft") > 90000).then(None).otherwise(pl.col("ceiling_ft")).alias("ceiling_ft"),
    ).with_columns(
        pl.lit(None, dtype=pl.Float64).alias("vis_mi"),        # not in TMP/WND/CIG
        pl.when(pl.col("ceiling_ft") < 1000).then(1)
          .when(pl.col("ceiling_ft").is_not_null()).then(0)
          .otherwise(None).cast(pl.Int8).alias("ifr"),          # IFR from ceiling
    )
    return out.drop_nulls("obs_time").select(
        "station", "obs_time", "wind_kt", "vis_mi", "ifr", "temp_c", "ceiling_ft"
    ).sort("obs_time")


def fetch_ncei_hourly(stations: list[str], start: str, end: str,
                      *, chunk_size: int = 5, dataset: str = "global-hourly") -> pl.DataFrame:
    """Historical hourly weather from NOAA NCEI (ISD). `stations` are NCEI
    station ids; `start`/`end` are ISO dates (YYYY-MM-DD). Fetches in station
    chunks to avoid oversized responses (matching OR568)."""
    import requests
    recs: list[dict] = []
    uniq = sorted(set(stations))
    for i in range(0, len(uniq), chunk_size):
        chunk = uniq[i:i + chunk_size]
        params = {"dataset": dataset, "stations": ",".join(chunk),
                  "startDate": start, "endDate": end, "format": "json",
                  "units": "metric", "includeAttributes": "false",
                  "dataTypes": "TMP,WND,CIG"}
        r = requests.get(_NCEI_URL, params=params, timeout=120)
        r.raise_for_status()
        recs.extend(r.json())
    return parse_ncei_records(recs)


def _empty_ncei() -> pl.DataFrame:
    return pl.DataFrame(schema={
        "station": pl.Utf8, "obs_time": pl.Datetime("us"),
        "wind_kt": pl.Float64, "vis_mi": pl.Float64, "ifr": pl.Int8,
        "temp_c": pl.Float64, "ceiling_ft": pl.Float64})