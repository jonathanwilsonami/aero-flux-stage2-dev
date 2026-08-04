#!/usr/bin/env python3
"""Build a GOLD training table from a BTS On-Time Performance file.

This is the training-side twin of the live pipeline. It:
  1. reads BTS (CSV or Parquet),
  2. maps it to the canonical schema via `from_bts` — IATA->ICAO codes and
     LOCAL->UTC times using each airport's timezone (so it lines up with live
     SWIM, which is already UTC),
  3. attaches archived-METAR weather over the file's date range using the SAME
     feature core as live serving (score-time = scheduled departure, no leakage),
  4. writes gold with a real arrival-delay label (delayed >= threshold minutes).

    python build_bts_gold.py --in bts_2026_07.csv --out ./out_bts
    python build_bts_gold.py --in bts.parquet --no-weather        # skip weather
    python build_bts_gold.py --in bts.csv --delay-threshold 15

The output is dense and fully-labelled — the training set live data can't provide.
"""

from __future__ import annotations

import argparse
import sys

import polars as pl

from aeroflux_ml import FeatureEngineer
from aeroflux_ml.config import FeatureConfig
from aeroflux_ml.schema import from_bts
from aeroflux_ml.io import write_table
from aeroflux_parser.airports import DEFAULT_AIRPORTS
from aeroflux_parser.airlines import DEFAULT_TABLE as AIRLINES


def load_bts(path: str) -> pl.DataFrame:
    if path.lower().endswith(".parquet"):
        return pl.read_parquet(path)
    return pl.read_csv(path, infer_schema_length=None, ignore_errors=True)


def _airline_iata_to_icao(iata: str) -> str:
    rec = AIRLINES.by_iata(iata)
    return rec.icao if rec else (iata or "")


def _tz_map(bts: pl.DataFrame) -> dict[str, str]:
    """ICAO -> IANA tz for every airport in the file (both IATA input and the
    ICAO it normalizes to, so from_bts can localize by the post-map code)."""
    codes = set()
    for col in ("ORIGIN", "DEST"):
        if col in bts.columns:
            codes |= {c for c in bts[col].drop_nulls().unique().to_list() if c}
    tz = {}
    for c in codes:
        icao = DEFAULT_AIRPORTS.to_icao(c)
        z = DEFAULT_AIRPORTS.tz(c)
        if icao and z:
            tz[icao] = z
            tz[c] = z          # tolerate either form
    return tz


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--in", dest="infile", help="a BTS CSV/Parquet file")
    src.add_argument("--months", help="fetch+cache BTS months, e.g. 2026-07 or 2026-05:2026-07")
    ap.add_argument("--cache", default="data/bts", help="BTS cache dir (for --months)")
    ap.add_argument("--out", default="./out_bts")
    ap.add_argument("--no-weather", action="store_true",
                    help="skip weather entirely (offline / faster)")
    ap.add_argument("--weather-cache",
                    help="dir of cached NCEI weather_clean_YYYY_MM.parquet (no re-fetch)")
    ap.add_argument("--station-bridge",
                    help="station->ICAO map (CSV/Parquet with [station, icao]) for --weather-cache")
    ap.add_argument("--delay-threshold", type=int, default=15,
                    help="minutes of arrival delay that count as 'delayed' (label)")
    ap.add_argument("--show", type=int, default=3)
    args = ap.parse_args()

    if args.months:
        from aeroflux_ml.bts_source import fetch_bts_months, parse_month_range
        print(f"Fetching BTS months {args.months} (cache: {args.cache}) ...")
        bts = fetch_bts_months(parse_month_range(args.months), args.cache)
    else:
        print(f"Reading BTS from {args.infile} ...")
        bts = load_bts(args.infile)
    print(f"  {bts.height} BTS rows")

    # map to canonical: IATA->ICAO codes, local->UTC via airport tz
    canonical = from_bts(
        bts,
        airline_map=_airline_iata_to_icao,
        airport_map=DEFAULT_AIRPORTS.to_icao,
        airport_tz=_tz_map(bts),
    )

    # attach weather (same feature core as live). Prefer the local NCEI cache
    # (already-fetched years, no network) when --weather-cache is given; else
    # fall back to archived METAR over the file's date range.
    context = None
    cfg = FeatureConfig()
    if not args.no_weather:
        sd = canonical["sched_dep"].drop_nulls()
        months = sorted({(d.year, d.month) for d in sd.to_list()}) if sd.len() else []
        obs = None
        if args.weather_cache and args.station_bridge:
            from aeroflux_ml.weather_cache import load_weather_cache, load_station_bridge
            print(f"Loading cached NCEI weather for {len(months)} month(s) "
                  f"from {args.weather_cache} ...")
            bridge = load_station_bridge(args.station_bridge)
            obs = load_weather_cache(args.weather_cache, months, bridge)
            print(f"  {obs.height} observations from cache "
                  f"({obs['station'].n_unique()} airports)")
        elif sd.len():
            from aeroflux_ml.weather import fetch_metar_history
            start, end = sd.min(), (canonical["sched_arr"].drop_nulls().max() or sd.max())
            airports = sorted({*canonical["origin"].drop_nulls().to_list(),
                               *canonical["destination"].drop_nulls().to_list()})
            print(f"Fetching archived METAR for {len(airports)} airport(s), "
                  f"{start.date()}..{end.date()} ...")
            try:
                obs = fetch_metar_history(airports, start, end)
                print(f"  got {obs.height} observations")
            except Exception as e:
                print(f"  WARNING: weather fetch failed ({e}); continuing without weather")
        if obs is not None and obs.height:
            cfg.channels["weather"] = True
            context = {"weather_obs": obs}

    eng = FeatureEngineer(cfg)
    gold = eng.build_matrix(canonical, context=context)

    # real label: arrival delayed >= threshold (only where we have an actual arrival)
    gold = gold.with_columns(
        pl.when(pl.col("arr_delay_min").is_not_null())
        .then((pl.col("arr_delay_min") >= args.delay_threshold).cast(pl.Int8))
        .otherwise(None)
        .alias("label_delayed"),
    )

    import os
    os.makedirs(args.out, exist_ok=True)
    pq = f"{args.out}/bts_gold.parquet"
    write_table(gold, pq)
    gold.write_csv(f"{args.out}/bts_gold.csv")

    labelled = gold.filter(pl.col("label_delayed").is_not_null())
    pos = labelled.filter(pl.col("label_delayed") == 1).height
    print("=" * 56)
    print(f"gold rows: {gold.height}   labelled: {labelled.height}   "
          f"delayed>= {args.delay_threshold}m: {pos} "
          f"({100*pos/max(labelled.height,1):.1f}%)")
    print(f"wrote {pq} (+ .csv)")
    if args.show:
        with pl.Config(tbl_cols=-1, tbl_width_chars=200):
            print(gold.select("flight_key", "sched_dep_hour", "sched_block_min",
                              "inbound_resolved", "prev_leg_arr_delay_min",
                              "arr_delay_min", "label_delayed").head(args.show))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())