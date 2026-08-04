"""BTS On-Time Performance source — fetch, cache, and normalize.

The training-data half of the pipeline. Downloads monthly On-Time Performance
files from the BTS TranStats PREZIP endpoint, caches each month as Parquet (so
re-runs are instant and offline), and normalizes BTS's long column names to the
short names `from_bts` expects. Reuses the OR568 download approach; the timestamp
/ timezone work is handled downstream by `schema.from_bts` + the airport
dimension, so it isn't duplicated here.

    from aeroflux_ml.bts_source import fetch_bts_months, parse_month_range
    df = fetch_bts_months(parse_month_range("2026-05:2026-07"))   # cached parquet
    # df has FL_DATE, OP_UNIQUE_CARRIER, ... ready for from_bts

CLI:
    python -m aeroflux_ml.bts_source --months 2026-05:2026-07 --cache data/bts
"""

from __future__ import annotations

import io
import time
import zipfile
from pathlib import Path

import polars as pl

_PREZIP = "https://transtats.bts.gov/PREZIP"
_ZIP_TEMPLATE = "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"

# BTS ships long column names; map each field to the short name from_bts expects,
# tolerating either form and the several carrier/flight-number variants.
_ALIASES: dict[str, list[str]] = {
    "FL_DATE": ["FL_DATE", "FlightDate"],
    "OP_UNIQUE_CARRIER": ["OP_UNIQUE_CARRIER", "IATA_CODE_Reporting_Airline",
                          "Reporting_Airline", "Operating_Airline", "OP_CARRIER"],
    "OP_CARRIER_FL_NUM": ["OP_CARRIER_FL_NUM", "Flight_Number_Reporting_Airline",
                          "Flight_Number_Operating_Airline"],
    "TAIL_NUM": ["TAIL_NUM", "Tail_Number"],
    "ORIGIN": ["ORIGIN", "Origin"],
    "DEST": ["DEST", "Dest"],
    "CRS_DEP_TIME": ["CRS_DEP_TIME", "CRSDepTime"],
    "DEP_TIME": ["DEP_TIME", "DepTime"],
    "CRS_ARR_TIME": ["CRS_ARR_TIME", "CRSArrTime"],
    "ARR_TIME": ["ARR_TIME", "ArrTime"],
    "ARR_DELAY": ["ARR_DELAY", "ArrDelay"],
    "DEP_DELAY": ["DEP_DELAY", "DepDelay"],
    "CANCELLED": ["CANCELLED", "Cancelled"],
    "DIVERTED": ["DIVERTED", "Diverted"],
}


def normalize_bts_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Rename BTS long/short columns to the canonical short names, picking the
    first available alias for each target (handles the carrier-name variants)."""
    ren = {}
    for target, aliases in _ALIASES.items():
        if target in df.columns:
            continue
        for a in aliases:
            if a in df.columns:
                ren[a] = target
                break
    return df.rename(ren)


def parse_month_range(spec: str) -> list[tuple[int, int]]:
    """'2026-07' -> [(2026,7)]; '2026-05:2026-07' -> [(2026,5),(2026,6),(2026,7)]."""
    def _ym(s: str) -> tuple[int, int]:
        y, m = s.strip().split("-")
        return int(y), int(m)
    if ":" in spec:
        (y1, m1), (y2, m2) = (_ym(p) for p in spec.split(":"))
    else:
        y1, m1 = _ym(spec); y2, m2 = y1, m1
    out, y, m = [], y1, m1
    while (y, m) <= (y2, m2):
        out.append((y, m))
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y
    return out


def _download_zip(url: str, retries: int = 4, timeout: int = 180) -> bytes:
    import requests
    last = None
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout, verify=True,
                             headers={"User-Agent": "aeroflux-bts/0.1"})
            r.raise_for_status()
            return r.content
        except requests.exceptions.RequestException as e:
            last = e
            time.sleep(2 ** attempt)      # 1,2,4,8s backoff (TranStats can be slow)
    raise RuntimeError(f"BTS download failed after {retries} tries: {url} ({last})")


def _read_first_csv(zip_bytes: bytes) -> pl.DataFrame:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(name) as fh:
            df = pl.read_csv(fh.read(), infer_schema_length=10000,
                             null_values=["", "NA", "NULL", "null"])
    return df.select([c for c in df.columns if c != ""])   # drop trailing empty col


def fetch_bts_month(year: int, month: int, cache_dir: str = "data/bts",
                    force: bool = False) -> pl.DataFrame:
    """One month of BTS, normalized. Cached as Parquet; re-downloads only with
    force=True or a cache miss."""
    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    cached = cache / f"bts_{year}_{month:02d}.parquet"
    if cached.exists() and not force:
        print(f"  cached BTS {year}-{month:02d} -> {cached}")
        return pl.read_parquet(cached)

    url = f"{_PREZIP}/{_ZIP_TEMPLATE.format(year=year, month=month)}"
    print(f"  downloading BTS {year}-{month:02d} ...")
    df = normalize_bts_columns(_read_first_csv(_download_zip(url)))
    df.write_parquet(cached)
    print(f"  cached {df.height:,} rows -> {cached}")
    return df


def fetch_bts_months(months: list[tuple[int, int]], cache_dir: str = "data/bts",
                     force: bool = False) -> pl.DataFrame:
    frames = [fetch_bts_month(y, m, cache_dir, force) for y, m in months]
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Fetch + cache BTS On-Time months.")
    ap.add_argument("--months", required=True, help="e.g. 2026-07 or 2026-05:2026-07")
    ap.add_argument("--cache", default="data/bts")
    ap.add_argument("--force", action="store_true", help="re-download even if cached")
    args = ap.parse_args()
    months = parse_month_range(args.months)
    print(f"Fetching {len(months)} month(s) into {args.cache} ...")
    df = fetch_bts_months(months, args.cache, args.force)
    print(f"Done: {df.height:,} rows across {len(months)} month(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
