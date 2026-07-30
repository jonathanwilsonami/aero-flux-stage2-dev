#!/usr/bin/env python3
"""Build the gold ML feature/label table from the silver flight-instance data.

Input is the silver layer you already have — either the dataset.jsonl produced
by build_dataset.py, or the Postgres flight_instance table. Output is a flat,
model-ready table (CSV + Parquet) plus a readiness summary: label coverage,
delay distribution, and per-feature null rates.

    python build_features.py --in dataset.jsonl
    python build_features.py --dsn "postgresql://..." --table flight_instance
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd

from aeroflux_parser import (
    build_feature_table, FEATURE_COLUMNS, LABEL_COLUMNS, ID_COLUMNS, ALL_COLUMNS,
)


def read_jsonl(path: str) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return rows


def read_postgres(dsn: str, table: str) -> list[dict]:
    try:
        import psycopg
        conn = psycopg.connect(dsn)
    except ImportError:
        import psycopg2 as psycopg
        conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table}")
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        conn.close()


def summarize(df: pd.DataFrame, n_silver: int) -> None:
    print("=" * 62)
    print(f"{n_silver} silver flight(s) -> {len(df)} gold row(s) with a label")
    print("-" * 62)
    print("Label coverage:")
    for col in LABEL_COLUMNS:
        n = int(df[col].notna().sum())
        print(f"  {col:16} {n:6}  ({100*n/len(df):5.1f}%)")
    print("Delay distribution (minutes):")
    for col in ("dep_delay_min", "arr_delay_min"):
        s = df[col].dropna()
        if len(s):
            print(f"  {col:16} n={len(s):5}  mean={s.mean():6.1f}  "
                  f"median={s.median():6.1f}  p90={s.quantile(0.9):6.1f}")
    for col in ("dep_delay_15", "arr_delay_15"):
        s = df[col].dropna()
        if len(s):
            print(f"  {col:16} positive rate = {100*s.mean():.1f}%  (n={len(s)})")
    print("Feature null rate:")
    for col in FEATURE_COLUMNS:
        null = 100 * df[col].isna().mean()
        print(f"  {col:18} {null:5.1f}% null")
    print("=" * 62)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--in", dest="infile", help="silver dataset.jsonl")
    ap.add_argument("--dsn", help="Postgres DSN (alternative to --in)")
    ap.add_argument("--table", default="flight_instance")
    ap.add_argument("--out-csv", default="gold.csv")
    ap.add_argument("--out-parquet", default="gold.parquet")
    args = ap.parse_args()

    if args.infile:
        silver = read_jsonl(args.infile)
    elif args.dsn:
        silver = read_postgres(args.dsn, args.table)
    else:
        sys.exit("Provide --in dataset.jsonl or --dsn <postgres>.")

    if not silver:
        sys.exit("No silver records read.")

    rows = build_feature_table(silver)
    if not rows:
        sys.exit("No rows had a computable delay label. "
                 "(This SWIM window may be mostly planned/in-flight; "
                 "for training, run this transform over BTS historical.)")

    df = pd.DataFrame(rows, columns=ALL_COLUMNS)
    df.to_csv(args.out_csv, index=False)
    try:
        df.to_parquet(args.out_parquet, index=False)
        wrote = f"{args.out_csv} and {args.out_parquet}"
    except Exception as exc:  # pyarrow missing etc.
        wrote = f"{args.out_csv} (parquet skipped: {exc})"

    summarize(df, len(silver))
    print(f"\nFeatures: {FEATURE_COLUMNS}")
    print(f"Labels:   {LABEL_COLUMNS}")
    print(f"Wrote {len(df)} rows -> {wrote}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
