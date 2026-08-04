"""One-command runner for the local ML pipeline.

    python -m aeroflux_ml.run postgres --dsn "$DSN" --table public.flight_instance
    python -m aeroflux_ml.run jsonl --in dataset.jsonl
    # add --model models/xgb.json to also score, --out ./out to choose output dir

Writes gold_features.parquet/.csv (and predictions.* if a model is given) to the
output directory, and prints a readable summary of coverage and a sample row.
"""

from __future__ import annotations

import argparse
import sys

import polars as pl

from .config import load_config
from . import pipeline


def _print_summary(s: dict) -> None:
    print("=" * 60)
    print(f"silver rows in: {s['silver_rows']}   ->   gold rows out: {s['gold_rows']}")
    print("-" * 60)
    print("Feature coverage (non-null %):")
    for col, pct in s["feature_coverage"]:
        bar = "#" * int(pct / 5)
        print(f"  {col:26} {pct:5.1f}%  {bar}")
    print("-" * 60)
    print("Wrote:")
    for p in s["outputs"]:
        print(f"  {p}")
    if s["predictions"]:
        print(f"  {s['predictions']}")
    print("=" * 60)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="source", required=True)

    pg = sub.add_parser("postgres", help="read silver from Postgres flight_instance")
    pg.add_argument("--dsn", required=True)
    pg.add_argument("--table", default="public.flight_instance")
    pg.add_argument("--limit", type=int, default=2000000)

    js = sub.add_parser("jsonl", help="read silver from a dataset.jsonl file")
    js.add_argument("--in", dest="infile", required=True)

    for p in (pg, js):
        p.add_argument("--config", default="configs/pipeline.yaml")
        p.add_argument("--out", default="./out")
        p.add_argument("--model", default=None, help="optional trained model to score with")
        p.add_argument("--state-db", default=None, help="optional sqlite path for predictions")
        p.add_argument("--weather", choices=["live", "off"], default="off",
                       help="fetch live METAR for airports in the data and enable the weather channel")
        p.add_argument("--weather-hours", type=int, default=24,
                       help="hours of METAR history to fetch (wider = more of the 48h flight backlog matches)")
        p.add_argument("--show", type=int, default=3, help="sample gold rows to print")

    args = ap.parse_args()
    cfg = load_config(args.config)

    if args.source == "postgres":
        silver = pipeline.read_silver_postgres(args.dsn, args.table, args.limit)
    else:
        silver = pipeline.read_silver_jsonl(args.infile)

    if len(silver) == 0:
        sys.exit("No silver rows read from the source.")

    context = None
    if args.weather == "live":
        from aeroflux_ml import fetch_metar_live
        stations = sorted({s for s in
                           silver["origin"].drop_nulls().to_list()
                           + silver["destination"].drop_nulls().to_list()})
        print(f"Fetching live METAR for {len(stations)} airport(s)...")
        try:
            obs = fetch_metar_live(stations, hours=args.weather_hours)
            cfg.features.channels["weather"] = True
            context = {"weather_obs": obs}
            print(f"  got {obs.height} observations")
        except Exception as e:
            # a transient weather-API issue shouldn't leave stale gold: warn and
            # regenerate gold without weather features this cycle.
            print(f"  WARNING: weather fetch failed ({e}); continuing without weather")

    summary = pipeline.run(cfg, silver, args.out, model_path=args.model,
                           state_db=args.state_db, context=context)
    _print_summary(summary)

    if args.show:
        gold = pl.read_parquet(f"{args.out}/gold_features.parquet")
        print(f"\nSample gold rows (the model's input):")
        with pl.Config(tbl_cols=-1, tbl_width_chars=200):
            print(gold.head(args.show))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())