#!/usr/bin/env python3
"""Build the canonical AeroFlux flight-instance dataset from SWIM data.

One row per flight, fields fused across message types, identities resolved, and
every record tagged with a plain-language `resolution_status`. Writes JSONL and
CSV, and prints a readable summary (status mix, field fill rates, a sample row).

    python build_dataset.py file --path "samples/*.xml"
    python build_dataset.py postgres --dsn "..." --table T --column C --limit 3000
    python build_dataset.py postgres --dsn "..." --table T --column C --live 25   # ADS-B tails

Output defaults: dataset.jsonl + dataset.csv
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter

from aeroflux_parser import (
    from_kafka_value, normalize, FlightInstanceReducer, enrich_record,
    DATASET_FIELDS, AirlineTable,
)
from aeroflux_parser.sinks import JsonlSink, CsvSink
from run_parser import iter_file, iter_postgres, iter_kafka


def build(raw_iter, live: int, adsb_store_dsn: str | None = None):
    reducer = FlightInstanceReducer()
    n_msg = 0
    for raw in raw_iter:
        for rec in from_kafka_value(raw):
            normalize(rec)
            reducer.add(rec)
            n_msg += 1
    fused = reducer.records()

    table = AirlineTable()
    resolver = None
    remaining = {"n": live}
    if adsb_store_dsn:
        # Preferred: resolve from the rolling store the poller fills (no API calls).
        from aeroflux_parser.adsb_store import PostgresAirframeStore
        resolver = PostgresAirframeStore(adsb_store_dsn).resolve
    elif live > 0:
        # Fallback: live per-callsign lookups (rate-limited; capped).
        from aeroflux_parser import AdsbClient
        client = AdsbClient()

        def capped(callsign):
            if remaining["n"] <= 0:
                return None
            remaining["n"] -= 1
            return client.resolve_tail(callsign)

        resolver = capped

    enriched = [enrich_record(r, table, adsb_resolver=resolver) for r in fused]
    return n_msg, enriched


def summarize(n_msg: int, rows: list[dict]) -> None:
    print("=" * 64)
    print(f"{n_msg} message(s) -> {len(rows)} flight instance(s)")
    print("-" * 64)

    status = Counter(r["resolution_status"] for r in rows)
    print("Resolution status:")
    for label, n in status.most_common():
        print(f"  {label:26} {n:5}  ({100*n/len(rows):4.1f}%)")

    tail = Counter(r["tail_source"] for r in rows)
    print("Tail number source:")
    for label, n in tail.most_common():
        print(f"  {label:26} {n:5}  ({100*n/len(rows):4.1f}%)")

    print("Field fill rate (non-null):")
    for field in DATASET_FIELDS:
        filled = sum(1 for r in rows if r.get(field) not in (None, ""))
        print(f"  {field:26} {100*filled/len(rows):5.1f}%")
    print("=" * 64)

    # a readable sample: prefer a record that actually has schedule + status
    rich = [r for r in rows if r.get("flight_status")] or rows
    print("\nSample canonical record:")
    print(json.dumps({k: rich[0].get(k) for k in DATASET_FIELDS}, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="source", required=True)

    pf = sub.add_parser("file"); pf.add_argument("--path", default="samples/*.xml")
    pp = sub.add_parser("postgres")
    pp.add_argument("--dsn", required=True); pp.add_argument("--table", required=True)
    pp.add_argument("--column", required=True); pp.add_argument("--limit", type=int, default=5000)
    pk = sub.add_parser("kafka")
    pk.add_argument("--bootstrap", default="localhost:9092")
    pk.add_argument("--topic", default="swim.raw.flight")
    pk.add_argument("--group", default="aeroflux-dataset"); pk.add_argument("--limit", type=int, default=2000)

    for p in (pf, pp, pk):
        p.add_argument("--out-jsonl", default="dataset.jsonl")
        p.add_argument("--out-csv", default="dataset.csv")
        p.add_argument("--out-invalid", default="dataset.invalid.jsonl")
        p.add_argument("--no-validate", dest="validate", action="store_false",
                       help="skip the pydantic schema-contract validation")
        p.set_defaults(validate=True)
        p.add_argument("--adsb-store", dest="adsb_store", default=None,
                       help="DSN of the rolling ADS-B store (from adsb_poller.py) to resolve tails from")
        p.add_argument("--live", type=int, default=0,
                       help="resolve up to N airline tails via live ADS-B")

    args = parser.parse_args()
    if args.source == "file":
        raw_iter = iter_file(args.path)
    elif args.source == "postgres":
        raw_iter = iter_postgres(args.dsn, args.table, args.column, args.limit)
    else:
        raw_iter = iter_kafka(args.bootstrap, args.topic, args.group, args.limit)

    n_msg, rows = build(raw_iter, args.live, getattr(args, "adsb_store", None))
    if not rows:
        sys.exit("No flight instances produced.")

    fields = ["schema_version"] + DATASET_FIELDS

    if args.validate:
        try:
            from aeroflux_parser.schema import validate_batch
        except ImportError:
            sys.exit("Validation needs pydantic. Install: pip install pydantic "
                     "(or run with --no-validate)")
        valid, invalid = validate_batch(rows)
        JsonlSink(args.out_jsonl).write(valid)
        CsvSink(args.out_csv, fields=fields).write(valid)
        if invalid:
            JsonlSink(args.out_invalid).write(invalid)
        summarize(n_msg, valid)
        print(f"\nValidation: {len(valid)} passed, {len(invalid)} failed "
              f"(schema contract).")
        if invalid:
            reasons = Counter(e for r in invalid for e in r.get("_errors", []))
            for reason, k in reasons.most_common(5):
                print(f"  {k:5}  {reason}")
            print(f"  quarantined -> {args.out_invalid}")
        print(f"Wrote {len(valid)} valid row(s) -> {args.out_jsonl} and {args.out_csv}")
    else:
        JsonlSink(args.out_jsonl).write(rows)
        CsvSink(args.out_csv, fields=DATASET_FIELDS).write(rows)
        summarize(n_msg, rows)
        print(f"\nWrote {len(rows)} rows -> {args.out_jsonl} and {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())