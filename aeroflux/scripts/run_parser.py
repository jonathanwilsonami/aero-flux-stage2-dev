#!/usr/bin/env python3
"""Validation harness for aeroflux_parser.

Runs the parser against one of three sources and prints a summary you can
eyeball for correctness:

    file      saved .xml documents (zero infra; start here)
    postgres  a batch of rows from your existing DB (repeatable; validate here)
    kafka     a live/replayed topic (final integration check)

Every source yields raw strings; each is fed through `from_kafka_value`, which
transparently handles both the JSON envelope from swim_to_kafka.py AND bare XML,
so the same code path works for all three.

Examples
--------
    # 1. Zero-infra smoke test (uses the bundled sample):
    python run_parser.py file --path "samples/*.xml"

    # 2. Against your Postgres data (plug in your table + column):
    python run_parser.py postgres --table swim_messages --column value --limit 500 \
        --dsn "postgresql://user:pass@localhost:5432/aeroflux"

    # 3. Against the live stream:
    python run_parser.py kafka --topic swim.raw.flight --limit 200 \
        --bootstrap localhost:9092
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from collections import Counter
from typing import Iterator

from aeroflux_parser import from_kafka_value, normalize, to_canonical, ParseStatus
from aeroflux_parser import FlightInstanceReducer, make_sink
from aeroflux_parser.result import ParsedMessage


# --- sources: each yields raw payload strings ------------------------------


def iter_file(pattern: str) -> Iterator[str]:
    paths = sorted(glob.glob(pattern))
    if not paths:
        sys.exit(f"No files matched: {pattern}")
    for path in paths:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            yield fh.read()


def iter_postgres(dsn: str, table: str, column: str, limit: int,
                   order_by: str | None = None) -> Iterator[str]:
    """order_by is optional (None = old behavior, no ORDER BY) so this stays
    safe for any table, not just swim.raw_messages -- but "no ORDER BY" +
    LIMIT against a table taking continuous concurrent inserts/deletes
    means Postgres can return an arbitrary, unstable subset of rows on
    each call (confirmed live 2026-08-15: swim.raw_messages at 2.3M+ rows
    vs. LIMIT=500000 covers roughly a fifth of the 48h retention window,
    and without an explicit order the ~500k rows selected aren't
    guaranteed recency-biased or even stable between cycles -- a flight's
    raw messages could be sampled into one pipeline cycle's
    flight_instance/gold and silently miss the next one, well before
    actually aging out of retention). Callers that care about recency
    (build_dataset.py's postgres source, via run.sh) should pass an
    explicit order_by naming an indexed, NOT NULL recency column --
    swim.raw_messages.stored_at is that column here (NOT NULL, already
    has `idx_raw_messages_stored_at btree (stored_at DESC)`, and is the
    same column retention/staleness-checks already treat as ground truth
    elsewhere in this codebase)."""
    conn = _pg_connect(dsn)
    try:
        with conn.cursor() as cur:
            # table/column/order_by are your own CLI inputs, not user data
            # -> f-string is fine.
            sql = f"SELECT {column} FROM {table}"
            if order_by:
                sql += f" ORDER BY {order_by}"
            sql += " LIMIT %s"
            cur.execute(sql, (limit,))
            for (value,) in cur.fetchall():
                if value is None:
                    continue
                yield value if isinstance(value, str) else str(value)
    finally:
        conn.close()


def iter_kafka(bootstrap: str, topic: str, group: str, limit: int) -> Iterator[str]:
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    seen = 0
    idle = 0
    try:
        while seen < limit:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                if idle >= 10:  # ~10s with no data -> stop rather than hang
                    print("No more messages; stopping.", file=sys.stderr)
                    break
                continue
            idle = 0
            if msg.error():
                print(f"Kafka error: {msg.error()}", file=sys.stderr)
                continue
            value = msg.value()
            yield value.decode("utf-8", "replace") if isinstance(value, bytes) else value
            seen += 1
    finally:
        consumer.close()


def _pg_connect(dsn: str):
    try:
        import psycopg  # psycopg 3
        return psycopg.connect(dsn)
    except ImportError:
        pass
    try:
        import psycopg2
        return psycopg2.connect(dsn)
    except ImportError:
        sys.exit("Install a driver: pip install psycopg   (or psycopg2-binary)")


# --- reporting -------------------------------------------------------------


def summarize(records: list[ParsedMessage]) -> None:
    total = len(records)
    by_status = Counter(r.parse_status for r in records)
    by_type = Counter(r.msg_type for r in records)
    by_parser = Counter(r.parser for r in records)

    print("=" * 60)
    print(f"Parsed {total} message(s)")
    print("-" * 60)
    print("By status:")
    for status in (ParseStatus.OK, ParseStatus.PARTIAL, ParseStatus.FAILED):
        if by_status.get(status):
            print(f"  {status:8} {by_status[status]}")
    print("By parser:")
    for name, n in by_parser.most_common():
        print(f"  {name:12} {n}")
    print(f"Message types seen: {len([t for t in by_type if t])}")
    for mtype, n in by_type.most_common():
        print(f"  {str(mtype):32} {n}")
    print("=" * 60)


def show_samples(records: list[ParsedMessage], n: int) -> None:
    if n <= 0:
        return
    print(f"\n--- first {n} record(s) ---")
    for r in records[:n]:
        d = r.to_dict()
        # trim body to top-level keys so the console stays readable
        d["body"] = {k: "..." for k in d["body"]} if d["body"] else {}
        d["raw_xml"] = d["raw_xml"][:80] + "..." if len(d["raw_xml"]) > 80 else d["raw_xml"]
        # normalized stays untrimmed -- it's the point of the flag
        print(json.dumps(d, indent=2))


def show_problems(records: list[ParsedMessage]) -> None:
    problems = [r for r in records if r.parse_status != ParseStatus.OK]
    if not problems:
        print("\nNo PARTIAL/FAILED records. ✅")
        return
    print(f"\n--- {len(problems)} PARTIAL/FAILED record(s) ---")
    for r in problems[:10]:
        print(f"[{r.parse_status}] {r.msg_type} :: {r.errors}")
        print(f"  raw: {r.raw_xml[:160]}")


# --- main ------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="source", required=True)

    p_file = sub.add_parser("file", help="parse saved .xml documents")
    p_file.add_argument("--path", default="samples/*.xml", help="glob of XML files")

    p_pg = sub.add_parser("postgres", help="parse a batch of DB rows")
    p_pg.add_argument("--dsn", required=True, help="postgresql://user:pass@host:port/db")
    p_pg.add_argument("--table", required=True)
    p_pg.add_argument("--column", required=True, help="column holding the payload/envelope")
    p_pg.add_argument("--limit", type=int, default=500)
    p_pg.add_argument("--order-by", default=None,
                       help="e.g. 'stored_at DESC' -- biases LIMIT toward the "
                            "newest rows instead of an arbitrary/unstable subset "
                            "(recommended for any table taking concurrent inserts)")

    p_kafka = sub.add_parser("kafka", help="parse from a live/replayed topic")
    p_kafka.add_argument("--bootstrap", default="localhost:9092")
    p_kafka.add_argument("--topic", default="swim.raw.flight")
    p_kafka.add_argument("--group", default="aeroflux-parser-validation")
    p_kafka.add_argument("--limit", type=int, default=200)

    for p in (p_file, p_pg, p_kafka):
        p.add_argument("--show", type=int, default=2, help="pretty-print N sample records")
        p.add_argument("--normalize", action="store_true",
                       help="run the typed normalization layer on each record")
        p.add_argument("--canonical", action="store_true",
                       help="also print canonical flight-instance records (implies --normalize)")
        p.add_argument("--fuse", action="store_true",
                       help="fuse messages into one record per flight, then write to --sink")
        p.add_argument("--sink", default="memory", choices=["memory", "jsonl", "postgres"],
                       help="where fused records go (default: memory)")
        p.add_argument("--sink-path", default="flight_instances.jsonl",
                       help="output path for the jsonl sink")
        p.add_argument("--sink-table", default="flight_instance",
                       help="target table for the postgres sink")

    args = parser.parse_args()

    if args.source == "file":
        raw_iter = iter_file(args.path)
    elif args.source == "postgres":
        raw_iter = iter_postgres(args.dsn, args.table, args.column, args.limit,
                                  order_by=args.order_by)
    else:
        raw_iter = iter_kafka(args.bootstrap, args.topic, args.group, args.limit)

    records: list[ParsedMessage] = []
    for raw in raw_iter:
        records.extend(from_kafka_value(raw))

    if args.normalize or args.canonical or args.fuse:
        for r in records:
            normalize(r)
        n_norm = sum(1 for r in records if r.normalized)
        print(f"Normalized {n_norm}/{len(records)} record(s) "
              f"(types with a normalizer registered).\n")

    summarize(records)
    show_problems(records)
    show_samples(records, args.show)

    if args.canonical:
        show_canonical(records, args.show)

    if args.fuse:
        run_fusion(records, args)
    return 0


def run_fusion(records: list[ParsedMessage], args) -> None:
    reducer = FlightInstanceReducer()
    for r in records:
        reducer.add(r)
    fused = reducer.records()

    dsn = getattr(args, "dsn", None)
    sink = make_sink(args.sink, path=args.sink_path, dsn=dsn, table=args.sink_table)
    written = sink.write(fused)

    print(f"\nFused {len(records)} message(s) -> {len(fused)} flight instance(s); "
          f"wrote {written} to '{args.sink}' sink.")
    for rec in fused[:args.show]:
        print(json.dumps(rec, indent=2))


def show_canonical(records: list[ParsedMessage], n: int) -> None:
    # surface records that actually carry schedule/status, not just any first N
    rich = [r for r in records if r.normalized.get("flight_status")] or records
    print(f"\n--- {min(n, len(rich))} canonical flight-instance record(s) ---")
    for r in rich[:n]:
        print(json.dumps(to_canonical(r), indent=2))


if __name__ == "__main__":
    raise SystemExit(main())
