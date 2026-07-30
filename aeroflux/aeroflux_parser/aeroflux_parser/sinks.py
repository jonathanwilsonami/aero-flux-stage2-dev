"""Configurable sinks for fused flight-instance records.

The reducer produces plain dicts; a Sink decides where they land. This is the
"decide where the data goes" seam -- swap sinks without touching fusion logic.

    MemorySink()                 -> keep in process (validate the merge first)
    JsonlSink("out.jsonl")       -> one JSON object per line (zero infra)
    PostgresSink(dsn, table)     -> upsert onto a flight_instance table

All sinks share one method: write(records) -> count.
"""

from __future__ import annotations

import json
from typing import Any, Iterable, Protocol

# Canonical columns, in order. Keep in sync with canonical.to_canonical().
CANONICAL_FIELDS = [
    "flight_instance_id",
    "tail_number",
    "callsign",
    "operating_carrier",
    "origin",
    "destination",
    "scheduled_gate_departure",
    "scheduled_gate_arrival",
    "estimated_gate_arrival",
    "flight_status",
]


class Sink(Protocol):
    def write(self, records: Iterable[dict[str, Any]]) -> int: ...


class MemorySink:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def write(self, records: Iterable[dict[str, Any]]) -> int:
        self.records = list(records)
        return len(self.records)


class JsonlSink:
    def __init__(self, path: str) -> None:
        self.path = path

    def write(self, records: Iterable[dict[str, Any]]) -> int:
        count = 0
        with open(self.path, "w", encoding="utf-8") as fh:
            for record in records:
                fh.write(json.dumps(record) + "\n")
                count += 1
        return count


class CsvSink:
    """Flat, spreadsheet-friendly output. Column order from `fields` if given,
    else the union of keys across records (stable, first-seen order)."""

    def __init__(self, path: str, fields: list[str] | None = None) -> None:
        self.path = path
        self.fields = fields

    def write(self, records: Iterable[dict[str, Any]]) -> int:
        import csv
        rows = list(records)
        if self.fields is not None:
            fields = self.fields
        else:
            fields = []
            for r in rows:
                for k in r:
                    if k not in fields:
                        fields.append(k)
        with open(self.path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        return len(rows)


class PostgresSink:
    """Upserts onto a flight_instance table keyed by flight_instance_id.

    DDL (run once):

        CREATE TABLE flight_instance (
            flight_instance_id        text PRIMARY KEY,
            tail_number               text,
            callsign                  text,
            operating_carrier         text,
            origin                    text,
            destination               text,
            scheduled_gate_departure  timestamptz,
            scheduled_gate_arrival    timestamptz,
            estimated_gate_arrival    timestamptz,
            flight_status             text,
            updated_at                timestamptz DEFAULT now()
        );
    """

    def __init__(self, dsn: str, table: str = "flight_instance") -> None:
        self.dsn = dsn
        self.table = table

    def _connect(self):
        try:
            import psycopg
            return psycopg.connect(self.dsn)
        except ImportError:
            import psycopg2
            return psycopg2.connect(self.dsn)

    def write(self, records: Iterable[dict[str, Any]]) -> int:
        cols = CANONICAL_FIELDS
        placeholders = ", ".join(["%s"] * len(cols))
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != "flight_instance_id")
        sql = (
            f"INSERT INTO {self.table} ({', '.join(cols)}) VALUES ({placeholders}) "
            f"ON CONFLICT (flight_instance_id) DO UPDATE SET {updates}, updated_at = now()"
        )
        conn = self._connect()
        count = 0
        try:
            with conn.cursor() as cur:
                for record in records:
                    cur.execute(sql, tuple(record.get(c) for c in cols))
                    count += 1
            conn.commit()
        finally:
            conn.close()
        return count


def make_sink(name: str, *, path: str | None = None, dsn: str | None = None,
              table: str = "flight_instance") -> Sink:
    if name == "memory":
        return MemorySink()
    if name == "jsonl":
        return JsonlSink(path or "flight_instances.jsonl")
    if name == "csv":
        return CsvSink(path or "flight_instances.csv")
    if name == "postgres":
        if not dsn:
            raise ValueError("postgres sink requires --dsn")
        return PostgresSink(dsn, table)
    raise ValueError(f"unknown sink: {name}")
