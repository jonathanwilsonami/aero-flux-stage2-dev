"""Persistence seams.

Two concerns, both written so the cloud swap is config, not a rewrite:

  * Parquet/Iceberg writer — a local path today; an `s3://...` path in AWS with
    no code change (polars/pyarrow resolve S3 via fsspec + env credentials).
  * State repository — the NoSQL "current flight state + latest prediction"
    store. A protocol plus a local implementation (in-memory for tests, SQLite
    for local dev / inspection). Swap in DynamoDB or MongoDB in production by
    implementing the same three methods; nothing upstream changes.

Idempotent scoring: `upsert_prediction` keys on prediction_key, so re-scoring a
flight with the same feature/model version overwrites rather than duplicates.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Protocol

import polars as pl


def write_table(df: pl.DataFrame, path: str) -> str:
    """Write Parquet. `path` may be local or 's3://bucket/key' (needs
    AWS creds in the environment). Returns the path written."""
    df.write_parquet(path)
    return path


class StateRepository(Protocol):
    def upsert_flight_state(self, record: dict[str, Any]) -> None: ...
    def upsert_prediction(self, prediction: dict[str, Any]) -> None: ...
    def recent_flight_states(self, hours: int) -> list[dict[str, Any]]: ...


class InMemoryStateRepository:
    """For tests and quick local runs."""

    def __init__(self) -> None:
        self.flights: dict[str, dict] = {}
        self.predictions: dict[str, dict] = {}

    def upsert_flight_state(self, record: dict[str, Any]) -> None:
        self.flights[record["flight_instance_id"]] = record

    def upsert_prediction(self, prediction: dict[str, Any]) -> None:
        self.predictions[prediction["prediction_key"]] = prediction  # idempotent

    def recent_flight_states(self, hours: int) -> list[dict[str, Any]]:
        return list(self.flights.values())


class SqliteStateRepository:
    """Local, persistent, zero-server. Good for local dev + inspection. The
    prod equivalent is DynamoDB (flight_instance_id / prediction_key as keys)
    or MongoDB (upsert by the same keys)."""

    def __init__(self, path: str = "aeroflux_state.db") -> None:
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS flight_state ("
            "flight_instance_id TEXT PRIMARY KEY, updated_at TEXT, doc TEXT)")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS prediction ("
            "prediction_key TEXT PRIMARY KEY, flight_key TEXT, scored_at TEXT, doc TEXT)")
        self.conn.commit()

    def upsert_flight_state(self, record: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO flight_state (flight_instance_id, updated_at, doc) VALUES (?,?,?) "
            "ON CONFLICT(flight_instance_id) DO UPDATE SET updated_at=excluded.updated_at, doc=excluded.doc",
            (record["flight_instance_id"], record.get("last_position_time"), json.dumps(record)))
        self.conn.commit()

    def upsert_prediction(self, prediction: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO prediction (prediction_key, flight_key, scored_at, doc) VALUES (?,?,?,?) "
            "ON CONFLICT(prediction_key) DO UPDATE SET scored_at=excluded.scored_at, doc=excluded.doc",
            (prediction["prediction_key"], prediction["flight_key"],
             prediction.get("scored_at"), json.dumps(prediction)))
        self.conn.commit()

    def recent_flight_states(self, hours: int) -> list[dict[str, Any]]:
        cur = self.conn.execute("SELECT doc FROM flight_state")
        return [json.loads(r[0]) for r in cur.fetchall()]
