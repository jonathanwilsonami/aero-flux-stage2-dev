"""Persistence seams.

Two concerns, both written so the cloud swap is config, not a rewrite:

  * **State** — the "current flight state + latest prediction" store. A
    `StateRepository` Protocol plus local implementations (in-memory for
    tests, SQLite/Postgres for local dev) and, in production, DynamoDB.
    Callers never instantiate a backend directly — go through
    `state_backend_from_env()` so nothing upstream knows which is active.
  * **Lake** — gold/silver/raw/analytics parquet. A `LakeStore` Protocol
    plus a local filesystem implementation and, in production, S3. Same
    rule: go through `lake_backend_from_env()`.

Idempotent scoring: `upsert_prediction` keys on prediction_key (or
flight_key, for the Postgres `predictions` table's current single-row-per-
flight shape), so re-scoring a flight with the same feature/model version
overwrites rather than duplicates.

Selected via env vars (see AWS storage plan / DEPLOYMENT.md for the full
list): `STATE_BACKEND=postgres|dynamodb` (default `postgres`),
`LAKE_BACKEND=local|s3` (default `local`).
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Protocol


import polars as pl


def write_table(df: pl.DataFrame, path: str) -> str:
    """Write Parquet. `path` may be local or 's3://bucket/key' (needs
    AWS creds in the environment). Returns the path written."""
    df.write_parquet(path)
    return path


# =============================================================================
# State — current flight state + latest prediction
# =============================================================================

class StateRepository(Protocol):
    def upsert_flight_state(self, record: dict[str, Any]) -> None: ...
    def upsert_prediction(self, prediction: dict[str, Any]) -> None: ...
    def recent_flight_states(self, hours: int, limit: int | None = None) -> list[dict[str, Any]]: ...


class InMemoryStateRepository:
    """For tests and quick local runs."""

    def __init__(self) -> None:
        self.flights: dict[str, dict] = {}
        self.predictions: dict[str, dict] = {}

    def upsert_flight_state(self, record: dict[str, Any]) -> None:
        self.flights[record["flight_instance_id"]] = record

    def upsert_prediction(self, prediction: dict[str, Any]) -> None:
        self.predictions[prediction["prediction_key"]] = prediction  # idempotent

    def recent_flight_states(self, hours: int, limit: int | None = None) -> list[dict[str, Any]]:
        vals = list(self.flights.values())
        return vals[:limit] if limit else vals


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

    def recent_flight_states(self, hours: int, limit: int | None = None) -> list[dict[str, Any]]:
        sql = "SELECT doc FROM flight_state"
        params: tuple = ()
        if limit:
            sql += " LIMIT ?"
            params = (limit,)
        cur = self.conn.execute(sql, params)
        return [json.loads(r[0]) for r in cur.fetchall()]


# Columns accepted by `upsert_flight_state` — matches the `flight_instance`
# DDL in run.sh's cmd_setup / schema.sql. Extra keys on the input dict are
# ignored (silver records commonly carry more than this).
_FLIGHT_INSTANCE_COLS = [
    "schema_version", "flight_instance_id", "gufi", "flight_ref", "callsign",
    "flight_number", "carrier_icao", "carrier_iata", "carrier_name",
    "resolution_status", "tail_number", "tail_source", "hex", "aircraft_type",
    "aircraft_category", "origin", "destination", "scheduled_gate_departure",
    "scheduled_gate_arrival", "estimated_arrival", "actual_off", "actual_on",
    "flight_status", "last_latitude", "last_longitude", "last_altitude_ft",
    "last_ground_speed", "last_position_time",
]


class PostgresStateRepository:
    """Local Postgres backend. Formalizes what `score_live.py` used to do
    with an inline `_upsert_postgres` helper, so the same `StateRepository`
    Protocol covers both local Postgres and DynamoDB — `score_live.py` (and
    everything else) calls the Protocol's three methods and never knows
    which backend answered.

    `upsert_prediction` targets the same `predictions` table/DDL
    `score_live.py` already created in production use (flight_key-keyed,
    no separate `prediction_key`/`feature_version` columns yet — matching
    real current behavior, not the aspirational schema in
    AeroFlux_DataSchemas.md, since this is a pure refactor and must not
    change what's actually running).
    """

    _PREDICTIONS_DDL = """
    CREATE TABLE IF NOT EXISTS predictions (
        flight_key TEXT PRIMARY KEY,
        delay_probability DOUBLE PRECISION,
        predicted_delayed SMALLINT,
        model_version TEXT,
        scored_at TIMESTAMP
    );"""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn = None
        self._predictions_ready = False

    def _connection(self):
        import psycopg
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn)
        return self._conn

    def upsert_flight_state(self, record: dict[str, Any]) -> None:
        cols = [c for c in _FLIGHT_INSTANCE_COLS if c in record]
        placeholders = ",".join(["%s"] * len(cols))
        updates = ",".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "flight_instance_id")
        sql = (f"INSERT INTO flight_instance ({','.join(cols)}) VALUES ({placeholders}) "
               f"ON CONFLICT (flight_instance_id) DO UPDATE SET {updates}, updated_at=now()")
        conn = self._connection()
        conn.execute(sql, [record[c] for c in cols])
        conn.commit()

    def upsert_prediction(self, prediction: dict[str, Any]) -> None:
        conn = self._connection()
        if not self._predictions_ready:
            conn.execute(self._PREDICTIONS_DDL)
            self._predictions_ready = True
        conn.execute(
            """INSERT INTO predictions
               (flight_key, delay_probability, predicted_delayed, model_version, scored_at)
               VALUES (%(flight_key)s,%(delay_probability)s,%(predicted_delayed)s,
                       %(model_version)s,%(scored_at)s)
               ON CONFLICT (flight_key) DO UPDATE SET
                 delay_probability=EXCLUDED.delay_probability,
                 predicted_delayed=EXCLUDED.predicted_delayed,
                 model_version=EXCLUDED.model_version,
                 scored_at=EXCLUDED.scored_at""", prediction)
        conn.commit()

    def recent_flight_states(self, hours: int, limit: int | None = None) -> list[dict[str, Any]]:
        conn = self._connection()
        cols = ",".join(_FLIGHT_INSTANCE_COLS)
        # make_interval(hours => %s), not interval '%s hours' — a bind
        # parameter inside a quoted string literal is not real substitution;
        # psycopg/Postgres silently mis-parse it (verified: always came out
        # as exactly 1 hour regardless of the value passed). make_interval
        # is a real function call, so %s binds normally.
        sql = (f"SELECT {cols} FROM flight_instance "
               f"WHERE updated_at > now() - make_interval(hours => %s)")
        params: list[Any] = [hours]
        if limit:
            sql += " LIMIT %s"
            params.append(limit)
        cur = conn.execute(sql, params)
        names = [d.name for d in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]


# Prediction dict attributes stored on the item (mirrors what score_live.py's
# preds frame actually carries today: flight_key + these five; prediction_key/
# feature_version are accepted too, for forward compatibility, but only
# written if the caller's dict actually has them).
_PREDICTION_ATTRS = ["delay_probability", "predicted_delayed", "model_version",
                     "scored_at", "prediction_key", "feature_version"]
# flight_key is the table's partition key, not a stored attribute.
_STATE_ATTRS = [c for c in _FLIGHT_INSTANCE_COLS if c != "flight_instance_id"]


def _boto3_client(service: str, region: str):
    """boto3's default credential chain already does exactly what we need —
    explicit AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars (how the
    Lightsail container gets the aeroflux-app role) win if set, otherwise
    AWS_PROFILE (how local dev picks aeroflux-local) is used, otherwise an
    attached instance/task role. We deliberately never pass `profile_name=`
    or otherwise override this — hardcoding a profile here would break
    whichever of the two environments didn't use it."""
    import boto3
    return boto3.client(service, region_name=region)


class DynamoDBStateRepository:
    """Production current-state backend. One item per `flight_key` (the
    table's actual partition key — verified against the live, empty
    `aeroflux-current-state` table: single HASH key `flight_key`, no sort
    key, PAY_PER_REQUEST).

    State and prediction are DISJOINT attribute groups on that one item —
    `upsert_flight_state` only ever names `_STATE_ATTRS` in its
    UpdateExpression, `upsert_prediction` only ever names `_PREDICTION_ATTRS`.
    Neither does a full-item `put_item`, so one can never clobber the
    other's fields regardless of write order. Both refresh the shared
    `updated_at` (ISO8601 UTC string — sorts lexicographically the same as
    chronologically, so `recent_flight_states`'s Scan filter works) and
    `expires_at` (epoch-seconds Number — DynamoDB TTL silently ignores any
    other type, so this must never become a string).

    `recent_flight_states` is a Scan+FilterExpression, not a GSI query —
    deliberate demo-scale choice (table is small, 48h-bounded,
    PAY_PER_REQUEST). If item counts grow enough for Scan cost/latency to
    matter, the documented scale path is a GSI on `updated_at` (or
    `flight_status`) queried instead of scanned.
    """

    def __init__(self, table: str, region: str, ttl_hours: int) -> None:
        import boto3
        self.ttl_hours = ttl_hours
        # resource (not the low-level client) for plain-Python-type items —
        # same credential-chain rule as _boto3_client, just via the resource API.
        self._table = boto3.resource("dynamodb", region_name=region).Table(table)

    def _expires_at(self) -> int:
        import time
        return int(time.time()) + self.ttl_hours * 3600

    def _updated_at(self) -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _dynamo_value(v: Any) -> Any:
        # DynamoDB has no native float or datetime type. boto3's resource API
        # rejects float outright ("Float types are not supported. Use Decimal
        # types instead.") — Decimal(str(v)), not Decimal(v), to avoid
        # binary-float artifacts. psycopg hands back native `datetime` for
        # every timestamptz column (scheduled_gate_departure, actual_off,
        # last_position_time, ...) — those get the same ISO-string treatment
        # already used for updated_at, so a Scan/get_item shows a consistent,
        # directly comparable string for every timestamp on the item.
        from datetime import date, datetime
        from decimal import Decimal
        if isinstance(v, float):
            return Decimal(str(v))
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    def _update(self, flight_key: str, attrs: dict[str, Any]) -> None:
        if not attrs:
            return
        keys = list(attrs)
        names = {f"#{i}": k for i, k in enumerate(keys)}
        values = {f":{i}": self._dynamo_value(attrs[k]) for i, k in enumerate(keys)}
        values[":updated_at"] = self._updated_at()
        values[":expires_at"] = self._expires_at()
        set_parts = [f"#{i}=:{i}" for i in range(len(keys))]
        set_parts += ["updated_at=:updated_at", "expires_at=:expires_at"]
        self._table.update_item(
            Key={"flight_key": flight_key},
            UpdateExpression="SET " + ", ".join(set_parts),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def upsert_flight_state(self, record: dict[str, Any]) -> None:
        flight_key = record["flight_instance_id"]
        attrs = {c: record[c] for c in _STATE_ATTRS if c in record}
        self._update(flight_key, attrs)

    def upsert_prediction(self, prediction: dict[str, Any]) -> None:
        flight_key = prediction["flight_key"]
        attrs = {c: prediction[c] for c in _PREDICTION_ATTRS if c in prediction}
        self._update(flight_key, attrs)

    def recent_flight_states(self, hours: int, limit: int | None = None) -> list[dict[str, Any]]:
        from datetime import datetime, timedelta, timezone
        from boto3.dynamodb.conditions import Attr
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        filter_expr = Attr("updated_at").gt(cutoff)
        if limit:
            # Single-page Scan capped at `Limit=limit` items EVALUATED (the
            # DynamoDB semantics — Limit bounds read cost, not matches, so a
            # low match rate could return fewer than `limit` rows; that's
            # the correct trade for "bound the cost/latency hard", which is
            # the actual goal here, not "guarantee exactly N rows"). No
            # LastEvaluatedKey pagination when limited — one page only, by
            # design, so this can never degrade into the full-table scan
            # it's meant to avoid.
            resp = self._table.scan(FilterExpression=filter_expr, Limit=limit)
            return resp.get("Items", [])
        items: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {"FilterExpression": filter_expr}
        while True:
            resp = self._table.scan(**kwargs)
            items.extend(resp.get("Items", []))
            if "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        return items


def _require_env(name: str, *, context: str) -> str:
    """A bare `os.environ[name]` raises a bare `KeyError: 'NAME'` with no
    indication of WHICH backend needed it or why — indistinguishable from a
    typo three stack frames away. Fail loud, once, with an actionable
    message instead."""
    val = os.getenv(name)
    if not val:
        raise RuntimeError(
            f"{context} requires ${name} to be set, but it's unset or empty "
            f"in this process's environment. If this is a loop invoking a "
            f"subprocess, confirm the loop actually passes {name} through "
            f"(don't rely on implicit inheritance) rather than assuming it.")
    return val


def state_backend_from_env() -> StateRepository:
    """The one place callers ask for "the current state backend" — reads
    `STATE_BACKEND` (default `postgres`) plus the matching connection info,
    so `pipeline.py`, `score_live.py`, `sync_cloud.py`, and
    `data_access.py` all resolve the same way and none of them branch on
    the backend themselves."""
    backend = os.getenv("STATE_BACKEND", "postgres").lower()
    if backend == "postgres":
        dsn = _require_env("DSN", context="STATE_BACKEND=postgres")
        return PostgresStateRepository(dsn)
    if backend == "dynamodb":
        table = os.getenv("DYNAMODB_TABLE", "aeroflux-current-state")
        region = os.getenv("AWS_REGION", "us-east-1")
        ttl_hours = int(os.getenv("DYNAMODB_TTL_HOURS", "48"))
        return DynamoDBStateRepository(table, region, ttl_hours)
    raise ValueError(f"unknown STATE_BACKEND={backend!r} (expected postgres|dynamodb)")


# =============================================================================
# Lake — gold/silver/raw/analytics parquet
# =============================================================================

class LakeStore(Protocol):
    def write_parquet(self, df: pl.DataFrame, key: str) -> str: ...
    def read_parquet(self, key: str) -> pl.DataFrame: ...
    def list(self, prefix: str) -> list[str]: ...


class LocalLakeStore:
    """Filesystem lake — the local default (also what you'd point at a
    mounted MinIO volume). `key` is a path relative to `base_dir` (e.g.
    "gold/gold_features.parquet", "analytics/by_route.parquet")."""

    def __init__(self, base_dir: str = "out") -> None:
        self.base_dir = base_dir

    def _path(self, key: str) -> str:
        return os.path.join(self.base_dir, key)

    def write_parquet(self, df: pl.DataFrame, key: str) -> str:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return write_table(df, path)

    def read_parquet(self, key: str) -> pl.DataFrame:
        return pl.read_parquet(self._path(key))

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not os.path.isdir(base):
            return []
        out = []
        for root, _dirs, files in os.walk(base):
            for f in files:
                if f.endswith(".parquet"):
                    full = os.path.join(root, f)
                    out.append(os.path.relpath(full, self.base_dir))
        return sorted(out)


class S3LakeStore:
    """Production lake backend. `key` is relative (same contract as
    `LocalLakeStore`) — this class owns turning it into a full S3 key under
    `prefix`, so callers never build an `s3://...` string themselves.

    Reads/writes go through an in-memory buffer (`put_object`/`get_object`),
    not `s3fs`/`fsspec` — the app already has boto3 creds configured via the
    standard chain (see `_boto3_client`), so this avoids a second,
    differently-configured S3 client library.
    """

    def __init__(self, bucket: str, prefix: str, region: str) -> None:
        self.bucket = bucket
        self.prefix = prefix
        self._client = _boto3_client("s3", region)

    def _key(self, key: str) -> str:
        return f"{self.prefix}{key}" if self.prefix else key

    def write_parquet(self, df: pl.DataFrame, key: str) -> str:
        import io as _io
        buf = _io.BytesIO()
        df.write_parquet(buf)
        full_key = self._key(key)
        self._client.put_object(Bucket=self.bucket, Key=full_key, Body=buf.getvalue())
        return f"s3://{self.bucket}/{full_key}"

    def read_parquet(self, key: str) -> pl.DataFrame:
        import io as _io
        resp = self._client.get_object(Bucket=self.bucket, Key=self._key(key))
        return pl.read_parquet(_io.BytesIO(resp["Body"].read()))

    def list(self, prefix: str) -> list[str]:
        full_prefix = self._key(prefix)
        out: list[str] = []
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": full_prefix}
        while True:
            resp = self._client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                k = obj["Key"]
                if k.endswith(".parquet"):
                    # strip our own prefix back off, so keys are relative —
                    # same contract as LocalLakeStore, regardless of backend.
                    out.append(k[len(self.prefix):] if self.prefix and k.startswith(self.prefix) else k)
            if resp.get("IsTruncated"):
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            else:
                break
        return sorted(out)


def lake_backend_from_env() -> LakeStore:
    """The one place callers ask for "the lake backend" — reads
    `LAKE_BACKEND` (default `local`) plus the matching bucket/prefix, so no
    caller ever builds an `s3://...` string or a local path itself."""
    backend = os.getenv("LAKE_BACKEND", "local").lower()
    if backend == "local":
        return LocalLakeStore(os.getenv("LAKE_LOCAL_DIR", "out"))
    if backend == "s3":
        bucket = _require_env("S3_BUCKET", context="LAKE_BACKEND=s3")
        prefix = os.getenv("S3_PREFIX", "")
        region = os.getenv("AWS_REGION", "us-east-1")
        return S3LakeStore(bucket, prefix, region)
    raise ValueError(f"unknown LAKE_BACKEND={backend!r} (expected local|s3)")
