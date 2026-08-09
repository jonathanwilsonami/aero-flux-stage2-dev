"""Local -> cloud sync — the seam between the local pipeline (which keeps
doing all the real work: streaming, fusion, feature engineering, scoring)
and the durable cloud stores an always-on deployed app reads from.

Runs after each local pipeline+score cycle: writes the local gold parquet to
the LakeStore, and upserts current flight state + predictions into the
StateStore — both through the same factories the app reads through
(`state_backend_from_env` / `lake_backend_from_env`), so this module is the
only place that ever has "local" and "cloud" in view at once. Everything
downstream (the app, the next sync cycle) only ever sees one active backend.

No-op by design when both backends resolve to their local defaults
(`STATE_BACKEND=postgres`, `LAKE_BACKEND=local`) — checked and returned on
before anything else runs, so the existing local-only workflow is
byte-for-byte unchanged unless you deliberately opt into a cloud backend.

The `meta/sync_status.parquet` marker (what the System page's "last synced"
freshness number reads) is written ONLY after every step below succeeds —
never at cycle start, never on a partial/failed cycle. If the process dies
mid-sync, the marker simply doesn't advance past whatever the last clean
run left it at.
"""

from __future__ import annotations

import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import polars as pl

from .io import PostgresStateRepository, lake_backend_from_env, state_backend_from_env

_SYNC_WORKERS = int(os.getenv("SYNC_WORKERS", "20"))


def is_local_only() -> bool:
    """True when both backends resolve to the local defaults — the
    condition under which sync_cloud must do nothing at all."""
    return (os.getenv("STATE_BACKEND", "postgres").lower() == "postgres"
            and os.getenv("LAKE_BACKEND", "local").lower() == "local")


def _upsert_all(method, items: list[dict]) -> int:
    """Fan out independent per-item upserts concurrently. DynamoDB's
    UpdateItem — required for the disjoint-attribute, no-clobber design —
    has no batch equivalent that supports partial attribute updates
    (BatchWriteItem only does full-item Put/Delete), so this is plain
    independent I/O per item; a thread pool keeps tens of thousands of rows
    from taking tens of minutes. `f.result()` re-raises on the first
    failure, so one bad item fails the whole cycle loudly rather than
    silently under-counting."""
    if not items:
        return 0
    with ThreadPoolExecutor(max_workers=_SYNC_WORKERS) as ex:
        futures = [ex.submit(method, item) for item in items]
        n = 0
        for f in as_completed(futures):
            f.result()
            n += 1
    return n


def sync_once(*, gold_path: str, predictions_path: str, dsn: str,
              state_hours: int = 48) -> dict:
    """Run one sync cycle. Returns a summary dict. Raises on any failure —
    the caller (main(), or whatever calls this directly) decides what to do
    with that; the synced_at marker is only written if this returns
    normally, i.e. every step below completed."""
    lake = lake_backend_from_env()
    state = state_backend_from_env()
    summary = {"gold_rows": 0, "state_rows": 0, "prediction_rows": 0}

    if os.path.exists(gold_path):
        gold = pl.read_parquet(gold_path)
        lake.write_parquet(gold, "gold/gold_features.parquet")
        summary["gold_rows"] = gold.height

    # Postgres flight_instance is the source of truth for "current state"
    # regardless of which STATE_BACKEND is active — sync reads from it
    # explicitly (not through state_backend_from_env(), which resolves to
    # whichever backend we're writing *to*).
    source = PostgresStateRepository(dsn)
    rows = source.recent_flight_states(hours=state_hours)
    summary["state_rows"] = _upsert_all(state.upsert_flight_state, rows)

    if os.path.exists(predictions_path):
        preds = pl.read_parquet(predictions_path)
        summary["prediction_rows"] = _upsert_all(state.upsert_prediction, preds.to_dicts())

    marker = pl.DataFrame({
        "synced_at": [datetime.now(timezone.utc).isoformat()],
        "gold_rows": [summary["gold_rows"]],
        "state_rows": [summary["state_rows"]],
        "prediction_rows": [summary["prediction_rows"]],
    })
    lake.write_parquet(marker, "meta/sync_status.parquet")
    return summary


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Sync local gold/state/predictions to the cloud backends.")
    ap.add_argument("--gold", default=os.getenv("GOLD", "out/gold_features.parquet"))
    ap.add_argument("--predictions", default=os.getenv("PREDICTIONS", "out/predictions.parquet"))
    ap.add_argument("--dsn", default=os.getenv(
        "DSN", "postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"))
    ap.add_argument("--state-hours", type=int, default=int(os.getenv("DYNAMODB_TTL_HOURS", "48")))
    args = ap.parse_args(argv)

    if is_local_only():
        print("sync_cloud: STATE_BACKEND=postgres, LAKE_BACKEND=local — nothing to sync (no-op).")
        return 0

    print(f"sync_cloud: STATE_BACKEND={os.getenv('STATE_BACKEND')} "
          f"LAKE_BACKEND={os.getenv('LAKE_BACKEND')} starting ...")
    try:
        summary = sync_once(gold_path=args.gold, predictions_path=args.predictions,
                            dsn=args.dsn, state_hours=args.state_hours)
    except Exception as e:
        print(f"sync_cloud: FAILED ({type(e).__name__}: {e}) — synced_at marker not advanced.")
        return 1
    print(f"sync_cloud: done. gold_rows={summary['gold_rows']} "
          f"state_rows={summary['state_rows']} prediction_rows={summary['prediction_rows']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
