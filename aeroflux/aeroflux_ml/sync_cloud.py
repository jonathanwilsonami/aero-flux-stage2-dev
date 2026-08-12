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

State + prediction upserts are deduped against a local content-hash cache
(SYNC_DEDUPE=1, the default) — a flight whose state/prediction hasn't
actually changed since last sync, and was synced recently enough to still
be within DYNAMODB_REFRESH_HOURS, is skipped rather than re-written every
cycle. See filter_changed()'s docstring for the TTL-safety guarantee this
relies on. SYNC_DEDUPE=0 restores the old every-row-every-cycle behavior.

sync_eval_outputs() is a separate, much lower-frequency sync (live-
evaluation JSON + reconciled pairs, called from evaluate_live.py's
report() every few hours) — not part of the SYNC_EVERY flight-state loop
above. See its own docstring.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import polars as pl

from .io import (PostgresStateRepository, _PREDICTION_ATTRS, _STATE_ATTRS,
                  lake_backend_from_env, state_backend_from_env)

_SYNC_WORKERS = int(os.getenv("SYNC_WORKERS", "20"))

# ---- write-volume dedup ------------------------------------------------
# Cost Explorer confirmed (2026-08) that DynamoDB WRITES, not reads, drive
# the bill: sync upserts every tracked flight's state + prediction every
# cycle regardless of whether anything actually changed. Most of the
# tracked set at any moment is PLANNED (not yet departed) or COMPLETED
# (already landed) — static between cycles — while only a small fraction
# is ACTIVE (position genuinely updating). Diffing against a local cache
# skips re-writing unchanged items, while still refreshing each item at
# least every DYNAMODB_REFRESH_HOURS regardless of whether it changed —
# without that, an item that's genuinely static for a long stretch (e.g.
# PLANNED, hours before departure) would never get its `expires_at` TTL
# refreshed and would silently disappear from the app once TTL lapsed,
# even though Postgres still considers it current. That refresh floor is
# the correctness guarantee: skip means "unchanged AND refreshed recently
# enough," never just "unchanged."
SYNC_DEDUPE = os.getenv("SYNC_DEDUPE", "1") == "1"
DYNAMODB_REFRESH_HOURS = float(os.getenv("DYNAMODB_REFRESH_HOURS", "12"))
_STATE_CACHE_PATH = "out/.sync_diff_state.parquet"
_PREDICTION_CACHE_PATH = "out/.sync_diff_predictions.parquet"
# scored_at is excluded on purpose — score_live.py re-stamps it every
# cycle even when the model's output for a flight hasn't changed at all
# (same gold features in, same prediction out), so including it would
# make every prediction look "changed" every cycle and defeat the dedup
# entirely for the one record type where it matters most (predictions
# outnumber state changes for the static majority of flights).
_PREDICTION_HASH_ATTRS = [c for c in _PREDICTION_ATTRS if c != "scored_at"]


def _content_hash(record: dict, cols: list[str]) -> str:
    payload = json.dumps({c: record.get(c) for c in cols}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def _load_diff_cache(path: str) -> dict[str, tuple[str, datetime]]:
    if not os.path.exists(path):
        return {}
    try:
        df = pl.read_parquet(path)
        return {r["flight_key"]: (r["content_hash"], r["last_synced_at"])
                for r in df.iter_rows(named=True)}
    except Exception:
        return {}  # corrupt/missing cache -> safe fallback is "sync everything this cycle"


def _save_diff_cache(path: str, cache: dict[str, tuple[str, datetime]]) -> None:
    if not cache:
        if os.path.exists(path):
            os.remove(path)
        return
    pl.DataFrame({
        "flight_key": list(cache.keys()),
        "content_hash": [v[0] for v in cache.values()],
        "last_synced_at": [v[1] for v in cache.values()],
    }).write_parquet(path)


def filter_changed(rows: list[dict], *, key_field: str, hash_cols: list[str],
                    cache_path: str, refresh_hours: float) -> tuple[list[dict], dict]:
    """Return (rows that still need writing, stats). A row is skipped only
    if its content hash is unchanged from last sync AND that last sync was
    within refresh_hours — never skipped just for being unchanged. Cache is
    rebuilt from `rows` every call, so flight_keys no longer present (fell
    out of the source window) are naturally pruned, not carried forever."""
    old_cache = _load_diff_cache(cache_path)
    now = datetime.now(timezone.utc)
    refresh = refresh_hours * 3600
    new_cache: dict[str, tuple[str, datetime]] = {}
    to_write: list[dict] = []

    for r in rows:
        key = r.get(key_field)
        if key is None:
            to_write.append(r)  # can't dedupe what we can't key — write it, don't drop it
            continue
        h = _content_hash(r, hash_cols)
        cached = old_cache.get(key)
        needs_write = True
        if cached is not None:
            cached_hash, cached_at = cached
            try:
                age_s = (now - cached_at).total_seconds()
            except TypeError:
                age_s = None  # malformed cache entry -> treat as stale, write it
            unchanged = h == cached_hash
            fresh_enough = age_s is not None and age_s < refresh
            needs_write = not (unchanged and fresh_enough)
        if needs_write:
            to_write.append(r)
            new_cache[key] = (h, now)
        else:
            new_cache[key] = cached  # carry forward untouched — preserves last_synced_at

    _save_diff_cache(cache_path, new_cache)
    stats = {"considered": len(rows), "written": len(to_write),
              "skipped": len(rows) - len(to_write)}
    return to_write, stats


def _dedupe(rows: list[dict], *, key_field: str, hash_cols: list[str],
            cache_path: str) -> tuple[list[dict], dict]:
    """SYNC_DEDUPE=0 escape hatch — write every row every cycle, old
    behavior, for debugging or if the diff cache is ever suspected wrong."""
    if not SYNC_DEDUPE:
        return rows, {"considered": len(rows), "written": len(rows), "skipped": 0}
    return filter_changed(rows, key_field=key_field, hash_cols=hash_cols,
                           cache_path=cache_path, refresh_hours=DYNAMODB_REFRESH_HOURS)


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
    if not rows and summary["gold_rows"] > 0:
        # gold (a file snapshot from the last successful pipeline cycle)
        # says there's real current data, but the live Postgres query came
        # back empty — almost certainly landed inside run.sh's
        # TRUNCATE-then-reload window for flight_instance, not a genuine
        # "no flights" state. Reporting `done` here would silently advance
        # synced_at past a sync that wrote zero state rows to the state
        # backend — a real data-loss bug, not a quiet no-op. Fail loud; the
        # next cycle (SYNC_EVERY later) retries against a stable table.
        # NOTE: this checks the RAW row count from Postgres, before dedup —
        # a fully-deduped cycle (every tracked flight genuinely unchanged)
        # is a legitimate 0-*write* outcome and must never trip this.
        raise RuntimeError(
            f"recent_flight_states() returned 0 rows but gold_rows="
            f"{summary['gold_rows']} (a real recent pipeline snapshot) — "
            f"likely raced run.sh's flight_instance TRUNCATE+reload. "
            f"Refusing to report this cycle as done.")

    state_to_write, state_stats = _dedupe(
        rows, key_field="flight_instance_id", hash_cols=_STATE_ATTRS,
        cache_path=_STATE_CACHE_PATH)
    summary["state_rows"] = _upsert_all(state.upsert_flight_state, state_to_write)
    summary["state_rows_considered"] = state_stats["considered"]
    summary["state_rows_skipped"] = state_stats["skipped"]

    if os.path.exists(predictions_path):
        preds = pl.read_parquet(predictions_path).to_dicts()
        preds_to_write, pred_stats = _dedupe(
            preds, key_field="flight_key", hash_cols=_PREDICTION_HASH_ATTRS,
            cache_path=_PREDICTION_CACHE_PATH)
        summary["prediction_rows"] = _upsert_all(state.upsert_prediction, preds_to_write)
        summary["prediction_rows_considered"] = pred_stats["considered"]
        summary["prediction_rows_skipped"] = pred_stats["skipped"]

    marker = pl.DataFrame({
        "synced_at": [datetime.now(timezone.utc).isoformat()],
        "gold_rows": [summary["gold_rows"]],
        "state_rows": [summary["state_rows"]],
        "prediction_rows": [summary["prediction_rows"]],
    })
    lake.write_parquet(marker, "meta/sync_status.parquet")
    return summary


def sync_eval_outputs(eval_dir: str = "out/eval") -> dict:
    """Sync the live-evaluation outputs (live_metrics_latest.json,
    reconciled_pairs.parquet) to the lake, so the deployed app's Model
    Performance page can read real data instead of "no data yet."

    Deliberately separate from sync_once()'s per-cycle flight-state sync —
    called from evaluate_live.py's report() step (every few hours, whenever
    the eval loop actually produces a fresh report), not from the
    SYNC_EVERY flight sync loop. These files are small (reconciled_pairs
    was ~200KB at 4,577 rows) and infrequent, so there's no tension with
    the write-volume-cost work in sync_once()/filter_changed() — this is a
    handful of writes every few hours, not tens of thousands every cycle.

    No-op (like sync_once) when both backends are local, and best-effort:
    a sync failure here is logged and swallowed by the caller, never
    allowed to break local report generation, which must keep working
    with zero cloud credentials.
    """
    if is_local_only():
        return {"synced": False, "reason": "local-only"}
    lake = lake_backend_from_env()
    synced: dict[str, int | bool] = {}

    metrics_path = os.path.join(eval_dir, "live_metrics_latest.json")
    if os.path.exists(metrics_path):
        with open(metrics_path, "rb") as f:
            lake.write_bytes(f.read(), "eval/live_metrics_latest.json")
        synced["live_metrics_latest.json"] = True

    pairs_path = os.path.join(eval_dir, "reconciled_pairs.parquet")
    if os.path.exists(pairs_path):
        df = pl.read_parquet(pairs_path)
        lake.write_parquet(df, "eval/reconciled_pairs.parquet")
        synced["reconciled_pairs.parquet_rows"] = df.height

    return {"synced": bool(synced), "files": synced}


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
    dedupe_note = "dedupe=on" if SYNC_DEDUPE else "dedupe=OFF"
    print(f"sync_cloud: done. gold_rows={summary['gold_rows']} "
          f"state_rows={summary['state_rows']}/{summary.get('state_rows_considered', summary['state_rows'])} "
          f"prediction_rows={summary['prediction_rows']}/{summary.get('prediction_rows_considered', summary['prediction_rows'])} "
          f"({dedupe_note}, written/considered)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
