"""sync_cloud.py's write-volume dedup (filter_changed) — the fix for
DynamoDB write cost: only re-upsert a flight's state/prediction when it
actually changed, but never let an unchanged-but-still-current flight's TTL
lapse by skipping it forever.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl

from aeroflux_ml import sync_cloud as sc


def _now():
    return datetime.now(timezone.utc)


def test_unchanged_row_is_skipped(tmp_path, monkeypatch):
    cache = str(tmp_path / "cache.parquet")
    row = {"flight_key": "A1", "origin": "KATL", "flight_status": "PLANNED"}

    to_write, stats = sc.filter_changed(
        [row], key_field="flight_key", hash_cols=["origin", "flight_status"],
        cache_path=cache, refresh_hours=12)
    assert stats == {"considered": 1, "written": 1, "skipped": 0}, "first sight is always written"

    to_write, stats = sc.filter_changed(
        [row], key_field="flight_key", hash_cols=["origin", "flight_status"],
        cache_path=cache, refresh_hours=12)
    assert to_write == []
    assert stats == {"considered": 1, "written": 0, "skipped": 1}, "unchanged, recently synced -> skipped"


def test_changed_row_is_written(tmp_path):
    cache = str(tmp_path / "cache.parquet")
    row1 = {"flight_key": "A1", "origin": "KATL", "flight_status": "PLANNED"}
    row2 = {"flight_key": "A1", "origin": "KATL", "flight_status": "ACTIVE"}  # status changed

    sc.filter_changed([row1], key_field="flight_key", hash_cols=["origin", "flight_status"],
                       cache_path=cache, refresh_hours=12)
    to_write, stats = sc.filter_changed(
        [row2], key_field="flight_key", hash_cols=["origin", "flight_status"],
        cache_path=cache, refresh_hours=12)
    assert to_write == [row2]
    assert stats["written"] == 1


def test_unchanged_but_stale_is_refreshed(tmp_path):
    """The TTL-safety guarantee: unchanged alone is never enough to skip —
    it must also have been synced within refresh_hours."""
    cache = str(tmp_path / "cache.parquet")
    row = {"flight_key": "A1", "origin": "KATL", "flight_status": "PLANNED"}

    # Seed the cache with an entry that's already 13h old (older than a 12h
    # refresh floor) by writing it directly, bypassing the "now" timestamp
    # filter_changed would otherwise use.
    old_ts = _now() - timedelta(hours=13)
    h = sc._content_hash(row, ["origin", "flight_status"])
    pl.DataFrame({"flight_key": ["A1"], "content_hash": [h],
                  "last_synced_at": [old_ts]}).write_parquet(cache)

    to_write, stats = sc.filter_changed(
        [row], key_field="flight_key", hash_cols=["origin", "flight_status"],
        cache_path=cache, refresh_hours=12)
    assert to_write == [row], "unchanged but past the refresh floor must still be written"
    assert stats["written"] == 1


def test_departed_flight_key_is_pruned_from_cache(tmp_path):
    cache = str(tmp_path / "cache.parquet")
    row_a = {"flight_key": "A1", "origin": "KATL"}
    row_b = {"flight_key": "B2", "origin": "KDFW"}

    sc.filter_changed([row_a, row_b], key_field="flight_key", hash_cols=["origin"],
                       cache_path=cache, refresh_hours=12)
    # Next cycle: A1 fell out of the tracked window (e.g. past its own
    # retention), only B2 remains.
    to_write, stats = sc.filter_changed(
        [row_b], key_field="flight_key", hash_cols=["origin"],
        cache_path=cache, refresh_hours=12)
    assert to_write == [], "B2 unchanged -> skipped"
    cache_after = sc._load_diff_cache(cache)
    assert set(cache_after.keys()) == {"B2"}, "A1 must not linger in the cache forever"


def test_row_missing_key_field_is_always_written(tmp_path):
    cache = str(tmp_path / "cache.parquet")
    row = {"origin": "KATL"}  # no flight_key
    to_write, stats = sc.filter_changed(
        [row], key_field="flight_key", hash_cols=["origin"],
        cache_path=cache, refresh_hours=12)
    assert to_write == [row]


def test_sync_dedupe_off_writes_everything(tmp_path, monkeypatch):
    monkeypatch.setattr(sc, "SYNC_DEDUPE", False)
    cache = str(tmp_path / "cache.parquet")
    row = {"flight_key": "A1", "origin": "KATL"}
    sc._dedupe([row], key_field="flight_key", hash_cols=["origin"], cache_path=cache)
    to_write, stats = sc._dedupe([row], key_field="flight_key", hash_cols=["origin"], cache_path=cache)
    assert to_write == [row], "SYNC_DEDUPE=0 must bypass the cache entirely, every cycle"
    assert stats["skipped"] == 0
