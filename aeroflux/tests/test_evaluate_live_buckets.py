"""evaluate_live.py's multi-lag-bucket reconciliation — the fix for the
original "keep only the latest prediction" bug, which silently discarded
every genuine multi-hour-lag prediction for a flight tracked repeatedly
before landing.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest

from aeroflux_ml import evaluate_live as el


@pytest.fixture(autouse=True)
def isolated_paths(tmp_path, monkeypatch):
    """Redirect every output path to a tmp dir so tests never touch the
    real out/eval/ — and start each test with a clean slate."""
    monkeypatch.setattr(el, "OUT_DIR", tmp_path)
    monkeypatch.setattr(el, "PAIRS_PATH", tmp_path / "reconciled_pairs.parquet")
    monkeypatch.setattr(el, "STATE_PATH", tmp_path / "reconcile_state.json")
    monkeypatch.setattr(el, "PENDING_PATH", tmp_path / "_pending_predictions.parquet")
    yield tmp_path


def _local_filename_ts(dt_utc: datetime) -> str:
    """Inverse of _local_to_utc: what local-time filename stamp would
    produce this UTC instant, on THIS machine's current offset — keeps the
    test independent of which timezone it happens to run in."""
    offset = datetime.now().astimezone().utcoffset() or timedelta(0)
    local = dt_utc + offset
    return local.strftime("%Y%m%d_%H%M")


def _write_pred_snapshot(pred_dir, dt_utc: datetime, rows: list[dict]):
    pred_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "flight_key": [r["flight_key"] for r in rows],
        "delay_probability": [r.get("delay_probability", 0.5) for r in rows],
        "predicted_delayed": [r.get("predicted_delayed", 0) for r in rows],
        "model_version": [r.get("model_version", "test") for r in rows],
        "scored_at": [r["scored_at"] for r in rows],
    })
    df.write_parquet(pred_dir / f"predictions_{_local_filename_ts(dt_utc)}.parquet")


def _write_gold_snapshot(gold_dir, dt_utc: datetime, rows: list[dict]):
    gold_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame({
        "flight_key": [r["flight_key"] for r in rows],
        "arr_delay_min": [r.get("arr_delay_min") for r in rows],
    })
    df.write_parquet(gold_dir / f"gold_{_local_filename_ts(dt_utc)}.parquet")


def test_flight_predicted_repeatedly_yields_multiple_bucket_pairs(tmp_path):
    """The core fix: a flight predicted at 30h, 10h, 3h, and 10min before
    landing must produce up to 4 pairs (one per bucket it touches), not 1."""
    pred_dir = tmp_path / "predictions"
    gold_dir = tmp_path / "gold_live"
    landing = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    for hours_before, prob in [(30, 0.2), (10, 0.3), (3, 0.4), (10 / 60, 0.9)]:
        t = landing - timedelta(hours=hours_before)
        _write_pred_snapshot(pred_dir, t, [
            {"flight_key": "FL1", "scored_at": t.replace(tzinfo=None), "delay_probability": prob}])

    _write_gold_snapshot(gold_dir, landing, [{"flight_key": "FL1", "arr_delay_min": 20}])

    summary = el.reconcile(pred_dir=pred_dir, gold_dir=gold_dir, quiet=True)
    assert summary["total_pairs"] == 4, "one pair per lag bucket the flight actually touched"

    pairs = pl.read_parquet(el.PAIRS_PATH)
    buckets = set(pairs["lag_bucket"].to_list())
    assert buckets == {"24h+", "6-24h", "2-6h", "<30min"}, buckets
    # every pair must be the SAME flight, all correctly marked delayed
    assert set(pairs["flight_key"].to_list()) == {"FL1"}
    assert pairs["actual_delayed"].to_list() == [1, 1, 1, 1]


def test_within_bucket_keeps_largest_lag_not_arbitrary(tmp_path):
    """Two predictions both land in the 6-24h bucket (8h and 20h before
    outcome) — must keep the 20h one (max lag = most lead time), not
    whichever happened to be read last."""
    pred_dir = tmp_path / "predictions"
    gold_dir = tmp_path / "gold_live"
    landing = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    t8 = landing - timedelta(hours=8)
    t20 = landing - timedelta(hours=20)
    _write_pred_snapshot(pred_dir, t8, [{"flight_key": "FL2", "scored_at": t8.replace(tzinfo=None), "delay_probability": 0.11}])
    _write_pred_snapshot(pred_dir, t20, [{"flight_key": "FL2", "scored_at": t20.replace(tzinfo=None), "delay_probability": 0.77}])
    _write_gold_snapshot(gold_dir, landing, [{"flight_key": "FL2", "arr_delay_min": 5}])

    el.reconcile(pred_dir=pred_dir, gold_dir=gold_dir, quiet=True)
    pairs = pl.read_parquet(el.PAIRS_PATH)
    assert pairs.height == 1
    assert pairs["lag_bucket"][0] == "6-24h"
    assert pairs["delay_probability"][0] == pytest.approx(0.77), "must keep the 20h-lag (max lag) prediction"


def test_prediction_after_outcome_is_excluded_no_hindsight(tmp_path):
    pred_dir = tmp_path / "predictions"
    gold_dir = tmp_path / "gold_live"
    landing = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    after = landing + timedelta(hours=1)
    _write_pred_snapshot(pred_dir, after, [{"flight_key": "FL3", "scored_at": after.replace(tzinfo=None)}])
    _write_gold_snapshot(gold_dir, landing, [{"flight_key": "FL3", "arr_delay_min": 0}])

    summary = el.reconcile(pred_dir=pred_dir, gold_dir=gold_dir, quiet=True)
    assert summary["total_pairs"] == 0, "a prediction scored AFTER the outcome must never be paired"


def test_only_completed_flights_count(tmp_path):
    pred_dir = tmp_path / "predictions"
    gold_dir = tmp_path / "gold_live"
    t = datetime(2026, 8, 10, 10, 0, tzinfo=timezone.utc)
    g = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)

    _write_pred_snapshot(pred_dir, t, [{"flight_key": "FL4", "scored_at": t.replace(tzinfo=None)}])
    _write_gold_snapshot(gold_dir, g, [{"flight_key": "FL4", "arr_delay_min": None}])  # still in flight

    summary = el.reconcile(pred_dir=pred_dir, gold_dir=gold_dir, quiet=True)
    assert summary["total_pairs"] == 0


def test_rerun_with_no_new_snapshots_is_a_no_op(tmp_path):
    pred_dir = tmp_path / "predictions"
    gold_dir = tmp_path / "gold_live"
    landing = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    t = landing - timedelta(hours=3)
    _write_pred_snapshot(pred_dir, t, [{"flight_key": "FL5", "scored_at": t.replace(tzinfo=None)}])
    _write_gold_snapshot(gold_dir, landing, [{"flight_key": "FL5", "arr_delay_min": 0}])

    first = el.reconcile(pred_dir=pred_dir, gold_dir=gold_dir, quiet=True)
    second = el.reconcile(pred_dir=pred_dir, gold_dir=gold_dir, quiet=True)
    assert first["new_pairs"] == 1
    assert second["new_pairs"] == 0
    assert second["total_pairs"] == 1


def test_compute_metrics_per_bucket_shape():
    df = pl.DataFrame({
        "flight_key": ["A", "B", "C"],
        "lag_bucket": ["24h+", "24h+", "<30min"],
        "actual_delayed": [0, 1, 0],
        "delay_probability": [0.2, 0.8, 0.3],
    })
    out = el.compute_metrics(df)
    assert set(out.keys()) == {"overall", "buckets"}
    assert out["overall"]["n"] == 3
    assert out["buckets"]["24h+"]["n"] == 2
    assert out["buckets"]["<30min"]["n"] == 1
    assert out["buckets"]["6-24h"]["n"] == 0  # empty bucket handled, not an error
    assert out["buckets"]["6-24h"]["roc_auc"] is None
