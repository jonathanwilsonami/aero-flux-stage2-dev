"""Tests for the training pipeline: config loading, validation, execution."""
import polars as pl
import pytest

from aeroflux_ml.training.config import load_config, DEFAULTS
from aeroflux_ml.training.validation import validate_gold, ValidationError
from aeroflux_ml.training import run


def _tiny_gold(n=400):
    import random; random.seed(0)
    rows = []
    for i in range(n):
        hr = random.randint(6, 22); delayed = 1 if (hr >= 17 or random.random() < 0.3) else 0
        rows.append({
            "flight_key": f"2015-01-{1+i%27:02d}_AA{1000+i}",
            "sched_dep_hour": hr, "sched_dep_dow": 1+i%7, "sched_dep_month": 1,
            "is_weekend": int(i%7 in (5,6)), "sched_block_min": random.randint(60,240),
            "prev_leg_arr_delay_min": None, "turnaround_buffer_min": None,
            "legs_into_day": None, "inbound_resolved": 0,
            "origin_dep_demand": random.randint(1,50), "origin_recent_dep_delay": None,
            "dest_arr_demand": random.randint(1,50), "dest_recent_arr_delay": None,
            "origin_wx_wind_kt": float(random.randint(0,25)), "origin_wx_ifr": 0,
            "dest_wx_wind_kt": float(random.randint(0,25)), "dest_wx_ifr": 0,
            "origin_wx_vis_mi": None, "origin_wx_temp_c": None, "origin_wx_ceiling_ft": None,
            "dest_wx_vis_mi": None, "dest_wx_temp_c": None, "dest_wx_ceiling_ft": None,
            "arr_delay_min": 20 if delayed else -3, "label_delayed": delayed,
        })
    return pl.DataFrame(rows)


def test_config_defaults_and_merge(tmp_path):
    cfg = load_config(None)                      # defaults only
    assert cfg["run"]["seed"] == 42 and cfg["models"]
    y = tmp_path / "c.yaml"
    y.write_text("run:\n  name: custom\nsplit:\n  test_size: 0.3\n")
    cfg2 = load_config(y)
    assert cfg2["run"]["name"] == "custom"       # override
    assert cfg2["split"]["test_size"] == 0.3
    assert cfg2["run"]["seed"] == 42             # default preserved


def test_config_validation_rejects_bad():
    from aeroflux_ml.training.config import _validate
    bad = {**DEFAULTS, "split": {"strategy": "nope", "test_size": 0.2}}
    with pytest.raises(ValueError):
        _validate(bad)


def test_feature_validation(tmp_path):
    cfg = load_config(None)
    good = _tiny_gold(300)
    rep = validate_gold(good, cfg)
    assert rep["labelled"] == 300 and rep["rows"] == 300
    with pytest.raises(ValidationError):           # missing target
        validate_gold(good.drop("label_delayed"), cfg)
    with pytest.raises(ValidationError):           # too few rows
        validate_gold(good.head(10), cfg)


def test_pipeline_runs_and_saves(tmp_path):
    gold = _tiny_gold(600)
    gp = tmp_path / "gold.parquet"; gold.write_parquet(gp)
    cfg = load_config(None)
    cfg["data"]["gold_path"] = str(gp)
    cfg["run"]["output_dir"] = str(tmp_path / "runs")
    cfg["run"]["name"] = "test"
    cfg["outputs"]["save_plots"] = False          # skip matplotlib in unit test
    summary = run(cfg)
    assert summary["ranking"] and summary["ranking"][0]["rank"] == 1
    from pathlib import Path
    assert (Path(summary["run_dir"]) / "run.json").exists()
    assert (Path(summary["run_dir"]) / "models" / "xgb_full.joblib").exists()
