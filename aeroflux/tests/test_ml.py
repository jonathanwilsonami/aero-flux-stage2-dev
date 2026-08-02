"""Tests for aeroflux_ml. The parity tests are the load-bearing ones."""

import polars as pl
import pytest

from aeroflux_ml import (
    from_bts, from_silver, FeatureEngineer, FeatureConfig, ModelConfig,
    InferenceEngine, InMemoryStateRepository, CANONICAL_COLUMNS,
)


# --- fixtures ---------------------------------------------------------------

def silver_frame():
    # a live canonical (silver) frame: two legs of the same airframe (hex)
    return pl.DataFrame({
        "flight_instance_id": ["L1", "L2"],
        "hex": ["a1b2c3", "a1b2c3"],
        "tail_number": [None, None],
        "carrier_icao": ["AAL", "AAL"],
        "origin": ["KBOS", "KATL"],
        "destination": ["KATL", "KMIA"],
        "scheduled_gate_departure": ["2026-07-04T12:00:00Z", "2026-07-04T15:00:00Z"],
        "scheduled_gate_arrival": ["2026-07-04T14:30:00Z", "2026-07-04T17:00:00Z"],
        "actual_off": ["2026-07-04T12:52:00Z", None],   # leg 1 left 52 min late
        "actual_on": ["2026-07-04T15:19:00Z", None],    # leg 1 arrived 49 min late
    })


def bts_frame():
    # the SAME two legs in BTS shape (IATA codes, HHMM local times, TAIL_NUM)
    return pl.DataFrame({
        "FL_DATE": ["2026-07-04", "2026-07-04"],
        "OP_UNIQUE_CARRIER": ["AAL", "AAL"],
        "OP_CARRIER_FL_NUM": [471, 472],
        "TAIL_NUM": ["N471AA", "N471AA"],
        "ORIGIN": ["BOS", "ATL"],
        "DEST": ["ATL", "MIA"],
        "CRS_DEP_TIME": [1200, 1500],
        "CRS_ARR_TIME": [1430, 1700],
        "DEP_TIME": [1252, None],
        "ARR_TIME": [1519, None],
    })


# --- parity (the point of the whole design) --------------------------------

def test_adapters_produce_the_canonical_schema():
    for canon in (from_silver(silver_frame()), from_bts(bts_frame())):
        assert canon.columns == CANONICAL_COLUMNS


def test_bts_and_live_yield_identical_feature_columns():
    cfg = FeatureConfig()  # defaults: flight+rotation+airport_state
    eng = FeatureEngineer(cfg)
    live = eng.build_matrix(from_silver(silver_frame()))
    hist = eng.build_matrix(from_bts(bts_frame()))
    # IDENTICAL feature column sets -> a BTS-trained model consumes live features
    assert live.columns == hist.columns
    assert eng.feature_columns()  # non-empty


def test_bts_carrier_airport_normalized_to_icao():
    canon = from_bts(bts_frame())
    assert canon["origin"].to_list() == ["KBOS", "KATL"]   # IATA -> ICAO (K+)
    assert canon["destination"].to_list() == ["KATL", "KMIA"]


# --- channel correctness ----------------------------------------------------

def test_rotation_propagates_previous_leg_delay():
    eng = FeatureEngineer(FeatureConfig())
    df = eng.build(from_silver(silver_frame()))
    row2 = df.filter(pl.col("flight_key") == "L2").row(0, named=True)
    # leg 1 arrived 49 min late -> becomes leg 2's inbound delay
    assert row2["prev_leg_arr_delay_min"] == 49
    assert row2["legs_into_day"] == 1
    assert row2["inbound_resolved"] == 1
    # leg 1 is first of the day -> no inbound
    row1 = df.filter(pl.col("flight_key") == "L1").row(0, named=True)
    assert row1["prev_leg_arr_delay_min"] is None


def test_flight_channel_temporal_features():
    eng = FeatureEngineer(FeatureConfig(channels={"flight": True}))
    df = eng.build(from_silver(silver_frame()))
    r = df.filter(pl.col("flight_key") == "L1").row(0, named=True)
    assert r["sched_dep_hour"] == 12
    assert r["sched_block_min"] == 150   # 12:00 -> 14:30


def test_config_toggles_channels():
    eng = FeatureEngineer(FeatureConfig(channels={"flight": True, "rotation": False,
                                                  "airport_state": False}))
    assert "prev_leg_arr_delay_min" not in eng.feature_columns()
    assert "sched_dep_hour" in eng.feature_columns()


# --- inference + versioning -------------------------------------------------

def _train_tiny_model(tmp_path, feature_cols):
    import numpy as np, xgboost as xgb
    rng = np.random.default_rng(0)
    X = rng.normal(size=(200, len(feature_cols)))
    y = (X[:, 0] + rng.normal(scale=0.3, size=200) > 0).astype(int)
    d = xgb.DMatrix(X, label=y, feature_names=feature_cols)
    booster = xgb.train({"objective": "binary:logistic", "max_depth": 3}, d, 10)
    path = str(tmp_path / "m.json")
    booster.save_model(path)
    return path


def test_inference_scores_and_versions(tmp_path):
    eng = FeatureEngineer(FeatureConfig())
    feats = eng.build_matrix(from_bts(bts_frame()))
    fcols = eng.feature_columns()
    model_path = _train_tiny_model(tmp_path, fcols)

    engine = InferenceEngine(ModelConfig(path=model_path, version="v1"), feature_version="1.0")
    preds = engine.predict(feats)

    assert set(["flight_key", "delay_probability", "predicted_delayed",
                "model_version", "feature_version", "prediction_key"]).issubset(preds.columns)
    assert preds["delay_probability"].min() >= 0.0 and preds["delay_probability"].max() <= 1.0
    # deterministic dedup key
    k = preds["prediction_key"].to_list()[0]
    assert k.endswith(":1.0:v1")


def test_inference_tolerates_missing_channel(tmp_path):
    # train on flight+rotation+airport_state; serve a frame missing weather cols
    eng = FeatureEngineer(FeatureConfig())
    fcols = eng.feature_columns() + ["origin_wx_wind_kt"]  # model expects a wx col
    model_path = _train_tiny_model(tmp_path, fcols)
    engine = InferenceEngine(ModelConfig(path=model_path, version="v1"))
    feats = eng.build_matrix(from_silver(silver_frame()))   # has no weather col
    preds = engine.predict(feats)   # must not raise; missing feature -> NaN
    assert len(preds) == 2


# --- state repository dedup -------------------------------------------------

def test_prediction_upsert_is_idempotent():
    repo = InMemoryStateRepository()
    p = {"prediction_key": "L1:1.0:v1", "flight_key": "L1", "delay_probability": 0.7}
    repo.upsert_prediction(p)
    repo.upsert_prediction({**p, "delay_probability": 0.8})  # same key, rescored
    assert len(repo.predictions) == 1                         # no duplicate
    assert repo.predictions["L1:1.0:v1"]["delay_probability"] == 0.8


def test_rotation_nulls_out_unresolved_airframes():
    # all airframe keys null (e.g. live data with no ADS-B hex) must NOT form a
    # fake rotation -- every rotation feature null, inbound_resolved 0
    frame = silver_frame().with_columns(pl.lit(None).alias("hex"))
    eng = FeatureEngineer(FeatureConfig())
    df = eng.build(from_silver(frame, airframe_key="hex"))
    assert df["inbound_resolved"].sum() == 0
    assert df["prev_leg_arr_delay_min"].null_count() == len(df)
    assert df["turnaround_buffer_min"].null_count() == len(df)   # no garbage turnaround


def test_airport_state_handles_null_times_and_counts_correctly():
    # mix: two KATL departures 20 min apart (should see demand build), plus a
    # row with a NULL scheduled time (must not crash the rolling window)
    frame = pl.DataFrame({
        "flight_instance_id": ["A", "B", "C"],
        "hex": [None, None, None], "tail_number": [None, None, None],
        "carrier_icao": ["DAL", "DAL", "DAL"],
        "origin": ["KATL", "KATL", "KATL"], "destination": ["KMCO", "KMCO", "KMCO"],
        "scheduled_gate_departure": ["2026-07-04T12:00:00Z", "2026-07-04T12:20:00Z", None],
        "scheduled_gate_arrival": ["2026-07-04T14:00:00Z", "2026-07-04T14:20:00Z", None],
        "actual_off": [None, None, None], "actual_on": [None, None, None],
    })
    eng = FeatureEngineer(FeatureConfig(channels={"airport_state": True}, window_minutes=60))
    df = eng.build(from_silver(frame, airframe_key="hex"))
    by = {r["flight_key"]: r for r in df.to_dicts()}
    assert by["A"]["origin_dep_demand"] == 1          # first in window
    assert by["B"]["origin_dep_demand"] == 2          # A + B within 60 min
    assert by["C"]["origin_dep_demand"] is None       # null time -> excluded, null feature


# --- weather channel: temporal + geographic as-of join ---------------------

from aeroflux_ml import WEATHER_OBS_COLUMNS

def _obs():
    # KATL obs at 11:00 and 11:45; KMIA at 11:30 (stale) and 13:00 (fresh)
    return pl.DataFrame({
        "station":  ["KATL", "KATL", "KMIA", "KMIA"],
        "obs_time": ["2026-07-04T11:00:00", "2026-07-04T11:45:00",
                     "2026-07-04T11:30:00", "2026-07-04T13:00:00"],
        "wind_kt":  [8.0, 15.0, 5.0, 12.0],
        "vis_mi":   [10.0, 2.0, 10.0, 9.0],
        "ifr":      [0, 1, 0, 0],
    }).with_columns(pl.col("obs_time").str.to_datetime())

def _wx_flights():
    return pl.DataFrame({
        "flight_instance_id": ["W1", "W2"],
        "hex": [None, None], "tail_number": [None, None],
        "carrier_icao": ["DAL", "DAL"],
        "origin": ["KATL", "KATL"], "destination": ["KMIA", "KMIA"],
        # W1 departs 11:50 (matches 11:45 KATL obs); W2 at 10:30 (before any obs)
        "scheduled_gate_departure": ["2026-07-04T11:50:00Z", "2026-07-04T10:30:00Z"],
        "scheduled_gate_arrival":  ["2026-07-04T13:40:00Z", "2026-07-04T12:20:00Z"],
        "actual_off": [None, None], "actual_on": [None, None],
    })


def test_weather_asof_matches_latest_prior_observation():
    eng = FeatureEngineer(FeatureConfig(channels={"weather": True}))
    df = eng.build(from_silver(_wx_flights(), airframe_key="hex"),
                   context={"weather_obs": _obs()})
    by = {r["flight_key"]: r for r in df.to_dicts()}
    # W1 @ 11:50 -> latest prior KATL obs is 11:45 (wind 15, IFR)
    assert by["W1"]["origin_wx_wind_kt"] == 15.0
    assert by["W1"]["origin_wx_ifr"] == 1
    # W1 arrives 13:40 -> latest prior KMIA obs is 13:00 (wind 12), within tolerance
    assert by["W1"]["dest_wx_wind_kt"] == 12.0
    # W2 departs before any KATL obs -> null (no leakage from future obs)
    assert by["W2"]["origin_wx_wind_kt"] is None


def test_weather_null_without_obs_keeps_schema_stable():
    eng = FeatureEngineer(FeatureConfig(channels={"weather": True}))
    df = eng.build(from_silver(_wx_flights(), airframe_key="hex"))  # no obs supplied
    assert "origin_wx_wind_kt" in df.columns
    assert df["origin_wx_wind_kt"].null_count() == len(df)


def test_weather_channel_column_manifest():
    from aeroflux_ml import CHANNEL_OUTPUTS
    assert set(CHANNEL_OUTPUTS["weather"]) == {
        "origin_wx_wind_kt", "origin_wx_vis_mi", "origin_wx_ifr",
        "dest_wx_wind_kt", "dest_wx_vis_mi", "dest_wx_ifr"}


# --- BTS local-time -> UTC conversion via airport tz ------------------------

def test_from_bts_converts_local_times_to_utc():
    from aeroflux_ml.schema import from_bts
    # KDFW is America/Chicago; late July -> CDT = UTC-5. 08:00 local -> 13:00 UTC.
    bts = pl.DataFrame({
        "FL_DATE": ["2026-07-30"], "OP_UNIQUE_CARRIER": ["AA"],
        "OP_CARRIER_FL_NUM": ["100"], "TAIL_NUM": ["N826AA"],
        "ORIGIN": ["DFW"], "DEST": ["LGA"],
        "CRS_DEP_TIME": [800], "CRS_ARR_TIME": [1230],
        "DEP_TIME": [805], "ARR_TIME": [1235],
    })
    tz = {"DFW": "America/Chicago", "LGA": "America/New_York",
          "KDFW": "America/Chicago", "KLGA": "America/New_York"}
    out = from_bts(bts, airport_tz=tz)
    dep = out["sched_dep"][0]
    assert dep.hour == 13 and dep.minute == 0        # 08:00 CDT -> 13:00 UTC
    # arrival is New York (EDT = UTC-4): 12:30 local -> 16:30 UTC
    arr = out["sched_arr"][0]
    assert arr.hour == 16 and arr.minute == 30


def test_from_bts_without_tz_stays_naive_local():
    from aeroflux_ml.schema import from_bts
    bts = pl.DataFrame({
        "FL_DATE": ["2026-07-30"], "OP_UNIQUE_CARRIER": ["AA"],
        "OP_CARRIER_FL_NUM": ["100"], "TAIL_NUM": ["N1"],
        "ORIGIN": ["DFW"], "DEST": ["LGA"],
        "CRS_DEP_TIME": [800], "CRS_ARR_TIME": [1230],
        "DEP_TIME": [800], "ARR_TIME": [1230],
    })
    out = from_bts(bts)                    # no tz map -> unchanged local wall clock
    assert out["sched_dep"][0].hour == 8