"""Gold layer — the ML-ready feature/label table.

Turns validated silver flight instances (fused state) into a flat table you can
load straight into pandas/scikit-learn: engineered features + computed delay
labels, one row per flight, only rows where a label is computable.

HONEST NOTE ON LABELS (read this before modeling):
From live SWIM the available actuals are wheels-off (`actual_off`) and wheels-on
(`actual_on`), while the schedule fields are *gate* times. So the delay labels
here are PROXIES that include taxi time:
    dep_delay_min = actual_off  - scheduled_gate_departure   (includes taxi-out)
    arr_delay_min = actual_on   - scheduled_gate_arrival     (includes taxi-in)
For the BTS gate-to-gate delay definition, run this same transform over BTS
historical records (which carry true gate-out/gate-in). The column names and
feature logic are identical, so a model trained on BTS lines up with live
inference features. This is the definitional-parity gap, made explicit.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

# Column groups (documented so downstream knows what to feed a model).
ID_COLUMNS = ["flight_instance_id", "callsign", "flight_number"]
FEATURE_COLUMNS = [
    "carrier", "origin", "destination", "aircraft_type", "aircraft_category",
    "sched_dep_hour", "sched_dep_dow", "sched_dep_month", "is_weekend",
    "sched_block_min",
]
LABEL_COLUMNS = ["dep_delay_min", "arr_delay_min", "dep_delay_15", "arr_delay_15"]
ALL_COLUMNS = ID_COLUMNS + FEATURE_COLUMNS + LABEL_COLUMNS


def _to_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _minutes_between(later: datetime, earlier: datetime) -> float:
    return round((later - earlier).total_seconds() / 60.0, 1)


def flight_features(record: dict[str, Any]) -> Optional[dict[str, Any]]:
    """One canonical (silver) record -> one gold feature/label row, or None if
    no delay label can be computed (nothing to train on)."""
    sched_dep = _to_dt(record.get("scheduled_gate_departure"))
    sched_arr = _to_dt(record.get("scheduled_gate_arrival"))
    actual_off = _to_dt(record.get("actual_off"))
    actual_on = _to_dt(record.get("actual_on"))

    dep_delay = _minutes_between(actual_off, sched_dep) if actual_off and sched_dep else None
    arr_delay = _minutes_between(actual_on, sched_arr) if actual_on and sched_arr else None
    if dep_delay is None and arr_delay is None:
        return None  # no usable label

    row: dict[str, Any] = {
        # identifiers (not features)
        "flight_instance_id": record.get("flight_instance_id"),
        "callsign": record.get("callsign"),
        "flight_number": record.get("flight_number"),
        # categorical features
        "carrier": record.get("carrier_icao"),
        "origin": record.get("origin"),
        "destination": record.get("destination"),
        "aircraft_type": record.get("aircraft_type"),
        "aircraft_category": record.get("aircraft_category"),
        # temporal features (from scheduled gate departure)
        "sched_dep_hour": sched_dep.hour if sched_dep else None,
        "sched_dep_dow": sched_dep.weekday() if sched_dep else None,   # 0=Mon
        "sched_dep_month": sched_dep.month if sched_dep else None,
        "is_weekend": int(sched_dep.weekday() >= 5) if sched_dep else None,
        # numeric feature
        "sched_block_min": (
            _minutes_between(sched_arr, sched_dep) if sched_arr and sched_dep else None
        ),
        # labels
        "dep_delay_min": dep_delay,
        "arr_delay_min": arr_delay,
        "dep_delay_15": int(dep_delay >= 15) if dep_delay is not None else None,
        "arr_delay_15": int(arr_delay >= 15) if arr_delay is not None else None,
    }
    return row


def build_feature_table(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """All canonical records -> the gold rows that have a computable label."""
    out = []
    for rec in records:
        row = flight_features(rec)
        if row is not None:
            out.append(row)
    return out
