"""The flight-instance schema contract.

This is the validation gate for the canonical (silver) layer: every fused,
enriched record must satisfy this model before it is allowed to land. It turns
"reliable data" from a hope into something enforced on write.

Design choices:
  - flight_instance_id is the only REQUIRED field (it is the primary key; a
    record without one is meaningless). Everything else is nullable, because a
    flight legitimately may not have observed every field yet.
  - Timestamps are validated as parseable ISO-8601 but kept as STRINGS, so the
    downstream JSONL/CSV/Postgres flow is unchanged (Postgres casts on insert).
  - Enumerated fields (resolution_status, tail_source) and coordinate ranges are
    checked, so a malformed record is caught here, not three layers downstream.

Requires pydantic (v2). Install with:  pip install "aeroflux-parser[validate]"
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, field_validator

from .enrich import ResolutionStatus

SCHEMA_VERSION = "1.0"

_RESOLUTION_STATUSES = {
    ResolutionStatus.AIRLINE, ResolutionStatus.GA_TAIL,
    ResolutionStatus.UNKNOWN_AIRLINE, ResolutionStatus.UNPARSEABLE,
}
_TAIL_SOURCES = {"swim_ga", "adsb", "adsb_hex_only", "none"}

_TIMESTAMP_FIELDS = (
    "scheduled_gate_departure", "scheduled_gate_arrival", "estimated_arrival",
    "actual_off", "actual_on", "last_position_time",
)


def _parse_iso(value: str) -> datetime:
    # Accept a trailing Z across Python versions.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class FlightInstance(BaseModel):
    """One fused flight. Mirrors enrich.DATASET_FIELDS + schema_version."""

    model_config = ConfigDict(extra="forbid")  # unexpected fields are an error

    schema_version: str = SCHEMA_VERSION

    # identity
    flight_instance_id: str            # required
    gufi: Optional[str] = None
    flight_ref: Optional[str] = None
    callsign: Optional[str] = None
    flight_number: Optional[str] = None
    carrier_icao: Optional[str] = None
    carrier_iata: Optional[str] = None
    carrier_name: Optional[str] = None
    resolution_status: Optional[str] = None
    # airframe
    tail_number: Optional[str] = None
    tail_source: Optional[str] = None
    hex: Optional[str] = None
    aircraft_type: Optional[str] = None
    aircraft_category: Optional[str] = None
    # route
    origin: Optional[str] = None
    destination: Optional[str] = None
    # times
    scheduled_gate_departure: Optional[str] = None
    scheduled_gate_arrival: Optional[str] = None
    estimated_arrival: Optional[str] = None
    actual_off: Optional[str] = None
    actual_on: Optional[str] = None
    # status
    flight_status: Optional[str] = None
    # live state
    last_latitude: Optional[float] = None
    last_longitude: Optional[float] = None
    last_altitude_ft: Optional[int] = None
    last_ground_speed: Optional[int] = None
    last_position_time: Optional[str] = None

    @field_validator("flight_instance_id")
    @classmethod
    def _id_nonempty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("flight_instance_id must be non-empty")
        return v

    @field_validator("resolution_status")
    @classmethod
    def _known_status(cls, v):
        if v is not None and v not in _RESOLUTION_STATUSES:
            raise ValueError(f"unknown resolution_status: {v}")
        return v

    @field_validator("tail_source")
    @classmethod
    def _known_tail_source(cls, v):
        if v is not None and v not in _TAIL_SOURCES:
            raise ValueError(f"unknown tail_source: {v}")
        return v

    @field_validator("last_latitude")
    @classmethod
    def _lat_range(cls, v):
        if v is not None and not (-90.0 <= v <= 90.0):
            raise ValueError(f"latitude out of range: {v}")
        return v

    @field_validator("last_longitude")
    @classmethod
    def _lon_range(cls, v):
        if v is not None and not (-180.0 <= v <= 180.0):
            raise ValueError(f"longitude out of range: {v}")
        return v

    @field_validator(*_TIMESTAMP_FIELDS)
    @classmethod
    def _iso_timestamp(cls, v):
        if v in (None, ""):
            return v
        try:
            _parse_iso(v)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"not an ISO-8601 timestamp: {v!r}") from exc
        return v


def validate_record(record: dict[str, Any]) -> FlightInstance:
    """Validate one record; raises pydantic.ValidationError on failure."""
    return FlightInstance(**record)


def validate_batch(
    records: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split records into (valid, invalid). Valid rows come back as dicts
    stamped with schema_version; invalid rows carry a `_errors` list."""
    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for rec in records:
        try:
            model = FlightInstance(**rec)
            valid.append(model.model_dump())
        except Exception as exc:  # pydantic.ValidationError (+ any coercion error)
            bad = dict(rec)
            bad["_errors"] = _format_errors(exc)
            invalid.append(bad)
    return valid, invalid


def _format_errors(exc: Exception) -> list[str]:
    errors = getattr(exc, "errors", None)
    if callable(errors):
        return [f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()]
    return [str(exc)]
