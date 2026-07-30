"""Canonical flight-instance projection.

Maps a ParsedMessage onto the stable, business-level schema you actually want
downstream. This is where the *opinionated* choices live (which of several TFMS
times counts as "scheduled gate departure"), kept separate from the faithful
per-message extraction in normalizers.py so you can change the mapping without
re-touching parsing.

Important scope note: a single message rarely fills every field. A FlightModify
gives you status + gate times; trackInformation gives the live ETA;
departureInformation gives actual-off. The *complete* canonical record is the
result of merging many messages that share a flight identity (GUFI) over time --
that fusion layer is the next increment. `to_canonical` here gives the
best-effort projection from ONE message.
"""

from __future__ import annotations

import re
from typing import Any

from .result import ParsedMessage

# US N-number: 'N' + digit, then up to 4 alphanumerics. Good enough to tell a
# registration-as-callsign (GA) from an airline callsign like AAL2033.
_US_REGISTRATION = re.compile(r"^N[1-9][0-9]{0,4}[A-Z]{0,2}$")


def looks_like_registration(acid: str | None) -> bool:
    return bool(acid and _US_REGISTRATION.match(acid))


def _find_first(obj: Any, key: str) -> Any:
    """Recursively find the first value for `key` anywhere in a nested dict/list."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            found = _find_first(v, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_first(item, key)
            if found is not None:
                return found
    return None


def _aircraft_category(body: dict[str, Any]) -> str | None:
    qac = _find_first(body, "qualifiedAircraftId")
    if isinstance(qac, dict):
        return qac.get("@userCategory")
    return None


def _aircraft_type(body: dict[str, Any], normalized: dict[str, Any]) -> str | None:
    if normalized.get("aircraft_model"):
        return normalized["aircraft_model"]
    # flight-plan / amendment specs carry the ICAO type as element text
    for key in ("newFlightAircraftSpecs", "flightAircraftSpecs", "aircraftSpecification"):
        node = _find_first(body, key)
        if isinstance(node, str) and node:
            return node
        if isinstance(node, dict) and node.get("#text"):
            return node["#text"]
    return None


def to_canonical(record: ParsedMessage) -> dict[str, Any]:
    idy = record.identity
    nz = record.normalized
    body = record.body
    acid = idy.get("acid")

    carrier = idy.get("airline")
    if not carrier or carrier == "XXX":  # 'XXX' is the placeholder for GA
        carrier = idy.get("major")

    return {
        # Identity ------------------------------------------------------------
        "flight_instance_id": idy.get("gufi") or idy.get("flight_ref"),
        "gufi": idy.get("gufi"),
        "flight_ref": idy.get("flight_ref"),
        "callsign": acid,
        "operating_carrier": carrier,
        # Airframe ------------------------------------------------------------
        # Tail is NOT in TFMS for airline flights; only when callsign is an
        # N-number (GA). Otherwise resolved downstream via ADS-B.
        "tail_number": acid if looks_like_registration(acid) else None,
        "aircraft_type": _aircraft_type(body, nz),
        "aircraft_category": _aircraft_category(body),
        # Route ---------------------------------------------------------------
        "origin": idy.get("dep_arpt"),
        "destination": idy.get("arr_arpt"),
        # Times: scheduled / estimated ---------------------------------------
        "scheduled_gate_departure": (
            nz.get("gate_out")
            or idy.get("igtd")
            or (nz.get("etd") if nz.get("etd_type") == "SCHEDULED" else None)
        ),
        "scheduled_gate_arrival": (
            nz.get("gate_in")
            or nz.get("original_arrival")
            or (nz.get("eta") if nz.get("eta_type") == "SCHEDULED" else None)
        ),
        "estimated_arrival": (
            nz.get("eta") if nz.get("eta_type") == "ESTIMATED" else None
        ),
        # Times: actual OOOI --------------------------------------------------
        "actual_off": nz.get("actual_off"),
        "actual_on": nz.get("actual_on"),
        # Status --------------------------------------------------------------
        "flight_status": nz.get("flight_status"),
        # Live state (latest observed, from trackInformation) -----------------
        "last_latitude": nz.get("lat"),
        "last_longitude": nz.get("lon"),
        "last_altitude_ft": nz.get("altitude_ft"),
        "last_ground_speed": nz.get("speed"),
        "last_position_time": nz.get("time_at_position"),
    }
