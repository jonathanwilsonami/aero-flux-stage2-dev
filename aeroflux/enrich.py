"""Enrichment: turn a fused flight instance into a finished, clearly-labeled
canonical record.

This is the last mile of the pipeline. It runs AFTER fusion and:
  - resolves the callsign to carrier + passenger flight number,
  - tags every record with a plain-language `resolution_status` so the oddballs
    are labeled, never silently dropped,
  - fills the tail number: from the callsign for GA, or (optionally) from a live
    ADS-B lookup for airline flights, recording where it came from in
    `tail_source`,
  - fills the ICAO `hex` when ADS-B is consulted (the tail-free join key).

ADS-B is optional and injected, so the builder runs fully offline (tail stays
null, cleanly labeled) or online (tail/hex filled) with the same code.
"""

from __future__ import annotations

from typing import Any, Callable

from .airlines import AirlineTable, DEFAULT_TABLE
from .identity import parse_callsign, callsign_to_flight_number


class ResolutionStatus:
    AIRLINE = "airline_resolved"           # callsign -> known carrier
    GA_TAIL = "ga_tail_from_callsign"      # callsign IS the tail (GA)
    UNKNOWN_AIRLINE = "unknown_airline"    # parses, carrier not in crosswalk
    UNPARSEABLE = "unparseable"            # not a usable callsign


# Fields the finished dataset carries, in a readable order. Used by CSV output.
DATASET_FIELDS = [
    # identity
    "flight_instance_id", "gufi", "flight_ref", "callsign", "flight_number",
    "carrier_icao", "carrier_iata", "carrier_name", "resolution_status",
    # airframe
    "tail_number", "tail_source", "hex", "aircraft_type", "aircraft_category",
    # route
    "origin", "destination",
    # times
    "scheduled_gate_departure", "scheduled_gate_arrival", "estimated_arrival",
    "actual_off", "actual_on",
    # status
    "flight_status",
    # live state
    "last_latitude", "last_longitude", "last_altitude_ft",
    "last_ground_speed", "last_position_time",
]


def classify(callsign: str | None, table: AirlineTable = DEFAULT_TABLE) -> str:
    p = parse_callsign(callsign or "", table)
    if p.is_registration:
        return ResolutionStatus.GA_TAIL
    if p.resolved:
        return ResolutionStatus.AIRLINE
    if p.flight_number:
        return ResolutionStatus.UNKNOWN_AIRLINE
    return ResolutionStatus.UNPARSEABLE


def enrich_record(
    record: dict[str, Any],
    table: AirlineTable = DEFAULT_TABLE,
    adsb_resolver: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    """Add resolved identity + label + tail/hex to a fused canonical record.

    `adsb_resolver` is an optional callable: callsign -> Airframe|None
    (e.g. AdsbClient().resolve_tail). If absent, airline tails stay null.
    """
    out = dict(record)  # never mutate the input
    callsign = out.get("callsign")
    parsed = parse_callsign(callsign or "", table)

    out["resolution_status"] = classify(callsign, table)
    out["carrier_icao"] = parsed.airline_icao
    out["carrier_iata"] = parsed.airline_iata
    out["carrier_name"] = parsed.airline_name
    out["flight_number"] = callsign_to_flight_number(callsign or "", table)

    # Tail number provenance.
    out.setdefault("tail_number", None)
    out.setdefault("hex", None)
    if out.get("tail_number"):
        out["tail_source"] = "swim_ga"            # GA: callsign was the tail
    else:
        out["tail_source"] = "none"

    # Optional live airframe resolution for airline flights.
    if adsb_resolver is not None and out["tail_source"] == "none" and callsign:
        frame = adsb_resolver(callsign)
        if frame is not None:
            if getattr(frame, "hex", None):
                out["hex"] = frame.hex
            if getattr(frame, "registration", None):
                out["tail_number"] = frame.registration
                out["tail_source"] = "adsb"
            elif out["hex"]:
                out["tail_source"] = "adsb_hex_only"   # airframe keyed, no tail
            if getattr(frame, "aircraft_type", None) and not out.get("aircraft_type"):
                out["aircraft_type"] = frame.aircraft_type

    # Guarantee every dataset column exists (readable, uniform rows).
    for field in DATASET_FIELDS:
        out.setdefault(field, None)
    return out
