"""Optional normalization layer.

Parsing (parsers.py) captures *everything* losslessly into `body`. Normalizing
is the opposite concern: pull out a small, typed, model-ready set of fields for
the message types your model actually consumes, and coerce their types.

It is deliberately separate from parsing so the two can evolve independently:
  - `body` and `raw_xml` are never touched (still lossless / reprocessable).
  - A normalizer only ever *adds* to `record.normalized`.
  - A missing or malformed field is skipped, never raised. Nothing breaks the
    stream just because one flight's altitude was garbage.

Add a new type by writing one function and registering it in NORMALIZERS.
"""

from __future__ import annotations

from typing import Any

from .result import ParsedMessage


# --- small, forgiving coercion helpers -------------------------------------


def _to_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> float | None:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _first(value: Any) -> Any:
    """element_to_obj collapses single children to scalars and repeats to
    lists; this lets callers treat both uniformly."""
    return value[0] if isinstance(value, list) else value


def _scalar(value: Any) -> Any:
    """Extract the text of a node whether it came back as a bare string or as
    a dict carrying attributes plus '#text'."""
    value = _first(value)
    if isinstance(value, dict):
        return value.get("#text")
    return value


def dms_to_decimal(dms: Any) -> float | None:
    """{'@degrees':'081','@minutes':'14','@direction':'WEST'} -> -81.233333"""
    dms = _first(dms)
    if not isinstance(dms, dict):
        return None
    degrees = _to_float(dms.get("@degrees"))
    if degrees is None:
        return None
    minutes = _to_float(dms.get("@minutes")) or 0.0
    seconds = _to_float(dms.get("@seconds")) or 0.0
    decimal = degrees + minutes / 60.0 + seconds / 3600.0
    if str(dms.get("@direction", "")).upper() in ("WEST", "SOUTH", "W", "S"):
        decimal = -decimal
    return round(decimal, 6)


def _dms_under(position: dict[str, Any], side: str, dms_tag: str) -> float | None:
    """position -> latitude/longitude -> latitudeDMS/longitudeDMS -> decimal."""
    node = _first(position.get(side))
    if not isinstance(node, dict):
        return None
    return dms_to_decimal(node.get(dms_tag))


# --- normalizers (one per msg_type) ----------------------------------------


def normalize_track(body: dict[str, Any]) -> dict[str, Any]:
    """trackInformation: live position / speed / altitude / rolling ETA.

    Notes on units, made explicit so downstream code isn't guessing:
      - speed        : ground speed, knots (as reported)
      - altitude_ft  : simpleAltitude is in hundreds of feet -> x100
      - lat / lon    : decimal degrees, converted from the reported DMS
    """
    ti = _first(body.get("trackInformation"))
    if not isinstance(ti, dict):
        return {}

    out: dict[str, Any] = {}

    speed = _to_int(_scalar(ti.get("speed")))
    if speed is not None:
        out["speed"] = speed

    reported = _first(ti.get("reportedAltitude"))
    if isinstance(reported, dict):
        assigned = _first(reported.get("assignedAltitude"))
        if isinstance(assigned, dict):
            simple = _to_int(_scalar(assigned.get("simpleAltitude")))
            if simple is not None:
                out["altitude_ft"] = simple * 100

    tap = _scalar(ti.get("timeAtPosition"))
    if tap:
        out["time_at_position"] = tap

    position = _first(ti.get("position"))
    if isinstance(position, dict):
        lat = _dms_under(position, "latitude", "latitudeDMS")
        lon = _dms_under(position, "longitude", "longitudeDMS")
        if lat is not None:
            out["lat"] = lat
        if lon is not None:
            out["lon"] = lon

    ncsm = _first(ti.get("ncsmTrackData"))
    if isinstance(ncsm, dict):
        eta = _first(ncsm.get("eta"))
        if isinstance(eta, dict):
            if eta.get("@timeValue"):
                out["eta"] = eta["@timeValue"]
            if eta.get("@etaType"):
                out["eta_type"] = eta["@etaType"]

        fix = _first(ncsm.get("arrivalFixAndTime"))
        if isinstance(fix, dict):
            if fix.get("@fixName"):
                out["arrival_fix"] = fix["@fixName"]
            if fix.get("@arrTime"):
                out["arrival_fix_eta"] = fix["@arrTime"]

        # nextEvent already carries decimal coordinates -> handy cross-check
        nxt = _first(ncsm.get("nextEvent"))
        if isinstance(nxt, dict):
            nlat = _to_float(nxt.get("@latitudeDecimal"))
            nlon = _to_float(nxt.get("@longitudeDecimal"))
            if nlat is not None:
                out["next_lat"] = nlat
            if nlon is not None:
                out["next_lon"] = nlon

    return out


def normalize_airline_times(body: dict[str, Any]) -> dict[str, Any]:
    """FlightModify / FlightTimes: airline-reported schedule, status, and OOOI.

    Faithful extraction only -- it exposes exactly what the message carries and
    makes no interpretation. The canonical mapping (which of these is 'the'
    scheduled gate departure) lives in canonical.py, so those decisions are
    easy to change without touching extraction.

    OOOI times (only present on FlightModify's flightTimeData):
      gate_out  = airlineOutTime  (gate pushback)
      gate_off  = airlineOffTime  (wheels up)
      gate_on   = airlineOnTime   (wheels down)
      gate_in   = airlineInTime   (gate arrival)
    """
    root = _first(body.get("ncsmFlightModify"))
    if not isinstance(root, dict):
        root = _first(body.get("ncsmFlightTimes"))
    if not isinstance(root, dict):
        return {}

    # FlightModify wraps everything in <airlineData>; FlightTimes does not.
    container = _first(root.get("airlineData"))
    if not isinstance(container, dict):
        container = root

    out: dict[str, Any] = {}

    fss = _first(container.get("flightStatusAndSpec"))
    if not isinstance(fss, dict):
        fss = _first(root.get("flightStatusAndSpec"))
    if isinstance(fss, dict):
        status = _scalar(fss.get("flightStatus"))
        if status:
            out["flight_status"] = status
        model = _scalar(fss.get("aircraftModel"))
        if model:
            out["aircraft_model"] = model

    for key, type_attr in (("etd", "@etdType"), ("eta", "@etaType")):
        node = _first(container.get(key))
        if isinstance(node, dict):
            if node.get("@timeValue"):
                out[key] = node["@timeValue"]
            if node.get(type_attr):
                out[f"{key}_type"] = node[type_attr]

    ftd = _first(container.get("flightTimeData"))
    if isinstance(ftd, dict):
        oooi = {
            "@airlineOutTime": "gate_out",
            "@airlineOffTime": "gate_off",
            "@airlineOnTime": "gate_on",
            "@airlineInTime": "gate_in",
            "@originalDeparture": "original_departure",
            "@originalArrival": "original_arrival",
        }
        for attr, key in oooi.items():
            if ftd.get(attr):
                out[key] = ftd[attr]

    return out


def normalize_departure(body: dict[str, Any]) -> dict[str, Any]:
    """departureInformation: actual wheels-off time + filed times."""
    di = _first(body.get("departureInformation"))
    if not isinstance(di, dict):
        return {}
    out: dict[str, Any] = {}

    tod = _first(di.get("timeOfDeparture"))
    if isinstance(tod, dict):
        # estimated="false" => this is the ACTUAL off-time
        if str(tod.get("@estimated", "")).lower() == "false" and tod.get("#text"):
            out["actual_off"] = tod["#text"]
    elif isinstance(tod, str) and tod:
        out["actual_off"] = tod

    ftd = _first(di.get("ncsmFlightTimeData"))
    if isinstance(ftd, dict):
        for key, type_attr in (("etd", "@etdType"), ("eta", "@etaType")):
            node = _first(ftd.get(key))
            if isinstance(node, dict) and node.get("@timeValue"):
                out[key] = node["@timeValue"]
                if node.get(type_attr):
                    out[f"{key}_type"] = node[type_attr]
    return out


def normalize_arrival(body: dict[str, Any]) -> dict[str, Any]:
    """arrivalInformation: actual wheels-on time (best-effort; structure varies)."""
    ai = _first(body.get("arrivalInformation"))
    if not isinstance(ai, dict):
        return {}
    out: dict[str, Any] = {}

    toa = _first(ai.get("timeOfArrival"))
    if isinstance(toa, dict):
        if str(toa.get("@estimated", "")).lower() == "false" and toa.get("#text"):
            out["actual_on"] = toa["#text"]
    elif isinstance(toa, str) and toa:
        out["actual_on"] = toa

    ftd = _first(ai.get("ncsmFlightTimeData"))
    if isinstance(ftd, dict):
        eta = _first(ftd.get("eta"))
        if isinstance(eta, dict) and eta.get("@etaType") == "ACTUAL" and eta.get("@timeValue"):
            out["actual_on"] = eta["@timeValue"]
    return out


NORMALIZERS = {
    "trackInformation": normalize_track,
    "FlightModify": normalize_airline_times,
    "FlightTimes": normalize_airline_times,
    "departureInformation": normalize_departure,
    "arrivalInformation": normalize_arrival,
}


def normalize(record: ParsedMessage) -> ParsedMessage:
    """Attach typed fields for recognized types; leave others untouched.
    Mutates and returns the record for convenient chaining."""
    fn = NORMALIZERS.get(record.msg_type or "")
    if fn is None:
        return record
    try:
        record.normalized = fn(record.body)
    except Exception as exc:  # a normalizer bug must not kill the stream
        record.normalized = {}
        record.errors.append(f"normalize failed: {exc}")
    return record
