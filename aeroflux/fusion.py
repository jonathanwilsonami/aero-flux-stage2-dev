"""Flight-instance fusion.

Merges many per-message canonical projections into one evolving record per
flight, keyed by flight identity. This is the Python, single-purpose analogue
of the NASA ATD-2 Fuser's core idea: organize by flight, and when sources
disagree on a field, resolve by SOURCE PRIORITY rather than latest-wins.

Merge key: flightRef is present on every fltdMessage, so it is the reliable
join across message types. GUFI is globally unique but absent on airline-sourced
messages, so it is preferred for the *displayed* flight_instance_id but not used
as the merge key.
"""

from __future__ import annotations

from typing import Any

from .canonical import to_canonical
from .result import ParsedMessage

# Per canonical field, which message types are authoritative, best first.
# "*" means any source is acceptable (stable identity fields); ties break on
# recency. A field whose incoming source isn't listed is ignored for that field.
FIELD_SOURCE_PRIORITY: dict[str, list[str]] = {
    "flight_instance_id": ["*"],
    "gufi": ["*"],
    "flight_ref": ["*"],
    "callsign": ["*"],
    "operating_carrier": ["*"],
    "origin": ["*"],
    "destination": ["*"],
    "tail_number": ["*"],
    "aircraft_type": ["*"],
    "aircraft_category": ["*"],
    "scheduled_gate_departure": ["FlightModify", "FlightTimes"],
    "scheduled_gate_arrival": ["FlightModify", "FlightTimes"],
    # live track ETA outranks a planned airline estimate:
    "estimated_arrival": ["trackInformation", "FlightModify", "FlightTimes"],
    "actual_off": ["departureInformation"],
    "actual_on": ["arrivalInformation"],
    "flight_status": [
        "arrivalInformation", "departureInformation", "FlightModify", "FlightTimes",
    ],
    # live state: only trackInformation, newest wins (handled by recency):
    "last_latitude": ["trackInformation"],
    "last_longitude": ["trackInformation"],
    "last_altitude_ft": ["trackInformation"],
    "last_ground_speed": ["trackInformation"],
    "last_position_time": ["trackInformation"],
}


def _priority_rank(field_name: str, msg_type: str) -> int:
    """Higher = more authoritative. 0 = any-source field. -1 = not allowed."""
    prefs = FIELD_SOURCE_PRIORITY.get(field_name, ["*"])
    if "*" in prefs:
        return 0
    if msg_type in prefs:
        return len(prefs) - prefs.index(msg_type)
    return -1


class FlightInstanceReducer:
    """Feed it ParsedMessages (already normalized); read fused records out.

    Keying: GUFI is the stable flight-instance identity. flight_ref is NOT --
    it changes when a flight plan is amended/refiled -- so it is used only as a
    bridge: airline messages (which carry no GUFI) join the GUFI record via a
    shared flight_ref. When a bridge is discovered late, the two buckets merge.
    """

    def __init__(self) -> None:
        # key -> {"fields": {name: (value, rank, source_time)}, "gufi": str|None}
        self._flights: dict[str, dict[str, Any]] = {}
        self._ref_to_gufi: dict[str, str] = {}   # flight_ref -> gufi bridge

    def _resolve_key(self, gufi: str | None, ref: str | None) -> str | None:
        if gufi:
            # New bridge? Fold any pre-existing ref-keyed bucket into the gufi one.
            if ref and self._ref_to_gufi.get(ref) != gufi:
                self._ref_to_gufi[ref] = gufi
                if ref in self._flights and ref != gufi:
                    self._absorb(into=gufi, frm=ref)
            return gufi
        if ref:
            return self._ref_to_gufi.get(ref) or ref
        return None

    def _absorb(self, into: str, frm: str) -> None:
        target = self._flights.setdefault(into, {"fields": {}, "gufi": into})
        source = self._flights.pop(frm)
        for name, (value, rank, stime) in source["fields"].items():
            self._offer(target["fields"], name, value, rank, stime)

    @staticmethod
    def _offer(fields, name, value, rank, source_time) -> None:
        current = fields.get(name)
        if (current is None or rank > current[1]
                or (rank == current[1] and source_time >= current[2])):
            fields[name] = (value, rank, source_time)

    def add(self, record: ParsedMessage) -> None:
        idy = record.identity
        gufi, ref = idy.get("gufi"), idy.get("flight_ref")
        key = self._resolve_key(gufi, ref)
        if not key:
            return

        state = self._flights.setdefault(key, {"fields": {}, "gufi": None})
        if gufi:
            state["gufi"] = gufi

        msg_type = record.msg_type or ""
        source_time = idy.get("source_time") or record.ingested_at
        for name, value in to_canonical(record).items():
            if value is None:
                continue
            rank = _priority_rank(name, msg_type)
            if rank < 0:
                continue
            self._offer(state["fields"], name, value, rank, source_time)

    def records(self) -> list[dict[str, Any]]:
        out = []
        for key, state in self._flights.items():
            rec = {name: v[0] for name, v in state["fields"].items()}
            rec["flight_instance_id"] = state["gufi"] or key
            out.append(rec)
        return out

    def __len__(self) -> int:
        return len(self._flights)
