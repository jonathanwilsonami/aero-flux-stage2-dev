"""Identity resolution across the naming worlds.

Two directions:
  - parse_callsign('AAL2033')        -> airline ICAO + flight number   (SWIM side)
  - resolve_flight_number('AA2033')  -> the SWIM callsign to look for   (user side)

Honest limitation, flagged loudly: the ATC callsign is NOT always the marketed
flight number. Airlines frequently fly a marketed flight (AA 2033) under a
different ATC callsign (e.g. AAL1234) to deconflict similar-sounding numbers.
Pure code substitution resolves the common case; the general case needs a
schedule crosswalk or an ADS-B callsign match. `resolve_flight_number` therefore
returns *candidates* and marks the assumption.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from .airlines import AirlineTable, DEFAULT_TABLE

# US registration used as a callsign (general aviation): N + digit + up to 4.
_US_REGISTRATION = re.compile(r"^N[1-9][0-9]{0,4}[A-Z]{0,2}$")
# A callsign is a letter-prefix (airline) followed by an alphanumeric suffix.
_CALLSIGN = re.compile(r"^([A-Z]{2,3})([0-9][0-9A-Z]*)$")


@dataclass
class ParsedCallsign:
    raw: str
    airline_icao: str | None = None
    airline_iata: str | None = None
    airline_name: str | None = None
    flight_number: str | None = None      # numeric part, e.g. "2033"
    is_registration: bool = False         # GA: the callsign IS the tail number
    resolved: bool = False


def parse_callsign(acid: str, table: AirlineTable = DEFAULT_TABLE) -> ParsedCallsign:
    raw = (acid or "").strip().upper()
    out = ParsedCallsign(raw=raw)
    if not raw:
        return out

    if _US_REGISTRATION.match(raw):
        out.is_registration = True
        out.resolved = True  # already an airframe id; no airline to resolve
        return out

    m = _CALLSIGN.match(raw)
    if not m:
        return out
    prefix, suffix = m.group(1), m.group(2)

    airline = table.by_icao(prefix)
    if airline is not None:
        out.airline_icao = airline.icao
        out.airline_iata = airline.iata or None
        out.airline_name = airline.name
        out.resolved = True
    else:
        out.airline_icao = prefix  # keep the guess even if unknown

    out.flight_number = suffix
    return out


@dataclass
class ResolvedFlight:
    user_input: str
    airline_iata: str | None = None
    airline_icao: str | None = None
    airline_name: str | None = None
    flight_number: str | None = None
    callsign_candidates: list[str] = field(default_factory=list)
    assumption: str = ""


def resolve_flight_number(user_input: str, table: AirlineTable = DEFAULT_TABLE) -> ResolvedFlight:
    """Turn what a passenger types into the SWIM callsign(s) to search for.

    The airline code is 2 chars (IATA, may contain a digit like B6/9E) or 3
    chars (ICAO). We can't tell by shape alone, so we try IATA-length first,
    then ICAO-length, and keep whichever resolves to a known airline.
    """
    raw = (user_input or "").strip().upper()
    out = ResolvedFlight(user_input=raw)
    if not raw:
        return out

    compact = raw.replace(" ", "")
    candidates: list[tuple[str, str]] = []
    for code_len in (2, 3):  # IATA before ICAO
        code, rest = compact[:code_len], compact[code_len:]
        if rest.isdigit():
            candidates.append((code, rest))

    for code, number in candidates:
        airline = table.by_iata(code) or table.by_icao(code)
        if airline is not None:
            out.airline_iata = airline.iata or None
            out.airline_icao = airline.icao
            out.airline_name = airline.name
            out.flight_number = number
            out.callsign_candidates = [f"{airline.icao}{number}"]
            out.assumption = "callsign assumed = ICAO code + flight number (not always true)"
            return out
    return out


def callsign_to_flight_number(acid: str, table: AirlineTable = DEFAULT_TABLE) -> str | None:
    """AAL2033 -> AA2033 (the IATA form a passenger would recognize)."""
    parsed = parse_callsign(acid, table)
    if parsed.airline_iata and parsed.flight_number:
        return f"{parsed.airline_iata}{parsed.flight_number}"
    return None
