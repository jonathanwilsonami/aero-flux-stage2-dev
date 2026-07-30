"""Airline code crosswalk: ICAO <-> IATA <-> name.

This is the bridge between three naming worlds:
  - SWIM speaks ICAO      (acid callsign 'AAL2033', airline 'AAL')
  - Passengers + BTS speak IATA ('AA2033', reporting carrier 'AA')
  - Humans speak names    ('American Airlines')

Data is a trimmed OpenFlights airline table bundled at data/airlines.csv, so
this works offline. Refresh it from
https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

_CSV = Path(__file__).parent / "data" / "airlines.csv"


@dataclass(frozen=True)
class Airline:
    icao: str
    iata: str
    name: str
    callsign: str  # radio telephony, e.g. "AMERICAN"
    country: str
    active: bool


class AirlineTable:
    def __init__(self, path: Path = _CSV) -> None:
        self._by_icao: dict[str, Airline] = {}
        self._by_iata: dict[str, Airline] = {}
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                a = Airline(
                    icao=row["icao"].upper(),
                    iata=row["iata"].upper(),
                    name=row["name"],
                    callsign=row["callsign"],
                    country=row["country"],
                    active=row["active"] == "Y",
                )
                self._by_icao[a.icao] = a
                if a.iata:  # many small operators have no IATA code
                    # prefer active carriers when an IATA code is reused
                    existing = self._by_iata.get(a.iata)
                    if existing is None or (a.active and not existing.active):
                        self._by_iata[a.iata] = a

    def by_icao(self, icao: str) -> Airline | None:
        return self._by_icao.get((icao or "").upper())

    def by_iata(self, iata: str) -> Airline | None:
        return self._by_iata.get((iata or "").upper())

    def icao_to_iata(self, icao: str) -> str | None:
        a = self.by_icao(icao)
        return a.iata or None if a else None

    def iata_to_icao(self, iata: str) -> str | None:
        a = self.by_iata(iata)
        return a.icao if a else None

    def __len__(self) -> int:
        return len(self._by_icao)


# Module-level default table; construct once, reuse.
DEFAULT_TABLE = AirlineTable()
