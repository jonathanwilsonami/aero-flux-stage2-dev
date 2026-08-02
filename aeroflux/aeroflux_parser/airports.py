"""Airport dimension table.

Solves three problems that showed up in live data:
  1. Code system mismatch — SWIM sends a mix of ICAO (KDFW) and IATA (DFW); BTS
     is IATA. `to_icao` normalizes any token to one canonical ICAO so a flight's
     destination and the next leg's origin actually compare equal.
  2. Geography — lat/lon per airport, for weather (nearest-station) and distance.
  3. Time zones — the IANA tz per airport, so BTS local times convert to UTC and
     UTC can be shown as local when that's what a feature needs.

Data is bundled at data/airports.csv (generated from the `airportsdata` package)
so the table loads offline with no runtime dependency — same pattern as
airlines.csv.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_CSV = Path(__file__).parent / "data" / "airports.csv"


@dataclass(frozen=True)
class Airport:
    icao: str
    iata: Optional[str]
    name: Optional[str]
    lat: Optional[float]
    lon: Optional[float]
    tz: Optional[str]          # IANA, e.g. "America/Chicago"
    country: Optional[str]


class AirportTable:
    def __init__(self, path: Path = _CSV) -> None:
        self._by_icao: dict[str, Airport] = {}
        self._by_iata: dict[str, Airport] = {}
        with open(path, encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                a = Airport(
                    icao=row["icao"].strip().upper(),
                    iata=(row.get("iata") or "").strip().upper() or None,
                    name=(row.get("name") or "").strip() or None,
                    lat=float(row["lat"]) if row.get("lat") else None,
                    lon=float(row["lon"]) if row.get("lon") else None,
                    tz=(row.get("tz") or "").strip() or None,
                    country=(row.get("country") or "").strip() or None,
                )
                if a.icao:
                    self._by_icao[a.icao] = a
                    if a.iata and a.iata not in self._by_iata:
                        self._by_iata[a.iata] = a

    def to_icao(self, code: Optional[str]) -> Optional[str]:
        """Normalize any airport token to canonical ICAO. DFW->KDFW, KDFW->KDFW.
        Unknown 4-letter tokens pass through (likely a valid ICAO we don't list);
        unknown 3-letter tokens return None."""
        if not code:
            return None
        c = code.strip().upper()
        if c in self._by_icao:
            return c
        if len(c) == 3 and c in self._by_iata:      # IATA -> ICAO
            return self._by_iata[c].icao
        if len(c) == 3 and ("K" + c) in self._by_icao:  # US "K"+IATA fallback
            return "K" + c
        return c if len(c) == 4 else None

    def get(self, code: Optional[str]) -> Optional[Airport]:
        icao = self.to_icao(code)
        return self._by_icao.get(icao) if icao else None

    def latlon(self, code: Optional[str]) -> tuple[Optional[float], Optional[float]]:
        a = self.get(code)
        return (a.lat, a.lon) if a else (None, None)

    def tz(self, code: Optional[str]) -> Optional[str]:
        a = self.get(code)
        return a.tz if a else None

    def __len__(self) -> int:
        return len(self._by_icao)


# Loaded once and reused (mirrors airlines.DEFAULT_TABLE).
DEFAULT_AIRPORTS = AirportTable()
