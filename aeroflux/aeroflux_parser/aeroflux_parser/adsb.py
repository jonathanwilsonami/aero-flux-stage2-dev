"""ADS-B airframe resolution -- the tail number SWIM doesn't carry.

Given a callsign, query a live ADS-B feed and get back the airframe: ICAO hex,
registration (tail), and aircraft type. Two free, non-commercial, real-time
providers are supported; both are ADSBExchange-v2 compatible.

Response parsing is separated from the HTTP call so it can be unit-tested
against captured samples without network access. The core stays dependency-free
(urllib), so importing aeroflux_parser never requires requests.

Field notes (ADSBExchange v2): in each aircraft object,
  hex   = 24-bit ICAO address       flight = callsign (space padded)
  r     = registration (tail)       t      = aircraft ICAO type (e.g. B738)
  type  = message SOURCE type (adsb_icao, ...) -- NOT the aircraft type
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class AdsbProvider:
    name: str
    base_url: str


AIRPLANES_LIVE = AdsbProvider("airplanes.live", "https://api.airplanes.live/v2")
ADSB_LOL = AdsbProvider("adsb.lol", "https://api.adsb.lol/v2")


@dataclass
class Airframe:
    hex: str | None = None
    registration: str | None = None   # the tail number
    aircraft_type: str | None = None  # ICAO type designator, e.g. B738
    callsign: str | None = None


def parse_adsb_response(payload: dict, want_callsign: str | None = None) -> list[Airframe]:
    """Extract airframes from an ADSBExchange-v2-style JSON body.

    If want_callsign is given, prefer an exact callsign match (feeds return
    everything nearby when queried loosely)."""
    aircraft = payload.get("ac") or payload.get("aircraft") or []
    frames: list[Airframe] = []
    for ac in aircraft:
        frames.append(
            Airframe(
                hex=(ac.get("hex") or "").strip() or None,
                registration=(ac.get("r") or ac.get("registration") or "").strip() or None,
                aircraft_type=(ac.get("t") or "").strip() or None,
                callsign=(ac.get("flight") or "").strip() or None,
            )
        )
    if want_callsign:
        want = want_callsign.strip().upper()
        exact = [f for f in frames if (f.callsign or "").upper() == want]
        if exact:
            return exact
    return frames


class AdsbClient:
    """Thin real-time client. Kept minimal on purpose."""

    def __init__(self, provider: AdsbProvider = AIRPLANES_LIVE, timeout: float = 8.0) -> None:
        self.provider = provider
        self.timeout = timeout

    def _get(self, path: str) -> dict:
        url = f"{self.provider.base_url}{path}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def by_callsign(self, callsign: str) -> list[Airframe]:
        return parse_adsb_response(self._get(f"/callsign/{callsign.strip().upper()}"), callsign)

    def by_hex(self, hex_code: str) -> list[Airframe]:
        return parse_adsb_response(self._get(f"/hex/{hex_code.strip().lower()}"))

    def resolve_tail(self, callsign: str) -> Airframe | None:
        """Best single airframe for a callsign, or None if the feed hasn't seen
        it (coverage gap). Never raises on a miss -- returns None."""
        try:
            frames = self.by_callsign(callsign)
        except Exception:
            return None
        for f in frames:
            if f.registration or f.hex:
                return f
        return None
