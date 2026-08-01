#!/usr/bin/env python3
"""Continuous ADS-B poller -> rolling airframe store (free-tier safe).

Why this exists: SWIM has no tail for airline flights. This poller fills the gap
by continuously ingesting live ADS-B into a 24-48h rolling callsign->hex store,
which fusion then joins by callsign. Because a flight is airborne for hours, it
gets captured at *some* sweep even if it wasn't broadcasting at pipeline time --
turning "0% at snapshot" into "whatever the network saw over the window".

STAYING FREE / NOT GETTING BANNED:
  * airplanes.live is rate-limited to 1 request/second (no key, non-commercial).
  * We use the BULK /point endpoint: one request returns every aircraft in a
    ~250 nm circle. A dozen circles cover the CONUS, so a full sweep is ~12
    requests. We never do per-flight lookups.
  * We enforce >= MIN_INTERVAL seconds between requests, sweep only every
    SWEEP_SECONDS, and back off on HTTP 429. All configurable via env.

    python adsb_poller.py            # runs until Ctrl-C
"""

from __future__ import annotations

import logging
import os
import signal
import time
import urllib.error

from aeroflux_parser.adsb import AdsbClient, AIRPLANES_LIVE, ADSB_LOL
from aeroflux_parser.adsb_store import PostgresAirframeStore

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("adsb.poller")

# ~12 circles (<=250 nm) covering the contiguous US airspace. Override with
# ADSB_REGIONS="lat,lon,radius; lat,lon,radius; ..." to focus on hubs / regions.
CONUS_REGIONS = [
    (47.5, -122.0, 250), (46.0, -110.0, 250), (47.0, -95.0, 250), (44.0, -78.0, 250),
    (40.0, -118.0, 250), (39.0, -105.0, 250), (39.5, -90.0, 250), (40.0, -76.0, 250),
    (33.0, -117.0, 250), (32.0, -100.0, 250), (31.0, -88.0, 250), (30.5, -82.0, 250),
]


def _regions() -> list[tuple[float, float, int]]:
    raw = os.getenv("ADSB_REGIONS", "").strip()
    if not raw:
        return CONUS_REGIONS
    out = []
    for chunk in raw.split(";"):
        if chunk.strip():
            lat, lon, rad = chunk.split(",")
            out.append((float(lat), float(lon), int(rad)))
    return out


def main() -> int:
    dsn = os.getenv("ADSB_DSN") or os.getenv("DSN") or (
        f"postgresql://{os.getenv('POSTGRES_USER','aeroflux')}:"
        f"{os.getenv('POSTGRES_PASSWORD','aeroflux-db')}@"
        f"{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}/"
        f"{os.getenv('POSTGRES_DB','aeroflux')}")
    provider = ADSB_LOL if os.getenv("ADSB_SOURCE", "").lower() == "adsb.lol" else AIRPLANES_LIVE
    min_interval = float(os.getenv("ADSB_MIN_INTERVAL", "1.1"))   # >= 1 req/sec, politely
    sweep_seconds = float(os.getenv("ADSB_SWEEP_SECONDS", "120"))
    ttl_hours = int(os.getenv("ADSB_TTL_HOURS", "48"))
    regions = _regions()

    client = AdsbClient(provider)
    store = PostgresAirframeStore(dsn)
    log.info("poller up: source=%s regions=%d min_interval=%.1fs sweep=%.0fs ttl=%dh",
             provider.name, len(regions), min_interval, sweep_seconds, ttl_hours)

    running = {"go": True}
    signal.signal(signal.SIGINT, lambda *_: running.update(go=False))
    signal.signal(signal.SIGTERM, lambda *_: running.update(go=False))

    while running["go"]:
        swept = stored = 0
        for lat, lon, radius in regions:
            if not running["go"]:
                break
            t0 = time.monotonic()
            try:
                frames = client.by_point(lat, lon, radius)
                stored += store.upsert(frames)
                swept += len(frames)
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    log.warning("429 rate-limited; backing off 30s")
                    time.sleep(30)
                else:
                    log.warning("HTTP %s at (%.1f,%.1f)", e.code, lat, lon)
            except Exception as e:  # network hiccup -> skip this circle
                log.warning("fetch failed at (%.1f,%.1f): %s", lat, lon, e)
            # enforce the rate limit
            elapsed = time.monotonic() - t0
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

        purged = store.purge(older_than_hours=ttl_hours)
        log.info("sweep done: %d aircraft seen, %d upserted, %d purged", swept, stored, purged)

        # wait out the rest of the sweep interval
        for _ in range(int(sweep_seconds)):
            if not running["go"]:
                break
            time.sleep(1)

    log.info("poller stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())