"""Rolling ADS-B airframe store.

The poller writes here; fusion reads here. This decouples the rate-limited live
polling from transform time: the poller accumulates callsign -> (hex, tail,
type) continuously into a 24-48h rolling window, and `build_dataset` resolves
airframes from the store with zero API calls.

`.resolve(callsign)` returns an Airframe (or None), matching the signature
`enrich_record` expects for `adsb_resolver` — so the store is a drop-in for the
old per-callsign live lookup, minus the rate-limit risk.

Two implementations: InMemory (tests / quick runs) and Postgres (the real store,
keyed on callsign with a last_seen timestamp for TTL).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Protocol

from .adsb import Airframe


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AirframeStore(Protocol):
    def upsert(self, frames: Iterable[Airframe], seen_at: datetime | None = None) -> int: ...
    def resolve(self, callsign: str) -> Optional[Airframe]: ...
    def purge(self, older_than_hours: int = 48) -> int: ...


class InMemoryAirframeStore:
    def __init__(self) -> None:
        self._by_callsign: dict[str, tuple[Airframe, datetime]] = {}

    def upsert(self, frames: Iterable[Airframe], seen_at: datetime | None = None) -> int:
        ts = seen_at or _now()
        n = 0
        for f in frames:
            cs = (f.callsign or "").strip().upper()
            if not cs or not (f.hex or f.registration):
                continue
            # newest observation wins
            existing = self._by_callsign.get(cs)
            if existing is None or ts >= existing[1]:
                self._by_callsign[cs] = (f, ts)
                n += 1
        return n

    def resolve(self, callsign: str) -> Optional[Airframe]:
        hit = self._by_callsign.get((callsign or "").strip().upper())
        return hit[0] if hit else None

    def purge(self, older_than_hours: int = 48) -> int:
        cutoff = _now() - timedelta(hours=older_than_hours)
        stale = [k for k, (_, ts) in self._by_callsign.items() if ts < cutoff]
        for k in stale:
            del self._by_callsign[k]
        return len(stale)

    def __len__(self) -> int:
        return len(self._by_callsign)


_DDL = """
CREATE TABLE IF NOT EXISTS adsb_airframe (
    callsign       text PRIMARY KEY,
    hex            text,
    registration   text,
    aircraft_type  text,
    last_seen      timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_adsb_last_seen ON adsb_airframe (last_seen);
"""


class PostgresAirframeStore:
    """The real rolling store. `dsn` is a standard libpq connection string."""

    def __init__(self, dsn: str) -> None:
        import psycopg
        self._connect = lambda: psycopg.connect(dsn)
        with self._connect() as c:
            c.execute(_DDL)
            c.commit()

    def upsert(self, frames: Iterable[Airframe], seen_at: datetime | None = None) -> int:
        ts = seen_at or _now()
        rows = [
            ((f.callsign or "").strip().upper(), f.hex, f.registration, f.aircraft_type, ts)
            for f in frames
            if (f.callsign or "").strip() and (f.hex or f.registration)
        ]
        if not rows:
            return 0
        with self._connect() as c:
            with c.cursor() as cur:
                cur.executemany(
                    "INSERT INTO adsb_airframe (callsign, hex, registration, aircraft_type, last_seen) "
                    "VALUES (%s,%s,%s,%s,%s) "
                    "ON CONFLICT (callsign) DO UPDATE SET "
                    "  hex=EXCLUDED.hex, registration=EXCLUDED.registration, "
                    "  aircraft_type=EXCLUDED.aircraft_type, last_seen=EXCLUDED.last_seen "
                    "WHERE EXCLUDED.last_seen >= adsb_airframe.last_seen",
                    rows,
                )
            c.commit()
        return len(rows)

    def resolve(self, callsign: str) -> Optional[Airframe]:
        with self._connect() as c:
            with c.cursor() as cur:
                cur.execute(
                    "SELECT hex, registration, aircraft_type FROM adsb_airframe WHERE callsign=%s",
                    ((callsign or "").strip().upper(),),
                )
                row = cur.fetchone()
        if not row:
            return None
        return Airframe(hex=row[0], registration=row[1], aircraft_type=row[2],
                        callsign=(callsign or "").strip().upper())

    def purge(self, older_than_hours: int = 48) -> int:
        with self._connect() as c:
            with c.cursor() as cur:
                cur.execute(
                    "DELETE FROM adsb_airframe WHERE last_seen < now() - make_interval(hours => %s)",
                    (older_than_hours,),
                )
                n = cur.rowcount
            c.commit()
        return n