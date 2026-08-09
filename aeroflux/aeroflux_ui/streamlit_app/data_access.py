"""Data layer for the AeroFlux demo UI.

Reads live state from Postgres when AEROFLUX_DSN is set; otherwise generates
realistic sample flights so the demo always runs (no DB, no network needed).
Everything the pages need comes through here, cached.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st
import functools

# A compact set of major US hubs (icao, iata, name, lat, lon) — enough for a
# lively map without shipping the full 28k dimension.
HUBS = [
    ("KATL", "ATL", "Atlanta", 33.6407, -84.4277),
    ("KDFW", "DFW", "Dallas–Ft Worth", 32.8998, -97.0403),
    ("KDEN", "DEN", "Denver", 39.8561, -104.6737),
    ("KORD", "ORD", "Chicago O'Hare", 41.9742, -87.9073),
    ("KLAX", "LAX", "Los Angeles", 33.9416, -118.4085),
    ("KJFK", "JFK", "New York JFK", 40.6413, -73.7781),
    ("KLGA", "LGA", "New York LaGuardia", 40.7769, -73.8740),
    ("KLAS", "LAS", "Las Vegas", 36.0840, -115.1537),
    ("KMCO", "MCO", "Orlando", 28.4312, -81.3081),
    ("KMIA", "MIA", "Miami", 25.7959, -80.2870),
    ("KSEA", "SEA", "Seattle", 47.4502, -122.3088),
    ("KSFO", "SFO", "San Francisco", 37.6213, -122.3790),
    ("KEWR", "EWR", "Newark", 40.6895, -74.1745),
    ("KBOS", "BOS", "Boston", 42.3656, -71.0096),
    ("KPHX", "PHX", "Phoenix", 33.4342, -112.0116),
    ("KIAH", "IAH", "Houston", 29.9902, -95.3368),
    ("KCLT", "CLT", "Charlotte", 35.2140, -80.9431),
    ("KMSP", "MSP", "Minneapolis", 44.8848, -93.2223),
    ("KDTW", "DTW", "Detroit", 42.2162, -83.3554),
    ("KBWI", "BWI", "Baltimore", 39.1754, -76.6683),
    ("KSLC", "SLC", "Salt Lake City", 40.7899, -111.9791),
    ("KSAN", "SAN", "San Diego", 32.7338, -117.1933),
    ("KTPA", "TPA", "Tampa", 27.9755, -82.5332),
    ("KPDX", "PDX", "Portland", 45.5898, -122.5951),
]
_COORDS = {icao: (lat, lon, name, iata) for icao, iata, name, lat, lon in HUBS}
CARRIERS = [("AAL", "American"), ("DAL", "Delta"), ("UAL", "United"),
            ("SWA", "Southwest"), ("JBU", "JetBlue"), ("ASA", "Alaska")]


def dsn() -> str | None:
    return os.getenv("AEROFLUX_DSN")


def _bridge_dsn_env() -> None:
    """state_backend_from_env()'s postgres branch reads $DSN — the shared
    convention score_live.py/sync_cloud.py already use. This app's existing,
    documented convention is $AEROFLUX_DSN. Bridge one to the other here
    rather than forking the factory's contract just for this caller, so
    local dev keeps working exactly as documented (README: export
    AEROFLUX_DSN=...) with zero code changes to switch backends."""
    if os.getenv("AEROFLUX_DSN") and not os.getenv("DSN"):
        os.environ["DSN"] = os.environ["AEROFLUX_DSN"]


def is_live() -> bool:
    """True if configured to read from a real backend rather than sample
    data: STATE_BACKEND=dynamodb always counts; the postgres backend (the
    default) still needs a real $AEROFLUX_DSN, same as before."""
    if os.getenv("STATE_BACKEND", "postgres").lower() == "dynamodb":
        return True
    return bool(dsn())


def _merge_live_predictions(df):
    """Overlay predictions written by aeroflux_ml.score_live (AEROFLUX_PREDICTIONS
    parquet), so the UI reflects the live XGBoost model, not a placeholder."""
    pq = os.getenv("AEROFLUX_PREDICTIONS")
    if not pq or not os.path.exists(pq):
        return df
    try:
        import pandas as pd
        pr = pd.read_parquet(pq)[["flight_key", "delay_probability"]].rename(
            columns={"flight_key": "flight_instance_id", "delay_probability": "_p"})
        df = df.merge(pr, on="flight_instance_id", how="left")
        df["delay_prob"] = df["_p"].where(df["_p"].notna(), df.get("delay_prob", 0.25))
        df = df.drop(columns=["_p"])
    except Exception:
        pass
    return df


# Columns the map/KPI/inference pages expect on every flight row, regardless
# of which backend answered — Postgres's SELECT always returns all of these;
# a DynamoDB item only carries whichever attributes were actually upserted,
# so missing ones get reindexed to None rather than KeyError-ing downstream.
_STATE_DISPLAY_COLS = [
    "flight_instance_id", "callsign", "carrier_icao", "carrier_name",
    "origin", "destination", "flight_status", "hex", "tail_number",
    "scheduled_gate_departure", "scheduled_gate_arrival",
    "last_latitude", "last_longitude",
]


_FLIGHTS_LIMIT = 4000  # mirrors the old SQL's LIMIT 4000 — the map only ever shows a few
                       # thousand at once, and on DynamoDB this bounds Scan cost directly

# 300s, not 30s: sync_cloud only refreshes the cloud backends every SYNC_EVERY
# (default 300s), so re-querying every 30s was pure wasted read cost/latency
# for data that hadn't changed. Aligned to the actual write cadence.
@st.cache_data(ttl=300)
def load_flights() -> pd.DataFrame:
    """Current flights with origin/dest coords + a delay probability. Live
    from the configured StateStore — Postgres (AEROFLUX_DSN) by default, or
    DynamoDB when STATE_BACKEND=dynamodb — if reachable, else sample."""
    if is_live():
        try:
            return _load_flights_backend()
        except Exception as e:  # fall back so the demo never dies
            st.warning(f"Live backend unavailable ({e}); showing sample data.")
    return _merge_live_predictions(_sample_flights())


def _load_flights_backend() -> pd.DataFrame:
    _bridge_dsn_env()
    from aeroflux_ml import state_backend_from_env
    repo = state_backend_from_env()
    hours = int(os.getenv("STATE_HOURS", "48"))
    rows = repo.recent_flight_states(hours=hours, limit=_FLIGHTS_LIMIT)
    if not rows:
        raise RuntimeError("backend returned zero flight states")
    df = pd.DataFrame(rows)
    # DynamoDB items key on `flight_key` (the table's actual partition key);
    # Postgres rows key on `flight_instance_id`. Normalize to one name so
    # every downstream consumer (coords, KPIs, the inference page) only
    # ever has to know about one.
    if "flight_instance_id" not in df.columns and "flight_key" in df.columns:
        df = df.rename(columns={"flight_key": "flight_instance_id"})
    for c in _STATE_DISPLAY_COLS:
        if c not in df.columns:
            df[c] = None
    df = (df[df["origin"].notna() & df["destination"].notna()]
          .head(4000).reset_index(drop=True))  # same cap the old SQL's LIMIT 4000 had
    df = _attach_coords(df)
    df["delay_prob"] = _try_predictions_backend(df)
    return df.dropna(subset=["o_lat", "d_lat"])


def _try_predictions_backend(df: pd.DataFrame) -> pd.Series:
    """Prefer delay_probability already on the state rows — DynamoDB embeds
    prediction + state on the same item (the disjoint-attribute design), so
    for that backend this is already there, no second lookup needed.
    Otherwise (the Postgres/local path, where predictions live separately)
    fall back to the predictions parquet via the lake backend."""
    if "delay_probability" in df.columns:
        vals = pd.to_numeric(df["delay_probability"], errors="coerce")
        if vals.notna().any():
            return vals.fillna(0.25)
    pq = os.getenv("AEROFLUX_PREDICTIONS")
    if pq:
        try:
            from aeroflux_ml import lake_backend_from_env
            pr = lake_backend_from_env().read_parquet(pq).to_pandas()[["flight_key", "delay_probability"]]
            pmap = dict(zip(pr["flight_key"], pr["delay_probability"]))
            return df["flight_instance_id"].map(pmap).fillna(0.25)
        except Exception:
            pass
    return pd.Series([0.25] * len(df), index=df.index)


@functools.lru_cache(maxsize=1)
def _airport_coords() -> dict:
    """Load full ICAO -> (lat, lon) from the parser's airports.csv (28k airports).
    Falls back to the bundled HUBS if the file isn't found."""
    import csv
    # look for airports.csv relative to the repo (adjust if your layout differs)
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "..", "..", "aeroflux_parser", "data", "airports.csv"),
        os.path.join(here, "airports.csv"),
    ]
    for path in candidates:
        if os.path.exists(path):
            coords = {}
            with open(path, newline="") as fh:
                for r in csv.DictReader(fh):
                    try:
                        coords[r["icao"]] = (float(r["lat"]), float(r["lon"]))
                    except (ValueError, KeyError):
                        continue
            if coords:
                return coords
    # fallback: the bundled hubs
    return {icao: (lat, lon) for icao, iata, name, lat, lon in HUBS}

def _attach_coords(df: pd.DataFrame) -> pd.DataFrame:
    coords = _airport_coords()
    df["o_lat"] = df["origin"].map(lambda x: coords.get(x, (None, None))[0])
    df["o_lon"] = df["origin"].map(lambda x: coords.get(x, (None, None))[1])
    df["d_lat"] = df["destination"].map(lambda x: coords.get(x, (None, None))[0])
    df["d_lon"] = df["destination"].map(lambda x: coords.get(x, (None, None))[1])
    return df


def _sample_flights(n: int = 240) -> pd.DataFrame:
    random.seed(7)
    now = datetime.now(timezone.utc)
    icaos = [h[0] for h in HUBS]
    rows = []
    for i in range(n):
        o, d = random.sample(icaos, 2)
        car, cname = random.choice(CARRIERS)
        status = random.choices(["ACTIVE", "PLANNED", "COMPLETED"], [0.5, 0.3, 0.2])[0]
        prob = round(min(0.98, max(0.02, random.betavariate(2, 5))), 2)
        olat, olon, oname, oiata = _COORDS[o]
        dlat, dlon, dname, diata = _COORDS[d]
        # crude in-air position: interpolate for ACTIVE
        t = random.random() if status == "ACTIVE" else 0.0
        rows.append({
            "flight_instance_id": f"F{i:04d}",
            "callsign": f"{car}{random.randint(100,9999)}",
            "carrier_icao": car, "carrier_name": cname,
            "origin": o, "destination": d,
            "flight_status": status,
            "hex": f"a{random.randint(0x10000,0xfffff):05x}" if random.random() < 0.4 else None,
            "tail_number": f"N{random.randint(100,999)}{car[0]}" if random.random() < 0.5 else None,
            "scheduled_gate_departure": now - timedelta(minutes=random.randint(-120, 240)),
            "scheduled_gate_arrival": now + timedelta(minutes=random.randint(30, 300)),
            "o_lat": olat, "o_lon": olon, "d_lat": dlat, "d_lon": dlon,
            "cur_lat": olat + (dlat - olat) * t, "cur_lon": olon + (dlon - olon) * t,
            "delay_prob": prob,
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=30)
def kpis() -> dict:
    df = load_flights()
    df["carrier_name"] = df["carrier_name"].fillna("Unknown")
    df["origin"] = df["origin"].fillna("UNK")
    df["destination"] = df["destination"].fillna("UNK")
    df["flight_status"] = df["flight_status"].fillna("UNKNOWN")
    active = int((df["flight_status"] == "ACTIVE").sum()) if "flight_status" in df else 0
    hex_cov = float(df["hex"].notna().mean()) if "hex" in df else 0.0
    at_risk = int((df["delay_prob"] >= 0.5).sum()) if "delay_prob" in df else 0
    return {
        "total": len(df),
        "active": active,
        "hex_cov": hex_cov,
        "at_risk": at_risk,
        "avg_prob": float(df["delay_prob"].mean()) if "delay_prob" in df else 0.0,
        "mode": "LIVE" if is_live() else "SAMPLE",
    }


def airport_name(icao: str) -> str:
    c = _COORDS.get(icao)
    return f"{c[3]} · {c[2]}" if c else icao
