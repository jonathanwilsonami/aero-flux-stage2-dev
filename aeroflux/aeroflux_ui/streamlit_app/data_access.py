"""Data layer for the AeroFlux demo UI.

Reads live state from Postgres when AEROFLUX_DSN is set; otherwise generates
realistic sample flights so the demo always runs (no DB, no network needed).
Everything the pages need comes through here, cached.
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

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


FLIGHTS_LIMIT = int(os.getenv("FLIGHTS_LIMIT", "5000"))
# Tunable without a redeploy. Measured on the Lightsail box (2026-08-09):
# the real 48h-active set is ~1,653 items (DynamoDB's raw ItemCount is much
# larger — ~158k — but that counts everything not yet TTL-swept, not what's
# actually active; the Scan's `updated_at` filter is what matters). Scan
# latency was flat at ~0.4-0.5s from limit=2000 up through 16000, so 5000
# gives headroom over today's active set without any measured cost. Recall
# DynamoDBStateRepository.recent_flight_states: `Limit` bounds items
# EVALUATED per the single-page Scan, not items MATCHED — if the active set
# ever outgrows this, raise it; don't assume a returned count == the true
# active count without checking (see recent_flight_states's own comment).

# 300s, not 30s: sync_cloud only refreshes the cloud backends every SYNC_EVERY
# (default 300s), so re-querying every 30s was pure wasted read cost/latency
# for data that hadn't changed. Aligned to the actual write cadence.
@st.cache_data(ttl=300)
def load_flights() -> pd.DataFrame:
    """The full tracked set (up to FLIGHTS_LIMIT, within STATE_HOURS) with
    origin/dest coords + a delay probability. Live from the configured
    StateStore — Postgres (AEROFLUX_DSN) by default, or DynamoDB when
    STATE_BACKEND=dynamodb — if reachable, else sample. This mixes flights
    at every stage of their lifecycle (planned, airborne, landed hours ago)
    — use `current_flights()` for "what's happening right now"."""
    if is_live():
        try:
            return _load_flights_backend()
        except Exception as e:  # fall back so the demo never dies
            st.warning(f"Live backend unavailable ({e}); showing sample data.")
    return _merge_live_predictions(_sample_flights())


RECENT_HOURS = float(os.getenv("RECENT_HOURS", "2"))
# Tunable without a redeploy (same .env pattern as FLIGHTS_LIMIT/MAP_LIMIT).
#
# Deliberately NOT a filter on the backend's `updated_at` (when the
# state/prediction item was last upserted) — measured live on the box
# (2026-08-09) that updated_at does NOT track real flight activity: median
# age was ~810min (13.5h) for ACTIVE and PLANNED items alike (most are
# untouched leftovers riding out their DynamoDB TTL, not fresh syncs),
# while COMPLETED items looked freshest (one final write on landing).
# Filtering on updated_at recency alone actually made the airborne share
# *worse* in testing (91 ACTIVE of 1655 total -> only 28 of 733 within a
# 1-6h updated_at window, flat regardless of the window size — a hard
# cliff, not a gradual decay).
#
# Instead this filters on the flight's own lifecycle timestamps: is it
# airborne right now (flight_status, the one reliable ground-truth field),
# or near its scheduled/actual departure or arrival. Measured on the box:
# recent_hours=2 -> 183 flights, 93 ACTIVE (51%) — vs. 91 of 1,655 (5%)
# with no filter at all.
def _current_mask(df: pd.DataFrame, recent_hours: float) -> pd.Series:
    now = pd.Timestamp.now(tz="UTC")
    window = pd.Timedelta(hours=recent_hours)

    def _ts(col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series(pd.NaT, index=df.index)
        return pd.to_datetime(df[col], utc=True, errors="coerce")

    def _within(col: str, *, past_only: bool = False) -> pd.Series:
        delta = now - _ts(col)
        if past_only:
            return delta.between(pd.Timedelta(0), window)
        return delta.abs() <= window

    is_active = (df["flight_status"] == "ACTIVE") if "flight_status" in df.columns \
        else pd.Series(False, index=df.index)
    return (is_active.fillna(False)
            | _within("scheduled_gate_departure").fillna(False)
            | _within("actual_off", past_only=True).fillna(False)
            | _within("actual_on", past_only=True).fillna(False))


def current_flights(recent_hours: float | None = None) -> pd.DataFrame:
    """The subset of load_flights() that's genuinely current right now —
    airborne, or within `recent_hours` of scheduled/actual departure or
    arrival. Use this for the map and any "current activity" display;
    load_flights() itself (the fuller tracked set) is unchanged, for
    headline "how much are we tracking" counts."""
    df = load_flights()
    hours = RECENT_HOURS if recent_hours is None else recent_hours
    return df[_current_mask(df, hours)].reset_index(drop=True)


def _load_flights_backend() -> pd.DataFrame:
    _bridge_dsn_env()
    from aeroflux_ml import state_backend_from_env
    repo = state_backend_from_env()
    hours = int(os.getenv("STATE_HOURS", "48"))
    rows = repo.recent_flight_states(hours=hours, limit=FLIGHTS_LIMIT)
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
    # No second .head() cap here on purpose — recent_flight_states(limit=FLIGHTS_LIMIT)
    # above already bounds the row count at the source; re-capping here used to
    # silently override FLIGHTS_LIMIT with a stale hardcoded 4000 regardless of
    # what was configured.
    df = df[df["origin"].notna() & df["destination"].notna()].reset_index(drop=True)
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


# --- Live Overview section (app.py) ----------------------------------------
# All four functions below read from load_flights() -- the same cached, full
# (unfiltered by current_flights()) tracked population the existing KPI row
# and charts already use -- or from the local out/predictions/ snapshot
# directory (never the cloud). Nothing here adds a new backend call; each
# just wraps already-cached/local data in its own ttl=600 cache so repeated
# reruns/widget interactions don't repeat the groupby/histogram work.
RISK_TIER_BOUNDS = (0.3, 0.6)  # <0.3 Low, 0.3-0.6 Medium, >=0.6 High
RISK_TIER_COLORS = {"Low": "#22c55e", "Medium": "#f59e0b", "High": "#ef4444"}


def _risk_tier(prob: pd.Series) -> pd.Series:
    lo, hi = RISK_TIER_BOUNDS
    return pd.cut(prob.fillna(0.25), bins=[-0.01, lo, hi, 1.01],
                  labels=["Low", "Medium", "High"])


@st.cache_data(ttl=600)
def live_overview_metrics() -> dict:
    """Big-number cards for the Live Overview section -- same underlying
    population as kpis() (full tracked set, not current_flights()'s active
    subset), just this section's own 600s cadence and a percentage instead
    of a raw at-risk count."""
    df = load_flights()
    total = len(df)
    active = int((df["flight_status"] == "ACTIVE").sum()) if "flight_status" in df else 0
    prob = df["delay_prob"] if "delay_prob" in df and total else pd.Series(dtype=float)
    high = int((_risk_tier(prob) == "High").sum()) if len(prob) else 0
    return {
        "total": total,
        "active": active,
        "pct_high_risk": (high / total * 100) if total else 0.0,
        "avg_prob": float(prob.mean()) if len(prob) else 0.0,
    }


@st.cache_data(ttl=600)
def carrier_risk_breakdown(top_n: int = 15) -> pd.DataFrame:
    """Top-N carriers by flight count, broken down by risk tier -- for the
    stacked horizontal bar. Empty DataFrame (never an exception) if there's
    no data yet."""
    df = load_flights()
    if df.empty or "carrier_name" not in df or "delay_prob" not in df:
        return pd.DataFrame(columns=["carrier_name", "risk_tier", "flights"])
    d = df.copy()
    d["carrier_name"] = d["carrier_name"].fillna("Unknown")
    d["risk_tier"] = _risk_tier(d["delay_prob"])
    top = d["carrier_name"].value_counts().head(top_n).index
    d = d[d["carrier_name"].isin(top)]
    return (d.groupby(["carrier_name", "risk_tier"], observed=True)
             .size().reset_index(name="flights"))


@st.cache_data(ttl=600)
def risk_distribution() -> pd.Series:
    """delay_prob across the full tracked population, for the risk histogram."""
    df = load_flights()
    if df.empty or "delay_prob" not in df:
        return pd.Series(dtype=float)
    return df["delay_prob"].dropna()


# Same resolution _GOLD_LIVE_DIR uses in pages/4_Model_Performance.py, applied
# to the sibling "predictions" snapshot dir instead of "gold_live" -- see that
# page's _OUT_DIR comment. out/predictions/ (score_live.py's
# _maybe_write_snapshot, ~hourly) is the directory that actually carries
# delay_probability + scored_at; out/gold_live/ (e2e.sh's cmd_archive) is raw
# gold features only, no prediction columns at all -- confirmed against a
# real snapshot file on disk. Both are local-filesystem only (sync_cloud.py
# never syncs either to S3), so reading this one instead adds no cloud cost.
_PREDICTIONS_SNAPSHOT_DIR = (
    Path(os.getenv("AEROFLUX_PREDICTIONS", "out/predictions.parquet")).resolve().parent
    / "predictions"
)


@st.cache_data(ttl=600)
def live_overview_timeseries(hours: int = 24) -> pd.DataFrame:
    """Average delay probability + flight count per hour, over the last
    `hours` hours, from the local out/predictions/*.parquet snapshots.
    Empty DataFrame (never an exception) if the directory or any in-window
    snapshots don't exist yet."""
    empty = pd.DataFrame(columns=["hour", "avg_delay_prob", "flight_count"])
    if not _PREDICTIONS_SNAPSHOT_DIR.exists():
        return empty
    files = sorted(_PREDICTIONS_SNAPSHOT_DIR.glob("predictions_*.parquet"))
    if not files:
        return empty
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=hours)
    # Filenames are predictions_YYYYMMDD_HH[MM].parquet, lexicographically
    # sortable -- files[-(hours+2):] is a cheap pre-filter before the real
    # scored_at cutoff below (throttled to ~1/hour, so this is generous).
    frames = []
    for f in files[-(hours + 2):]:
        try:
            frames.append(pd.read_parquet(f, columns=["flight_key", "delay_probability", "scored_at"]))
        except Exception:
            continue
    if not frames:
        return empty
    d = pd.concat(frames, ignore_index=True)
    d["scored_at"] = pd.to_datetime(d["scored_at"], utc=True, errors="coerce")
    d = d[d["scored_at"] >= cutoff]
    if d.empty:
        return empty
    d["hour"] = d["scored_at"].dt.floor("h")
    out = (d.groupby("hour")
             .agg(avg_delay_prob=("delay_probability", "mean"),
                  flight_count=("flight_key", "nunique"))
             .reset_index().sort_values("hour"))
    return out


@functools.lru_cache(maxsize=1)
def _airport_coords() -> dict:
    """Load full ICAO -> (lat, lon) from the parser's airports.csv (28k airports).
    Falls back to the bundled HUBS if the file isn't found."""
    import csv
    # look for airports.csv relative to the repo (adjust if your layout differs)
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(here, "..", "..", "aeroflux_parser", "data", "airports.csv"),  # local dev checkout
        os.path.join(here, "aeroflux_parser", "data", "airports.csv"),  # Docker image (WORKDIR /app is flat,
                                                                          # can't go two levels up — see Dockerfile)
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
