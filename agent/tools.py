"""
AeroFlux Aviation Operations Analyst -- tool implementations.

Level 3 (2026-08-14): flight_query, model_inference, and shap_explanation
read LIVE AeroFlux data -- current state + predictions from DynamoDB, gold
features from S3 -- through the SAME `aeroflux_ml.io` abstraction
(`state_backend_from_env()` / `lake_backend_from_env()`) the Streamlit
app's `data_access.py` uses. Same read-only `aeroflux-app` credentials,
same bounded-Scan discipline (never an unbounded Scan -- see CLAUDE.md's
DynamoDB cost gotcha), same "always demos" fallback: if the cloud backend
isn't configured (STATE_BACKEND != dynamodb) or a read fails for any
reason, every tool falls back to the local `data/sample_flights.json`
dataset, same as before. event_reconstruction stays sample-only -- SWIM's
raw event history (bronze, `swim.raw_messages`) isn't exposed via
DynamoDB/S3 at all (see AeroFlux_DataSchemas.md's own [AGENT] scoping),
so there's no live equivalent to wire up yet.
"""
import os
import json
import time
from typing import Optional

import psycopg
from embeddings import embed_texts

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://aeroflux:aeroflux_local_dev@localhost:5433/aeroflux_rag",
)
FLIGHTS_PATH = os.path.join(os.path.dirname(__file__), "data", "sample_flights.json")


def _load_flights() -> list[dict]:
    with open(FLIGHTS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_flight(flight_number: Optional[str] = None, callsign: Optional[str] = None) -> Optional[dict]:
    flights = _load_flights()
    for fl in flights:
        if flight_number and fl["flight_number"].upper() == flight_number.upper():
            return fl
        if callsign and fl["callsign"].upper() == callsign.upper():
            return fl
    return None


# ---------------------------------------------------------------------------
# Cloud read layer (Level 3) -- read-only, bounded, short-TTL cached,
# never raises (every caller gets None back on any failure and falls
# through to the sample-data path).
# ---------------------------------------------------------------------------
_STATE_LIMIT = int(os.environ.get("AGENT_STATE_LIMIT", "3000"))
# Same bounded-Scan reasoning as data_access.py's FLIGHTS_LIMIT: `Limit`
# caps items EVALUATED, not matched -- this is the actual cost/latency
# control, not a "how many flights exist" guess. Kept smaller than the
# UI's default (5000) since the agent only ever needs one match or a
# top-N ranking, not a full map render.
_CACHE_TTL = int(os.environ.get("AGENT_CACHE_TTL", "120"))
# Short-lived in-process cache so a single conversation (prefetch_node
# calling flight_query + model_inference + shap_explanation back to back
# for the same flight, or a follow-up question) doesn't re-Scan/re-GET
# for data that hasn't changed -- same spirit as the DynamoDB write-cost
# incident in CLAUDE.md's Gotchas, applied to reads: don't pay for the
# same fetch twice in a few seconds.
_state_cache: dict = {"at": 0.0, "rows": None}
_gold_cache: dict = {"at": 0.0, "df": None}


def _cloud_enabled() -> bool:
    """Same signal data_access.py's is_live() uses for the DynamoDB
    branch -- STATE_BACKEND=dynamodb means cloud is configured. Doesn't
    guarantee reachability; every read below is still wrapped and falls
    back on failure regardless of this flag."""
    return os.environ.get("STATE_BACKEND", "postgres").lower() == "dynamodb"


def _num(v, cast=float):
    """DynamoDB's Number type always comes back as decimal.Decimal via
    boto3's resource API (even for values that started as plain ints,
    e.g. predicted_delayed) -- json.dumps() can't serialize Decimal, and
    callers want real float/int/bool. None-safe."""
    if v is None:
        return None
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


def _prob(v) -> Optional[float]:
    """Same as _num(v) but rounded to 4dp -- Decimal's full precision
    (e.g. 0.7087000012397766, a float-rounding artifact from how the
    probability got stored, not real precision) reads badly once it's
    echoed back verbatim in a chat answer."""
    n = _num(v)
    return round(n, 4) if n is not None else None


def _bulk_state() -> Optional[list[dict]]:
    """Bounded, cached read of current flight state + embedded predictions
    (DynamoDB's disjoint-attribute-groups design puts both on the same
    item -- see aeroflux_ml/io.py's DynamoDBStateRepository docstring).
    Returns None (never raises) if cloud isn't configured or the read
    fails for any reason -- callers treat None as "fall back to sample"."""
    if not _cloud_enabled():
        return None
    now = time.time()
    if _state_cache["rows"] is not None and now - _state_cache["at"] < _CACHE_TTL:
        return _state_cache["rows"]
    try:
        from aeroflux_ml import state_backend_from_env
        repo = state_backend_from_env()
        rows = repo.recent_flight_states(hours=48, limit=_STATE_LIMIT)
    except Exception:
        return None
    _state_cache["at"], _state_cache["rows"] = now, rows
    return rows


def _bulk_gold():
    """Bounded (single S3 GET, not per-flight) + cached read of gold
    features -- there's no per-flight-key indexed read available (the
    lake isn't partitioned that way), so this is a full-table read
    filtered client-side, same tradeoff data_access.py's own gold/
    predictions reads already make. Returns None (never raises) on any
    failure."""
    if not _cloud_enabled():
        return None
    now = time.time()
    if _gold_cache["df"] is not None and now - _gold_cache["at"] < _CACHE_TTL:
        return _gold_cache["df"]
    try:
        from aeroflux_ml import lake_backend_from_env
        df = lake_backend_from_env().read_parquet("gold/gold_features.parquet")
    except Exception:
        return None
    _gold_cache["at"], _gold_cache["df"] = now, df
    return df


def _match_state(rows: list[dict], flight_number: Optional[str], callsign: Optional[str]) -> Optional[dict]:
    fn = (flight_number or "").upper()
    cs = (callsign or "").upper()
    for r in rows:
        if fn and str(r.get("flight_number") or "").upper() == fn:
            return r
        if cs and str(r.get("callsign") or "").upper() == cs:
            return r
    return None


# ---------------------------------------------------------------------------
# Tool 1: Document search (RAG over the aviation-doc corpus via pgvector)
# ---------------------------------------------------------------------------
def document_search(query: str, top_k: int = 4) -> list[dict]:
    """
    Semantic search over the ingested aviation-document corpus.
    Returns [{source_name, chunk_index, content, distance}, ...] so the
    synthesis step can cite source_name for every claim it makes.
    """
    query_embedding = embed_texts([query])[0]
    # psycopg's default Python-list adapter formats a list as a Postgres
    # ARRAY literal ("{1,2,3}"), not pgvector's own "[1,2,3]" input syntax
    # -- binding the raw list here silently produced zero rows on every
    # query (found while wiring the HTTP server, 2026-08-13; verified by
    # comparing raw SQL against the manually-built literal below, which
    # returns real results). Build the literal explicitly instead of
    # relying on psycopg's default adaptation.
    vector_literal = "[" + ",".join(str(x) for x in query_embedding) + "]"
    with psycopg.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_name, chunk_index, content,
                       embedding <=> %s::vector AS distance
                FROM doc_chunks
                ORDER BY distance ASC
                LIMIT %s
                """,
                (vector_literal, top_k),
            )
            rows = cur.fetchall()
    return [
        {"source_name": r[0], "chunk_index": r[1], "content": r[2], "distance": float(r[3])}
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Tool 2: Flight / operational-data query
# ---------------------------------------------------------------------------
def flight_query(flight_number: Optional[str] = None, callsign: Optional[str] = None) -> dict:
    """
    Look up a flight's current canonical state (and, if scored, its delay
    prediction) -- live from DynamoDB when STATE_BACKEND=dynamodb, else
    the local sample dataset.
    """
    rows = _bulk_state()
    if rows is not None:
        match = _match_state(rows, flight_number, callsign)
        if not match:
            return {"found": False, "source": "live",
                    "message": f"No matching flight in the live current-state set "
                               f"(checked up to {len(rows)} recently-active flights)."}
        return {
            "found": True,
            "source": "live",
            "flight_instance_id": match.get("flight_key") or match.get("flight_instance_id"),
            "callsign": match.get("callsign"),
            "flight_number": match.get("flight_number"),
            "carrier_name": match.get("carrier_name"),
            "origin": match.get("origin"),
            "destination": match.get("destination"),
            "aircraft_type": match.get("aircraft_type"),
            "scheduled_gate_departure": match.get("scheduled_gate_departure"),
            "estimated_arrival": match.get("estimated_arrival"),
            "flight_status": match.get("flight_status"),
            "resolution_status": match.get("resolution_status"),
            "last_latitude": _num(match.get("last_latitude")),
            "last_longitude": _num(match.get("last_longitude")),
            # Embedded on the same item (DynamoDB's disjoint-attribute
            # design) -- null if this flight hasn't been scored yet.
            "delay_probability_15min": _prob(match.get("delay_probability")),
            "predicted_delayed": _num(match.get("predicted_delayed"), cast=int),
        }

    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "source": "sample", "message": "No matching flight in the current dataset."}
    return {
        "found": True,
        "source": "sample",
        "flight_instance_id": flight["flight_instance_id"],
        "callsign": flight["callsign"],
        "flight_number": flight["flight_number"],
        "carrier_name": flight["carrier_name"],
        "origin": flight["origin"],
        "destination": flight["destination"],
        "aircraft_type": flight["aircraft_type"],
        "scheduled_gate_departure": flight["scheduled_gate_departure"],
        "estimated_arrival": flight["estimated_arrival"],
        "flight_status": flight["flight_status"],
        "resolution_status": flight["resolution_status"],
    }


# ---------------------------------------------------------------------------
# Tool 3: Model inference (delay prediction)
# ---------------------------------------------------------------------------
def model_inference(flight_number: Optional[str] = None, callsign: Optional[str] = None) -> dict:
    """
    Return the delay prediction for a flight -- live from DynamoDB
    (embedded on the same item as current state) when configured, else
    the local sample dataset.
    """
    rows = _bulk_state()
    if rows is not None:
        match = _match_state(rows, flight_number, callsign)
        if not match:
            return {"found": False, "source": "live",
                    "message": f"No matching flight in the live current-state set "
                               f"(checked up to {len(rows)} recently-active flights)."}
        prob = _prob(match.get("delay_probability"))
        if prob is None:
            return {"found": True, "source": "live",
                    "message": "Flight found, but it hasn't been scored yet (no prediction on record)."}
        return {
            "found": True,
            "source": "live",
            "flight_number": match.get("flight_number") or match.get("callsign"),
            "delay_probability_15min": prob,
            "predicted_delayed": _num(match.get("predicted_delayed"), cast=int),
            "model_version": match.get("model_version"),
        }

    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "source": "sample", "message": "No matching flight in the current dataset."}
    pred = flight["prediction"]
    return {
        "found": True,
        "source": "sample",
        "flight_number": flight["flight_number"],
        "delay_probability_15min": pred["delay_probability_15min"],
        "predicted_delay_minutes": pred["predicted_delay_minutes"],
        "model_version": pred["model_version"],
    }


# ---------------------------------------------------------------------------
# Tool 4: Feature explanation ("why is this flight at risk")
# ---------------------------------------------------------------------------
# Human-readable labels for the gold feature columns worth surfacing to an
# analyst -- matches AeroFlux_DataDictionary.md's grouping (propagation/
# rotation, demand, weather). Excludes pure model-input encodings that
# wouldn't mean anything in prose (sched_dep_dow, is_weekend, etc.).
_GOLD_FEATURE_LABELS = {
    "prev_leg_arr_delay_min": "previous leg's arrival delay (propagation pressure input)",
    "turnaround_buffer_min": "scheduled turnaround buffer before this leg",
    "legs_into_day": "leg number for this airframe today",
    "inbound_resolved": "whether the inbound aircraft/rotation was resolved (1) or unknown (0)",
    "origin_dep_demand": "departures from origin in the rolling window",
    "origin_recent_dep_delay": "mean recent departure delay at origin",
    "dest_arr_demand": "arrivals into destination in the rolling window",
    "dest_recent_arr_delay": "mean recent arrival delay at destination",
    "origin_wx_wind_kt": "origin wind speed (kt)",
    "origin_wx_ifr": "origin IFR/low-visibility conditions (1=yes)",
    "dest_wx_wind_kt": "destination wind speed (kt)",
    "dest_wx_ifr": "destination IFR/low-visibility conditions (1=yes)",
}


def shap_explanation(flight_number: Optional[str] = None, callsign: Optional[str] = None) -> dict:
    """
    Explain why a flight is predicted delayed or not, using the REAL gold
    feature values the model actually scored it on -- propagation
    pressure, demand, weather, rotation (live from S3 when configured, via
    a two-step resolve: DynamoDB state lookup for identity -> flight_key,
    then that flight_key against gold_features.parquet). These are the
    model's real INPUT VALUES, not computed SHAP per-feature contribution
    scores (that needs the loaded model + an explainer, not done here) --
    named `shap_explanation` for continuity with the original tool/prompt
    naming, but the response is explicit about what it actually contains
    so nothing downstream (the LLM, the reader) mistakes one for the other.
    Missing values are genuinely missing (not zero) per feature_prep's
    fill policy -- e.g. inbound_resolved=0 for most live flights just
    means the rotation channel couldn't be resolved, not "no delay risk
    from rotation." Falls back to the sample dataset's mock
    shap_top_features when cloud isn't configured or the flight has no
    gold row yet.
    """
    state_rows = _bulk_state()
    if state_rows is not None:
        match = _match_state(state_rows, flight_number, callsign)
        if not match:
            return {"found": False, "source": "live",
                    "message": f"No matching flight in the live current-state set "
                               f"(checked up to {len(state_rows)} recently-active flights)."}
        flight_key = match.get("flight_key") or match.get("flight_instance_id")
        gold = _bulk_gold()
        if gold is None:
            return {"found": True, "source": "live",
                    "message": "Flight found, but gold features are unavailable right now."}
        import polars as pl
        hit = gold.filter(pl.col("flight_key") == str(flight_key))
        if hit.height == 0:
            return {"found": True, "source": "live",
                    "message": "Flight found in current state, but no gold feature row "
                               "recorded for it yet (it may not have reached scoring time)."}
        row = hit.row(0, named=True)
        features = {}
        for col, label in _GOLD_FEATURE_LABELS.items():
            val = row.get(col)
            if val is not None:
                features[col] = {"value": val, "meaning": label}
        return {
            "found": True,
            "source": "live",
            "flight_key": flight_key,
            "note": "these are the model's real input feature values for this flight, "
                    "not computed SHAP contribution scores -- a field absent here means "
                    "that input was genuinely unresolved/missing for this flight (not zero).",
            "features": features,
        }

    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "source": "sample", "message": "No matching flight in the current dataset."}
    return {
        "found": True,
        "source": "sample",
        "flight_number": flight["flight_number"],
        "top_features": flight["shap_top_features"],
    }


# ---------------------------------------------------------------------------
# Tool 5: Event reconstruction (recent history for a flight)
# ---------------------------------------------------------------------------
def event_reconstruction(flight_number: Optional[str] = None, callsign: Optional[str] = None) -> dict:
    """
    Return the recent event/state-change history for a flight.
    TODO(integration): swap for a real query against the Kafka-backed event
    history / bronze tier (48h replayable window) -- unlike state/
    predictions/gold, this isn't exposed via DynamoDB or S3 today (see
    AeroFlux_DataSchemas.md's own scoping: swim.raw_messages is bronze,
    not marked [AGENT]), so there's no live path to wire up yet. Sample
    data only, regardless of STATE_BACKEND.
    """
    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "message": "No matching flight in the current dataset."}

    return {
        "found": True,
        "flight_number": flight["flight_number"],
        "events": flight["event_history"],
    }


# ---------------------------------------------------------------------------
# Tool 6: Fleet-wide "what's most at risk right now" -- new in Level 3.
# Closes the exact gap Ryan's own EVALUATION.md flagged ("Scope gap: no
# fleet-wide queries... A list_flights tool would be needed").
# ---------------------------------------------------------------------------
def at_risk_flights(limit: int = 10) -> dict:
    """
    The flights with the highest predicted delay probability right now,
    across the live tracked set -- live from DynamoDB when configured,
    else ranked from the sample dataset.
    """
    rows = _bulk_state()
    if rows is not None:
        scored = [(r, _prob(r.get("delay_probability"))) for r in rows]
        scored = [(r, p) for r, p in scored if p is not None]
        scored.sort(key=lambda rp: rp[1], reverse=True)
        top = scored[:limit]
        return {
            "found": True,
            "source": "live",
            "count_considered": len(rows),
            "count_with_prediction": len(scored),
            "flights": [
                {
                    "callsign": r.get("callsign"),
                    "flight_number": r.get("flight_number"),
                    "origin": r.get("origin"),
                    "destination": r.get("destination"),
                    "flight_status": r.get("flight_status"),
                    "delay_probability_15min": p,
                }
                for r, p in top
            ],
        }

    flights = sorted(
        _load_flights(),
        key=lambda fl: fl.get("prediction", {}).get("delay_probability_15min", 0.0),
        reverse=True,
    )[:limit]
    return {
        "found": True,
        "source": "sample",
        "count_considered": len(_load_flights()),
        "flights": [
            {
                "callsign": fl["callsign"],
                "flight_number": fl["flight_number"],
                "origin": fl["origin"],
                "destination": fl["destination"],
                "flight_status": fl["flight_status"],
                "delay_probability_15min": fl["prediction"]["delay_probability_15min"],
            }
            for fl in flights
        ],
    }


# Registry used by agent.py to expose these as LangGraph/Claude tool-calling tools
TOOL_REGISTRY = {
    "document_search": document_search,
    "flight_query": flight_query,
    "model_inference": model_inference,
    "shap_explanation": shap_explanation,
    "event_reconstruction": event_reconstruction,
    "at_risk_flights": at_risk_flights,
}
