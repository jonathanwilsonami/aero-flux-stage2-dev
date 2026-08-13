"""
AeroFlux Aviation Operations Analyst -- tool implementations.

Each tool has a narrow, typed input/output contract on purpose: when Jon's
live pipeline (DynamoDB / Mongo state store, real XGBoost model, real SHAP
service) is ready, only the *inside* of these functions needs to change --
the LangGraph graph in agent.py never has to know the difference.
"""
import os
import json
from typing import Optional

import psycopg
from embeddings import embed_texts

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://aeroflux:aeroflux_local_dev@localhost:5432/aeroflux_rag",
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
# Tool 1: Document search (RAG over the aviation-doc corpus via pgvector)
# ---------------------------------------------------------------------------
def document_search(query: str, top_k: int = 4) -> list[dict]:
    """
    Semantic search over the ingested aviation-document corpus.
    Returns [{source_name, chunk_index, content, distance}, ...] so the
    synthesis step can cite source_name for every claim it makes.
    """
    query_embedding = embed_texts([query])[0]
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
                (query_embedding, top_k),
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
    Look up a flight's current canonical state.
    TODO(integration): swap _load_flights() for a real call against Jon's
    current-state store (DynamoDB in the cloud), keyed the same way.
    """
    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "message": "No matching flight in the current dataset."}

    return {
        "found": True,
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
    Return the delay prediction for a flight.
    TODO(integration): swap for a real call to the XGBoost inference service
    described in the proposal (Spark foreachBatch / FastAPI endpoint).
    """
    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "message": "No matching flight in the current dataset."}

    pred = flight["prediction"]
    return {
        "found": True,
        "flight_number": flight["flight_number"],
        "delay_probability_15min": pred["delay_probability_15min"],
        "predicted_delay_minutes": pred["predicted_delay_minutes"],
        "model_version": pred["model_version"],
    }


# ---------------------------------------------------------------------------
# Tool 4: SHAP explanation
# ---------------------------------------------------------------------------
def shap_explanation(flight_number: Optional[str] = None, callsign: Optional[str] = None) -> dict:
    """
    Return the top SHAP feature contributions behind a flight's prediction.
    TODO(integration): swap for a real call to the SHAP explanation service.
    """
    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "message": "No matching flight in the current dataset."}

    return {
        "found": True,
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
    history / bronze tier (48h replayable window).
    """
    flight = _find_flight(flight_number=flight_number, callsign=callsign)
    if not flight:
        return {"found": False, "message": "No matching flight in the current dataset."}

    return {
        "found": True,
        "flight_number": flight["flight_number"],
        "events": flight["event_history"],
    }


# Registry used by agent.py to expose these as LangGraph/Claude tool-calling tools
TOOL_REGISTRY = {
    "document_search": document_search,
    "flight_query": flight_query,
    "model_inference": model_inference,
    "shap_explanation": shap_explanation,
    "event_reconstruction": event_reconstruction,
}
