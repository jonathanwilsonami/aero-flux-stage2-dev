"""
AeroFlux Aviation Operations Analyst -- LangGraph agent.

Graph shape:
    START -> prefetch -> analyst (LLM w/ tool-calling) -> [tools loop] -> guardrail -> END

`prefetch` deterministically pulls flight data in plain Python whenever a
flight number appears in the question, instead of relying on the LLM to
remember to call the right tool -- smaller models are inconsistent about
that, so this removes the failure mode entirely for the core data.
"""
import json
import os
import re
from typing import Annotated, TypedDict

import groq
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langchain_core.messages import AIMessage, AnyMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages

from tools import (document_search, flight_query, model_inference, shap_explanation,
                    event_reconstruction, at_risk_flights)

load_dotenv()

FLIGHT_NUMBER_PATTERN = re.compile(r"\b[A-Z]{2,3}\d{2,4}\b")
# Widened from {2} to {2,3} letters, 2026-08-15: this pattern is really
# "flight-number OR callsign", not just flight-number -- it used to only
# match 2-letter IATA-style numbers ("AA2033"), silently missing every
# 3-letter ICAO callsign ("ENY3350", "AAL1076", "SWA2606"...). Confirmed
# for real: `flight_query("ENY3350")` failed for a flight that was
# genuinely live in DynamoDB -- the regex never even extracted a token
# to look up, prefetch never fired, and the LLM was left to either
# hallucinate "unknown flight" or spontaneously call the tool with the
# wrong parameter (see the fetch call below, and _match_state in
# tools.py, for the second half of that bug).
# Fleet-wide (no specific flight number) risk questions -- deterministically
# prefetched below for the same reason per-flight data is: smaller models
# are unreliable about remembering to call the right tool on their own (see
# agent/EVALUATION.md issue #1), and this is a brand-new tool with no track
# record yet.
FLEET_RISK_KEYWORDS = (
    "most at risk", "highest risk", "highest-risk", "at risk right now",
    "at-risk", "which flights", "what flights are", "riskiest",
)

SYSTEM_PROMPT = """You are the AeroFlux Aviation Operations Analyst.

You answer questions about flight delays and NAS operations for a range of
users -- travelers, airline ops staff, and ATC staff -- by grounding every
factual claim in real retrieved data.

Rules you must follow:
1. If a system message in this conversation already contains "Retrieved
   data for flight ...", that data has ALREADY been fetched for you -- use
   it directly in your answer. Do NOT say you lack real-time data when this
   data is present, and do NOT call flight_query_tool, model_inference_tool,
   shap_explanation_tool, or event_reconstruction_tool again for that
   flight. Same for "Retrieved fleet-wide at-risk flight data" -- if present,
   don't call at_risk_flights_tool again either.
   Retrieved flight/fleet data has a "source" field: "live" means real
   current AeroFlux data (DynamoDB/S3); "sample" means the demo dataset --
   mention which one you're answering from if it's "sample", so nobody
   mistakes a demo answer for a real one.
   shap_explanation_tool's result is the model's REAL INPUT FEATURE VALUES
   for that flight (propagation pressure, demand, weather, rotation) --
   NOT computed SHAP contribution scores. Explain risk in terms of what
   those feature values show (e.g. "the origin is seeing high departure
   demand" or "the inbound aircraft's rotation couldn't be resolved"), but
   never claim you computed or are reporting a SHAP value/importance score
   -- you aren't and didn't. A feature absent from the result was
   genuinely unresolved/unknown for that flight, not zero risk.
2. Relevant document excerpts have ALREADY been retrieved and provided in
   a system message, each one prefixed with its bracketed source name,
   e.g. "[ground_delay_programs.txt] Ground Delay Programs slow...". Base
   conceptual answers on THOSE excerpts only.
   CITATION FORMAT (required, not optional): for every sentence or claim
   in your answer that draws on one of those excerpts, end it with that
   excerpt's exact bracketed tag, copied verbatim -- e.g.:
       "Ground Delay Programs are issued when arrival demand at an
       airport is forecast to exceed capacity [ground_delay_programs.txt]."
   - Cite ONLY source names that literally appear in the excerpts you were
     given for THIS question. Never invent a source name, never cite a
     document or system that wasn't shown to you, and never reuse a
     source name from an earlier turn if it wasn't given to you again now.
   - If a claim doesn't come from a retrieved excerpt (e.g. it comes from
     the retrieved flight data in rule 1, or is your own synthesis), do
     NOT attach a document tag to it.
   - If the provided excerpts don't cover the question, say so plainly
     rather than fabricating an answer or citation.
3. You explain and inform; you do NOT make or recommend operational
   decisions (e.g. do not tell a controller to hold a flight, do not tell an
   airline to cancel a flight). If asked to make such a decision, explain
   the relevant factors instead and state that the decision itself is
   outside your scope.
4. If no data is available for something (e.g. an unknown flight number),
   say so plainly rather than guessing or giving generic advice.
5. Keep answers concise and structured -- lead with the direct answer, then
   the supporting evidence.
6. If everything you need is already in the system messages above (rules
   1-2), just answer in plain text -- do NOT call a function/tool. Only
   call a tool when you genuinely need data that hasn't already been
   provided. Never call a tool named "none" or call a tool just because
   one is available.
"""


@tool
def document_search_tool(query: str) -> str:
    """Search the aviation-document corpus (FAA/SWIM/weather reference docs) for relevant passages."""
    return json.dumps(document_search(query))


@tool
def flight_query_tool(flight_number: str = "") -> str:
    """Look up a flight's current canonical state (status, route, times, position)
    and its delay prediction if scored, by flight number OR callsign (e.g.
    'AA2033' or 'ENY3350' -- either format, this one field). Live current data
    when available, otherwise the demo dataset -- check the result's "source"
    field."""
    ident = flight_number or None
    return json.dumps(flight_query(flight_number=ident, callsign=ident))


@tool
def model_inference_tool(flight_number: str = "") -> str:
    """Get the delay prediction (probability + predicted_delayed) for a flight
    by flight number OR callsign (either format, this one field). Live
    current prediction when available, otherwise the demo dataset -- check
    the result's "source" field."""
    ident = flight_number or None
    return json.dumps(model_inference(flight_number=ident, callsign=ident))


@tool
def shap_explanation_tool(flight_number: str = "") -> str:
    """Get the model's real input feature values behind a flight's delay
    prediction (propagation pressure, demand, weather, rotation) -- these are
    the actual data the model scored the flight on, NOT computed SHAP
    contribution scores. Takes a flight number OR callsign (either format,
    this one field). Live when available, otherwise the demo dataset --
    check the result's "source" field."""
    ident = flight_number or None
    return json.dumps(shap_explanation(flight_number=ident, callsign=ident))


@tool
def event_reconstruction_tool(flight_number: str = "") -> str:
    """Get the recent event/state-change history for a flight, by flight
    number OR callsign (either format, this one field). Demo dataset only --
    no live equivalent exists yet."""
    ident = flight_number or None
    return json.dumps(event_reconstruction(flight_number=ident, callsign=ident))


@tool
def at_risk_flights_tool(limit: int = 10) -> str:
    """Get the flights with the highest predicted delay probability right now,
    across the whole live tracked fleet -- use this for "what/which flights
    are most at risk" style questions that don't name a specific flight. Live
    when available, otherwise ranked from the demo dataset -- check the
    result's "source" field."""
    return json.dumps(at_risk_flights(limit=limit))


ALL_TOOLS = [
    document_search_tool,
    flight_query_tool,
    model_inference_tool,
    shap_explanation_tool,
    event_reconstruction_tool,
    at_risk_flights_tool,
]
TOOLS_BY_NAME = {t.name: t for t in ALL_TOOLS}


class AgentState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]


llm = ChatGroq(
    model=os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile"),
    temperature=0,
).bind_tools(ALL_TOOLS)

# Separate, tool-free instance for the guardrail's final text-only review --
# without this, the model can respond to the guardrail prompt with another
# tool call instead of text, leaving nothing to display.
llm_no_tools = ChatGroq(
    model=os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile"),
    temperature=0,
)


def prefetch_node(state: AgentState) -> AgentState:
    """
    Runs once at the start of each turn. Always retrieves relevant document
    chunks for the question (so the model can't skip document_search and
    fabricate an answer instead), and additionally fetches flight data
    directly in Python if a flight number is mentioned.
    """
    messages = state["messages"]
    if not messages or not isinstance(messages[-1], HumanMessage):
        return {}

    question = messages[-1].content
    if not isinstance(question, str):
        return {}

    new_messages = []

    # Always ground with document search, regardless of topic.
    doc_results = document_search(question, top_k=3)
    if doc_results:
        formatted = "\n\n".join(
            f"[{r['source_name']}] {r['content']}" for r in doc_results
        )
        new_messages.append(
            SystemMessage(
                content=(
                    "Relevant reference document excerpts (already retrieved -- "
                    "cite the bracketed source_name for any claim you use from "
                    f"here, do not fabricate other sources):\n\n{formatted}"
                )
            )
        )

    # Additionally fetch flight data if a flight number is mentioned, or
    # fleet-wide at-risk data if the question looks fleet-wide instead
    # (mutually exclusive -- a specific flight number takes priority).
    match = FLIGHT_NUMBER_PATTERN.search(question.upper())
    if match:
        # The extracted token could be an IATA-style flight number
        # ("AA2033") or an ICAO callsign ("ENY3350", "AAL1076") -- we
        # can't tell which from the string alone, and for some carriers
        # (confirmed live, 2026-08-15: Envoy/"ENY", carrier_iata never
        # resolved) flight_number is genuinely null and callsign is the
        # ONLY field that will ever match. Pass it as both; tools.py's
        # _match_state() tries flight_number first, then callsign, so
        # this can't double-match two different real flights.
        ident = match.group(0)
        data = {
            "flight_query": flight_query(flight_number=ident, callsign=ident),
            "model_inference": model_inference(flight_number=ident, callsign=ident),
            "shap_explanation": shap_explanation(flight_number=ident, callsign=ident),
            "event_reconstruction": event_reconstruction(flight_number=ident, callsign=ident),
        }
        new_messages.append(
            SystemMessage(
                content=(
                    f"Retrieved data for flight {ident} (already "
                    f"fetched -- use it directly, do not call the flight "
                    f"tools again for this flight):\n{json.dumps(data, indent=2)}"
                )
            )
        )
    elif any(kw in question.lower() for kw in FLEET_RISK_KEYWORDS):
        top = at_risk_flights(limit=10)
        new_messages.append(
            SystemMessage(
                content=(
                    "Retrieved fleet-wide at-risk flight data (already fetched -- "
                    "use it directly, do not call at_risk_flights_tool again):\n"
                    f"{json.dumps(top, indent=2)}"
                )
            )
        )

    return {"messages": new_messages}


def analyst_node(state: AgentState) -> AgentState:
    # ALWAYS prepend the real SYSTEM_PROMPT, first, ahead of anything
    # else. This used to be conditional ("only if no SystemMessage exists
    # yet"), which sounds reasonable but was actually a bug: prefetch_node
    # unconditionally adds its own SystemMessage (the doc-excerpts note)
    # before analyst_node ever runs, so the condition was False on every
    # normal turn -- SYSTEM_PROMPT (grounding rules, citation format,
    # the operational-decision guardrail instructions) was silently NEVER
    # sent to the model at all. Found 2026-08-14 while chasing why the
    # real Groq model wasn't emitting [source_name] citation tags: it had
    # never been told the citation format, or any other SYSTEM_PROMPT
    # rule, in the first place. Safe to always prepend -- this builds a
    # local list for this one llm.invoke() call only; it's never written
    # back to graph state (only `response` is, via the add_messages
    # reducer), so looping back through this node from the tools edge
    # can't duplicate it.
    messages = [SystemMessage(content=SYSTEM_PROMPT)] + state["messages"]
    try:
        response = llm.invoke(messages)
    except groq.BadRequestError as e:
        response = _handle_tool_use_failed(e, messages)
    return {"messages": [response]}


# Groq/Llama tool-calling quirk (documented, not rare on this model+host
# combo): the model sometimes emits a malformed tool call -- literally
# `<function=none>...` -- when it means "no tool needed, just answer
# directly" (rule 6 in SYSTEM_PROMPT above nudges against this, but
# doesn't eliminate it -- it's a sampling-level quirk, not a prompt bug).
# Groq's API rejects the malformed call with a 400 `tool_use_failed`
# before it ever reaches us as a normal AIMessage. Found 2026-08-14: the
# model's actual answer is usually sitting right there in the error
# body's `failed_generation` field, discarded for nothing. Two-step
# recovery, neither of which should ever surface as a 500:
#   1. Try to salvage real answer text out of failed_generation (fast, no
#      extra API call).
#   2. If that doesn't look like a real answer, retry once with tools
#      unbound (llm_no_tools) -- prefetch_node already supplied
#      everything this turn needs (doc excerpts + flight data), so
#      tool-calling was never actually required; removing the tool
#      schema removes the failure mode, not just papers over it.
_FUNCTION_TAG_RE = re.compile(r"<function=[^>]*>(.*?)(?:</function>|$)", re.DOTALL)
_MIN_SALVAGED_LEN = 20  # guards against salvaging "none" or empty noise as a real answer


def _salvage_failed_generation(text: str) -> str | None:
    if not text or not text.strip():
        return None
    m = _FUNCTION_TAG_RE.search(text)
    inner = (m.group(1) if m else text).strip()
    if not inner:
        return None
    # The malformed "tool call" body is often JSON with the real answer
    # under a plausible key; if so, prefer that over the raw JSON text.
    try:
        parsed = json.loads(inner)
    except (json.JSONDecodeError, TypeError):
        parsed = None
    if isinstance(parsed, dict):
        for key in ("answer", "content", "response", "text"):
            val = parsed.get(key)
            if isinstance(val, str) and len(val.strip()) >= _MIN_SALVAGED_LEN:
                return val.strip()
        return None  # a dict but no recognizable text field -- don't guess
    return inner if len(inner) >= _MIN_SALVAGED_LEN else None


def _handle_tool_use_failed(e: groq.BadRequestError, messages: list) -> AIMessage:
    body = e.body if isinstance(e.body, dict) else {}
    err = body.get("error", {}) if isinstance(body, dict) else {}
    if err.get("code") == "tool_use_failed":
        salvaged = _salvage_failed_generation(err.get("failed_generation", ""))
        if salvaged:
            return AIMessage(content=salvaged)
    # Salvage didn't produce a usable answer (or this was some other 400) --
    # retry once without tools bound. Let this one raise if it also fails;
    # server.py's ask() handler catches it there and returns a graceful
    # response rather than a bare 500.
    return llm_no_tools.invoke(messages)


def tools_node(state: AgentState) -> AgentState:
    last_message = state["messages"][-1]
    results = []
    for call in last_message.tool_calls:
        fn = TOOLS_BY_NAME[call["name"]]
        output = fn.invoke(call["args"])
        results.append(ToolMessage(content=output, tool_call_id=call["id"]))
    return {"messages": results}


OPERATIONAL_DECISION_TRIGGERS = [
    "you should hold", "should divert", "should cancel", "should ground",
    "recommend holding", "recommend diverting", "recommend canceling",
    "recommend cancelling", "must hold", "must divert", "must cancel",
    "i recommend", "atc should", "the controller should", "the airline should",
]


def guardrail_node(state: AgentState) -> AgentState:
    """
    Passes the analyst's answer through unchanged unless it looks like it
    contains an operational recommendation -- only then do we spend a
    second LLM call rewriting it. This avoids the smaller model
    unreliably "reviewing" (and sometimes gutting) a perfectly good answer
    on every single turn.
    """
    last_message = state["messages"][-1]
    content = last_message.content if isinstance(last_message.content, str) else ""
    lowered = content.lower()

    if not any(trigger in lowered for trigger in OPERATIONAL_DECISION_TRIGGERS):
        return {}

    check_prompt = (
        "Your previous answer appears to recommend or make an operational "
        "decision (e.g. telling ATC or an airline what action to take). "
        "Rewrite it to explain the relevant factors instead, without "
        "prescribing the decision. Keep all the same factual content and "
        "detail -- only remove the directive language."
    )
    messages = state["messages"] + [SystemMessage(content=check_prompt)]
    response = llm_no_tools.invoke(messages)
    return {"messages": [response]}


def should_continue(state: AgentState) -> str:
    last_message = state["messages"][-1]
    if getattr(last_message, "tool_calls", None):
        return "tools"
    return "guardrail"


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("prefetch", prefetch_node)
    graph.add_node("analyst", analyst_node)
    graph.add_node("tools", tools_node)
    graph.add_node("guardrail", guardrail_node)

    graph.set_entry_point("prefetch")
    graph.add_edge("prefetch", "analyst")
    graph.add_conditional_edges("analyst", should_continue, {"tools": "tools", "guardrail": "guardrail"})
    graph.add_edge("tools", "analyst")
    graph.add_edge("guardrail", END)

    return graph.compile()


aeroflux_analyst_graph = build_graph()


if __name__ == "__main__":
    result = aeroflux_analyst_graph.invoke(
        {"messages": [("user", "Is flight AA2033 going to be delayed, and why?")]}
    )
    print(result["messages"][-1].content)
