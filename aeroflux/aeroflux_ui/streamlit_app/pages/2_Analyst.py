"""Aviation Operations Analyst — conversational agent.

Ships with a lightweight local responder that answers from the live/sample flight
data, so the section demos immediately. Point AEROFLUX_AGENT_URL at your real
LangGraph/RAG agent to swap it in (the wire format is documented below).
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from data_access import load_flights, kpis

st.set_page_config(page_title="AeroFlux · Analyst", page_icon="💬", layout="wide")
st.title("💬 Aviation Operations Analyst")
st.caption("Ask about current operations. Answers are grounded in flight data + retrieved documents.")

AGENT_URL = os.getenv("AEROFLUX_AGENT_URL")


def agent_reply(question: str, history: list[dict]) -> tuple[str, list[str]]:
    """Call the real agent if configured, else answer locally from the data.

    Returns (answer, citations) -- citations is always a list (empty when
    there's nothing to cite, e.g. the local responder or an agent answer
    that didn't cite anything).

    AEROFLUX_AGENT_URL unset keeps the existing "always demos" behavior
    (local deterministic responder) -- that's a deliberate, pre-existing
    fallback (see the sidebar's "local responder" status), not something
    this change should take away. The new graceful-failure path is for
    when an agent WAS configured but couldn't actually be reached (down,
    wrong URL, timeout, bad response) -- that's a real failure, and it
    must never crash the page.
    """
    if AGENT_URL:
        try:
            import requests
            # Only send {role, content} per turn -- the documented wire
            # contract -- even though our own session-state dicts may
            # carry extra local-only keys (e.g. citations on past turns).
            wire_history = [{"role": h["role"], "content": h["content"]} for h in history]
            r = requests.post(AGENT_URL, json={"question": question, "history": wire_history},
                              timeout=60)
            r.raise_for_status()
            data = r.json()
            return data.get("answer", "(no answer)"), list(data.get("citations") or [])
        except Exception as e:
            return (
                f"⚠️ **Agent unavailable** — couldn't reach `{AGENT_URL}` "
                f"({type(e).__name__}: {e}). Check the agent server is running "
                f"and AEROFLUX_AGENT_URL is correct.",
                [],
            )
    return _local_reply(question), []


def _local_reply(q: str) -> str:
    """A grounded, deterministic fallback so the demo works with no LLM."""
    df = load_flights()
    k = kpis()
    ql = q.lower()
    if any(w in ql for w in ("risk", "delay", "worst", "at-risk", "problem")):
        top = (df.assign(route=df["origin"] + "→" + df["destination"])
                 .sort_values("delay_prob", ascending=False)
                 .head(5)[["callsign", "route", "delay_prob"]])
        lines = "\n".join(f"- **{r.callsign}** {r.route} — {r.delay_prob*100:.0f}% risk"
                          for r in top.itertuples())
        return (f"Right now {k['at_risk']} flights are at elevated delay risk "
                f"(≥50%). The highest-risk flights:\n\n{lines}\n\n"
                f"_This is the local data responder; connect the RAG agent for "
                f"document-grounded reasoning._")
    if any(w in ql for w in ("how many", "count", "total", "airborne", "status")):
        return (f"Tracking **{k['total']:,}** flights — {k['active']:,} airborne, "
                f"{k['at_risk']:,} at delay risk, airframe (hex) coverage "
                f"{k['hex_cov']*100:.0f}%.")
    if any(w in ql for w in ("route", "busiest", "hub")):
        r = (df.assign(route=df["origin"] + "→" + df["destination"])
               .groupby("route").size().sort_values(ascending=False).head(3))
        return "Busiest routes: " + ", ".join(f"{idx} ({n})" for idx, n in r.items()) + "."
    return ("I can answer about current delay risk, flight counts, busiest routes, "
            "or a specific callsign. Try: *“what are the highest-risk flights?”* "
            "Connect the RAG agent (`AEROFLUX_AGENT_URL`) for document-grounded answers.")


def _render_citations(citations: list[str]) -> None:
    if citations:
        st.caption("📎 Sources: " + ", ".join(f"`{c}`" for c in citations))


# --- chat state ------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "assistant",
        "content": "Hi — I'm the AeroFlux operations analyst. Ask me about current "
                   "delay risk, busy routes, or a specific flight.",
        "citations": [],
    }]

with st.sidebar:
    st.subheader("Agent")
    st.write("**Live agent:**", "🟢 connected" if AGENT_URL else "🟡 local responder")
    st.caption("Set `AEROFLUX_AGENT_URL` to your LangGraph/RAG endpoint.")
    for ex in ["What are the highest-risk flights right now?",
               "How many flights are airborne?",
               "What are the busiest routes?"]:
        if st.button(ex, use_container_width=True):
            st.session_state._pending = ex

for m in st.session_state.messages:
    with st.chat_message(m["role"], avatar="✈️" if m["role"] == "assistant" else "🧑‍✈️"):
        st.markdown(m["content"])
        if m["role"] == "assistant":
            _render_citations(m.get("citations", []))

prompt = st.chat_input("Ask the analyst…") or st.session_state.pop("_pending", None)
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar="🧑‍✈️"):
        st.markdown(prompt)
    with st.chat_message("assistant", avatar="✈️"):
        with st.spinner("Thinking…"):
            ans, citations = agent_reply(prompt, st.session_state.messages)
        st.markdown(ans)
        _render_citations(citations)
    st.session_state.messages.append({"role": "assistant", "content": ans, "citations": citations})
