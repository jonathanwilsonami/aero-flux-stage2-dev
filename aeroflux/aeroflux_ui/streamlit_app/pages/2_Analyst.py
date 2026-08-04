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


def agent_reply(question: str, history: list[dict]) -> str:
    """Call the real agent if configured, else answer locally from the data."""
    if AGENT_URL:
        try:
            import requests
            r = requests.post(AGENT_URL, json={"question": question, "history": history},
                              timeout=60)
            r.raise_for_status()
            return r.json().get("answer", "(no answer)")
        except Exception as e:
            return f"⚠️ Agent endpoint error: {e}"
    return _local_reply(question)


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


# --- chat state ------------------------------------------------------------
if "messages" not in st.session_state:
    st.session_state.messages = [{
        "role": "assistant",
        "content": "Hi — I'm the AeroFlux operations analyst. Ask me about current "
                   "delay risk, busy routes, or a specific flight.",
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

prompt = st.chat_input("Ask the analyst…") or st.session_state.pop("_pending", None)
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar="🧑‍✈️"):
        st.markdown(prompt)
    with st.chat_message("assistant", avatar="✈️"):
        with st.spinner("Thinking…"):
            ans = agent_reply(prompt, st.session_state.messages)
        st.markdown(ans)
    st.session_state.messages.append({"role": "assistant", "content": ans})
