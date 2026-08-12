"""
Streamlit chat UI for the AeroFlux Aviation Operations Analyst.

Local dev:
    streamlit run app.py

This is intentionally a thin wrapper around agent.py -- when this gets
ported into LightSail (or into the shared AeroFlux app later), only this
file should need to change, not the graph logic.
"""
import streamlit as st
from langchain_core.messages import AIMessage, HumanMessage

from agent import aeroflux_analyst_graph

st.set_page_config(page_title="AeroFlux Aviation Operations Analyst", page_icon="\u2708\ufe0f")
st.title("\u2708\ufe0f AeroFlux Aviation Operations Analyst")
st.caption(
    "Ask about a flight's delay status, why a delay is predicted, or general "
    "NAS operations questions. Answers are grounded in tool calls and cite "
    "their sources. This prototype uses sample data -- not yet the live feed."
)

if "history" not in st.session_state:
    st.session_state.history = []

for msg in st.session_state.history:
    role = "user" if isinstance(msg, HumanMessage) else "assistant"
    with st.chat_message(role):
        st.markdown(msg.content)

user_input = st.chat_input("e.g. Is flight AA2033 going to be delayed, and why?")

if user_input:
    st.session_state.history.append(HumanMessage(content=user_input))
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Checking tools and sources..."):
            result = aeroflux_analyst_graph.invoke({"messages": st.session_state.history})
            final_message = result["messages"][-1]
            st.markdown(final_message.content)

    st.session_state.history.append(AIMessage(content=final_message.content))

with st.sidebar:
    st.subheader("Sample questions")
    st.markdown(
        "- Is flight AA2033 going to be delayed?\n"
        "- Why is UA455 predicted to be delayed?\n"
        "- What is a Ground Delay Program?\n"
        "- How does AeroFlux resolve an aircraft's tail number?\n"
        "- What's the recent event history for DL817?"
    )
    st.subheader("Status")
    st.markdown("Data source: **local sample dataset** (`data/sample_flights.json`)")
    st.markdown("Doc corpus: **pgvector**, ingested from `data/sample_docs/`")
