"""
FastAPI wire layer for the AeroFlux Aviation Operations Analyst.

This is the missing piece from AGENT_INTEGRATION.md #1: a real HTTP
endpoint AeroFlux's Streamlit app can call. It does not contain any graph
logic itself -- it only translates HTTP <-> the existing LangGraph graph
in agent.py, and extracts citations out of the graph's answer text.

Wire contract (matches AGENT_INTEGRATION.md / aeroflux_ui/streamlit_app/
pages/2_Analyst.py's agent_reply()):

    POST /ask
    request:  {"question": str, "history": [{"role": "user"|"assistant", "content": str}, ...]}
    response: {"answer": str, "citations": [str, ...]}

Run locally:
    uvicorn server:app --host 0.0.0.0 --port 8010 --reload

Then point AeroFlux at it:
    AEROFLUX_AGENT_URL=http://localhost:8010/ask streamlit run app.py
    (from aeroflux_ui/streamlit_app/)
"""
from __future__ import annotations

import re
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage

from agent import aeroflux_analyst_graph

app = FastAPI(title="AeroFlux Aviation Operations Analyst")

# Matches the bracketed [source_name] tags the graph's SYSTEM_PROMPT
# instructs the model to cite documents with (see agent.py's prefetch_node
# / SYSTEM_PROMPT rule 2). Restricted to filename-shaped tokens (no
# whitespace allowed) so it doesn't accidentally match ordinary bracketed
# prose the model might write for some other reason -- real source names
# look like "ground_delay_programs.txt" (ingest.py uses os.path.basename),
# never contain spaces.
_CITATION_RE = re.compile(r"\[([A-Za-z0-9_\-./]+)\]")


class HistoryTurn(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class AskRequest(BaseModel):
    question: str
    history: list[HistoryTurn] = []


class AskResponse(BaseModel):
    answer: str
    citations: list[str]


def extract_citations(text: str) -> list[str]:
    """Pull every unique [source_name] tag out of the answer, in
    first-seen order. Deliberately does NOT strip the tags out of
    `answer` -- they're the reader's inline pointer to which claim came
    from which source (the SYSTEM_PROMPT tells the model to write them
    that way on purpose); `citations` is a structured summary alongside
    that, not a replacement for it."""
    seen: list[str] = []
    for m in _CITATION_RE.finditer(text):
        name = m.group(1)
        if name not in seen:
            seen.append(name)
    return seen


def _history_to_messages(history: list[HistoryTurn]) -> list[BaseMessage]:
    messages: list[BaseMessage] = []
    for turn in history:
        if turn.role == "user":
            messages.append(HumanMessage(content=turn.content))
        else:
            messages.append(AIMessage(content=turn.content))
    return messages


def _build_input_messages(req: AskRequest) -> list[BaseMessage]:
    """AeroFlux's caller (2_Analyst.py) appends the user's new message to
    its chat log BEFORE calling us, so `history`'s last entry is usually
    already the current question -- don't double-append it in that case.
    Handles a caller that sends `history` either way (with or without the
    current turn already in it), since the documented contract doesn't
    pin this down explicitly."""
    messages = _history_to_messages(req.history)
    already_included = (
        messages
        and isinstance(messages[-1], HumanMessage)
        and messages[-1].content == req.question
    )
    if not already_included:
        messages.append(HumanMessage(content=req.question))
    return messages


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    messages = _build_input_messages(req)
    result = aeroflux_analyst_graph.invoke({"messages": messages})
    final_message = result["messages"][-1]
    answer = final_message.content if isinstance(final_message.content, str) else str(final_message.content)
    return AskResponse(answer=answer, citations=extract_citations(answer))


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
