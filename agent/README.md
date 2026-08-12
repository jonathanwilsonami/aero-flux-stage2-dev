# AeroFlux Aviation Operations Analyst — Prototype

This is the "reasoning" half of AeroFlux Stage 2: a LangGraph agent that
answers flight-delay and NAS-operations questions by calling tools and
citing sources, per Section 5 ("AI orchestration prototype") of the project
proposal. It runs entirely locally today; the parts meant to be swapped for
the live/cloud versions later are marked `TODO(integration)` in `tools.py`.

## What's here

| File | Purpose |
|---|---|
| `agent.py` | LangGraph graph: analyst (tool-calling LLM) → tools → guardrail |
| `tools.py` | The 5 tools: document_search, flight_query, model_inference, shap_explanation, event_reconstruction |
| `embeddings.py` | One place to plug in a real embedding model (local or Bedrock) |
| `ingest.py` | Chunks + embeds `data/sample_docs/*.txt` into pgvector |
| `app.py` | Streamlit chat UI |
| `data/sample_flights.json` | Mock canonical flight records (matches the proposal's Listing 2 schema) + mock predictions/SHAP/event history |
| `data/sample_docs/` | Small sample aviation-reference corpus for RAG (replace with real FAA/SWIM docs) |
| `eval/eval_questions.json` | Starter eval set for groundedness / citation / tool-selection / guardrail checks |

## Setup

1. **Start Postgres+pgvector:**
   ```bash
   docker compose up -d
   ```

2. **Install deps:**
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Pick an embedding provider** in `embeddings.py` (local `sentence-transformers`
   for a zero-cost offline loop, or Bedrock Titan to match the AWS stack).
   If you change the embedding dimension, update `sql/init.sql`'s
   `VECTOR(1536)` to match, then restart the DB container so it re-runs the
   init script (or apply the DDL manually if the volume already exists).

4. **Copy `.env.example` to `.env`** and fill in your `GROQ_API_KEY` (free, no credit card, from console.groq.com).

5. **Ingest the sample doc corpus:**
   ```bash
   python ingest.py
   ```

6. **Run the app:**
   ```bash
   streamlit run app.py
   ```

## Swapping in real data later (Jon's integration step)

Every flight-related tool in `tools.py` reads from
`data/sample_flights.json` via `_load_flights()`. To point at the real
pipeline, replace `_load_flights()` / `_find_flight()` with calls to the
DynamoDB current-state store (or whatever the finalized backend is) — the
function signatures and return shapes should stay the same so `agent.py`
never needs to change.

## Evaluating

Run each question in `eval/eval_questions.json` through the agent and check:
- **Groundedness** — every factual claim traces back to a tool result
- **Citation accuracy** — `document_search` claims name the correct `source_name`
- **Tool selection** — the agent called the tools listed in `expected_tools`
- **Guardrail behavior** — q4 in particular should never issue an operational recommendation
