# Aviation Operations Analyst — Design & Evaluation Notes

## What this is

The "reasoning" half of AeroFlux Stage 2 (Section 5 of the proposal): a
LangGraph agent that answers flight-delay and NAS-operations questions,
grounded in retrieved data, with citations, and without making operational
decisions. Runs locally today against sample data; designed to swap to
Jon's live pipeline with no interface changes (see `tools.py` for the
`TODO(integration)` markers).

## Architecture

```
START -> prefetch -> analyst (LLM w/ tool-calling) -> [tools loop] -> guardrail -> END
```

- **prefetch**: deterministically retrieves document context (pgvector) and,
  if a flight number is mentioned, flight/prediction/SHAP/event data —
  in plain Python, before the LLM sees the question.
- **analyst**: Groq-hosted Llama 3.3 70B, synthesizes an answer from the
  retrieved context, can still call tools for anything not prefetched.
- **guardrail**: checks the answer for operational-decision language
  (e.g. telling ATC or an airline what to do) and only spends a second LLM
  call rewriting it if triggered — otherwise passes the real answer through
  unchanged.

## Stack

- LLM: Groq API, `llama-3.3-70b-versatile` (swapped in for the free tier;
  Claude API was the original plan per the dev-environment table and
  remains a drop-in swap later)
- Retrieval: pgvector (local Docker Postgres), local `sentence-transformers`
  embeddings (384-dim)
- Orchestration: LangGraph
- UI: Streamlit

## Tools implemented (from the proposal's dev-environment table)

| Tool | Status |
|---|---|
| document_search | Working, cites `source_name` |
| flight_query | Working (sample data) |
| model_inference | Working (sample data) |
| shap_explanation | Working (sample data) |
| event_reconstruction | Working (sample data) |

## Eval findings

Testing against `eval/eval_questions.json` plus ad-hoc questions surfaced
real reliability issues worth documenting for the "system baseline"
evaluation the proposal calls for:

1. **Tool-selection unreliability.** The smaller model (Llama 3.3 70B via
   Groq) frequently skipped calling tools even when explicitly instructed
   to, instead answering with plausible-sounding generic text (e.g. "check
   with the airline"). Forcing a tool call via `tool_choice="required"`
   didn't fully fix it either — the model would call an *unhelpful* tool
   (e.g. document_search) just to satisfy the requirement.
   **Fix:** moved flight-data and document retrieval out of the LLM's
   control entirely — `prefetch_node` fetches both deterministically in
   plain Python before the model is invoked, so grounding no longer
   depends on the model's tool-calling judgment.

2. **Hallucinated source.** Asked how AeroFlux resolves a tail number, the
   model fabricated a plausible-sounding but entirely made-up answer
   ("queries the FAA aircraft registry database") attributed to
   "AeroFlux documentation" — a citation-accuracy failure, one of the
   proposal's own stated eval criteria. **Fix:** same prefetch approach —
   document excerpts are now always provided with explicit `[source_name]`
   tags and an instruction not to cite anything else.

3. **Guardrail over-triggering.** An earlier guardrail design asked the
   model to "review and restate" every answer as a second LLM call; it
   sometimes replaced a good, detailed answer with a one-line disclaimer.
   **Fix:** the guardrail is now keyword-triggered — most turns pass the
   original answer straight through, and the LLM rewrite only runs when
   directive language ("you should hold...", "I recommend...") is actually
   detected.

4. **Citation mismatch (open issue).** In one test, a flight-prediction
   answer (grounded correctly in the SHAP prefetch data) was tagged with a
   document citation (`[swim_identifiers.txt]`) that had nothing to do with
   it. The model conflated "cite your sources" with "always add a
   citation." Not yet fixed — candidate fix is separating the prefetch
   system messages more explicitly by type so the model can't cross-cite.

5. **Scope gap: no fleet-wide queries.** All 5 tools are single-flight
   lookups (matches the proposal's tool list). Questions like "how many
   flights are in the dataset" or "list the 10 earliest flights" have no
   tool to answer from — the model sometimes reasoned from flights
   mentioned earlier in conversation history rather than admitting it
   couldn't answer, which looks correct but isn't a real capability. A
   `list_flights` tool would be needed to support this class of question
   for real.

## What's not yet done

- Real FAA/SWIM/NOAA documents (currently 3 short original placeholder docs)
- Integration with Jon's live DynamoDB/S3 backend (`tools.py` TODOs)
- Formal scoring against `eval/eval_questions.json` (so far manual/ad-hoc)
- Latency measurement
