# AeroFlux — Agent Integration

For Ryan's RAG/reasoning layer. Short on purpose — the data *contract*
(schemas, columns, sample records) already lives in
`AeroFlux_DataSchemas.md`; this doc covers the three things that aren't
there: the wire protocol the UI actually calls, how to read *production*
data specifically (S3 + DynamoDB, not local Postgres), and the boundary
between the two codebases.

Everything below reflects what's actually implemented today (2026-08-14) —
**including §2 as of the same day: Level 3, live cloud data access, is
now real, not aspirational.** The HTTP endpoint, the `{answer, citations}`
response, citation rendering, AND live DynamoDB/S3 reads for
`flight_query`/`model_inference`/`shap_explanation`/`at_risk_flights` are
all deployed and verified against real flights on the live box. The one
thing still not built is a dedicated read-only IAM identity for the agent
(§3) — it currently reuses the app's own `aeroflux-app` credentials, a
deliberate, documented simplification, not an oversight.

---

## 1. The wire contract — implemented and deployed

`aeroflux_ui/streamlit_app/pages/2_Analyst.py` is the caller;
`agent/server.py` (FastAPI/uvicorn, `POST /ask`) is the real, running
implementation of the other end — both sides are done, not just
documented. Deployed at `http://agent:8010/ask` (internal Docker-network
address of the `agent` service on the Lightsail box — see `DEPLOYMENT.md`
§9). Verified end-to-end with a real Groq key, both locally and on the
deployed box (2026-08-14): a real question returns a real grounded
answer with a correct citation, rendered in the actual page.

```python
# 2_Analyst.py's agent_reply()
AGENT_URL = os.getenv("AEROFLUX_AGENT_URL")
wire_history = [{"role": h["role"], "content": h["content"]} for h in history]
r = requests.post(AGENT_URL, json={"question": question, "history": wire_history}, timeout=60)
data = r.json()
answer, citations = data.get("answer", "(no answer)"), list(data.get("citations") or [])
```
```python
# agent/server.py
@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    messages = _build_input_messages(req)          # req.history + req.question, deduped
    result = aeroflux_analyst_graph.invoke({"messages": messages})
    answer = result["messages"][-1].content
    return AskResponse(answer=answer, citations=extract_citations(answer))
```

- **Request:** `POST {question: str, history: list[dict]}`. `history` is
  the Streamlit chat log so far, each entry `{"role": "user"|"assistant",
  "content": str}`.
- **Response:** `{"answer": str, "citations": [str, ...]}`. `citations`
  is extracted server-side (`server.py`'s `extract_citations()`, a regex
  over `[source_name]` tags) from what the graph's `SYSTEM_PROMPT`
  instructs the model to emit inline (`agent.py`, rule 2 — cite every
  document-grounded claim with its exact bracketed source name). The tags
  stay in `answer` too, deliberately — they're the reader's inline
  pointer to which claim came from where; `citations` is a structured
  summary alongside that, not a replacement for it.
- **Rendered:** `2_Analyst.py` shows `answer` as markdown and `citations`
  as a `📎 Sources: ...` caption, under both the live turn and replayed
  history.
- **Timeout:** 60s. A slower agent shows as a graceful error, not a hang.
- **Failure modes, both handled:** `AEROFLUX_AGENT_URL` unset falls back
  to the existing local deterministic responder (unchanged — still demos
  with no agent connected). Set but unreachable (down, wrong URL, bad
  response) shows a `⚠️ **Agent unavailable**` message — never crashes
  the page, never silently falls back mid-session once a real agent is
  configured.

**Previously a known gap, now closed:** the original plan called for a
`citations` field alongside `answer`, and the UI didn't read it — closed
2026-08-14, both sides.

**A real bug found and fixed while wiring this**, worth knowing if the
agent's citations ever go quiet again: `agent.py`'s `analyst_node` used
to only prepend `SYSTEM_PROMPT` (which contains the citation-format
instructions) *if no `SystemMessage` existed yet* in the conversation —
but `prefetch_node` always adds its own `SystemMessage` (the doc-excerpts
note) first, so that condition was false on every normal turn and the
real `SYSTEM_PROMPT` was silently never sent to the model at all. Fixed
by always prepending it (safe — built fresh per `llm.invoke()` call,
never written back to graph state, so the tools-loop can't duplicate it).

---

## 2. Reading AeroFlux's data — S3 + DynamoDB, implemented (2026-08-14)

**Done, not aspirational.** `agent/tools.py`'s `flight_query`,
`model_inference`, and `shap_explanation` read live AeroFlux data —
current state + predictions from DynamoDB, gold features from S3 —
through the exact same `aeroflux_ml.io` abstraction
(`state_backend_from_env()` / `lake_backend_from_env()`) `data_access.py`
already uses, with the same read-only `aeroflux-app` AWS credentials the
deployed app already has (see §3 for how — it's a deliberate deviation
from the identity-per-consumer plan originally written here). Verified
live against a real flight on the deployed box: real status, real route,
real delay prediction, real gold feature values. `event_reconstruction`
is the one exception — still sample-only, since SWIM's raw event history
isn't exposed via DynamoDB or S3 at all (bronze, not marked `[AGENT]` in
`AeroFlux_DataSchemas.md`).

**How each tool reads (`agent/tools.py`):**

| Tool | Reads | Notes |
|---|---|---|
| `flight_query` | `recent_flight_states()` — bounded Scan (`Limit=3000`), cached 120s in-process — matched by callsign/flight_number client-side (no callsign GSI exists; same tradeoff `data_access.py` already makes) | Returns status, route, times, position, AND the prediction (`delay_probability`/`predicted_delayed`) — DynamoDB's disjoint-attribute-groups design embeds both on the same item, so this is one read, not two |
| `model_inference` | Same bounded/cached state read as `flight_query` | Just the prediction fields |
| `shap_explanation` | Two-step: state lookup for identity → `flight_key`, then that key against `gold_features.parquet` (one S3 GET, cached 120s, filtered client-side — no per-flight-key index exists in the lake) | **Real model input feature values (propagation pressure, demand, weather, rotation) — NOT computed SHAP contribution scores.** Kept the `shap_explanation` name for continuity with the original tool naming, but the result, its docstring, the LangChain tool description, and a `SYSTEM_PROMPT` rule are all explicit that this is real feature data, not SHAP — so nothing downstream (the LLM, a reader of the raw JSON) claims otherwise. A feature absent from the result was genuinely unresolved for that flight (`feature_prep`'s fill policy: missing ≠ 0), not zero risk |
| `at_risk_flights` (new) | Same bounded state read, sorted by `delay_probability` descending | Fleet-wide "what's most at risk right now" — closes the gap `agent/EVALUATION.md` flagged itself ("Scope gap: no fleet-wide queries... a list_flights tool would be needed"). Deterministically prefetched (same reliability rationale as the per-flight prefetch) when a question looks fleet-wide but names no specific flight |

**Read-only, always, enforced at the AWS layer** (§3's IAM policies), not
just by convention — `tools.py` only ever calls `recent_flight_states()`
/ `read_parquet()`; there's no upsert/write call anywhere in it.

**Sample-data fallback preserved — same "always demos" discipline as the
app.** Every live read is wrapped and returns `None` on any failure
(cloud not configured, unreachable, permissions issue, whatever); every
caller falls straight through to the existing `data/sample_flights.json`
path on `None`. Every tool result carries a `"source": "live"|"sample"`
field for exactly this reason — so nothing downstream mistakes a demo
answer for a real one.

`AeroFlux_DataSchemas.md`'s "Access patterns for the agent tools" section
(bottom of that file) shows SQL against `flight_instance`/`predictions` —
that's the **local dev** shape (Postgres), not what's actually used above.
Reference table (same data, same `flight_key` join key, different
transport):

| Local dev (Postgres) | Production (what your agent should use) |
|---|---|
| `flight_instance` table | DynamoDB table `aeroflux-current-state`, `flight_key` HASH key (no sort key). State + prediction attributes live on the *same* item (disjoint attribute groups — see `aeroflux_ml/io.py`'s `DynamoDBStateRepository` docstring). `Scan`+`FilterExpression` on `updated_at`, capped with `Limit` — **do not run an unbounded Scan**, see `CLAUDE.md` Gotchas for why (a real, verified cost incident). |
| `gold_features.parquet` | S3 bucket (see `S3_BUCKET`, default `aeroflux-lake-<account>-<region>` — ask Jonathan for the actual deployed bucket name), key `gold/gold_features.parquet`. |
| `predictions` table | Embedded in the same DynamoDB item as state (see above) — `delay_probability`, `predicted_delayed`, `model_version`, `scored_at` attributes. |
| — (new) | `eval/live_metrics_latest.json` and `eval/reconciled_pairs.parquet` in the same S3 bucket — live model-evaluation metrics (ROC-AUC/PR-AUC/calibration, per lag-bucket), if useful for an analyst-facing "how good is the model right now" question. **Read the structural-coverage-gap caveat in `PROJECT_CONTEXT.md` § Known Limitations before quoting any metric from this file** — most live predictions never get a resolved outcome at all (SWIM's `arrivalInformation` message is rare), and the ones that do skew hard toward on-time/early landings; an AUC pulled from here is not the model's real performance, and — unlike ordinary right-censoring — this will NOT self-correct just by waiting longer.

**This is the path that got taken** (not just a recommendation anymore):
reusing `aeroflux_ml.io.state_backend_from_env()` / `lake_backend_from_env()`
directly (same factories `data_access.py` and the Model Performance page
use) rather than writing separate boto3 calls — at the cost of the agent
needing the `aeroflux_ml` package importable. Made that real by widening
`agent/Dockerfile`'s build context from `agent/` to the repo root so it
can `COPY aeroflux/aeroflux_ml`, which needed a new root `.dockerignore`
(without one, `docker build` from the repo root would send `aeroflux/data`
(34G) + `aeroflux/bts_out` (14G) as build context — found doing exactly
that locally) and the same extra dependencies (`polars`, `boto3`, `pyyaml`,
plus `requests`/`xgboost`/`joblib` for `aeroflux_ml`'s other eager
imports) `aeroflux_ui/streamlit_app/requirements.txt` already proved
necessary for this exact import. If your agent isn't Python, read the
DynamoDB item / S3 parquet shapes directly instead — schemas are in
`AeroFlux_DataSchemas.md` §1–3, plus the disjoint-attribute note above.

Column/attribute definitions (all of them): `AeroFlux_DataDictionary.md`
(feature meanings) and `AeroFlux_DataSchemas.md` (table/column shapes,
sample records) — this doc doesn't repeat those.

---

## 3. Credentials

**Both credentials are done (2026-08-14) — but read this before assuming
§2's data-access story matches the original plan exactly.**

The agent's LLM key (Groq, `GROQ_API_KEY`) lives in `agent.env` on the
Lightsail box only, never committed, never touches the GitHub Action (see
`DEPLOYMENT.md` §9) — always was its own thing, unrelated to AWS.

**The AWS credentials are also live, but as a deliberate deviation from
the plan originally written below: the agent reuses the SAME
`aeroflux-app` credentials + policies the Streamlit app already has,**
via the same `.env` file on the box (`agent`'s compose service reads it
as a second `env_file:` entry, not a copied second set of values — see
`DEPLOYMENT.md` §9). Explicit tradeoff: faster to ship, one fewer
credential to provision and rotate — at the cost of losing the
rotate/audit-independently-per-consumer property a separate identity
would give. If that property becomes a real requirement (not just
tidiness), provisioning a second identity with the same two policies
below is a quick, well-scoped follow-up — `tools.py` doesn't care which
identity answers, it only ever goes through
`state_backend_from_env()`/`lake_backend_from_env()` like everything else.

The original plan, kept for context and as the spec for that follow-up if
it's ever needed:

> **Read-only policies already exist** (`aeroflux-s3-read-only-policy`,
> `aeroflux-dynamodb-policy-read-only` — see `scripts/aws_setup.sh`), the
> same ones the deployed app's `aeroflux-app` IAM identity uses. Don't
> reuse `aeroflux-app`'s own credentials for the agent — separate
> identities per consumer, so either side can be rotated/revoked
> independently and any billing/audit question ("who read what") stays
> answerable. The provisioning step (new IAM user + attach those two
> policies, or an SSO permission set if that's preferred over long-lived
> keys) is a quick one, but it's a real AWS-account change.

Needed (already set, via the shared `.env`): `AWS_ACCESS_KEY_ID` /
`AWS_SECRET_ACCESS_KEY`, `AWS_REGION=us-east-1`, `S3_BUCKET=<the deployed
bucket>`, `DYNAMODB_TABLE=aeroflux-current-state`.

---

## 4. The boundary (revised: same box, still separate everything else)

The original plan assumed a wholly separate host. What's actually
deployed (2026-08-14) is **Option A — a second container on the same
Lightsail box**, chosen after measuring (`docker stats`, not guessing)
that the full stack fits comfortably. Level 2 (HTTP wiring only) measured
~776MiB (~20%) of the box's 3.747GiB; Level 3 (this doc's update, live
cloud reads — boto3/polars/xgboost/scikit-learn added to the image) pushed
that to **~1.28GiB total (~34%)**, still with ~2.4Gi available. Moving to
a separate instance remains the documented fallback if that ever tightens
further; still not needed. Still holds, same as the original plan:

- **Your agent owns its own vector store + LLM calls.** AeroFlux has no
  opinion on which model/framework/vector DB you use. (Deployed: its own
  `agent-pgvector` container, entirely separate from AeroFlux's own
  Postgres — different container, no shared host port, nothing shared
  but the box itself.)
- **Separate process, separate deploy, separate image — but not zero
  code coupling anymore.** No shared process, no shared container. As of
  Level 3 (§2), the image DOES `COPY` in `aeroflux_ml` at build time (its
  own Dockerfile, widened to a repo-root build context — see §2) — the
  one real exception to "no import of your code into this repo," and an
  explicitly accepted one (see §2 for why: avoids re-solving already-fixed
  bugs like the DynamoDB Scan-cost incident by hand-rolling a second boto3
  path). Still its own `Dockerfile`, its own GitHub Actions workflow
  (`deploy-agent.yml`), its own image on GHCR
  (`ghcr.io/jonathanwilsonami/aeroflux-agent`). The only RUNTIME coupling
  is the HTTP contract in §1 — over the box's internal Docker network
  only; neither the agent nor its pgvector publish a host port, so
  nothing outside the box's Docker network can reach either directly —
  and the read-only data access in §2, now implemented.
- **Read-only, always.** The agent produces evidence-grounded answers; it
  never writes to AeroFlux's state, predictions, or gold data. Enforced
  at the AWS layer by the read-only IAM policies in §3 (shared with the
  app, see §3's note on that), not just by convention — `tools.py` never
  calls an upsert/write path.

If the agent ever needs to move to its own instance (heavier real
workload, a bigger embedding model, etc.), it's already fully separable —
its container/image/CI workflow don't assume they're on the same box;
only the deploy target (`docker-compose.lightsail.yml`) does.

---

## 5. Wiring it up + verifying — done (2026-08-14); here's how it's set up

`AEROFLUX_AGENT_URL=http://agent:8010/ask` is set directly in
`docker-compose.lightsail.yml`'s `app.environment:` block — not `.env`,
since it's the compose file's own internal topology (not secret, not
per-environment), so it can't be forgotten or drift out of sync. Full
deploy mechanics: `DEPLOYMENT.md` §9.

Verified working end-to-end with a real Groq key, two ways: (1) an
internal HTTP call from inside `aeroflux-ui` straight to
`http://agent:8010/ask`, matching the app's own access path exactly; (2)
the actual `2_Analyst.py` page, run inside the real deployed container —
a real question returned a real grounded answer with the correct
`📎 Sources:` citation, zero exceptions.

Re-verified the same way after Level 3 (§2) shipped, with a real flight —
"tell me about flight AA1076" returned its real `PLANNED` status, real
route (KDFW→KBNA), a real delay probability that visibly changes between
checks (confirming live, not cached-forever, data), and real gold
feature values (origin/destination demand, rotation-resolution status).
"What flights are most at risk right now?" correctly triggered the new
fleet-wide prefetch and returned a real ranked list.

**If you need to redeploy or change something:**
- Agent code change → push to `agent/**`; `deploy-agent.yml` builds+pushes
  the image and (if `DEPLOY_ENABLED=true`) redeploys just the `agent`
  service — mirrors `deploy-ui.yml`.
- Local dev/testing without touching the box: `agent/docker-compose.yml`
  (the agent's own local pgvector, port 5433) + `uvicorn server:app
  --port 8010` from `agent/`, then `AEROFLUX_AGENT_URL=http://localhost:8010/ask
  streamlit run app.py` from `aeroflux_ui/streamlit_app/`.
- To point at a different agent entirely (e.g. a future separate
  instance, per §4's fallback): change `AEROFLUX_AGENT_URL`'s value in
  `docker-compose.lightsail.yml` and redeploy `app`.
