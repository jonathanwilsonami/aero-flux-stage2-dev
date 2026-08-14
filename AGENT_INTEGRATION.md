# AeroFlux — Agent Integration

For Ryan's RAG/reasoning layer. Short on purpose — the data *contract*
(schemas, columns, sample records) already lives in
`AeroFlux_DataSchemas.md`; this doc covers the three things that aren't
there: the wire protocol the UI actually calls, how to read *production*
data specifically (S3 + DynamoDB, not local Postgres), and the boundary
between the two codebases.

Everything below reflects what's actually implemented today (2026-08-14).
§1 (the wire contract) and §4 (the boundary) are now real and deployed —
the HTTP endpoint, the `{answer, citations}` response, and citation
rendering all exist and are running on the live box, not just documented.
§2 (reading production data) and the AWS half of §3 are still the
aspirational future step — flagged clearly there, not glossed over.

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

## 2. Reading AeroFlux's data — production is S3 + DynamoDB, not Postgres

**Still the future step — not built yet, even though the agent itself is
now deployed.** The agent is live on the Lightsail box (`DEPLOYMENT.md`
§9), but its tools (`agent/tools.py`) still read `data/sample_flights.json`
— mock data, unchanged since the prototype. Being deployed and being
wired to real data are two different milestones; only the first one is
done. Everything below is the unchanged plan for the second.

`AeroFlux_DataSchemas.md`'s "Access patterns for the agent tools" section
(bottom of that file) shows SQL against `flight_instance`/`predictions` —
that's the **local dev** shape (Postgres). **If your agent runs as its own
separate service (the intended design — see boundary below), it can't
reach local Postgres at all; it needs to read the same S3/DynamoDB copies
the deployed app itself reads.** Same data, same `flight_key` join key,
different transport:

| Local dev (Postgres) | Production (what your agent should use) |
|---|---|
| `flight_instance` table | DynamoDB table `aeroflux-current-state`, `flight_key` HASH key (no sort key). State + prediction attributes live on the *same* item (disjoint attribute groups — see `aeroflux_ml/io.py`'s `DynamoDBStateRepository` docstring). `Scan`+`FilterExpression` on `updated_at`, capped with `Limit` — **do not run an unbounded Scan**, see `CLAUDE.md` Gotchas for why (a real, verified cost incident). |
| `gold_features.parquet` | S3 bucket (see `S3_BUCKET`, default `aeroflux-lake-<account>-<region>` — ask Jonathan for the actual deployed bucket name), key `gold/gold_features.parquet`. |
| `predictions` table | Embedded in the same DynamoDB item as state (see above) — `delay_probability`, `predicted_delayed`, `model_version`, `scored_at` attributes. |
| — (new) | `eval/live_metrics_latest.json` and `eval/reconciled_pairs.parquet` in the same S3 bucket — live model-evaluation metrics (ROC-AUC/PR-AUC/calibration, per lag-bucket), if useful for an analyst-facing "how good is the model right now" question. **Read the structural-coverage-gap caveat in `PROJECT_CONTEXT.md` § Known Limitations before quoting any metric from this file** — most live predictions never get a resolved outcome at all (SWIM's `arrivalInformation` message is rare), and the ones that do skew hard toward on-time/early landings; an AUC pulled from here is not the model's real performance, and — unlike ordinary right-censoring — this will NOT self-correct just by waiting longer.

If your agent is Python, the simplest path is reusing
`aeroflux_ml.io.state_backend_from_env()` / `lake_backend_from_env()`
directly (same factories `data_access.py` and the Model Performance page
use) rather than writing your own boto3 calls — but that means your
service needs the `aeroflux_ml` package importable (pip install from this
repo, or vendor it), and the env vars below set. If your agent isn't
Python, read the DynamoDB item / S3 parquet shapes directly — schemas are
in `AeroFlux_DataSchemas.md` §1–3, plus the disjoint-attribute note above.

Column/attribute definitions (all of them): `AeroFlux_DataDictionary.md`
(feature meanings) and `AeroFlux_DataSchemas.md` (table/column shapes,
sample records) — this doc doesn't repeat those.

---

## 3. Credentials

**Two different credentials, two different states.** The agent's LLM key
(Groq, `GROQ_API_KEY`) is done — it lives in `agent.env` on the Lightsail
box only, never committed, never touches the GitHub Action (see
`DEPLOYMENT.md` §9). The AWS read-only credentials below, for the real
S3/DynamoDB data path in §2, are NOT done — that's still the future step.

**Read-only policies already exist** (`aeroflux-s3-read-only-policy`,
`aeroflux-dynamodb-policy-read-only` — see `scripts/aws_setup.sh`), the
same ones the deployed app's `aeroflux-app` IAM identity uses. **Don't
reuse `aeroflux-app`'s own credentials for the agent** — separate
identities per consumer, so either side can be rotated/revoked
independently and any billing/audit question ("who read what") stays
answerable. The provisioning step (new IAM user + attach those two
policies, or an SSO permission set if that's preferred over long-lived
keys) is a quick one, but it's a real AWS-account change — that's on
Jonathan to do when you're ready to actually connect, not something to
self-serve from this doc.

Needed once that's set up: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
(or an SSO profile), `AWS_REGION=us-east-1`, `S3_BUCKET=<the deployed
bucket>`, `DYNAMODB_TABLE=aeroflux-current-state`.

---

## 4. The boundary (revised: same box, still separate everything else)

The original plan assumed a wholly separate host. What's actually
deployed (2026-08-14) is **Option A — a second container on the same
Lightsail box**, chosen after measuring (`docker stats`, not guessing)
that the full stack — app + caddy + agent + agent's own pgvector — uses
~776MiB (~20%) of the box's 3.747GiB, comfortable headroom. Moving to a
separate instance was the documented fallback if memory was tight; it
wasn't needed. Still holds, same as the original plan:

- **Your agent owns its own vector store + LLM calls.** AeroFlux has no
  opinion on which model/framework/vector DB you use. (Deployed: its own
  `agent-pgvector` container, entirely separate from AeroFlux's own
  Postgres — different container, no shared host port, nothing shared
  but the box itself.)
- **Separate process, separate deploy, separate image.** No shared
  process, no shared container, no import of your code into this repo
  (or vice versa) beyond the optional `aeroflux_ml` reuse in §2 (not used
  yet — see §2). Its own `Dockerfile`, its own GitHub Actions workflow
  (`deploy-agent.yml`), its own image on GHCR
  (`ghcr.io/jonathanwilsonami/aeroflux-agent`). The only coupling is the
  HTTP contract in §1 — over the box's internal Docker network only;
  neither the agent nor its pgvector publish a host port, so nothing
  outside the box's Docker network can reach either directly — and the
  read-only data access in §2 (not built yet).
- **Read-only, always.** The agent produces evidence-grounded answers; it
  never writes to AeroFlux's state, predictions, or gold data. (There's
  no write path exposed to it anyway — the read-only IAM policies in §3
  will enforce this at the AWS layer once §2 is built, not just by
  convention; today it can't reach AWS at all, since it only reads
  sample data.)

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
