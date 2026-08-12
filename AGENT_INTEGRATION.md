# AeroFlux — Agent Integration

For Ryan's RAG/reasoning layer. Short on purpose — the data *contract*
(schemas, columns, sample records) already lives in
`AeroFlux_DataSchemas.md`; this doc covers the three things that aren't
there: the wire protocol the UI actually calls, how to read *production*
data specifically (S3 + DynamoDB, not local Postgres), and the boundary
between the two codebases.

Everything below reflects what's actually implemented today (2026-08-12),
not an aspirational design — flagged inline anywhere the real code falls
short of the original plan.

---

## 1. The wire contract (verified against the real code)

`aeroflux_ui/streamlit_app/pages/2_Analyst.py` is the caller. It already
works today — ships with a local, non-LLM responder so the page demos with
no agent connected — and swaps to your endpoint the moment `AEROFLUX_AGENT_URL`
is set:

```python
AGENT_URL = os.getenv("AEROFLUX_AGENT_URL")
r = requests.post(AGENT_URL, json={"question": question, "history": history}, timeout=60)
answer = r.json().get("answer", "(no answer)")
```

- **Request:** `POST {question: str, history: list[dict]}`. `history` is
  the Streamlit chat log so far, each entry `{"role": "user"|"assistant",
  "content": str}` — pass it through to your agent for conversational
  context; AeroFlux doesn't do anything with it itself.
- **Response:** JSON with an `"answer"` key (string, rendered directly as
  markdown in the chat).
- **Timeout:** 60s. A slower agent will show as a UI error, not hang.
- **Failure mode:** any non-2xx or connection error is caught and shown
  as `⚠️ Agent endpoint error: ...` in the chat — it never crashes the
  page or falls back to the local responder mid-session (once
  `AEROFLUX_AGENT_URL` is set, it's used for every message).

**Known gap, not yet built:** the original plan called for a `citations`
field alongside `answer`. **The UI does not read or render it today** —
`agent_reply()` only extracts `.get("answer", ...)`, so if your response
includes `citations`, it's silently dropped. If you want citations to
actually show up, this needs a small change to `2_Analyst.py` (display
them under the answer, e.g. an expander listing source doc names) — ask
Jonathan or send a PR; it's a ~10-line change once you know the shape you
want to send (list of strings? `{title, snippet, url}` objects?).

---

## 2. Reading AeroFlux's data — production is S3 + DynamoDB, not Postgres

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
| — (new) | `eval/live_metrics_latest.json` and `eval/reconciled_pairs.parquet` in the same S3 bucket — live model-evaluation metrics (ROC-AUC/PR-AUC/calibration, per lag-bucket), if useful for an analyst-facing "how good is the model right now" question. **Read the right-censoring caveat in `PROJECT_CONTEXT.md` § Known Limitations before quoting any metric from this file** — the sample is still young and under-represents delayed flights; an AUC pulled from here today is not the model's real performance.

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

## 4. The boundary (unchanged from the original plan)

- **Your agent owns its own vector store + LLM calls.** AeroFlux has no
  opinion on which model/framework/vector DB you use.
- **Runs as a separate service/compute.** No shared process, no shared
  deploy, no import of your code into this repo (or vice versa) beyond
  the optional `aeroflux_ml` reuse in §2. The only coupling is the HTTP
  contract in §1 and the read-only data access in §2.
- **Read-only, always.** The agent produces evidence-grounded answers; it
  never writes to AeroFlux's state, predictions, or gold data. (There's
  no write path exposed to it anyway — the read-only IAM policies in §3
  enforce this at the AWS layer, not just by convention.)

---

## 5. Wiring it up + verifying

1. Point the deployed app at your endpoint: on the Lightsail box's
   `.env`, add `AEROFLUX_AGENT_URL=https://your-agent-host/...` (use the
   same non-printing-secrets pattern as everything else in `.env` — see
   `CLAUDE.md` § Secrets handling — this URL itself isn't secret, but
   it's edited in the same file, so the same "don't cat the whole file"
   discipline applies).
2. Recreate the app container to pick up the new env var (`docker compose
   up -d --force-recreate app`, or just push a no-op change to
   `aeroflux_ui/**` now that auto-deploy works — see `DEPLOYMENT.md`).
3. Test in the Analyst page's chat — ask something the local responder
   couldn't ground in retrieved documents, confirm your agent's answer
   comes back within the 60s timeout.
4. Test locally first if you'd rather not touch the deployed box:
   `AEROFLUX_AGENT_URL=http://localhost:<your-port> streamlit run app.py`
   from `aeroflux_ui/streamlit_app/` (needs local Postgres running — see
   `CLAUDE.md` Key commands — or point your local agent instance at the
   cloud backends the same way §2 describes for production).
