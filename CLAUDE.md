# CLAUDE.md — AeroFlux Stage 2

Auto-loaded context for Claude Code. Read `PROJECT_CONTEXT.md` for the full story,
`AeroFlux_DataSchemas.md` for the data contract, `AeroFlux_DataDictionary.md`
for the feature contract, and `DEPLOYMENT.md` for the cloud/Lightsail deploy
flow and gotchas. Keep this file current as the project moves.

## What this is
AeroFlux is a real-time flight-delay prediction platform over FAA SWIM + ADS-B +
weather. Course: GMU **AIT 614** (Big Data). The thesis is **network-state delay
propagation**: aircraft rotation is *one nullable channel*; airport demand,
weather, and schedule are the always-available backbone. Train on historical BTS,
serve on live SWIM, with **train/serve parity by construction**.

## Golden rules (do not break these)
1. **Parity is sacred.** Training (BTS) and serving (live) must compute the *same*
   features. Both go through `aeroflux_ml/feature_prep.py`. If you add/change a
   feature, it changes there once, for both paths. Never fork the logic.
2. **No leakage.** Weather and any time-derived feature use the **score time =
   scheduled departure** (the prediction moment), never actual arrival. Never use
   `dep_delay_min`/`arr_delay_min` as model features (they're outcomes/labels).
3. **GUFI is the merge key, not `flight_ref`** (flight_ref changes on plan
   amendment). Dedup GUFI-fallback duplicates.
4. **Missing ≠ 0 unless it truly is.** `feature_prep` fills rotation/demand with 0
   (absence == 0) and leaves weather/recent-delay null (XGBoost routes NaN). Don't
   blanket-fill.
5. **Real data over fixtures. Validate each layer before advancing. Tests pass
   before moving on** (`python -m pytest`, currently 97 passing).
6. **The default model feature set is 18 features, weather included.**
   `feature_prep.feature_columns()` (default `include_gap_weather=False`) = 5
   structural + 5 rotation/propagation + 4 demand/recent-delay + 4 weather
   (`origin/dest_wx_wind_kt`, `origin/dest_wx_ifr` — wind + IFR only, both
   present in live METAR and BTS/NCEI). `temp_c`/`ceiling_ft`/`vis_mi` are the
   *actual* parity-gap weather (dense in BTS, null in live METAR) — those stay
   opt-in via `include_gap_weather=True`. The live pipeline's weather channel
   must be on (`WEATHER=1`, now the `run.sh` default) for the 4 wind/IFR
   features to be non-null live — this bit us once (see Gotchas).

## Secrets handling (hard rule)
**Never print, `cat`, `tail`, `head`, or `echo` the contents of `.env` files or
any file that may hold credentials** (AWS keys, DSNs with passwords, tokens,
API secrets) — locally, on the box, anywhere. This includes indirectly, e.g.
`tail -N` on a `.env` to check the last couple of lines you just added: it
prints everything else in that range too. This rule exists because it was
broken once — a `tail -5 .env` on the Lightsail box to confirm two new lines
landed also printed the real `AWS_SECRET_ACCESS_KEY` into the session
transcript.
- To inspect an env file: `grep -v -i 'secret\|password\|key\|token' file` to
  exclude secret-bearing lines, or `grep '^VAR_NAME='` to check one specific
  non-secret variable by name.
- To edit one: write the specific key(s) directly (e.g. a `grep -q ... ||
  echo "VAR=val" >> .env` guard, matching what `sync_cloud`/deploy tooling
  already does), or use an interactive editor (`nano`/`vim`) — never a
  command that echoes the whole file to append/modify it.
- To confirm a variable is *set* (not its value): test its effect — does the
  AWS call succeed, does the DB connect — rather than printing it.
- **`sed`'s `&` in a replacement re-inserts the ENTIRE matched text, not
  just the part you captured.** A "redaction" like `sed -E
  's/^([A-Za-z_]+)[=:].*/\1 <redacted> (&)/'` looks safe (it names a
  capture group) but `&` still expands to the whole original match —
  since the pattern's `.*` matched the whole line, `&` prints the whole
  line right back, secret value included. This nearly leaked a real key
  (2026-08-14). Only ever reference `\1`/`\2` (explicit numbered capture
  groups) in a redaction replacement — never `&`, and never a bare `.*`
  capture that could swallow the secret itself into a group you then
  print. When in doubt, don't build a redaction one-liner under time
  pressure — use the existence/name-only checks above instead (`grep -c`,
  `grep -oE '^[A-Za-z_]+='` for names only, `grep -noE
  '^[A-Za-z_]+:'` for malformed-line detection) — none of them need to
  touch the value at all.
- If a secret is ever exposed in a transcript or log despite this, say so
  plainly and recommend rotation; don't just quietly move on.

## Lifecycle/teardown testing (hard rule)
**Never test `cmd_down` (or any command that stops/kills processes —
`run.sh stop`, `pkill`, `kill` on a PID from a `.*_pid` file, etc.) against
the real running stack.** `./e2e.sh up`'s ingest/score/sync loop is a
continuously-running production pipeline (live SWIM ingest, scoring, and
cloud sync to S3/DynamoDB) whenever it's up — not a disposable dev
process. This rule exists because it was broken once: smoke-testing a new
`cmd_eval` addition's teardown integration by actually running `./e2e.sh
down` took down a real stack that had been running continuously since
2026-08-09 (had to be manually reconstructed and restarted from non-secret
config sources afterward).
- To test a lifecycle change (a new stage's start/stop wiring, a change to
  `cmd_down`'s teardown loop, etc.): use throwaway dummy processes (e.g.
  `sleep 999 & echo $! > out/.foo_pid`) or an isolated checkout (a `git
  worktree`, matching how PR review merges are checked — see the
  `pr1-merge-check` pattern) — never the PIDs in `out/.*_pid` from a stack
  you didn't start for the test itself.
- Before running any stop/kill command, check `ps aux` (or `cat
  out/.*_pid` + `kill -0`) for what's *actually* running and confirm with
  the user first if there's any chance it's the real stack, not a test
  artifact.
- If a live stack is ever taken down by mistake despite this, say so
  plainly and immediately (don't quietly restart and not mention it), then
  restore it — config for a cloud-syncing restart is reconstructable from
  non-secret sources (`scripts/aws_setup.sh`'s bucket-naming convention,
  `~/.aws/config` profile names, `logs/ingest.log`'s "REAL-TIME mode"
  restart cadence for `DURATION=continuous` vs a fixed duration) without
  needing to print any secret.

## Repo map (project dir: `aeroflux/`)
- `aeroflux_parser/` — SWIM parse → fuse → canonical silver. Key: `fusion`,
  `identity`, `adsb`/`adsb_store` (airframe store), `airports`/`airlines` dims.
- `aeroflux_ml/` — the ML side:
  - `schema.py` — `from_bts` / `from_silver` adapters (→ one canonical frame, tz-aware)
  - `channels.py`, `engineer.py` — feature channels + `FeatureEngineer`
  - `feature_prep.py` — **the feature contract** (fill policy, parity set, propagation)
  - `weather.py` — METAR live (AWC) + historical (IEM); NCEI fetch
  - `weather_cache.py` — cache-first NCEI loader + station→ICAO bridge
  - `bts_source.py` — BTS fetch/cache/discover-local-CSV
  - `pipeline.py` — silver → gold; `run.py` — pipeline CLI
  - `score_live.py` — score live gold → predictions (parquet + Postgres)
  - `io.py` — **StateRepository/LakeStore abstraction** (the cloud storage
    seam): `PostgresStateRepository`/`DynamoDBStateRepository`,
    `LocalLakeStore`/`S3LakeStore`, selected via `STATE_BACKEND`/
    `LAKE_BACKEND` env vars through `state_backend_from_env()`/
    `lake_backend_from_env()` — see `DEPLOYMENT.md`
  - `sync_cloud.py` — local → cloud sync step (gold → LakeStore, current
    state + predictions → StateStore, deduped against a content-hash cache
    — `SYNC_DEDUPE=1` default, see Gotchas); no-op unless a cloud backend
    is selected. `sync_eval_outputs()` is a separate, much-lower-frequency
    sync (live-eval JSON + reconciled pairs, called from
    `evaluate_live.py`'s `report()`, not the per-cycle flight loop)
  - `evaluate_live.py` — reconciles forward-captured live predictions
    (`out/predictions/*.parquet`) against realized outcomes
    (`out/gold_live/*.parquet`'s `arr_delay_min`) into
    `out/eval/reconciled_pairs.parquet`, one row per (flight_key, lag
    bucket) — see its module docstring for the guardrails and the
    keep-latest-only bug this replaced
  - `training/` — configurable ML pipeline (see below)
- `scripts/build_bts_gold.py` — BTS → gold training table
- `scripts/sync_cloud.sh`, `scripts/smoke_cloud_backends.py` — cloud sync
  wrapper + a standalone DynamoDB/S3 round-trip smoke test (run before
  trusting any cloud-backend change)
- `configs/` — `pipeline.yaml`, `training.yaml`
- `aeroflux_ui/streamlit_app/` — demo UI (Home, Live Map, Analyst, Live
  Inference, **Model Performance** — live eval metrics/calibration/lag
  buckets, DynamoDB item count via free `describe-table`, XGBoost feature
  importances, glossary; cloud-aware like everything else); reads through
  `data_access.py`, which reads through the same
  `state_backend_from_env()`/`lake_backend_from_env()` factories as
  everything else — Postgres/local by default, DynamoDB/S3 when deployed.
  Also: `Dockerfile`, `docker-compose.lightsail.yml`, `Caddyfile` (deploy
  target — see `DEPLOYMENT.md`)
- `run.sh` — live ingestion orchestrator · `e2e.sh` — full train→serve→UI
  (now also: sync-to-cloud stage, duplicate-stack guard — `FORCE=1` to
  override)
- `deploy.sh` (repo root) — manual build/push/deploy/rollback/status for the
  Lightsail app; `.github/workflows/deploy-ui.yml` — CI build+push (+ gated
  auto-deploy)
- `tests/` — `test_ml.py`, `test_parse.py`, `test_training.py`,
  `test_swim_reconnect.py`, `test_sync_dedupe.py`, `test_evaluate_live_buckets.py`

## Key commands
```bash
./run.sh setup                                   # Kafka + Postgres + tables
./run.sh stream 3600                             # live ingest (bridge+consumer+poller+gold+retention)
python scripts/build_bts_gold.py --months 2015-01:2015-12 --cache data/bts \
    --weather-cache data/weather --station-bridge data/reference/airport_to_station_2019.csv
python -m aeroflux_ml.training.cli train --config configs/training.yaml --gold <gold.parquet>
python -m aeroflux_ml.score_live --run-dir <run> --gold out/gold_features.parquet --out out/predictions.parquet
./e2e.sh up | health | down                      # everything, with health checks (FORCE=1 up to replace a running stack)
python -m pytest                                 # 91 tests
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux"

# Cloud backends (local Postgres/filesystem by default — opt in via env, fully reversible):
export STATE_BACKEND=dynamodb LAKE_BACKEND=s3 AWS_PROFILE=aeroflux-local \
       AWS_REGION=us-east-1 S3_BUCKET=<bucket> DYNAMODB_TABLE=aeroflux-current-state
python scripts/smoke_cloud_backends.py                    # verify creds/perms before trusting anything else
./scripts/sync_cloud.sh                                    # one local->cloud sync cycle (SYNC_DEDUPE=1 by default)
./e2e.sh up                                                 # same env vars -> live stack syncs to cloud too
```

## Training pipeline (`aeroflux_ml/training/`)
Config-driven (YAML). Components: `config`, `data` (time-aware split + CV),
`preprocess` (reuses feature_prep), `validation`, `models/` (XGBoost + logistic
now; Spark ML adapter + TF stub as extension points), `tune` (grid + time-aware
CV), `evaluate`, `compare`, `registry` (local runs + optional MLflow), `runner`,
`cli`. Add a model = add a class in `models/` + a registry entry + a YAML block.

## Gotchas that already bit us (don't rediscover)
- SWIM broker is **`ems2`** not ems1; feed is **`flight-data`** (TFMS) not flight-delay-tfms.
- ADS-B 403 = Cloudflare blocks default urllib UA → set a descriptive `ADSB_USER_AGENT`.
- Postgres `timestamptz` → Polars: read with `infer_schema_length=None`.
- METAR: chunk station requests (414) and retry/backoff (504); a live *batch* over
  48h of flight plans shows ~3% weather — that's a **time-window mismatch, not a
  bug** (plans are future-heavy). Real-time serving + historical training are fine.
- BTS `HHMM` come as floats when a column has nulls → cast via Int; BTS publishes
  ~2–3 months late (recent months 404).
- NCEI sentinels: wind `999.9 m/s`, ceiling `≥22000 m` → null.
- **`interval '%s hours'` in psycopg silently mis-parses** — a bind parameter
  inside a quoted SQL string literal is not real substitution; it always came
  out as exactly 1 hour regardless of the value passed. Use
  `make_interval(hours => %s)` (a real function call) instead. Bit
  `PostgresStateRepository.recent_flight_states`.
- **boto3 rejects native `float` and `datetime`** for DynamoDB items — convert
  `float` → `Decimal(str(v))` (not `Decimal(v)`, avoids binary-float
  artifacts) and `datetime`/`date` → `.isoformat()`. See
  `DynamoDBStateRepository._dynamo_value`.
- **DynamoDB `Scan` with no `Limit` reads the whole table** — a 32-second,
  full-table scan every 30s page-load is a real cost/latency problem, not a
  local-test quirk. `recent_flight_states(limit=N)` does a single-page
  `Scan(Limit=N)` (no `LastEvaluatedKey` pagination) to hard-bound it —
  demo-scale choice; a GSI + `Query` is the real scale path.
- **A `while true; do sync; sleep N; done` loop: `VAR="${VAR:-}"` on the
  subprocess command line is NOT the same as leaving `VAR` unset.** It sets
  an explicit empty string, which silently defeats every
  `os.getenv(NAME, default)` fallback downstream (broke
  `int(os.getenv("DYNAMODB_TTL_HOURS", "48"))` outright). Export
  conditionally (`[ -n "${!v:-}" ] && export "$v"`) instead.
- **Two un-torn-down `e2e.sh up` stacks running concurrently** looks exactly
  like "intermittent" bugs — same log file, interleaved output, doubled
  chance of hitting real races (e.g. a sync landing inside
  `flight_instance`'s `TRUNCATE`+reload window). `e2e.sh up` now refuses to
  start a second stack by default (`FORCE=1` to override) — see
  `DEPLOYMENT.md`.
- **Nginx from an old deploy left running on 80/443** blocks Caddy from
  binding those ports at all (`address already in use`) — `docker compose
  up` doesn't error loudly enough to make this obvious; check `sudo ss
  -tlnp` on the box first.
- **A container that failed to bind ports once (e.g. from the nginx
  conflict) needs `--force-recreate`, not just another `up -d`** — compose
  will happily restart the same broken network config otherwise.
- **GHCR images pushed via the built-in `GITHUB_TOKEN` are private by
  default**, even in a public repo — the deploy box's anonymous `docker pull`
  gets 401 until you flip the package to public (or give the box a
  `read:packages` token).
- **`python:3.11-slim` has no `curl`** — `HEALTHCHECK CMD curl ...` reports
  "unhealthy" forever despite the app genuinely serving traffic. Use stdlib
  `python -c "import urllib.request; ..."` instead.
- **A real ~$29 DynamoDB bill turned out to be WRITES, not the Scan
  pattern** — Cost Explorer confirmed it: `sync_cloud.py` was upserting
  every tracked flight's state + prediction (~30-48K items) every
  `SYNC_EVERY` cycle (300s) regardless of whether anything had actually
  changed, and writes cost 5x reads ($1.25/M WRU vs $0.25/M RRU). Fixed
  with a local content-hash cache (`SYNC_DEDUPE=1`, default) —
  `filter_changed()` in `sync_cloud.py` skips a flight's upsert only if
  it's both unchanged AND was synced within `DYNAMODB_REFRESH_HOURS`
  (default 12h) — the refresh floor exists so a genuinely-static-but-still-
  current flight (e.g. PLANNED, hours before departure) doesn't silently
  fall out of DynamoDB when its TTL lapses from never being re-written.
- **A relaunch loop with zero backoff turns any fast-failing subprocess
  into a silent, resource-burning busy-loop, not a clean retry.**
  `e2e.sh`'s `DURATION=continuous` ingest loop (`while true; do ./run.sh
  stream 3600; done`) relaunches unconditionally — which looks right for
  "a normal 1h session ended, start the next one," but also means a
  FAST-FAILING invocation (any reason) respawns immediately, forever, with
  no visibility. Real incident (2026-08-14): two lines accidentally landed
  in `aeroflux/.env` using YAML-style `KEY:` instead of `KEY=` (see next
  bullet) — under `run.sh`'s `set -e`, that killed `run.sh` at the `.env`
  sourcing line on every single invocation, before ingest ever started,
  and the zero-backoff loop respawned it instantly — 16M+ repeated log
  lines, an 800MB+ log file, and *hours* of zero real ingest that looked
  from the outside exactly like "a session ended and wasn't relaunched."
  Fixed two ways: `e2e.sh`'s loop now backs off 30s only on a non-zero
  exit (a clean completion still relaunches with zero delay — genuinely
  continuous, not slowed down); `run.sh`'s own `.env` sourcing is no
  longer fatal (`if ! . ./.env; then warn; fi` instead of a bare `set -e`
  sourcing line) so one bad line can't silently take the whole pipeline
  down for hours again.
- **`aeroflux/.env` and `agent/.env` are different files with different
  secrets — don't cross-contaminate them.** `GROQ_API_KEY` and
  `AEROFLUX_AGENT_URL` have no business in `aeroflux/.env` (nothing in
  `aeroflux/` reads either) but ended up there anyway (above bullet) —
  `GROQ_API_KEY` belongs only in `agent/.env` (local) / `agent.env` (the
  Lightsail box); `AEROFLUX_AGENT_URL` for local Streamlit testing is a
  shell env var passed to `streamlit run app.py`, not a `.env` line at
  all. If `run.sh`/`e2e.sh` suddenly can't source `.env`, check for a
  stray `KEY:` line (YAML-style colon) before assuming anything deeper is
  wrong — `grep -noE '^[A-Za-z_][A-Za-z0-9_]*:' .env` finds one without
  printing any values (see CLAUDE.md § Secrets handling for why that
  matters).
  Verified live: two back-to-back cycles went from 47,806/17,489
  (state/prediction) written to 0/0 once the cache was warm. `SYNC_EVERY`
  default is now 600s (was 300s) for the same reason. A GSI (Scan ->
  Query) was evaluated and explicitly NOT built for this — it would have
  roughly doubled write cost (every upsert also writes the GSI), the wrong
  direction once writes were confirmed as the actual driver.

## Current state / next
Milestones hit: live ingest (self-healing — SWIM auto-reconnect, real
liveness health check), BTS↔live parity, cached-weather training, feature
contract, training pipeline, live scoring, E2E orchestration, demo UI (incl.
Model Performance analyst page), cloud storage backends (S3/DynamoDB) with
`data_access.py` reading through them and write cost cut ~18x, live-
prediction evaluation (`evaluate_live.py`, per-lag-bucket), Lightsail
deployment with fully automated CI deploy now verified working,
`AGENT_INTEGRATION.md`, and — as of 2026-08-14/15 — Ryan's agent fully
wired and deployed (HTTP + citations + live DynamoDB/S3 reads, see
§Session Handoff below for the full current state). 97 tests pass. See
`PROJECT_CONTEXT.md` §Roadmap and `DEPLOYMENT.md` for the deploy flow.
Next: the live-eval sample maturing (right-censored, self-correcting —
don't trust today's AUC), the Spark batch analytics job (still open from
the original AWS-storage plan), and the presentation-phase wrap-up items
in §Session Handoff.

## Session Handoff (2026-08-15)
Read this first if picking up mid-stream — full detail is in git history/
prior session logs, this is the fast-orientation version.

**Just happened:** Pushed + deployed `4762a7a` (agent flight-lookup fix —
`FLIGHT_NUMBER_PATTERN` widened to also match 3-letter ICAO callsigns
like "ENY3350" (was 2-letter-only), and every flight tool now passes the
extracted token as both `flight_number=` and `callsign=` since some
carriers — confirmed live, Envoy/"ENY" — never get an IATA `flight_number`
resolved at all, so callsign is the ONLY field that will ever match).
**Confirmed live** on the deployed agent (not just locally): asked the
real deployed agent about ENY3350 (found, 3.5s), AA1076 (regression
check, still works, 0.9s), and a made-up flight ZZ9999 (correctly and
instantly "not found," 0.6s — proving a slower fallback design that was
built, measured, and deliberately NOT shipped didn't sneak in). Nothing
outstanding on this specific fix.

**System state, right now:**
- **4 containers on the Lightsail box** (`aeroflux.duckdns.org`):
  `aeroflux-ui` (Streamlit app, public via Caddy), `aeroflux-caddy` (TLS
  reverse proxy), `aeroflux-agent` (Ryan's LangGraph/RAG agent, FastAPI on
  `:8010`, internal-only — no host port), `aeroflux-agent-pgvector`
  (agent's own doc-corpus DB, internal-only, separate from AeroFlux's main
  Postgres). Memory: ~1.28GiB/3.747GiB total (~34%), comfortable headroom.
- **Agent is at Level 3**: reads live current-state + predictions from
  DynamoDB and gold features from S3, through the same
  `aeroflux_ml.io.state_backend_from_env()`/`lake_backend_from_env()`
  abstraction `data_access.py` uses, with the SAME read-only `aeroflux-app`
  AWS credentials the app has (shared `.env` file on the box, not a
  copy — see `DEPLOYMENT.md` §9 / `AGENT_INTEGRATION.md` §2-3). Sample-data
  fallback preserved if cloud is unreachable. Tools:
  `flight_query`/`model_inference`/`shap_explanation` (real gold feature
  values, explicitly NOT SHAP scores)/`at_risk_flights` (fleet-wide, new).
  `event_reconstruction` stays sample-only (no live event-history source
  exists). HTTP contract: `POST /ask {question, history} -> {answer,
  citations}`, rendered in `2_Analyst.py`.
- **Auto-deploy via GitHub Actions**: `deploy-ui.yml` (triggers on
  `aeroflux/aeroflux_ui/**`) and `deploy-agent.yml` (triggers on
  `agent/**` and `aeroflux/aeroflux_ml/**`), both gated by the
  `DEPLOY_ENABLED` repo **Variable** (not Secret — bit us once), both
  verified working hands-off end-to-end.
- **`AGENT_INTEGRATION.md` and `DEPLOYMENT.md` are already current** —
  both were updated (commit `c17095e`) to describe Level 3 as
  implemented, not future. No doc-sync debt there.

**Known open items (not urgent, need a human decision, not more building):**
1. **Bounded DynamoDB Scan can still occasionally miss a real flight.**
   `flight_query`'s fast lookup is `recent_flight_states(limit=3000)` — a
   single-page Scan. Measured live (2026-08-15): raising `Limit` up to
   60000 changed nothing (DynamoDB caps a single Scan page at ~1MB of
   evaluated data, not by the `Limit` number). A fully unbounded,
   paginated scan finds everything but takes ~73s — over the 60s
   app→agent HTTP timeout, and would slow down every genuinely-unknown-
   flight lookup too, so it was built, measured, and explicitly not
   shipped. Real fix is one of: a GSI on `callsign` (a true Query, not a
   Scan — but roughly doubles write cost, the same tradeoff already
   rejected once after the real ~$29 DynamoDB bill), or bounded multi-page
   pagination added to `aeroflux_ml/io.py` (smaller lift, not built).
   Needs Jonathan's call given the cost history.
2. **Agent reuses `aeroflux-app`'s own AWS identity**, not a separate
   read-only one — a deliberate, documented deviation from the original
   plan (`AGENT_INTEGRATION.md` §3), fine for now, a quick follow-up if
   audit/rotate-independently ever becomes a real requirement.
3. Live-eval sample still maturing (right-censoring artifact, not a bug —
   see `PROJECT_CONTEXT.md` § Known Limitations). Spark batch analytics
   job never built (original AWS-storage plan item 4, still open).

**Wrap-up remaining (presentation phase — writing/exporting, not
building):**
- Architecture diagram: `arch_diagrams/aeroflux_architecture-final.drawio`
  has newer exports appearing under `images/design/`
  (`aeroflux-arch-final.png`, `aeroflux-architecture-final.svg`) —
  evidence of active work outside this session (uncommitted local
  changes present as of 2026-08-15); confirm it's the version actually
  wired into the README/paper before presenting.
- README: rewritten and pushed (`4cebeab`) — confirmed landed on
  `origin/main`, nothing pending there.
- Final paper (`aeroflux-final-paper.qmd`) — in progress, not tracked by
  this session's work.
- Slides/video — not yet started, as of this handoff.

**Hard rules already in this file — don't rediscover, just scroll up:**
`## Secrets handling` (never cat/tail/head/echo `.env`; the `sed`'s `&`
redaction gotcha lives here too), `## Lifecycle/teardown testing` (test
teardown/kill commands against throwaway processes or an isolated
worktree, never the live stack — broke ingest for hours once), the
duplicate-stack guard (`e2e.sh up` refuses a second stack by default,
`FORCE=1` to override), and the DynamoDB cost lessons under `## Gotchas`
(writes were the real ~$29 driver, not reads — a GSI was evaluated and
rejected for doubling write cost; unbounded `Scan` reads the whole table,
~30s+, always bound with `Limit`).
