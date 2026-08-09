# DEPLOYMENT.md — AeroFlux cloud storage + Lightsail deploy

How the always-on demo actually gets its data, how the app actually gets
deployed, and every gotcha hit getting both working. Pairs with `CLAUDE.md`
(quick reference) and `PROJECT_CONTEXT.md` (decision log, §3 items 9–12).

---

## 1. Architecture in one paragraph

All streaming and ML processing stays on the local machine — nothing about
`run.sh`'s ingest pipeline or training moved. A `sync_cloud.py` step (no-op
unless opted into) pushes durable copies out: gold parquet → **S3**, current
per-flight state + latest predictions → **DynamoDB**. An always-on Streamlit
container on a **Lightsail** VM (Docker + Caddy for TLS, image built and
pushed by **GitHub Actions** to **GHCR**) reads *only* from S3 + DynamoDB,
using read-only credentials, and never touches the local Postgres. Both the
local app and the deployed app run the exact same `data_access.py` — the only
difference is which env vars are set.

## 2. Config toggles

| Var | Values | Default | Meaning |
|---|---|---|---|
| `STATE_BACKEND` | `postgres` \| `dynamodb` | `postgres` | current-state + predictions store (`aeroflux_ml/io.py`) |
| `LAKE_BACKEND` | `local` \| `s3` | `local` | gold/analytics parquet store |
| `DYNAMODB_TABLE` | table name | `aeroflux-current-state` | single HASH key `flight_key`, no sort key |
| `DYNAMODB_TTL_HOURS` | int | `48` | mirrors local retention; TTL attribute is `expires_at` |
| `S3_BUCKET` | bucket name | *(required if `LAKE_BACKEND=s3`)* | |
| `S3_PREFIX` | key prefix | `""` | |
| `AWS_REGION` | region | `us-east-1` | |
| AWS creds | — | — | `AWS_PROFILE` (local dev) or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (deployed box — no `~/.aws`, has to be env vars) — boto3's default chain picks whichever is present, code never hardcodes one |
| `SYNC_EVERY` | seconds | `300` | `e2e.sh`'s sync-loop cadence; matches `run.sh`'s `REFRESH_SECONDS` — no point syncing faster than gold refreshes |

Leaving `STATE_BACKEND`/`LAKE_BACKEND` unset (or `postgres`/`local`) makes
every cloud-touching piece — `sync_cloud.py`, `data_access.py` — a pure no-op
or local-only read, byte-identical to before any of this existed. That's the
hard requirement this was built to: **fully reversible to local.**

## 3. First-time box setup

1. Docker + compose plugin installed; old app service (if any) stopped and
   disabled (`sudo systemctl stop <old> && sudo systemctl disable <old>`) —
   **check `sudo ss -tlnp | grep -E ':80 |:443 '` is actually empty**, not
   just that the service is "disabled" (see Gotchas §5.1).
2. Ports 80/443/8501 open in the **Lightsail console's networking firewall**
   — this is a separate layer from `ufw` and from Docker's own port
   publishing; all three can independently block traffic (see §5.3).
3. DNS (`aeroflux.duckdns.org` or equivalent) pointed at the box's static IP.
4. A deploy SSH key added to the box's `authorized_keys`.
5. The deploying user should be in the `docker` group
   (`groups $(whoami) | grep docker`) so `docker`/`docker compose` don't
   need `sudo` — confirm this on your box; not independently verified in
   this session (every SSH-run `docker` command here happened to work
   without `sudo`, which only tells you the deploy user already had it set
   up correctly, not that it's automatic).
6. `mkdir -p <REMOTE_DIR>` (this repo's convention: `/home/ubuntu/aeroflux-app`).

## 4. Getting the app running (sample-data mode first)

Deploy mechanics and the cloud data path are deliberately separable —
verify the former with zero AWS dependency before touching credentials.

```bash
# from the repo root, once (or after docker-compose.lightsail.yml/Caddyfile change):
scp -i <key> aeroflux/aeroflux_ui/streamlit_app/docker-compose.lightsail.yml \
             aeroflux/aeroflux_ui/streamlit_app/Caddyfile \
             ubuntu@<ip>:<REMOTE_DIR>/

# build + push happens in CI (push to aeroflux_ui/** triggers .github/workflows/deploy-ui.yml)
# or manually with no SSH needed at all:
./deploy.sh build      # local image build/smoke-test only
./deploy.sh push        # + push to GHCR (needs `docker login ghcr.io` locally)

# on the box:
ssh ubuntu@<ip> "cd <REMOTE_DIR> && docker compose -f docker-compose.lightsail.yml pull && \
                  docker compose -f docker-compose.lightsail.yml up -d"
```

With no `.env` present (or `STATE_BACKEND`/`LAKE_BACKEND` unset in it), the
app runs in its existing built-in sample-data mode — this is the checkpoint
to confirm before wiring anything cloud-related: both
`http://<ip>:8501/_stcore/health` and `https://<domain>/_stcore/health`
should return `ok`.

## 5. Flipping to live cloud data

The box's `.env` (`REMOTE_DIR/.env`, referenced by `docker-compose.lightsail.yml`'s
`env_file: {path: .env, required: false}`) carries the cloud config —
**never committed, never touches the GitHub Action** (the Action only builds/
pushes the image and optionally SSHes to redeploy; it has no AWS credentials
of its own):

```
STATE_BACKEND=dynamodb
LAKE_BACKEND=s3
AWS_REGION=us-east-1
S3_BUCKET=<bucket>
DYNAMODB_TABLE=aeroflux-current-state
AWS_ACCESS_KEY_ID=<aeroflux-app, read-only>
AWS_SECRET_ACCESS_KEY=<aeroflux-app, read-only>
```

Create it directly on the box over SSH (never through a file that lands in
this repo's working tree) — e.g. read the values locally without printing
them, then `ssh ... "cat > <REMOTE_DIR>/.env && chmod 600 <REMOTE_DIR>/.env"`
with a heredoc. Then:

```bash
ssh ubuntu@<ip> "cd <REMOTE_DIR> && docker compose -f docker-compose.lightsail.yml pull app && \
                  docker compose -f docker-compose.lightsail.yml up -d --force-recreate app"
```

`--force-recreate` matters here even though `up -d` alone often works —
see Gotcha §5.2.

**Verify from outside the container isn't possible** (Streamlit renders
client-side over websocket, so `curl` never shows the KPI banner). Verify
*inside* the running container instead:

```bash
ssh ubuntu@<ip> "docker exec aeroflux-ui python -c \"
import data_access as da
print('is_live:', da.is_live())
print('kpis:', da.kpis())
\""
```

Expect `is_live: True`, `mode: 'LIVE'`, real flight counts, and — after the
DynamoDB Scan-cost fix (§5.4) — a read time around 1s, not 30+.

## 6. GitHub Actions secrets / variables

All live in the `aero-flux-stage2-dev` repo (confirm — these were briefly
added to the wrong repo during setup and had to be moved):

| Name | Kind | Value |
|---|---|---|
| `LIGHTSAIL_SSH_HOST` | secret | box's static IP |
| `LIGHTSAIL_SSH_KEY` | secret | private key authorized on the box |
| `LIGHTSAIL_SSH_USER` | secret (optional) | defaults to `ubuntu` |
| `DEPLOY_ENABLED` | **variable**, not secret | must literally be `true` to enable the `deploy` job |
| GHCR auth | — | built-in `GITHUB_TOKEN` with `packages: write` — no PAT |

`DEPLOY_ENABLED` is a `vars` entry on purpose, not a secret — checking
`vars.DEPLOY_ENABLED == 'true'` in the workflow's `if:` is unambiguous either
way, whereas relying on a secret's mere presence has more edge cases. Without
it, `build-and-push` still runs and succeeds on every push — only the SSH
redeploy step is gated, so a manual `deploy.sh`/SSH `pull && up -d` always
has a fresh image to grab regardless.

## 7. Rollback

```bash
./deploy.sh rollback <image-tag>     # any previously-pushed GHCR tag, e.g. the short sha
./deploy.sh status                    # confirm both :8501 and the https domain
```

Every CI build pushes both `:latest` and a short-sha tag
(`ghcr.io/jonathanwilsonami/aeroflux-ui:<sha12>`) — roll back to a specific
one if `:latest` regresses.

---

## 8. Gotchas hit getting this working

### 5.1 — Leftover nginx blocks Caddy from ever binding 80/443
An old systemd+gunicorn deploy's nginx reverse proxy was still running
(`systemctl disable` alone doesn't stop an already-running unit — needs
`systemctl stop` too, and even then, confirm with `ss`, don't trust the
service's reported state). Symptom: `docker compose up` fails with `failed to
bind host port 0.0.0.0:80/tcp: address already in use`, and the *app*
container starts fine (different port) while *Caddy*'s fails — easy to miss
if you only check the app's health. Fix: `sudo systemctl stop nginx &&
sudo systemctl disable nginx`, confirm `sudo ss -tlnp | grep -E ':80 |:443 '`
is empty, *then* bring Caddy up.

### 5.2 — A container that failed to bind ports once needs `--force-recreate`
After fixing 5.1, `docker compose up -d` alone **restarted Caddy's existing
broken container** — the one created during the failed attempt, whose network
config never got the port bindings — rather than recreating it fresh.
`docker ps` showed Caddy running with an *empty* PORTS column. Any time a
container failed to fully start once, use
`docker compose up -d --force-recreate <service>` to force Docker to redo its
networking from scratch, not just restart what's there.

### 5.3 — Three independent port-blocking layers, not one
Direct `:8501` access can be blocked by (a) the app not running, (b) `ufw` on
the box, or (c) the **Lightsail console's own networking firewall** — a
fourth, cloud-level layer entirely separate from anything inside the VM.
`ufw status` showing `inactive` and the container showing healthy locally
(`curl localhost:8501` → 200) does *not* mean external traffic reaches
it — check the Lightsail firewall rules specifically if external `:8501`
times out while `:80`/`:443` (already allowed) work fine.

### 5.4 — GHCR images are private by default, even in a public repo
The first CI-built image push was invisible to an anonymous `docker pull`
(401) despite the source repo being public — GHCR packages pushed via the
built-in `GITHUB_TOKEN` default to private. Fix: make the package public
(profile → Packages → package → Package settings → Change visibility), or
give the box a `read:packages` PAT and `docker login ghcr.io` once. No way
to flip this via the Action's own token — needs a human with package-admin
rights on github.com.

### 5.5 — `python:3.11-slim` has no `curl`
`HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health` reported
`unhealthy` forever despite the app genuinely serving traffic —
`curl: executable file not found in $PATH` inside the container. `curl`
isn't in the slim base image and wasn't installed. Fixed with stdlib
`python -c "import urllib.request; urllib.request.urlopen(...)"` — no image
bloat from an extra `apt install`.

### 5.6 — Docker build context was too narrow once `data_access.py` needed `aeroflux_ml`
Wiring `data_access.py` to `state_backend_from_env()`/`lake_backend_from_env()`
means the image needs the sibling `aeroflux_ml` package — not on PyPI, and
outside the old build context (`aeroflux_ui/streamlit_app/` only). Widened
the Dockerfile's context to `aeroflux/` (`docker build -f
aeroflux_ui/streamlit_app/Dockerfile aeroflux/`), updated CI and `deploy.sh`
to match. This alone doesn't guarantee the import works — `aeroflux_ml`'s
`__init__.py` eagerly imports its whole package tree, which needed `boto3`,
`polars`, and `pyyaml` added to `requirements.txt` (found one at a time by
actually running `docker run --entrypoint python <image> -c "import
aeroflux_ml"` inside the built container, not by inspecting `requirements.txt`
and assuming it was complete).

### 5.7 — `S3_BUCKET` `KeyError`, intermittent — was two duplicate stacks, not an env bug
Traced from `logs/sync_cloud.log`'s near-perfectly-alternating
`FAILED`/`done` lines to **two complete `e2e.sh up` stacks running
concurrently** (one leftover from a prior session, never torn down with
`e2e.sh down`), both logging to the same file. `os.environ["S3_BUCKET"]` is
deterministic — a single misconfigured process fails *every* cycle, not
intermittently; the intermittency only existed because two independently-
configured processes shared a log. Fixed two ways: (a) `e2e.sh up` now
refuses to start a second stack by default (`FORCE=1` to override — see
`e2e.sh`'s `_running_stage_pids`); (b) hardened anyway, since a real
one-off misconfiguration would still hit this — `io.py`'s
`_require_env()` gives a clear, actionable error instead of a bare
`KeyError` three stack frames from the actual cause.

### 5.8 — Silent data loss: `state_rows=0` reported as `done`
`sync_cloud.py`'s state read is a live Postgres query against
`flight_instance`; `run.sh`'s `cmd_pipeline` periodically `TRUNCATE`s and
reloads that same table. A sync landing in that window legitimately reads 0
rows — but `gold_rows`/`prediction_rows` come from local parquet
*snapshots* (unaffected by the truncate), so the cycle still looked
successful. `_upsert_all([])` returned `0` without raising, so `sync_once()`
sailed through and advanced `synced_at` as if nothing was wrong. Fixed:
`sync_once()` now raises if the state read comes back empty while
`gold_rows > 0` (proof there's real current data) — the marker doesn't
advance, the next cycle retries against a stable table.

### 5.9 — Regression in the fix for 5.7: empty-string env passthrough broke defaults
The first version of the `e2e.sh` explicit-passthrough hardening used
`VAR="${VAR:-}"` on the subprocess command line. That sets an **explicit
empty string** for anything unset in the parent — not the same as leaving it
truly unset. `os.getenv("X", default)` only falls back to `default` when `X`
is absent from `os.environ`, not when it's `""`. First reproduction run
crashed immediately: `int(os.getenv("DYNAMODB_TTL_HOURS", "48"))` →
`ValueError: invalid literal for int() with base 10: ''`. The same pattern
was latent for `AWS_REGION`, `DYNAMODB_TABLE`, and `STATE_BACKEND`/
`LAKE_BACKEND`'s "unknown backend" fallback. Fixed by exporting
conditionally: `[ -n "${!v:-}" ] && export "$v"` — never force an empty
variable into existence. **Caught by actually running the restarted loop**,
not by reviewing the diff.

### 5.10 — boto3 rejects native Python `float` and `datetime`
`DynamoDBStateRepository` items were failing with
`TypeError: Float types are not supported. Use Decimal types instead.`
(a `delay_probability` of `0.42`) and, separately,
`TypeError: Unsupported type "<class 'datetime.datetime'>"` (any
`timestamptz` column psycopg hands back natively, or any `datetime` column
from a Polars `.to_dicts()` call). Fixed in `_dynamo_value()`: `float` →
`Decimal(str(v))` (not `Decimal(v)` — avoids binary-float artifacts like
`Decimal(0.42) != Decimal('0.42')`), `datetime`/`date` → `.isoformat()`.

### 5.11 — `interval '%s hours'` silently mis-parses in psycopg
A bind parameter inside a quoted SQL string literal is not real parameter
substitution — `WHERE updated_at > now() - interval '%s hours'` with
`hours=72` always came out as **exactly 1 hour**, regardless of the value
passed, with no error. Verified directly (`SELECT now() - interval '%s
hours'` vs. the literal `interval '72 hours'` gave different results for
the same bound parameter). Fixed with `make_interval(hours => %s)` — a real
function call, so `%s` binds normally.

### 5.12 — DynamoDB `Scan` with no `Limit` reads the whole table
`recent_flight_states()`'s unbounded `Scan`+`FilterExpression` took ~32
seconds against the live table — a real cost/latency problem once
`data_access.py` started calling it on every page load (`st.cache_data`
ttl was 30s at the time, making it worse). Fixed with a `limit` parameter:
a single-page `Scan(Limit=N)` (no `LastEvaluatedKey` pagination when
limited) hard-bounds the read to ~1s, at the cost of possibly returning
fewer than N rows if the filter match rate is low — an accepted trade,
documented as the demo-scale choice (GSI + `Query` is the real scale path).
Paired with bumping the UI's cache TTL 30s→300s to match `sync_cloud`'s
actual write cadence.
