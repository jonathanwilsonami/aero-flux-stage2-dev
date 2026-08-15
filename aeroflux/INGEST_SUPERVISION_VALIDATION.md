# Ingest Supervision Redesign — Parallel Validation Runbook

**Status: NOT live.** This validates the bridge-supervision redesign in
`run.sh` (`cmd_ingest`/`cmd_stream`), `e2e.sh` (`cmd_ingest`'s single-
instance guard), and `swim_to_kafka.py` (heartbeat write) — written
2026-08-15 to replace three separate point-fixes this session (a receiver
crash, an `.env` busy-loop, and an elapsed-time drift bug that let a dead
bridge sit unsupervised for hours) with one coherent design. It is **not**
running anywhere yet. The live stack (`e2e.sh ingest` from 07:22,
PID tree rooted at `874113`) is still on the pre-redesign code and stays
that way until you're satisfied here and swap it over yourself.

What changed, briefly (full rationale is in the design discussion, not
repeated here):
- `cmd_stream` split into two independent loops (bridge supervision vs.
  pipeline refresh) so one can't starve the other's timing.
- Real wall-clock (`date +%s`) tracking everywhere, not a sleep-only
  accumulator that can drift.
- A heartbeat file (`INGEST_HEARTBEAT_FILE`, written by `swim_to_kafka.py`
  on connect + throttled per-publish) so a wedged-but-alive receiver can be
  detected, not just a dead PID.
- Bounded, escalating backoff (5s→10s→20s→...→60s) only on *rapid* repeat
  failures (<30s alive) — a clean recycle after a healthy session still
  relaunches immediately.
- `SUPERVISOR_PID`-based orphan self-termination — an orphaned
  `run.sh stream` (its `e2e.sh` parent gone) shuts itself down within one
  poll tick instead of running unsupervised.
- `e2e.sh cmd_ingest` now refuses to start a second tree (or cleanly
  replaces one with `FORCE=1`) — closes the exact gap that let the
  04:43 and 07:22 sessions coexist earlier today.
- The bridge's `--duration` recycle is now a generous 8h safety valve
  (`INGEST_SECONDS`, was the 1h value that forced periodic supervision
  check-ins), since supervision is reactive now, not timer-driven.

This runbook already went through unit-level isolated testing (dummy
processes, no real infra — see the earlier session's throwaway test
scripts) covering all 5 mechanisms individually. **This is the next,
longer step: watching the real, integrated code run for real, without
being anywhere near the live stack.**

## 1. Isolation setup

### 1a. Worktree (separate checkout, separate `out/`/`logs`/PID files for free)

```bash
cd /home/jon/AeroFlux/aero-flux-stage2-dev
git status --short aeroflux/run.sh aeroflux/e2e.sh aeroflux/swim_to_kafka.py   # review first
git worktree add /tmp/aeroflux-supervision-test main
cd /tmp/aeroflux-supervision-test

# Apply the redesign on top of this worktree's own main (it doesn't have
# your uncommitted changes from the primary checkout -- copy them over):
cp /home/jon/AeroFlux/aero-flux-stage2-dev/aeroflux/run.sh \
   /home/jon/AeroFlux/aero-flux-stage2-dev/aeroflux/e2e.sh \
   /home/jon/AeroFlux/aero-flux-stage2-dev/aeroflux/swim_to_kafka.py \
   aeroflux/
cd aeroflux
```

Because `$ROOT` in both scripts is derived from the script's own location
(`ROOT="$(cd "$(dirname "$0")" && pwd)"`), everything path-based —
`out/`, `logs/`, `out/.ingest_pid`, `out/.ingest_heartbeat`,
`swim_to_kafka.log` — is automatically a different filesystem location
from the real checkout the moment you run from this worktree. No extra
config needed for that part.

### 1b. Scratch Postgres DB (same running Postgres container, separate DB)

Don't stand up a second Postgres container — it'd try to bind the same
host port (5432) the real one already holds. Reuse the running container,
point at a new, empty database instead:

```bash
psql "postgresql://aeroflux:aeroflux-db@localhost:5432/postgres" \
  -c "CREATE DATABASE aeroflux_supervision_test OWNER aeroflux;"
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux_supervision_test"
```

`./run.sh setup` (run once below) creates its tables in whatever `$DSN`
points at — this keeps every row this validation writes fully separate
from the live stack's `flight_instance`/`swim.raw_messages`.

### 1c. Scratch Kafka topic (same running Kafka container, separate topic)

```bash
export KAFKA_TOPIC="swim.raw.flight.supervision_test"
```

Kafka auto-creates topics on first publish by default in this setup, so no
extra provisioning step — just make sure this env var is set before you
start ingest, so the bridge never touches the real `swim.raw.flight` topic
the live consumer reads.

### 1d. The one isolation `out`/`DSN`/topic alone DON'T cover: the real SWIM queue is exclusive

`swim_to_kafka.py` binds `SCDS_QUEUE_FLIGHT` as a **durable exclusive
queue** — only one consumer can hold it at a time. The live stack already
holds it right now. If this worktree's `.env` points at the same real SWIM
credentials, the test bridge will either fail to bind (Solace rejects the
second exclusive consumer) or, worse, contend with the live one — neither
is a real risk of *stealing/corrupting* the live stack's data (exclusivity
protects against that), but a rejected/never-connecting bridge also can't
give you a real heartbeat to validate stall-detection against, and
retrying forever against a broker it can't win isn't a useful signal
either.

**Recommended: don't rely on a real SWIM connection for this validation at
all.** Everything in the checklist below works by directly signaling the
bridge process (`kill -STOP`/`kill`/`kill -9`) — that's real, not
simulated, at the OS-process level the supervisor actually watches (PID
liveness + heartbeat file mtime), and it's what the earlier isolated
dummy-process tests already proved works for the logic itself. This
worktree's `.env` can keep real SWIM credentials (needed for `run.sh
setup`/`cmd_consume`/`cmd_adsb` to make sense as a real environment) — you
just won't be relying on messages actually flowing through a successful
SWIM connection to drive the checklist. If you separately want to confirm
the bridge can connect to real SWIM at all with the redesigned code, that
needs its own brief moment (seconds, to see one "Connected." log line) —
not a multi-hour concurrent soak against the same exclusive queue as live
ingest.

## 2. Exact commands to start it

```bash
cd /tmp/aeroflux-supervision-test/aeroflux
export DSN="postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux_supervision_test"
export KAFKA_TOPIC="swim.raw.flight.supervision_test"
export RAW_TABLE="swim.raw_messages"        # fine to share the name -- different DB entirely
export DURATION=continuous                   # so e2e.sh's outer loop is exercised too
export INGEST_SESSION_SECONDS=28800          # 8h safety valve (the real default -- override
                                              # lower here temporarily if you want to see a
                                              # recycle without waiting 8h, e.g. 300 for a
                                              # 5-minute run to watch one full cycle)

./run.sh setup                               # creates tables in the scratch DB only
./e2e.sh ingest                              # starts the self-supervised bridge tree
tail -f logs/ingest.log                      # watch it
```

Record the PID it prints (`out/.ingest_pid`) and the bridge's own PID from
the first `bridge started ... (PID ...)` line in `logs/ingest.log` — you'll
use both below.

## 3. Checklist — what to watch for

Do these roughly in order; each is independent, so failures don't block
testing the others.

### ☐ Clean bridge death → prompt relaunch

```bash
BRIDGE_PID=$(pgrep -f "swim_to_kafka.py.*supervision" || pgrep -f "python swim_to_kafka.py" | tail -1)
kill "$BRIDGE_PID"          # SIGTERM -- a clean-ish exit, not a crash
```
**Expect** in `logs/ingest.log`, within one `INGEST_POLL_SECONDS` (default
10s): `SWIM bridge exited -- relaunching (fail streak: 0)` immediately
followed by a new `bridge started ... (PID <new PID>)` line — no
`backoff` mentioned (fail streak 0 means no delay), since this looks like
a normal-length session ending, not a crash loop.

### ☐ Stall (heartbeat staleness) detected and recovered

```bash
BRIDGE_PID=$(pgrep -f "python swim_to_kafka.py")
kill -STOP "$BRIDGE_PID"    # freeze it -- alive (kill -0 succeeds) but can't write anything
```
Wait past `INGEST_STALE_MINUTES` (default 5min) plus the startup grace
window. **Expect**: `SWIM bridge (PID ...) stalled (no heartbeat in Ns) --
terminating it`, then a relaunch line. Confirm the frozen PID is actually
gone afterward (`kill -0 $BRIDGE_PID` should fail) — the supervisor sends
TERM then a KILL fallback 2s later, so a genuinely stuck process still
gets cleared.

### ☐ Single-instance guard refuses a second start

```bash
./e2e.sh ingest            # while the one from step 2 is still running
```
**Expect**: `ERROR: ingest is already running (pid <N>) -- refusing to
start a second one.` and a non-zero exit — confirm with `echo $?`. Then:
```bash
FORCE=1 ./e2e.sh ingest
```
**Expect**: the old tree stopped (`FORCE=1 -- stopping existing ingest
supervisor...`), a pause while it confirms the old PID is actually gone,
then a fresh `ingest pid <new N>` — check `out/.ingest_pid` changed and
the OLD outer PID is no longer alive (`kill -0 <old N>` fails).

### ☐ Orphan self-termination

```bash
OUTER_PID=$(cat out/.ingest_pid)
kill "$OUTER_PID"           # kill the e2e.sh outer loop directly, NOT run.sh stop
```
This mimics the exact 04:43/07:22 incident: the outer supervisor dies, but
nothing sends the inner `run.sh stream` (and its bridge) any signal at
all. **Expect**: within one poll tick, `logs/ingest.log` shows `supervisor
(PID ...) is gone -- this run.sh stream is orphaned, stopping`, and
`ps -ef --forest` shortly after shows the ENTIRE tree gone — `run.sh
stream`, the bridge, `kafka_to_postgres.py`, `adsb_poller.py`, and the
pipeline-refresh subshell. If anything is still alive after ~30s, that's a
finding — the old bug left orphans running for hours, so this is the one
check worth confirming most carefully.

### ☐ 8h safety-valve recycle behaves correctly

Full-length (8h) is impractical to sit and watch — instead, temporarily
run a short session to observe one complete recycle:
```bash
INGEST_SESSION_SECONDS=180 ./run.sh stream 180 >> logs/ingest.log 2>&1 &
```
**Expect**: the bridge runs the full ~180s (check its "Connected" log
timestamp vs. the eventual "Stopped after publishing N message(s)" or the
supervisor's `SWIM bridge exited` line — should be ~180s apart, not
early), then relaunches immediately with **fail streak: 0** (a full-length
session ending is not a "rapid failure," so no backoff should apply even
though this is a shorter-than-8h override — the 30s-rapid-failure window
is what matters, not the configured `$s` itself).

### ☐ Holds steady across multiple cycles without drift

This is the actual regression check for the original bug. Let it run
across **at least 3 relaunch cycles** (use a short `INGEST_SESSION_SECONDS`
like 120–300 for this specifically, not the real 8h value) and watch:
```bash
# Real wall-clock gap between consecutive "bridge started" lines should
# track the configured session length closely (seconds of overhead, not
# minutes/hours) -- this is exactly what the old code got wrong.
grep "bridge started" logs/ingest.log | awk '{print $1}'
```
**Expect**: the timestamps between consecutive relaunches stay
consistent — no growing gap. Also check process count doesn't creep up
over the cycles (`pgrep -c -f "python swim_to_kafka.py"` should read
**1** at almost every point in time — briefly 2 is fine right at a
relaunch handoff, but it should never sit at 2+ for more than a few
seconds):
```bash
watch -n 5 'pgrep -c -f "python swim_to_kafka.py"; ps -ef --forest | grep -E "run.sh stream|swim_to_kafka"'
```

## 4. Confirming it's NOT interfering with the live stack

Run these against the **real** checkout (`/home/jon/AeroFlux/aero-flux-stage2-dev/aeroflux`),
not the worktree, at any point during the validation window:

```bash
# The real tree's PIDs should be UNCHANGED from before you started the
# validation worktree -- if any of these differ, something crossed over.
ps -ef --forest | grep -E "run\.sh stream 3600|e2e\.sh ingest|swim_to_kafka.py --duration 3600"
# Expect: PIDs 874113 / 874115 / 876881 (or whatever they are when you
# actually run this) -- same numbers throughout, not replaced.

# Real message flow into the REAL topic/DB should be uninterrupted --
# confirm growth, same as any other health check:
psql "$DSN_REAL" -tAc "SELECT count(*) FROM swim.raw_messages WHERE stored_at > now() - interval '5 minutes';"

# The scratch DB/topic should be the ONLY place the validation worktree's
# activity shows up -- confirm the real DB's row count isn't being
# double-counted or the real topic isn't seeing duplicate/foreign messages:
psql "postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux" -tAc \
  "SELECT count(*) FROM swim.raw_messages;"   # compare to a baseline taken before starting
```

If the real PIDs ever change, or real-DB growth stalls, or the real topic
shows messages you can trace to the test worktree's callsigns/content —
stop the validation worktree immediately (`kill $(cat out/.ingest_pid)` in
*that* worktree only) and investigate before continuing.

## 5. Sign-off checklist before swapping into live ingest

- [ ] Clean death → immediate relaunch, `fail streak: 0`, no backoff logged
- [ ] Stall (frozen via `kill -STOP`) → detected, killed, relaunched
- [ ] Second `./e2e.sh ingest` refused without `FORCE=1`
- [ ] `FORCE=1 ./e2e.sh ingest` cleanly replaces (old PID confirmed dead, new PID confirmed alive)
- [ ] Killing the outer PID directly → inner tree self-terminates fully within ~30s, no orphans left (`ps -ef --forest` confirms)
- [ ] A full-length session recycle relaunches immediately (fail streak 0), not delayed
- [ ] ≥3 consecutive relaunch cycles show consistent timing (no growing gap) and process count never sits above 1 bridge process
- [ ] Real live stack's PIDs unchanged and `raw_messages` growth uninterrupted throughout
- [ ] No crossover between scratch DB/topic and the real ones (row counts checked before/after)
- [ ] Ran unattended for a stretch you're comfortable with (hours, not minutes) without manual intervention

Once all of these are checked, the swap-in is: stop the live tree the same
way section 3's orphan test did it cleanly (`kill "$(cat out/.ingest_pid)"`
in the **real** checkout, confirm the tree fully self-terminates via the
new orphan check — no more `run.sh stop` + hope), then `./e2e.sh ingest`
fresh from the real checkout, now running the validated code.
