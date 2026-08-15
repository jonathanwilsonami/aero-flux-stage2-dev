# AeroFlux

**Real-time flight-delay prediction over FAA SWIM + ADS-B + weather**

**Live demo:** [aeroflux.duckdns.org](https://aeroflux.duckdns.org)

---

## Table of contents

- [Problem](#problem)
- [Solution](#solution)
- [Sample input and output](#sample-input-and-output)
- [Architecture](#architecture)
- [Repository navigation](#repository-navigation)
- [Setup](#setup)
- [Running the system](#running-the-system)
- [Datasets](#datasets)
- [Reproducing results](#reproducing-results)
- [Contributing and Repository Access](#contributing-and-repository-access)

---

## Problem

The U.S. National Airspace System is a tightly-coupled physical network: an
aircraft, a crew, and a gate are shared resources that get reused all day, so
a disturbance at one airport doesn't stay local — it propagates through
aircraft rotation and shared airport queues until it shows up as a delay
somewhere else, hours later and states away. Predicting that propagation
in real time, from live, incomplete, streaming data, is a harder and more
general problem than predicting any single flight's delay from a clean
historical record.

AeroFlux is a proof-of-concept for that broader problem — **real-time
perception of cascading state in a tightly-coupled physical system** — with
flight-delay prediction as the concrete, measurable instance. The model
treats aircraft rotation (which airframe flew the previous leg, and how
late) as *one nullable signal channel* among several: airport demand,
weather, and schedule form an always-available backbone that the prediction
degrades onto gracefully when rotation can't be resolved from live data,
rather than depending on it outright.

## Solution

AeroFlux fuses **live FAA SWIM** flight-plan/track messages with **ADS-B**
(airframe identity) and **weather** (METAR/NCEI) into a validated per-flight
state, in three tiers:

- **Bronze** — raw SWIM messages, as received, with lineage.
- **Silver** — fused, deduplicated, canonical per-flight state, keyed on
  **GUFI** (not the mutable `flight_ref`).
- **Gold** — the ML feature/label table, computed by **one** feature
  contract (`aeroflux_ml/feature_prep.py`) shared by both training and
  serving, so a model trained on years of historical BTS data runs
  *unchanged* on live SWIM data — parity by construction, not convention.

An **XGBoost** classifier scores gold features into a delay probability.
Predictions and current flight state sync out to **S3 + DynamoDB**; an
always-on **Streamlit** app (containerized, deployed on Lightsail behind
Caddy/TLS, built and pushed by GitHub Actions) reads *only* from that cloud
copy — never the local database — so the live demo stays up independent of
whatever the local ingest machine is doing.

All of this is a **config flip, not two codebases**: every storage call goes
through a `StateRepository`/`LakeStore` abstraction
(`aeroflux_ml/io.py`) selected by `STATE_BACKEND`/`LAKE_BACKEND` env vars —
`postgres`/`local` (default, zero cloud dependency, what CI and local dev
use) or `dynamodb`/`s3` (what the deployed app uses). The whole pipeline is
container-first so it runs the same on a laptop and in the cloud.

## Demo Videos

Walkthroughs showcasing each part of the AeroFlux app.

### Live Flight Map & Network Overview

<video autoplay loop muted controls width="100%">
  <source src="assets/videos/test.mp4" type="video/mp4">
</video>

_Walkthrough of the live flight map, the real-time network overview panels
(flights tracked, carrier breakdown, delay-risk distribution), and how live
data updates as flights move through the system._

### Model Performance Page

https://github.com/jonathanwilsonami/aero-flux-stage2-dev/assets/<your-video-url>

_The model performance page — predictive metrics, feature importances, the
lag-bucketed live evaluation, and the structural-coverage caveat on live
outcome data._

### Analyst Agent

https://github.com/jonathanwilsonami/aero-flux-stage2-dev/assets/<your-video-url>

_The Aviation Analyst Agent answering live flight questions — pulling real
predictions, explaining delay risk through model features, and citing its
sources._

## Sample input and output

**Input** — a raw FAA SWIM TFMS message (`fltdMessage`, abridged):

```xml
<fdm:fltdMessage acid="SWA2606" airline="SWA" depArpt="KMCO" arrArpt="KBNA"
                  flightRef="153371795" msgType="trackInformation"
                  sourceTimeStamp="2026-07-31T21:02:50Z">
  <fdm:trackInformation>
    <nxcm:qualifiedAircraftId aircraftCategory="JET" userCategory="COMMERCIAL">
      <nxce:aircraftId>SWA2606</nxce:aircraftId>
      <nxce:gufi>KJ5957868p</nxce:gufi>
      <nxce:departurePoint><nxce:airport>KMCO</nxce:airport></nxce:departurePoint>
      <nxce:arrivalPoint><nxce:airport>KBNA</nxce:airport></nxce:arrivalPoint>
    </nxcm:qualifiedAircraftId>
    <nxcm:position>...</nxcm:position>
    <nxcm:ncsmTrackData>
      <nxcm:eta etaType="ESTIMATED" timeValue="2026-07-31T21:31:02Z"/>
    </nxcm:ncsmTrackData>
  </fdm:trackInformation>
</fdm:fltdMessage>
```

**Fused into canonical silver state** (`flight_instance`, one row per
flight — see `AeroFlux_DataSchemas.md` for the full schema):

```json
{
  "flight_instance_id": "KJ5957868p", "gufi": "KJ5957868p",
  "callsign": "SWA2606", "flight_number": "WN2606",
  "carrier_icao": "SWA", "carrier_name": "Southwest Airlines",
  "origin": "KMCO", "destination": "KBNA",
  "scheduled_gate_departure": "2026-07-31T19:45:00Z",
  "scheduled_gate_arrival": "2026-07-31T21:44:00Z",
  "estimated_arrival": "2026-07-31T21:31:02Z",
  "actual_off": "2026-07-31T20:02:00Z", "actual_on": null,
  "flight_status": "ACTIVE",
  "tail_number": null, "tail_source": "none", "hex": null
}
```

**Scored into a prediction** (`predictions`, joined on `flight_key` =
`flight_instance_id` — see `AeroFlux_DataSchemas.md`):

```json
{
  "flight_key": "KJ5957868p",
  "delay_probability": 0.82,
  "predicted_delayed": 1,
  "model_version": "xgb_v2",
  "feature_version": "fe_v1",
  "scored_at": "2026-07-31T21:05:00Z"
}
```

## Architecture

![AeroFlux architecture diagram](images/design/aeroflux-architecture-final.svg)

*(Source draw.io file: `arch_diagrams/aeroflux-architecture-final.drawio` —
exported PNG/SVG lands in `images/design/`.)*

AeroFlux is a **hybrid on-prem + cloud** system. This split is deliberate. The streaming pipeline is compute- and data-intensive but doesn't need to be publicly hosted, so it runs where compute
is cheap and controllable — on-prem. The serving layer needs to be always-on,
publicly reachable, and low-maintenance, so it lives in the cloud on a small
footprint, reading only a synced copy of the data. The two halves are decoupled
by a config toggle: the same code runs fully local (Postgres, filesystem) or
against cloud backends (S3, DynamoDB) with no changes, making the platform
portable across a laptop, on-prem hardware, or any cloud VM.

Left to right: **live sources** (SWIM, ADS-B, METAR/NCEI) feed a Kafka
bridge into **bronze** (raw Postgres), which is fused into **silver**
(canonical per-flight state, Postgres). In parallel, **historical BTS**
data + cached weather feed the same feature contract for **training**. Both
paths converge on **gold** (Parquet feature tables), which either trains the
XGBoost model (with SparkML GBT as the distributed scale path) or gets scored
by it live. A `sync_cloud.py` step pushes gold to **S3** and current state +
predictions to **DynamoDB**; the deployed **Streamlit app** reads only from
that cloud copy.

Alongside the app runs an **Aviation Analyst Agent** — a separate,
internal-only service (LangGraph + LLM + pgvector RAG) that the app calls over
HTTP. It reads the same live cloud data (DynamoDB predictions/state, S3 gold
features) through the same **read-only** credentials to answer flight-specific
questions and explain predictions in natural language.

The **local/cloud split** is the config toggle described above — everything
left of `sync_cloud.py` runs on the local ingest machine regardless of backend;
everything reading through `data_access.py` (the Streamlit app) and the agent's
tools is backend-aware and reads whichever store is configured. Only the app
and the agent's HTTP endpoint are exposed; all serving components read the cloud
copy **read-only**.

## Repository navigation

```
aeroflux/
├── aeroflux_parser/        # SWIM parse -> fuse -> resolve -> validate (silver)
│   ├── fusion.py, identity.py    #   GUFI-keyed fusion, carrier/tail resolution
│   └── adsb.py, adsb_store.py    #   ADS-B airframe identity (rolling store)
├── aeroflux_ml/             # the ML side
│   ├── feature_prep.py           #   THE feature contract — fill policy, parity set
│   ├── schema.py                 #   from_bts()/from_silver() -> one canonical frame
│   ├── io.py                     #   StateRepository/LakeStore — the cloud storage seam
│   ├── sync_cloud.py             #   local -> cloud sync (gold, state, predictions, eval)
│   ├── score_live.py             #   scores live gold -> predictions
│   ├── evaluate_live.py          #   reconciles live predictions against outcomes
│   ├── weather.py, weather_cache.py  # live METAR + cached historical NCEI
│   ├── bts_source.py             #   BTS fetch/cache/discover-local-CSV
│   ├── pipeline.py, run.py       #   silver -> gold pipeline + CLI
│   └── training/                 #   config-driven training pipeline (see below)
├── aeroflux_ui/streamlit_app/   # the demo UI
│   ├── app.py, data_access.py    #   entry point; cloud-aware reads (local/S3+DynamoDB)
│   └── pages/                    #   Live Map, Analyst (agent chat), Live Inference,
│                                  #   Model Performance (live eval + feature importances)
├── scripts/                 # build_bts_gold.py, sync_cloud.sh, aws_setup.sh,
│                             #   smoke_cloud_backends.py, baseline_metrics.sh
├── configs/                 # pipeline.yaml, training.yaml
├── run.sh                   # live ingestion orchestrator (setup/stream/status/stop)
├── e2e.sh                   # full train -> serve -> sync -> UI orchestration
└── tests/                   # 97 tests: parse, ML, training, sync, live-eval
```

**Docs** (repo root): [`CLAUDE.md`](CLAUDE.md) — operational quick-reference
and golden rules (start here for how the codebase is meant to be used);
[`PROJECT_CONTEXT.md`](PROJECT_CONTEXT.md) — full mission, architecture
decisions, current state, known limitations, roadmap;
[`DEPLOYMENT.md`](DEPLOYMENT.md) — cloud storage config + Lightsail deploy
flow, every gotcha hit getting there; [`AGENT_INTEGRATION.md`](AGENT_INTEGRATION.md)
— the wire contract and production data-read path for the reasoning/agent
layer; [`AeroFlux_DataSchemas.md`](AeroFlux_DataSchemas.md) — every dataset,
its schema, and sample records; [`AeroFlux_DataDictionary.md`](AeroFlux_DataDictionary.md)
— feature definitions and the train/live parity table.

## Setup

**Prerequisites**

If you plan to run this locally (on-prem, vs. cloud deployment) you will need at least the following:

- **Python 3.11** (3.10–3.12 supported per `pyproject.toml`)
- **Docker + Docker Compose** (tested with Docker v29.4.3)
- **Conda** (`environment.yml`) for the base Python environment, and **Poetry** (or `pip`) for the `aeroflux` package itself
- **Postgres** (started for you via Docker — see below) and optional **pgAdmin**
- **SWIM credentials** for live FAA ingestion (see `.env.example`)

> **Note:** The core infrastructure (Kafka, Postgres, and the deployed app) runs in Docker containers, orchestrated via `docker-compose.yml`. You don't install these services directly on your machine — Docker manages them for you.

For more detail on the Python/package setup, see the [Downloading and Installing the Project](#downloading-and-installing-the-project) section.

```bash
cd aeroflux
python -m pip install -e .          # or: pip install poetry && poetry install
cp .env.example .env                # fill in SWIM credentials + Postgres values
./run.sh setup                      # Kafka + Postgres up, topic + tables created
```

### Local Postgres

`./run.sh setup` starts Postgres in Docker (via `docker-compose.yml`) and creates the required tables and Kafka topic automatically — you do **not** need to install Postgres separately. Default local connection:

```bash
postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux
```

To inspect the database, either connect with `psql`:

```bash
psql postgresql://aeroflux:aeroflux-db@localhost:5432/aeroflux
```

or use **pgAdmin** (optional) at `http://localhost:5050` if enabled in the compose file — log in and register a new server pointing at host `localhost`, port `5432`, with the credentials above.

> **Note:** These are local development credentials for a throwaway container, not secrets. Production/cloud deployments use managed services and separate credentials (see `DEPLOYMENT.md`).

### Running and Viewing the App Locally

Once the stack is set up, bring up the full pipeline and UI:

```bash
source ~/aeroflux-cloud.env    # cloud backends (S3/DynamoDB); omit for local-only
./e2e.sh up                    # starts ingest, scoring, sync, archive, and UI
./e2e.sh health                # verify all stages are running
```

The Streamlit app is served at **http://localhost:8501** — open it in your browser to see the live flight map, live network overview, model performance, and analyst pages.

To stop everything:

```bash
./e2e.sh down
```

That's local infra only — no cloud dependency, no AWS account needed to
develop or run the tests.

**Cloud resources** (optional — only needed to sync to S3/DynamoDB or
deploy the always-on app): `scripts/aws_setup.sh` provisions the S3 bucket,
DynamoDB table, and IAM policies. Full env-var reference (`STATE_BACKEND`,
`LAKE_BACKEND`, `S3_BUCKET`, `DYNAMODB_TABLE`, credentials) and the
Lightsail deploy flow are in **[`DEPLOYMENT.md`](DEPLOYMENT.md)** — not
duplicated here.

## Running the system

```bash
# local-only (no cloud env vars set):
./e2e.sh up             # train -> serve -> UI, everything local
./e2e.sh health          # check every stage is actually running
./e2e.sh down            # tear it all down

# with cloud sync + the always-on deploy target (see DEPLOYMENT.md for the env vars):
export STATE_BACKEND=dynamodb LAKE_BACKEND=s3 AWS_REGION=us-east-1 \
       S3_BUCKET=<bucket> DYNAMODB_TABLE=aeroflux-current-state
./e2e.sh up              # same command — now also syncs to S3/DynamoDB each cycle
```

Expected output for system up with the system health check:
![system up](images/system/system-up.png)

Individual stages, if you want to run/inspect one at a time (see `e2e.sh`
and `run.sh` for the full command set): `cmd_ingest` (SWIM bridge +
consumer + ADS-B poller), `cmd_score` (score live gold), `cmd_sync_cloud`
(push to S3/DynamoDB), `cmd_archive` (retention), `cmd_ui` (Streamlit app).

**View the live app:** [aeroflux.duckdns.org](https://aeroflux.duckdns.org)
(always-on, cloud-backed) or `http://localhost:8501` when running `e2e.sh
up`/`cmd_ui` locally.

### Checking Logs

Each pipeline stage writes to its own log under `logs/`. To follow a stage live:

```bash
tail -f logs/ingest.log         # SWIM ingestion
tail -f logs/sync_cloud.log     # cloud sync (S3/DynamoDB)
tail -f swim_to_kafka.log       # raw SWIM → Kafka bridge
```

For a quick health snapshot across all stages (process status, message flow, row counts):

```bash
./e2e.sh health
```

This reports whether each stage (ingest, score, sync, archive, UI) is running, and flags issues like a stalled ingest.

Example health check output:

![health check](images/system/health-check.png)

Example log output:

![ingest log](images/system/kafka-logs.png)

## Datasets

| Source | Used for | Link |
|---|---|---|
| FAA SWIM (TFMS flight data) | Live flight plans, tracks, ETAs | [FAA SWIM](https://www.faa.gov/air_traffic/technology/swim) |
| ADS-B (airplanes.live / adsb.lol) | Live airframe identity (hex, tail, type) for rotation | [airplanes.live](https://airplanes.live), [adsb.lol](https://adsb.lol) |
| NOAA METAR (AWC) | Live weather (wind, visibility, IFR) | [aviationweather.gov](https://aviationweather.gov) |
| NOAA NCEI | Cached historical weather (10-year training) | [ncei.noaa.gov](https://www.ncei.noaa.gov) |
| BTS On-Time Performance | Historical training labels (true gate times, tail numbers) | [transtats.bts.gov](https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGJ) |
| Airport / airline reference dims | ICAO/IATA crosswalks, names, timezones | bundled CSVs, `aeroflux_parser/data/` |

**Retention:** the live pipeline keeps a rolling **48-hour** window (bronze
+ silver, Postgres) — this is a demo/cost choice, not a platform limit.
BTS training data and the gold/lake parquet are persistent, not
time-windowed.

## Reproducing Results

### The Production Model

The deployed model was trained on **~11 years of BTS On-Time Performance data
(2015–2025) joined with NCEI historical weather — roughly 70 million flight
records.** Training is driven entirely by a YAML config
(`aeroflux/configs/training.yaml`), so data range, features, model hyperparameters,
compute backend, and outputs are all editable without touching code.

### What You Need

1. **BTS training data** — `scripts/build_bts_gold.py` fetches and caches it
   (public, no credentials — just bandwidth/time for a multi-year pull), or
   point `--cache` at an existing local cache.
2. **Cached weather** — `data/weather` (NCEI) and the station→ICAO bridge
   (`data/reference/airport_to_station_2019.csv`), both bundled/fetchable.
3. **Environment** — see [Setup](#setup) (`pip install -e .` / `poetry install`);
   no cloud account needed for training or `pytest`.

### Training

Quick single-year run (fast, for validation):

```bash
python scripts/build_bts_gold.py --months 2015-01:2015-12 --cache data/bts \
    --weather-cache data/weather --station-bridge data/reference/airport_to_station_2019.csv
python -m aeroflux_ml.training.cli train --config configs/training.yaml --gold <gold.parquet>
python -m pytest      # 97 tests
```

Full production-scale run (~11 years, ~70M records — requires significant time
and memory and will most likly not run on a laptop. The model was trained on a system that had over 100 GB of RAM.):

```bash
python scripts/build_bts_gold.py --months 2013-01:2023-12 --cache data/bts \
    --weather-cache data/weather --station-bridge data/reference/airport_to_station_2019.csv
python -m aeroflux_ml.training.cli train --config configs/training.yaml --gold <gold.parquet>
```

### Configuring / Customizing the Model

Training is YAML-driven (`configs/training.yaml`), deep-merged over sensible
defaults — a minimal YAML still runs. Key options you can change without
touching code:

| Setting | Controls |
|---|---|
| `data.gold_path` / `data.target` | Training data and label column |
| `split.strategy` (`time` \| `random`) | Time-aware split (default) vs. random |
| `cv` / `tuning` | Cross-validation and hyperparameter grid search |
| `models[].params` | XGBoost hyperparameters (`max_depth`, `learning_rate`, `n_estimators`, etc.) |
| `compute.backend` (`local` \| `spark`) | Single-node Polars vs. distributed Spark |
| `registry.backend` (`local` \| `mlflow`) | Local run registry vs. MLflow tracking |

To train your own model, copy `configs/training.yaml`, edit the values, and pass
it with `--config`. The default is a single XGBoost model with a time-aware
80/20 split; enable `tuning.enabled` for grid search or `cv.enabled` for
cross-validated evaluation.

### Deploying a Trained Model Live

A completed training run writes a model artifact to `model_outputs/runs/<run_name>/`.
To serve it in the live pipeline, point the scoring stage at that run directory —
the live scorer (`score_live.py`) loads `current_model.joblib` and applies the
**same feature contract** used in training, so a model trained here scores live
flights without modification. See `DEPLOYMENT.md` for wiring a new model into the
deployed app.

### What Requires External Access

A *live* run (`./run.sh stream` / `./e2e.sh up` without cloud vars) needs your
own **FAA SWIM credentials** — SWIM access is account-gated by the FAA. Syncing
to the cloud or deploying the always-on app needs your own **AWS account** (S3 +
DynamoDB + IAM, via `scripts/aws_setup.sh`) and a **Lightsail** (or equivalent)
VM — see `DEPLOYMENT.md`. Training on BTS and running the test suite need
neither.

---

## Contributing and Repository Access

### Requesting Write Access

Contact the project owner to request write access to the repository. Contributors without direct write access may fork the repository and submit a pull request with their proposed changes.

You may also need to configure SSH keys for secure GitHub access. See the [GitHub SSH documentation](https://docs.github.com/en/authentication/connecting-to-github-with-ssh) for setup instructions.

### Clone the Repository

Use the green **Code** button on the GitHub repository page to copy the appropriate HTTPS or SSH clone URL.

```bash
# Navigate to your desired project directory
git clone <REPO_URL>

# Enter the project root
cd <REPO_FOLDER>
````

For setting up the system see the above section on system setups.

### Downloading and Installing the Project

**Python environment.** AeroFlux targets **Python 3.11**. Create and activate an isolated environment first (conda shown; a `venv` works too):

```bash
conda create -n aeroflux python=3.11 -y
conda activate aeroflux
```

Then install the `aeroflux` package. It is built and managed with **Poetry**, which is the preferred workflow:

```bash
pip install poetry
poetry install
```

This installs the package plus all dependencies into your active environment.

Alternatively, for a quick editable install without Poetry:

```bash
python -m pip install --upgrade pip
python -m pip install -e .
```

### Installing and running Quarto (Optional)

**This is only needed if you wish to update the informational `Project Site` (see below). This is not
the actual AeroFlux application but a site for items such as papers.**

Quarto is used to build our project site mentioned above. You will need Quarto if you want to make edits to any documents on the site pages.

To install Qaurto see [Quarto installation guide](https://quarto.org/docs/get-started/)

The following are useful Quarto commands:
```bash
# To render entire site - Note: Need to do this anytime you want your changes to be reflected on the site.
quarto render
# To see the site in your local browser. Make sure you do this to check for any issues.
quarto preview

# To render and view a single notebook
quarto preview <notebook>.qmd
```

## Project Site Overview

### Website Structure and Key Pages (Quarto Overview)

The main site is built using **Quarto**, which converts `.qmd` (and `.ipynb`) files into a static website. The overall structure and navigation are defined in the `_quarto.yml` file at the root of the project. This file controls the **navbar (top menu)**, theme, and where the rendered site is output (`docs/` folder for GitHub Pages).

- **Home Page** → `index.qmd`  
  Main landing page of the site

- **About Page** → `about.qmd`  
  Team bios and project context

- **Project Proposal** → `aeroflux_stage2_proposal.qmd`  
  Project Proposal

- **Final Paper** → `aeroflux-final-paper.qmd`  
  Project Proposal

More pages will be added as the project progresses.

#### Other Useful Quarto Docs

- **Images** → `images/`  
  Where the site grabs images.

- **docs** → `docs/`  
  When the site is built or rendered (quarto render) it will place all the html, css, js etc. code into this folder. This folder is basically the site. It's what the git workflow will pick up (part of the CI/CD) and what github pages will deploy on github. The GitHub Action (Workflow) is responsible for **publishing the rendered Quarto site to GitHub Pages**. It does **not build the site**—it simply takes the already-rendered files in the `docs/` folder and pushes them to the `gh-pages` branch, which GitHub uses to host the website.

### How Quarto Works (High-Level)

- Each `.qmd` or `.ipynb` file = **one page on the site**
- Quarto renders everything into the `docs/` folder (this is what GitHub Pages serves)
- The `_quarto.yml` file defines:
  - Navigation (navbar + sidebar)
  - Site layout and structure
  - Rendering behavior

---

### Git Workflow for Maintaining and Contributing to the Quarto Site

To keep the Quarto site stable and organized, all work should be done through feature branches rather than directly on `main`.

#### Recommended Step-by-Step Workflow

Note that you can also use the IDE extensions to do all of the following.

```bash
# BEFORE making changes
# 1. Move to main and get the latest updates
git checkout main # This is the default branch and you may already be on it
git pull origin main # Get latest updates

# 2. Create a new branch for your work
git checkout -b your-branch-name
# If you have already made changes you can move them to this new branch.

# 3. Make changes to the Quarto site files
#    Example: .qmd files, _quarto.yml, scripts, images, etc.

# 4. Preview locally to verify the site builds
quarto render # Builds the site. You only need to run this once before pushing or opening the PR to confirm a clean full build. Otherwise your IDE will usually build your site.
quarto preview # view changes locally. Make sure everything works before pushing!

# 5. Stage and commit your changes
# Add anything you do not want in git to .gitignore before you run the commands below.

# IMPORTANT: Before committing, sync your branch with latest changes from main
git fetch origin
git merge origin/main
or
git pull
# Resolve any conflicts if they appear before continuing

git status # Shows you what things are tracked or untracked. Can use this to know what you need to track or commit.

git add path/to/file1 path/to/file2
# or you can add everything like this. Caution: Make sure you know what you are pushing if you use git add .
git add .

git commit -m "update descriptive message here"
# You can also just run git commit and it will enter you into an editor to write the comment.
# If you prefer using an IDE you can do the same thing using buttons.

# 6. Push your branch to GitHub
git push -u origin your-branch-name

# 7. Open a Pull Request into main on GitHub
#    Review changes, discuss if needed, and merge after approval

# 8. After merge, update local main
git checkout main
git pull origin main

# 9. Delete the old branch locally only once the change has been made
git branch -d feature/your-branch-name
```

#### Best Practices
- Always preview the Quarto site locally before committing
- Do not commit large raw datasets, cached files, or environment-specific files
- If multiple people are editing, pull from main often to reduce merge conflicts.

This is a general workflow. You may have to do some additional things if you get stuck.

### Publish To Github Pages

I added a github workflow ci-cd to automatically push to Github pages. So when you add your changes and push it should automatically push the quarto site too. Note: This will only work if you are working directly on main. If you are working on your own branch your work will show up once your branch has been merged into main. Make sure you run quarto render to render before pushing your changes.  

If you need to manually push to Github Pages use the following command:

```bash
quarto publish gh-pages
```

This will push the quarto site to Github.

