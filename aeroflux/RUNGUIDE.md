# AeroFlux — RUNGUIDE

From a fresh machine to gold ML data, in one script. This guide serves two
audiences: (1) a teammate who just cloned the repo, and (2) future-us shifting
components to the cloud by changing config, not code.

---

## 1. Prerequisites

Install once on the machine (local or a cloud VM):

- **Docker** + **Docker Compose v2** — https://docs.docker.com/engine/install/
- **Python 3.11+**
- The repo cloned, and `pip install -e .` already run.
- Your **`.env`** in the repo root (FAA SWIM credentials: `SCDS_HOST`,
  `SCDS_USERNAME`, `SCDS_PASSWORD`, `SCDS_QUEUE_FLIGHT`, `SCDS_VPN`).

No Postgres or Kafka install needed — they run in containers. 

## 1a. Getting a SWIM subscription (required for live data)

AeroFlux ingests live flight data from the FAA's **SWIM** (System Wide
Information Management) program via the SCDS (SWIM Cloud Distribution Service).
You need your own subscription — it's free but requires registration and
approval.

1. Request access at the **FAA SCDS portal**: <https://scds.swim.faa.gov> (see
   the FAA SWIM program site at <https://www.faa.gov/air_traffic/technology/swim>
   for program details and eligibility).
2. Once approved, create a subscription to the **TFMS Flight Data** feed
   (data type `flight-data`). This is the feed AeroFlux parses — track
   positions, flight plans, and departure/arrival events. Do **not** use
   `flight-delay-tfms`; the parser is built for the Flight Data service.
3. From your subscription's **connection information** page, copy the values
   into your `.env` (see the secrets setup in section 3):

   | .env variable | Where it comes from | Example |
   |---|---|---|
   | `SCDS_HOST` | Broker host (note **ems1** vs **ems2** — your queue is provisioned on one) | `tcps://ems2.swim.faa.gov:55443` |
   | `SCDS_VPN` | Message VPN for the feed | `TFMS` |
   | `SCDS_USERNAME` | Your SCDS client username | `you.school.edu` |
   | `SCDS_PASSWORD` | Your SCDS client password | *(secret)* |
   | `SCDS_QUEUE_FLIGHT` | The exact durable queue name issued to you | `you.school.edu.TFMS.OUT` |

**Gotchas worth knowing up front:**

- **Broker matters.** Queues are provisioned on a specific broker. Connecting to
  `ems1` when your queue lives on `ems2` (or vice versa) fails with
  `SOLCLIENT_SUBCODE_UNKNOWN_QUEUE_NAME`. Match `SCDS_HOST` to where the queue
  was created.
- **Use the queue name verbatim.** `SCDS_QUEUE_FLIGHT` must be exactly the
  durable name from the portal. An "unknown queue" error usually means the name
  was altered.
- **Never commit `.env`.** These are credentials. `.env` is gitignored; only
  `.env.example` (no values) is tracked.

## 1b. pgAdmin (optional, for browsing the database)

pgAdmin gives you a GUI to inspect `swim.raw_messages`, `flight_instance`, and
your gold outputs. Optional — everything works from the CLI — but handy for
poking around the data.

1. **Install pgAdmin** for your OS: <https://www.pgadmin.org/download/>
   (steps vary by platform; follow the docs for your OS).
2. Make sure the stack is running (`./run.sh setup` / `./run.sh status`) so the
   `aeroflux-postgres` container is up.
3. **Register the server** in pgAdmin — right-click *Servers → Register →
   Server*:

   **General tab**
   - Name: `AeroFlux`

   **Connection tab**
   - Host name/address: `localhost`
   - Port: `5432`
   - Maintenance database: `aeroflux`
   - Username: `aeroflux`
   - Password: `aeroflux-db`  *(match your `POSTGRES_PASSWORD` in `.env`)*

4. Save. You can now browse `aeroflux → Schemas → swim → Tables →
   raw_messages` and `public → flight_instance`, or open the Query Tool to run
   SQL directly.

> Since pgAdmin connects to the container's published port `5432`, the
> credentials are whatever you set in `.env` (`POSTGRES_USER` / `POSTGRES_PASSWORD`
> / `POSTGRES_DB`). The defaults above match `.env.example`.

---

## 2. Quickstart (fresh machine → gold)

```bash
./run.sh setup            # infra up, Kafka topic, DB tables  (one-time)
./run.sh consume          # start the Kafka→Postgres consumer (background)
./run.sh ingest 3600      # collect 1h of live SWIM         (background; use 86400 for a day)
# ...wait for data to accumulate...
./run.sh pipeline         # raw → silver → load → gold
```

Or the whole thing in one call (setup, collect for N seconds, then process):

```bash
./run.sh all 3600
```

Outputs land in `./out/gold_features.parquet` (+ `.csv`) — the model's input,
ready for analysis or inference. Check health any time:

```bash
./run.sh status
```

### Commands

| Command | What it does |
|---|---|
| `./run.sh setup` | Bring up infra, create topic + tables (idempotent) |
| `./run.sh consume` | Start Kafka→Postgres consumer (background) |
| `./run.sh ingest [secs]` | Start SWIM→Kafka bridge for N seconds (background) |
| `./run.sh pipeline` | raw → silver → load → gold |
| `./run.sh all [secs]` | setup + consume + ingest + wait + pipeline |
| `./run.sh status` | Infra, processes, and row counts |
| `./run.sh stop` | Stop bridge + consumer (leaves infra up) |
| `./run.sh down` | Stop infra |

---

## 3. Configuration (the plug-and-play layer)

`run.sh` reads its settings from the environment (or `.env`), so the **same
script targets local today and cloud later** by changing values, not code.
Override inline or export them:

| Variable | Default | Swap for cloud |
|---|---|---|
| `DSN` | local Postgres | an RDS/Aurora connection string |
| `COMPOSE_FILE` | `compose.yaml` | a cloud/prod compose or leave infra managed |
| `RAW_TABLE` / `RAW_COLUMN` | `swim.raw_messages` / `raw_xml` | (confirm vs your `schema.sql`) |
| `LIMIT` / `LIVE` | `500000` / `100` | tune volume + ADS-B tail resolutions |
| `OUT` | `./out` | a mounted path or S3 sync target |
| `KAFKA_CONTAINER` / `PG_CONTAINER` | auto-detected | set explicitly if auto-detect misses |

Example — point the pipeline at a managed database with no code change:

```bash
DSN="postgresql://user:pass@my-rds.amazonaws.com:5432/aeroflux" ./run.sh pipeline
```

The ML layer has its own config too — `aeroflux_ml/configs/pipeline.yaml`
toggles feature channels and selects the model. Adding a feature or swapping a
model is a config edit, never a pipeline rewrite.

**Confirm-these on a new checkout** (auto-detect handles containers, but these
come from your files): `RAW_COLUMN` matches `\d swim.raw_messages`, and the
Kafka topic your `kafka_to_postgres.py` subscribes to matches `TOPIC`.

---

## 4. Team onboarding (copy/paste for a teammate)

```bash
git clone <repo> && cd <repo>
pip install -e .
cp /path/to/shared/.env .env      # SWIM credentials
./run.sh setup
./run.sh all 3600
```

That's the whole path from clone to gold data.

---

## 5. The cloud path (seamless, config-driven)

The design keeps components behind seams so the local→cloud shift is
incremental and reversible — no big rewrite:

- **Database:** `DSN` → RDS/Aurora. Nothing else changes.
- **Object storage / lake:** the ML writer takes a path; `./out` locally, an
  `s3://bucket/...` path in AWS (add AWS creds to the env). MinIO in compose is
  the local S3 stand-in, so code is identical.
- **NoSQL state store:** implement the `StateRepository` interface for DynamoDB
  or MongoDB; select it in config. Local uses SQLite; nothing upstream changes.
- **Streaming at scale:** the tested feature/inference core runs unchanged
  inside `aeroflux_ml/spark/streaming_job.py` (`foreachBatch`) — move from the
  local Spark container to a single cloud VM, then EMR/Glue later.
- **Weather:** `--weather live` (Aviation Weather Center) locally and in cloud;
  same code, no swap needed.

Rule of thumb: if it's an endpoint or a path, it's config; if it's logic, it's
in the tested core and doesn't move.

---

## 6. Optional: Ansible (provision a remote/cloud VM automatically)

Ansible is worth adding when you want to stand up a *remote* machine (a cloud
VM, a teammate's box) without SSHing in and running steps by hand. It automates
exactly what section 4 does, remotely. This is a starter — expand as needed.

**Install Ansible** (on your laptop, the "control node"):

```bash
pip install ansible
```

**Inventory** — `hosts.ini` (the target VM's IP + SSH user):

```ini
[aeroflux]
my-vm ansible_host=1.2.3.4 ansible_user=ubuntu
```

**Playbook** — `provision.yml`:

```yaml
- hosts: aeroflux
  become: true
  tasks:
    - name: Install Docker + compose plugin + Python
      apt:
        name: [docker.io, docker-compose-v2, python3-pip, git]
        update_cache: true
    - name: Clone the repo
      become: false
      git:
        repo: "https://github.com/<you>/aeroflux.git"
        dest: ~/aeroflux
    - name: Install the package
      become: false
      command: pip install -e . chdir=~/aeroflux
    - name: Copy .env (kept out of git)
      become: false
      copy: { src: ./.env, dest: ~/aeroflux/.env, mode: "0600" }
    - name: Run one-time setup
      become: false
      command: ./run.sh setup chdir=~/aeroflux
```

**Run it:**

```bash
ansible-playbook -i hosts.ini provision.yml
```

After that, the VM is set up exactly like a local machine; SSH in and use the
same `./run.sh` commands (or add tasks to run `./run.sh all 86400`). For real
cloud provisioning of the VM *itself* (not just configuring it), pair this with
Terraform later — Ansible configures, Terraform creates. Both stay optional
until you need repeatable remote environments.

---

## 7. Troubleshooting

- **Consumer exits `UNKNOWN_TOPIC_OR_PART`** — topic missing; `./run.sh setup`
  creates it. Order matters: setup (topic) → consume → ingest.
- **`swim.raw_messages` empty but bridge logs "Published"** — consumer not
  running or wrong topic. `./run.sh status`; `tail kafka_to_postgres.log`.
- **`flight_instance` empty** — `build_dataset` writes files; the load step in
  `pipeline` is what fills the table. Re-run `./run.sh pipeline`.
- **Auto-detect can't find a container** — set `PG_CONTAINER=... KAFKA_CONTAINER=...`
  explicitly (find names with `docker ps`).
- **Rotation / delay features mostly null** — expected on a short live window;
  they need completed flights (longer ingest) and ADS-B `hex` (`LIVE>0`, on by
  default). Weather + airport features populate regardless.
