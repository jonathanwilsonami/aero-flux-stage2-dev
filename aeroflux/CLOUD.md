# AeroFlux — storage, retention, and the local→cloud path

The design keeps a **rolling 48h** of hot data locally and archives durable
artifacts to object storage. The same `run.sh` runs both; cloud is a config flip.

## Where each tier lives

| Tier | Local (now) | Cloud (target) | Retention |
|---|---|---|---|
| **raw** (SWIM XML) | Postgres `swim.raw_messages` | RDS/Aurora, or skip and land Kafka → S3 | **48h** rolling (`run.sh retention`) |
| **adsb store** | Postgres `adsb_airframe` | RDS/Aurora | **48h** rolling |
| **silver** (`flight_instance`, `dataset.jsonl`) | Postgres + file | S3 `silver/dt=…/` | durable in S3; 48h in Postgres |
| **gold** (`gold_features.parquet`) | `./out` | S3 `gold/dt=…/` | durable in S3 |

Rule: **hot, queryable state stays in Postgres with a 48h window; durable
history goes to object storage as partitioned parquet.** Postgres never grows
without bound; the lake is the system of record for training.

## 48h retention (built)

`./run.sh retention` deletes `swim.raw_messages` and `adsb_airframe` rows older
than `RETENTION_HOURS` (default 48). In `stream` mode it runs automatically each
refresh. Tune with `RETENTION_HOURS=48`.

In the cloud, the same 48h applies two ways:
- **RDS**: the identical `retention` DELETE, run on a schedule (cron / ECS
  scheduled task / Lambda).
- **S3**: a bucket **lifecycle policy** expires objects under a prefix after N
  days — no code, the platform enforces it. (Set once via console/Terraform.)

## Streaming local → cloud (built + next)

**Built:** `./run.sh sync` (called automatically at the end of `pipeline` when
`STORAGE_DEST` is set) writes `gold` and `silver` to `STORAGE_DEST`, partitioned
by UTC timestamp (`dt=YYYYMMDDT…Z`). It handles both a local path and `s3://`:

```bash
STORAGE_DEST=s3://my-bucket/aeroflux ./run.sh pipeline   # gold+silver land in S3
# or archive locally first:
STORAGE_DEST=/mnt/archive ./run.sh sync
```

The ML writer also takes an `s3://` `--out` directly (`OUT=s3://…`), so gold can
skip the local disk entirely.

**Next (design):** land **raw** in the lake continuously instead of only 48h in
Postgres. Two clean options, both config-level:
1. **Kafka → S3 sink** (Kafka Connect S3 sink, or a small consumer that writes
   parquet batches to `s3://…/raw/dt=…/`). Postgres keeps only the 48h hot
   window for fusion; S3 keeps everything for replay/training.
2. **Archive-before-purge**: extend `retention` to `COPY` expiring raw to a
   parquet file and `sync` it to S3 before the DELETE. Simplest to add.

## The config flip (local → cloud, no code change)

| Variable | Local | Cloud |
|---|---|---|
| `DSN` / `POSTGRES_*` | container | RDS/Aurora endpoint |
| `STORAGE_DEST` | unset or `/mnt/archive` | `s3://bucket/aeroflux` |
| `OUT` | `./out` | `s3://bucket/aeroflux/gold` |
| `RETENTION_HOURS` | 48 | 48 (+ S3 lifecycle) |
| secrets | `.env` | AWS Secrets Manager (`SECRETS_BACKEND=aws`) |

Provision the VM/bucket/RDS with Terraform, configure with the Ansible starter
(see RUNGUIDE §6), then the same `./run.sh stream` runs in the cloud.

## Real-time operation (built)

`./run.sh stream [seconds]` brings up infra, starts the SWIM bridge + Kafka
consumer + ADS-B poller, then loops every `REFRESH_SECONDS` (default 300):
`pipeline` → `retention` → `status`. That's near-real-time: raw streams in
continuously; silver/gold refresh every few minutes; the 48h window is trimmed
each cycle. `WEATHER=1` adds live METAR features to each refresh.

```bash
REFRESH_SECONDS=300 WEATHER=1 ./run.sh stream 86400
```
