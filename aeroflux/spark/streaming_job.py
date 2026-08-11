"""Spark Structured Streaming job — SCAFFOLD (requires a Spark runtime + Kafka).

This is the streaming wrapper around the *tested* core. Note it contains almost
no feature logic: it reads canonical events from Kafka, and per micro-batch calls
the SAME `FeatureEngineer` and `InferenceEngine` used for BTS batch. That is the
"no rewrite for streaming" payoff — the streaming layer is orchestration only.

Not runnable in a plain Python sandbox; run with spark-submit against the
compose stack (see infra/docker-compose.yml) or an EMR/Glue job later. The
structure is deliberately cloud-agnostic: swap the Kafka bootstrap, the S3
path, and the state repo implementation via config — the body is unchanged.

Design points wired here:
  * checkpointing (exactly-once-ish via Kafka offsets + checkpointLocation)
  * per-batch feature build + inference (foreachBatch)
  * bronze/silver/gold Parquet (or Iceberg) writes to local or S3
  * NoSQL state upsert (idempotent on prediction_key -> no duplicate scoring)
  * dead-letter path for un-parseable / failing records
  * structured logging + error isolation per batch
"""

from __future__ import annotations

import json
import logging

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s %(message)s")
log = logging.getLogger("aeroflux.stream")


def run(config_path: str) -> None:
    import polars as pl
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    from aeroflux_ml import (
        load_config, from_silver, FeatureEngineer, InferenceEngine,
        SqliteStateRepository, write_table,
    )

    cfg = load_config(config_path)
    engineer = FeatureEngineer(cfg.features)
    engine = InferenceEngine(cfg.model, feature_version=cfg.features.feature_version)
    repo = SqliteStateRepository()          # swap for Dynamo/Mongo in prod

    spark = (
        SparkSession.builder.appName("aeroflux-stream")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/aeroflux-ckpt")
        .getOrCreate()
    )

    # Canonical events are produced upstream (parser) to this topic. We keep the
    # 24h context window in the state repo; here we score the incoming batch.
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "flight.canonical")
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) AS json")
    )

    def process_batch(batch_df, batch_id: int) -> None:
        try:
            rows = [json.loads(r["json"]) for r in batch_df.collect()]
            if not rows:
                return
            # 24h context from the state store + this batch -> one frame.
            # Bounded Limit on purpose — an unbounded call here would be a
            # full DynamoDB table scan on every micro-batch if this job (a
            # dormant scaffold — not currently deployed anywhere) is ever
            # actually run against the DynamoDB backend.
            context = repo.recent_flight_states(hours=24, limit=5000)
            frame = pl.DataFrame(context + rows)

            canonical = from_silver(frame, airframe_key=cfg.features.airframe_key)
            feats = engineer.build_matrix(canonical)
            preds = engine.predict(feats)

            # gold + predictions to the lake (local path or s3://...)
            write_table(feats, f"/data/gold/features/batch={batch_id}.parquet")
            write_table(preds, f"/data/gold/predictions/batch={batch_id}.parquet")

            # NoSQL current-state + latest prediction (idempotent)
            for r in rows:
                repo.upsert_flight_state(r)
            for p in preds.to_dicts():
                repo.upsert_prediction(p)

            log.info("batch %s: scored %d flight(s)", batch_id, len(preds))
        except Exception:  # isolate the batch; never kill the stream
            log.exception("batch %s failed; routing to dead-letter", batch_id)
            (batch_df.withColumn("batch_id", F.lit(batch_id))
                     .write.mode("append").json("/data/deadletter/"))

    query = stream.writeStream.foreachBatch(process_batch).start()
    query.awaitTermination()


if __name__ == "__main__":
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else "configs/pipeline.yaml")
