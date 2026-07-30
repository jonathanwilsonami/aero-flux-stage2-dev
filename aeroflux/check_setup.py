"""Check local PostgreSQL and Kafka connections before running the pipeline."""

from __future__ import annotations

import os
import sys

import psycopg
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    failed = False

    try:
        with psycopg.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user")
            database, user = cursor.fetchone()
            cursor.execute("SELECT to_regclass('swim.raw_messages')")
            table = cursor.fetchone()[0]
            print(f"PostgreSQL: OK database={database} user={user} table={table}")
            if table is None:
                print("  Run: psql ... -f schema.sql")
                failed = True
    except Exception as exc:
        print(f"PostgreSQL: FAILED: {exc}")
        failed = True

    try:
        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(
            timeout=10
        )
        print(
            f"Kafka: OK broker={bootstrap} topics={sorted(metadata.topics.keys())}"
        )
    except Exception as exc:
        print(f"Kafka: FAILED: {exc}")
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
