"""Inspect recently stored SWIM messages in PostgreSQL."""

from __future__ import annotations

import argparse
import os
import sys

import psycopg
from dotenv import load_dotenv

load_dotenv()


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or "YOUR_" in value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def connect() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=required_env("POSTGRES_DB"),
        user=required_env("POSTGRES_USER"),
        password=required_env("POSTGRES_PASSWORD"),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--raw", type=int, metavar="ID")
    args = parser.parse_args()

    try:
        with connect() as connection, connection.cursor() as cursor:
            if args.raw is not None:
                cursor.execute(
                    "SELECT raw_xml FROM swim.raw_messages WHERE id = %s",
                    (args.raw,),
                )
                row = cursor.fetchone()
                if row is None:
                    print(f"No row found with id={args.raw}")
                    return 1
                print(row[0])
                return 0

            cursor.execute(
                """
                SELECT id, stored_at, kafka_partition, kafka_offset,
                       xml_root_tag, flight_message_count, message_types,
                       payload_size_bytes
                FROM swim.raw_messages
                ORDER BY id DESC
                LIMIT %s
                """,
                (args.limit,),
            )
            rows = cursor.fetchall()

            if not rows:
                print("No SWIM messages have been stored yet.")
                return 0

            for row in rows:
                print(
                    f"id={row[0]} stored={row[1]} partition={row[2]} "
                    f"offset={row[3]} root={row[4]} flights={row[5]} "
                    f"types={row[6]} bytes={row[7]}"
                )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
