"""Consume raw FAA SWIM events from Kafka and store them in PostgreSQL."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import xml.etree.ElementTree as ET
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("kafka_to_postgres")

STOP_REQUESTED = False


def request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or "YOUR_" in value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def strip_namespace(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def summarize_xml(xml_text: str) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "xml_root_tag": "",
        "flight_message_count": 0,
        "message_types": [],
    }

    try:
        root = ET.fromstring(xml_text)
        summary["xml_root_tag"] = strip_namespace(root.tag)
        message_types: set[str] = set()

        for element in root.iter():
            if strip_namespace(element.tag) != "fltdMessage":
                continue
            summary["flight_message_count"] += 1
            message_type = (element.get("msgType") or "").strip()
            if message_type:
                message_types.add(message_type)

        summary["message_types"] = sorted(message_types)
    except ET.ParseError:
        summary["xml_root_tag"] = "INVALID_XML"

    return summary


def postgres_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=required_env("POSTGRES_DB"),
        user=required_env("POSTGRES_USER"),
        password=required_env("POSTGRES_PASSWORD"),
    )


INSERT_SQL = """
INSERT INTO swim.raw_messages (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    swim_received_at,
    solace_destination,
    xml_root_tag,
    flight_message_count,
    message_types,
    payload_size_bytes,
    raw_xml
)
VALUES (
    %(kafka_topic)s,
    %(kafka_partition)s,
    %(kafka_offset)s,
    %(swim_received_at)s,
    %(solace_destination)s,
    %(xml_root_tag)s,
    %(flight_message_count)s,
    %(message_types)s,
    %(payload_size_bytes)s,
    %(raw_xml)s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING;
"""


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            "group.id": os.getenv(
                "KAFKA_CONSUMER_GROUP", "aeroflux-postgres-writer"
            ),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def main() -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    topic = os.getenv("KAFKA_TOPIC", "swim.raw.flight")
    consumer = build_consumer()

    try:
        with postgres_connection() as connection:
            consumer.subscribe([topic])
            log.info("Listening to Kafka topic %s", topic)
            log.info("Writing to PostgreSQL table swim.raw_messages")

            while not STOP_REQUESTED:
                message = consumer.poll(1.0)
                if message is None:
                    continue

                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(str(message.error()))

                try:
                    envelope = json.loads(message.value().decode("utf-8"))
                    raw_xml = envelope["payload"]
                    summary = summarize_xml(raw_xml)

                    row = {
                        "kafka_topic": message.topic(),
                        "kafka_partition": message.partition(),
                        "kafka_offset": message.offset(),
                        "swim_received_at": envelope["received_at_utc"],
                        "solace_destination": envelope.get(
                            "source_destination", ""
                        ),
                        "xml_root_tag": summary["xml_root_tag"],
                        "flight_message_count": summary[
                            "flight_message_count"
                        ],
                        "message_types": summary["message_types"],
                        "payload_size_bytes": len(raw_xml.encode("utf-8")),
                        "raw_xml": raw_xml,
                    }

                    with connection.cursor() as cursor:
                        cursor.execute(INSERT_SQL, row)
                    connection.commit()

                    # Commit the Kafka offset only after PostgreSQL commits.
                    consumer.commit(message=message, asynchronous=False)

                    log.info(
                        "Stored partition=%d offset=%d root=%s flights=%d types=%s",
                        message.partition(),
                        message.offset(),
                        summary["xml_root_tag"],
                        summary["flight_message_count"],
                        summary["message_types"],
                    )
                except Exception:
                    connection.rollback()
                    log.exception(
                        "Could not store Kafka partition=%d offset=%d; offset not committed",
                        message.partition(),
                        message.offset(),
                    )
                    return 1
    except Exception as exc:
        log.error("%s", exc)
        return 1
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
