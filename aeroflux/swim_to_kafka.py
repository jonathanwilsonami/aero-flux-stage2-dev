"""Bridge raw FAA SWIM messages from a Solace queue into Kafka.

The live path intentionally performs no XML feature engineering. It preserves the
raw payload so the first prototype can answer one question reliably: what data
is arriving?
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import certifi
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("swim_to_kafka")

STOP_REQUESTED = False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def request_stop(_signum: int, _frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or "YOUR_" in value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def build_kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            "client.id": "aeroflux-swim-bridge",
            "acks": "all",
        }
    )


def publish_raw_message(
    producer: Producer,
    topic: str,
    payload: str,
    source_destination: str = "",
) -> None:
    """Publish one message and wait until Kafka confirms delivery.

    Waiting per message is deliberately conservative and easy to reason about.
    It can be batched later after the basic data flow is proven.
    """
    message_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    envelope = {
        "message_id": message_id,
        "received_at_utc": utc_now(),
        "source_destination": source_destination,
        "content_type": "application/xml",
        "payload": payload,
    }

    delivery_error: list[str] = []

    def delivery_report(err, _msg) -> None:
        if err is not None:
            delivery_error.append(str(err))

    producer.produce(
        topic=topic,
        key=message_id.encode("utf-8"),
        value=json.dumps(envelope).encode("utf-8"),
        on_delivery=delivery_report,
    )

    remaining = producer.flush(10.0)
    if remaining:
        raise RuntimeError(f"Kafka delivery timed out; {remaining} message(s) pending")
    if delivery_error:
        raise RuntimeError(f"Kafka delivery failed: {delivery_error[0]}")


def payload_as_text(message) -> str:
    payload = message.get_payload_as_string()
    if payload is not None:
        return payload

    payload_bytes = message.get_payload_as_bytes()
    if payload_bytes is None:
        return ""
    return bytes(payload_bytes).decode("utf-8", errors="replace")


def run_live(args: argparse.Namespace) -> None:
    try:
        from solace.messaging.messaging_service import MessagingService
        from solace.messaging.resources.queue import Queue
    except ImportError as exc:
        raise RuntimeError(
            "Solace Python API is not installed. Run: pip install -r requirements.txt"
        ) from exc

    host = required_env("SCDS_HOST")
    vpn = required_env("SCDS_VPN")
    username = required_env("SCDS_USERNAME")
    password = required_env("SCDS_PASSWORD")
    queue_name = required_env("SCDS_QUEUE_FLIGHT")

    trust_store = os.getenv("SOLACE_TRUST_STORE", "").strip() or certifi.where()
    if args.no_verify_tls:
        log.warning("TLS certificate verification is disabled. Use only for diagnosis.")
        tls_properties = {
            "solace.messaging.tls.cert-validated": False,
            "solace.messaging.tls.cert-validate-servername": False,
        }
    else:
        tls_properties = {
            "solace.messaging.tls.cert-validated": True,
            "solace.messaging.tls.cert-validate-servername": True,
            "solace.messaging.tls.trust-store-path": trust_store,
        }

    service = (
        MessagingService.builder()
        .from_properties(
            {
                "solace.messaging.transport.host": host,
                "solace.messaging.service.vpn-name": vpn,
                "solace.messaging.authentication.scheme.basic.username": username,
                "solace.messaging.authentication.scheme.basic.password": password,
                **tls_properties,
            }
        )
        .build()
    )

    producer = build_kafka_producer()
    topic = os.getenv("KAFKA_TOPIC", "swim.raw.flight")
    receiver = None
    published = 0
    started_at = time.monotonic()

    try:
        log.info("Connecting to SWIM Solace host %s", host)
        service.connect()
        receiver = (
            service.create_persistent_message_receiver_builder()
            .build(Queue.durable_exclusive_queue(queue_name))
        )
        receiver.start()
        log.info("Connected. Bridging queue %s -> Kafka topic %s", queue_name, topic)

        while not STOP_REQUESTED:
            if args.duration and time.monotonic() - started_at >= args.duration:
                break
            if args.max_messages and published >= args.max_messages:
                break

            message = receiver.receive_message(1000)
            if message is None:
                continue

            payload = payload_as_text(message)
            if not payload:
                log.warning("Received an empty payload; acknowledging and skipping")
                receiver.ack(message)
                continue

            destination = ""
            try:
                destination = message.get_destination_name() or ""
            except Exception:
                pass

            try:
                publish_raw_message(producer, topic, payload, destination)
                # Acknowledge only after Kafka confirms the write.
                receiver.ack(message)
                published += 1
                log.info(
                    "Published message %d (%d bytes) to %s",
                    published,
                    len(payload.encode("utf-8")),
                    topic,
                )
            except Exception:
                # Do not ACK. The persistent queue can redeliver after reconnect.
                log.exception("Failed to publish to Kafka; leaving SWIM message unacknowledged")
                break
    finally:
        producer.flush(5.0)
        if receiver is not None:
            try:
                receiver.terminate()
            except Exception:
                log.debug("Receiver termination failed", exc_info=True)
        try:
            service.disconnect()
        except Exception:
            log.debug("Solace disconnect failed", exc_info=True)

    log.info("Stopped after publishing %d message(s)", published)


def mock_xml(index: int) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<tfmDataService>
  <fltdOutput>
    <fltdMessage acid="TEST{index:03d}" airline="TST" depArpt="KIAD"
                 arrArpt="KJFK" flightRef="mock-{index}"
                 msgType="departureInformation"
                 sourceTimeStamp="{utc_now()}">
      <gufi>mock-gufi-{index}</gufi>
      <igtd>{utc_now()}</igtd>
    </fltdMessage>
  </fltdOutput>
</tfmDataService>"""


def run_mock(args: argparse.Namespace) -> None:
    producer = build_kafka_producer()
    topic = os.getenv("KAFKA_TOPIC", "swim.raw.flight")
    count = args.max_messages or 5

    for index in range(1, count + 1):
        publish_raw_message(producer, topic, mock_xml(index), "mock/SWIM")
        log.info("Published mock message %d of %d", index, count)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bridge raw FAA SWIM Solace messages into a Kafka topic."
    )
    parser.add_argument(
        "--mock",
        action="store_true",
        help="Publish generated test XML instead of connecting to FAA SWIM",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after N messages; 0 means unlimited in live mode",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Stop live mode after N seconds; 0 means unlimited",
    )
    parser.add_argument(
        "--no-verify-tls",
        action="store_true",
        help="Disable Solace TLS verification for diagnosis only",
    )
    return parser


def main() -> int:
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    args = build_parser().parse_args()

    try:
        if args.mock:
            run_mock(args)
        else:
            run_live(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        log.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
