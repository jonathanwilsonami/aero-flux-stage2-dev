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

# Reconnect backoff for the live SWIM bridge loop (see run_live) — starts
# fast, caps out so a genuinely-down broker doesn't get hammered.
RECONNECT_BACKOFF_BASE_S = 2.0
RECONNECT_BACKOFF_MAX_S = 60.0

# Heartbeat file: an external supervisor (run.sh's cmd_stream) polls this
# file's mtime to tell "bridge is alive AND actually receiving" apart from
# "bridge process is alive but wedged" (a hung receiver call that never
# raises, so the in-process reconnect loop below never sees it either) --
# the one failure mode nothing before this could detect. Opt-in: only
# writes if INGEST_HEARTBEAT_FILE is set, so direct/manual/test runs that
# don't set it see zero behavior change.
_HEARTBEAT_MIN_INTERVAL_S = 5.0
_last_heartbeat_write = 0.0


def _touch_heartbeat(published: int, *, force: bool = False) -> None:
    global _last_heartbeat_write
    path = os.getenv("INGEST_HEARTBEAT_FILE", "").strip()
    if not path:
        return
    now = time.monotonic()
    if not force and now - _last_heartbeat_write < _HEARTBEAT_MIN_INTERVAL_S:
        return  # throttled -- avoid a disk write per message at high rates
    try:
        with open(path, "w") as fh:
            fh.write(f"{utc_now()} published={published}\n")
        _last_heartbeat_write = now
    except OSError:
        log.debug("Could not write heartbeat file %s", path, exc_info=True)


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


def _limits_reached(args: argparse.Namespace, started_at: float, published: int) -> bool:
    if args.duration and time.monotonic() - started_at >= args.duration:
        return True
    if args.max_messages and published >= args.max_messages:
        return True
    return False


def _build_service(host: str, vpn: str, username: str, password: str, tls_properties: dict):
    from solace.messaging.messaging_service import MessagingService

    return (
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


def run_live(args: argparse.Namespace) -> None:
    try:
        from solace.messaging.messaging_service import MessagingService  # noqa: F401 — import check
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

    producer = build_kafka_producer()
    topic = os.getenv("KAFKA_TOPIC", "swim.raw.flight")
    published = 0
    started_at = time.monotonic()
    backoff = RECONNECT_BACKOFF_BASE_S
    attempt = 0

    # Outer reconnect loop. Every failure below — a terminated Solace
    # receiver, a dropped connection, a Kafka publish failure — used to fall
    # through to the same place: the process exited ("Stopped after
    # publishing N message(s)") and nothing brought it back. That's what let
    # ingest sit dead for ~2 days while `e2e.sh health` still showed the PID
    # as "running" (it was — just not doing anything; see e2e.sh's
    # cmd_health for the matching fix, which checks raw_messages growth
    # instead of just the PID). This can't silently die during a demo:
    # every failure here is treated as retriable — log it, clean up, back
    # off (2s, doubling to a 60s cap), reconnect, resume. Only
    # STOP_REQUESTED (SIGINT/SIGTERM) or the --duration/--max-messages caps
    # end the loop.
    while not STOP_REQUESTED and not _limits_reached(args, started_at, published):
        attempt += 1
        service = _build_service(host, vpn, username, password, tls_properties)
        receiver = None
        try:
            log.info("Connecting to SWIM Solace host %s (attempt %d)", host, attempt)
            service.connect()
            receiver = (
                service.create_persistent_message_receiver_builder()
                .build(Queue.durable_exclusive_queue(queue_name))
            )
            receiver.start()
            log.info("Connected. Bridging queue %s -> Kafka topic %s", queue_name, topic)
            backoff = RECONNECT_BACKOFF_BASE_S  # a live connection resets the backoff
            # Force (bypass throttle): a supervisor's stall check needs a
            # fresh timestamp right at reconnect, not up to
            # _HEARTBEAT_MIN_INTERVAL_S stale from before this connection.
            _touch_heartbeat(published, force=True)

            while not STOP_REQUESTED and not _limits_reached(args, started_at, published):
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

                publish_raw_message(producer, topic, payload, destination)
                # Acknowledge only after Kafka confirms the write.
                receiver.ack(message)
                published += 1
                _touch_heartbeat(published)
                log.info(
                    "Published message %d (%d bytes) to %s",
                    published,
                    len(payload.encode("utf-8")),
                    topic,
                )
        except Exception:
            # Covers a terminated/dead receiver, a dropped Solace
            # connection, and Kafka publish failures alike — any of these
            # used to exit the process. Do not ACK on the way out: the
            # persistent queue redelivers unacknowledged messages after
            # reconnect, so nothing already-consumed-but-unpublished is lost.
            log.exception(
                "SWIM bridge connection lost (attempt %d, %d published so far) "
                "— reconnecting in %.0fs",
                attempt, published, backoff,
            )
        finally:
            if receiver is not None:
                try:
                    receiver.terminate()
                except Exception:
                    log.debug("Receiver termination failed", exc_info=True)
            try:
                service.disconnect()
            except Exception:
                log.debug("Solace disconnect failed", exc_info=True)

        if STOP_REQUESTED or _limits_reached(args, started_at, published):
            break
        time.sleep(backoff)
        backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_S)

    producer.flush(5.0)
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
