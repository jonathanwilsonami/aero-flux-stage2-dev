"""Public entrypoints. This is the only module the rest of AeroFlux imports.

    from aeroflux_parser import parse_payload, from_kafka_value

Both return a list[ParsedMessage] and never raise on bad input.
"""

from __future__ import annotations

import json
from typing import Any

from .parsers import select_parser
from .result import ParsedMessage, ParseStatus
from .xmlutils import parse_xml


def parse_payload(
    payload: str | bytes,
    *,
    message_id: str | None = None,
    source: str | None = None,
) -> list[ParsedMessage]:
    """Turn one raw XML payload into ParsedMessage records.

    On malformed XML (or empty input) you get a single FAILED record with the
    raw payload preserved, not an exception. That record is your retry queue.
    """
    text = _as_text(payload).strip()
    if not text:
        return [_failed("empty payload", "", message_id, source)]

    try:
        root = parse_xml(text)
    except Exception as exc:
        return [_failed(f"xml parse error: {exc}", text, message_id, source)]

    parser = select_parser(root)
    try:
        results = parser.parse(root)
    except Exception as exc:  # defensive: a parser bug must not kill the stream
        return [_failed(f"parser {parser.name} raised: {exc}", text, message_id, source)]

    for r in results:
        r.message_id = message_id
        r.source = source
    return results


def from_kafka_value(value: str | bytes) -> list[ParsedMessage]:
    """Accept the JSON envelope produced by swim_to_kafka.py and parse its
    payload. If the value isn't that envelope, it's treated as raw XML."""
    text = _as_text(value)
    try:
        envelope: dict[str, Any] = json.loads(text)
    except Exception:
        return parse_payload(text)

    if isinstance(envelope, dict) and "payload" in envelope:
        return parse_payload(
            envelope["payload"],
            message_id=envelope.get("message_id"),
            source=envelope.get("source_destination"),
        )
    return parse_payload(text)


def _as_text(payload: str | bytes) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    return payload


def _failed(
    error: str, raw: str, message_id: str | None, source: str | None
) -> ParsedMessage:
    return ParsedMessage(
        parse_status=ParseStatus.FAILED,
        parser="none",
        parser_version="0.1.0",
        root_type=None,
        msg_type=None,
        identity={},
        body={},
        raw_xml=raw,
        errors=[error],
        message_id=message_id,
        source=source,
    )
