"""The single output type every parser produces.

A ParsedMessage is intentionally database-agnostic:

  - The scalar `identity` fields map cleanly onto SQL columns / indexes.
  - `body` is a nested dict that drops straight into a JSONB column or a
    NoSQL document.
  - `raw_xml` is kept verbatim so a message can always be reprocessed later
    if we improve the parser or discover we mis-handled a field.

Nothing here knows about Kafka, Postgres, or files. That is deliberate.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any


class ParseStatus:
    OK = "ok"            # parsed cleanly
    PARTIAL = "partial"  # produced a record but something was off
    FAILED = "failed"    # could not parse; raw payload preserved for retry


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ParsedMessage:
    parse_status: str
    parser: str
    parser_version: str
    root_type: str | None
    msg_type: str | None
    identity: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] = field(default_factory=dict)
    # Typed, model-ready fields lifted out of `body` by the optional
    # normalization layer. Empty unless a normalizer ran for this msg_type.
    normalized: dict[str, Any] = field(default_factory=dict)
    raw_xml: str = ""
    errors: list[str] = field(default_factory=list)
    source_format: str = "xml"
    message_id: str | None = None
    source: str | None = None
    ingested_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
