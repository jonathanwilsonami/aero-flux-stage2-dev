"""Parsers + a tiny registry.

Adding support for a new SWIM feed (SFDPS, TAIS, ITWS, ...) later means writing
one class and appending it to REGISTRY. The dispatch logic and the public
`parse_payload` entrypoint never change. That is the extension seam.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Protocol

from .result import ParsedMessage, ParseStatus
from .xmlutils import (
    element_to_obj,
    find_all_local,
    first_text,
    local_name,
    to_xml_string,
)


class Parser(Protocol):
    name: str
    version: str

    def can_parse(self, root: ET.Element) -> bool: ...

    def parse(self, root: ET.Element) -> list[ParsedMessage]: ...


# --- TFMS (the tfmDataService feed these samples come from) -----------------

# fltdMessage attribute -> the identity key we expose. Nested fields (gufi,
# igtd) are pulled separately because they live deeper in the tree.
_TFMS_IDENTITY_ATTRS = {
    "acid": "acid",
    "airline": "airline",
    "major": "major",
    "depArpt": "dep_arpt",
    "arrArpt": "arr_arpt",
    "flightRef": "flight_ref",
    "msgType": "msg_type",
    "fdTrigger": "fd_trigger",
    "sourceFacility": "source_facility",
    "sourceTimeStamp": "source_time",
    "sensitivity": "sensitivity",
    "cdmPart": "cdm_part",
}


class TFMSParser:
    """Parses tfmDataService documents. Explodes each fltdOutput into one
    ParsedMessage per fltdMessage, regardless of msgType."""

    name = "tfms"
    version = "0.1.0"

    def can_parse(self, root: ET.Element) -> bool:
        return local_name(root.tag) == "tfmDataService"

    def parse(self, root: ET.Element) -> list[ParsedMessage]:
        messages = find_all_local(root, "fltdMessage")
        if not messages:
            # Recognized the envelope but found nothing to explode. Keep the
            # whole thing rather than silently dropping it.
            return [self._whole_document(root)]
        return [self._one_message(el) for el in messages]

    def _one_message(self, el: ET.Element) -> ParsedMessage:
        errors: list[str] = []
        identity: dict[str, str] = {}
        for attr, key in _TFMS_IDENTITY_ATTRS.items():
            if attr in el.attrib:
                identity[key] = el.attrib[attr]

        for nested in ("gufi", "igtd"):
            value = first_text(el, nested)
            if value is not None:
                identity[nested] = value

        try:
            body = element_to_obj(el)
            status = ParseStatus.OK
        except Exception as exc:  # pragma: no cover - defensive
            body = {}
            errors.append(f"body flatten failed: {exc}")
            status = ParseStatus.PARTIAL

        try:
            raw = to_xml_string(el)
        except Exception:  # pragma: no cover - defensive
            raw = ""

        return ParsedMessage(
            parse_status=status,
            parser=self.name,
            parser_version=self.version,
            root_type="tfmDataService",
            msg_type=identity.get("msg_type"),
            identity=identity,
            body=body,
            raw_xml=raw,
            errors=errors,
        )

    def _whole_document(self, root: ET.Element) -> ParsedMessage:
        return ParsedMessage(
            parse_status=ParseStatus.PARTIAL,
            parser=self.name,
            parser_version=self.version,
            root_type="tfmDataService",
            msg_type=None,
            identity={},
            body=element_to_obj(root),
            raw_xml=to_xml_string(root),
            errors=["no fltdMessage elements found"],
        )


# --- Fallback for any XML root we don't recognize ---------------------------


class GenericXMLParser:
    """Last resort. Flattens whatever it is so the data is captured and can be
    understood later, instead of being lost. Always matches."""

    name = "generic-xml"
    version = "0.1.0"

    def can_parse(self, root: ET.Element) -> bool:
        return True

    def parse(self, root: ET.Element) -> list[ParsedMessage]:
        return [
            ParsedMessage(
                parse_status=ParseStatus.PARTIAL,
                parser=self.name,
                parser_version=self.version,
                root_type=local_name(root.tag),
                msg_type=None,
                identity={},
                body=element_to_obj(root),
                raw_xml=to_xml_string(root),
                errors=["unrecognized root; captured with generic flattener"],
            )
        ]


# Order matters: specific parsers first, generic fallback last.
REGISTRY: list[Parser] = [TFMSParser(), GenericXMLParser()]


def select_parser(root: ET.Element) -> Parser:
    for parser in REGISTRY:
        if parser.can_parse(root):
            return parser
    return REGISTRY[-1]  # GenericXMLParser always matches; belt and suspenders
