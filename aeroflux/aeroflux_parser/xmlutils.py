"""Small, dependency-free XML helpers built on the stdlib ElementTree.

The important function is `element_to_obj`, which turns any element subtree
into plain Python dicts/lists/strings with namespaces stripped. It makes no
assumptions about the SWIM schema, so it works on message types we have never
seen before -- which is the whole point.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

# Registering the known SWIM prefixes keeps re-serialized raw_xml readable
# (fdm:/nxcm:/nxce: instead of ns0:/ns1:). Unknown feeds still serialize fine.
_KNOWN_NAMESPACES = {
    "ds": "urn:us:gov:dot:faa:atm:tfm:tfmdataservice",
    "fdm": "urn:us:gov:dot:faa:atm:tfm:flightdata",
    "nxce": "urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements",
    "nxcm": "urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages",
}
for _prefix, _uri in _KNOWN_NAMESPACES.items():
    ET.register_namespace(_prefix, _uri)


def local_name(tag: str) -> str:
    """'{urn:...}fltdMessage' -> 'fltdMessage'."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def parse_xml(payload: str) -> ET.Element:
    """Parse a document string into its root element. Raises on malformed XML;
    callers are expected to catch and downgrade to a FAILED result."""
    return ET.fromstring(payload)


def to_xml_string(element: ET.Element) -> str:
    return ET.tostring(element, encoding="unicode")


def element_to_obj(element: ET.Element) -> Any:
    """Recursively convert an element into JSON-friendly Python objects.

    Rules:
      - Tag and attribute names are reduced to their local name.
      - Attributes are stored with an '@' prefix so they never collide with
        child element names.
      - Repeated child tags collapse into a list; single ones stay scalar.
      - A pure-text leaf with no attributes returns its text directly.
      - Text on an element that also has attributes/children lands in '#text'.
    """
    children = list(element)
    attrs = {f"@{local_name(k)}": v for k, v in element.attrib.items()}
    text = (element.text or "").strip()

    if not children:
        if attrs:
            if text:
                attrs["#text"] = text
            return attrs
        return text  # possibly "" for a truly empty element

    obj: dict[str, Any] = dict(attrs)
    if text:
        obj["#text"] = text

    grouped: dict[str, list[Any]] = {}
    for child in children:
        grouped.setdefault(local_name(child.tag), []).append(element_to_obj(child))

    for name, values in grouped.items():
        obj[name] = values[0] if len(values) == 1 else values
    return obj


def find_all_local(element: ET.Element, name: str) -> list[ET.Element]:
    """Find every descendant (and self) whose local tag name matches, ignoring
    namespaces. ElementTree's own search requires fully-qualified tags, so we
    walk manually."""
    matches: list[ET.Element] = []
    if local_name(element.tag) == name:
        matches.append(element)
    for el in element.iter():
        if el is not element and local_name(el.tag) == name:
            matches.append(el)
    return matches


def first_text(element: ET.Element, name: str) -> str | None:
    """Best-effort: return the text of the first descendant with this local
    name, or None. Used for lifting nested identity fields like gufi/igtd."""
    for el in element.iter():
        if local_name(el.tag) == name and el.text and el.text.strip():
            return el.text.strip()
    return None
