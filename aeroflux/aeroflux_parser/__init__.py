"""AeroFlux SWIM XML parser -- decoupled, incremental, streaming-tolerant."""

from .parse import parse_payload, from_kafka_value
from .parsers import REGISTRY, Parser, select_parser
from .result import ParsedMessage, ParseStatus
from .normalizers import normalize, NORMALIZERS, dms_to_decimal
from .canonical import to_canonical, looks_like_registration
from .fusion import FlightInstanceReducer, FIELD_SOURCE_PRIORITY
from .sinks import Sink, MemorySink, JsonlSink, CsvSink, PostgresSink, make_sink
from .airlines import AirlineTable, Airline, DEFAULT_TABLE
from .enrich import enrich_record, classify, ResolutionStatus, DATASET_FIELDS
from .gold import (
    flight_features, build_feature_table,
    ID_COLUMNS, FEATURE_COLUMNS, LABEL_COLUMNS, ALL_COLUMNS,
)

# Validation contract is optional (needs pydantic); expose it when available.
try:
    from .schema import (
        FlightInstance, validate_record, validate_batch, SCHEMA_VERSION,
    )
    _HAS_SCHEMA = True
except ImportError:  # pydantic not installed
    _HAS_SCHEMA = False
from .identity import (
    parse_callsign, resolve_flight_number, callsign_to_flight_number,
    ParsedCallsign, ResolvedFlight,
)
from .adsb import (
    AdsbClient, AdsbProvider, Airframe, parse_adsb_response,
    AIRPLANES_LIVE, ADSB_LOL,
)

__version__ = "0.1.0"

__all__ = [
    "parse_payload",
    "from_kafka_value",
    "ParsedMessage",
    "ParseStatus",
    "Parser",
    "REGISTRY",
    "select_parser",
    "normalize",
    "NORMALIZERS",
    "dms_to_decimal",
    "to_canonical",
    "looks_like_registration",
    "FlightInstanceReducer",
    "FIELD_SOURCE_PRIORITY",
    "Sink",
    "MemorySink",
    "JsonlSink",
    "CsvSink",
    "PostgresSink",
    "make_sink",
    "enrich_record",
    "classify",
    "ResolutionStatus",
    "DATASET_FIELDS",
    "flight_features",
    "build_feature_table",
    "ID_COLUMNS",
    "FEATURE_COLUMNS",
    "LABEL_COLUMNS",
    "ALL_COLUMNS",
    "AirlineTable",
    "Airline",
    "DEFAULT_TABLE",
    "parse_callsign",
    "resolve_flight_number",
    "callsign_to_flight_number",
    "ParsedCallsign",
    "ResolvedFlight",
    "AdsbClient",
    "AdsbProvider",
    "Airframe",
    "parse_adsb_response",
    "AIRPLANES_LIVE",
    "ADSB_LOL",
]
