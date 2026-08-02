"""Tests run against real SWIM message shapes drawn from the sample feed."""

import json

from aeroflux_parser import parse_payload, from_kafka_value, ParseStatus


TRACK_DOC = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<ds:tfmDataService xmlns:ds="urn:us:gov:dot:faa:atm:tfm:tfmdataservice"
 xmlns:fdm="urn:us:gov:dot:faa:atm:tfm:flightdata"
 xmlns:nxce="urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements"
 xmlns:nxcm="urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages">
<fltdOutput>
 <fdm:fltdMessage acid="EDW4K" airline="EDW" arrArpt="KTPA" depArpt="LSZH"
  flightRef="150950038" major="EDW" msgType="trackInformation"
  sourceFacility="KMCO" sourceTimeStamp="2026-07-03T21:38:20Z">
  <fdm:trackInformation>
   <nxcm:qualifiedAircraftId aircraftCategory="JET" userCategory="COMMERCIAL">
    <nxce:aircraftId>EDW4K</nxce:aircraftId>
    <nxce:gufi>KN6770564K</nxce:gufi>
    <nxce:igtd>2026-07-03T11:25:00Z</nxce:igtd>
   </nxcm:qualifiedAircraftId>
   <nxcm:speed>368</nxcm:speed>
   <nxcm:ncsmTrackData>
    <nxcm:eta etaType="ESTIMATED" timeValue="2026-07-03T22:03:34Z"/>
   </nxcm:ncsmTrackData>
  </fdm:trackInformation>
 </fdm:fltdMessage>
 <fdm:fltdMessage acid="DAL1150" airline="DAL" arrArpt="KMCO" depArpt="KATL"
  flightRef="150911022" major="DAL" msgType="trackInformation"
  sourceFacility="KMCO" sourceTimeStamp="2026-07-03T21:38:21Z">
  <fdm:trackInformation>
   <nxcm:speed>347</nxcm:speed>
  </fdm:trackInformation>
 </fdm:fltdMessage>
</fltdOutput>
</ds:tfmDataService>"""


FLIGHT_MODIFY_DOC = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<ds:tfmDataService xmlns:ds="urn:us:gov:dot:faa:atm:tfm:tfmdataservice"
 xmlns:fdm="urn:us:gov:dot:faa:atm:tfm:flightdata"
 xmlns:nxce="urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements"
 xmlns:nxcm="urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages">
<fltdOutput>
 <fdm:fltdMessage acid="AAL2033" airline="AAL" arrArpt="KLGA" depArpt="KCLT"
  flightRef="150976233" major="AAL" msgType="FlightModify" sourceFacility="AAL"
  sourceTimeStamp="2026-07-03T21:38:27Z">
  <fdm:ncsmFlightModify>
   <nxcm:airlineData>
    <nxcm:flightStatusAndSpec>
     <nxcm:flightStatus>PLANNED</nxcm:flightStatus>
     <nxcm:aircraftModel>A320</nxcm:aircraftModel>
    </nxcm:flightStatusAndSpec>
    <nxcm:eta etaType="SCHEDULED" timeValue="2026-07-04T17:27:00Z"/>
   </nxcm:airlineData>
  </fdm:ncsmFlightModify>
 </fdm:fltdMessage>
 <fdm:fltdMessage acid="AAL2033" airline="AAL" arrArpt="KLGA" depArpt="KCLT"
  flightRef="150976233" major="AAL" msgType="FlightTimes" sourceFacility="AAL"
  sourceTimeStamp="2026-07-03T21:38:27Z">
  <fdm:ncsmFlightTimes>
   <nxcm:etd etdType="SCHEDULED" timeValue="2026-07-04T15:59:00Z"/>
  </fdm:ncsmFlightTimes>
 </fdm:fltdMessage>
</fltdOutput>
</ds:tfmDataService>"""


def test_track_doc_explodes_into_one_record_per_message():
    results = parse_payload(TRACK_DOC)
    assert len(results) == 2
    assert all(r.parse_status == ParseStatus.OK for r in results)
    assert [r.msg_type for r in results] == ["trackInformation", "trackInformation"]


def test_identity_fields_are_lifted():
    first = parse_payload(TRACK_DOC)[0]
    assert first.identity["acid"] == "EDW4K"
    assert first.identity["dep_arpt"] == "LSZH"
    assert first.identity["arr_arpt"] == "KTPA"
    assert first.identity["flight_ref"] == "150950038"
    # nested fields dug out of the tree
    assert first.identity["gufi"] == "KN6770564K"
    assert first.identity["igtd"] == "2026-07-03T11:25:00Z"


def test_body_is_nested_and_namespace_free():
    first = parse_payload(TRACK_DOC)[0]
    track = first.body["trackInformation"]
    # attribute captured with @ prefix, no namespace
    assert track["qualifiedAircraftId"]["@aircraftCategory"] == "JET"
    # scalar text leaf
    assert track["speed"] == "368"
    # empty element with only attributes -> dict of attributes
    assert track["ncsmTrackData"]["eta"]["@timeValue"] == "2026-07-03T22:03:34Z"


def test_raw_xml_is_preserved_per_message():
    first = parse_payload(TRACK_DOC)[0]
    assert "EDW4K" in first.raw_xml
    assert "fltdMessage" in first.raw_xml


def test_different_message_types_in_one_topic():
    results = parse_payload(FLIGHT_MODIFY_DOC)
    assert [r.msg_type for r in results] == ["FlightModify", "FlightTimes"]
    assert results[0].body["ncsmFlightModify"]["airlineData"]["flightStatusAndSpec"][
        "flightStatus"
    ] == "PLANNED"


def test_malformed_xml_yields_failed_record_not_exception():
    results = parse_payload("<ds:tfmDataService><fltdOutput><broken>")
    assert len(results) == 1
    assert results[0].parse_status == ParseStatus.FAILED
    assert results[0].raw_xml.startswith("<ds:tfmDataService>")
    assert results[0].errors


def test_empty_payload():
    results = parse_payload("   ")
    assert results[0].parse_status == ParseStatus.FAILED


def test_unknown_root_uses_generic_fallback():
    results = parse_payload('<someOtherFeed><record id="1">hi</record></someOtherFeed>')
    assert len(results) == 1
    r = results[0]
    assert r.parser == "generic-xml"
    assert r.root_type == "someOtherFeed"
    assert r.body["record"]["@id"] == "1"


def test_kafka_envelope_roundtrip():
    envelope = {
        "message_id": "abc123",
        "received_at_utc": "2026-07-03T21:40:00Z",
        "source_destination": "queue/SWIM/flight",
        "content_type": "application/xml",
        "payload": TRACK_DOC,
    }
    results = from_kafka_value(json.dumps(envelope).encode("utf-8"))
    assert len(results) == 2
    assert all(r.message_id == "abc123" for r in results)
    assert all(r.source == "queue/SWIM/flight" for r in results)


def test_from_kafka_value_accepts_bare_xml():
    results = from_kafka_value(TRACK_DOC)
    assert len(results) == 2


def test_to_dict_is_json_serializable():
    r = parse_payload(TRACK_DOC)[0]
    json.dumps(r.to_dict())  # must not raise


# --- normalization layer ---------------------------------------------------

from aeroflux_parser import normalize, dms_to_decimal

TRACK_WITH_POSITION = """<?xml version="1.0"?>
<ds:tfmDataService xmlns:ds="urn:us:gov:dot:faa:atm:tfm:tfmdataservice"
 xmlns:fdm="urn:us:gov:dot:faa:atm:tfm:flightdata"
 xmlns:nxce="urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements"
 xmlns:nxcm="urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages">
<fltdOutput>
 <fdm:fltdMessage acid="EDW4K" msgType="trackInformation">
  <fdm:trackInformation>
   <nxcm:speed>368</nxcm:speed>
   <nxcm:reportedAltitude><nxce:assignedAltitude>
     <nxce:simpleAltitude>160</nxce:simpleAltitude>
   </nxce:assignedAltitude></nxcm:reportedAltitude>
   <nxcm:position>
    <nxce:latitude><nxce:latitudeDMS degrees="28" direction="NORTH" minutes="45"/></nxce:latitude>
    <nxce:longitude><nxce:longitudeDMS degrees="081" direction="WEST" minutes="14"/></nxce:longitude>
   </nxcm:position>
   <nxcm:timeAtPosition>2026-07-03T21:38:20Z</nxcm:timeAtPosition>
   <nxcm:ncsmTrackData>
    <nxcm:eta etaType="ESTIMATED" timeValue="2026-07-03T22:03:34Z"/>
    <nxcm:arrivalFixAndTime arrTime="2026-07-03T21:24:21Z" fixName="RAYZZ"/>
    <nxcm:nextEvent latitudeDecimal="28.640748" longitudeDecimal="-81.346126"/>
   </nxcm:ncsmTrackData>
  </fdm:trackInformation>
 </fdm:fltdMessage>
</fltdOutput>
</ds:tfmDataService>"""


def test_dms_to_decimal_signs():
    assert dms_to_decimal({"@degrees": "28", "@minutes": "45", "@direction": "NORTH"}) == 28.75
    assert dms_to_decimal({"@degrees": "081", "@minutes": "14", "@direction": "WEST"}) == -81.233333


def test_normalize_track_lifts_typed_fields():
    record = normalize(parse_payload(TRACK_WITH_POSITION)[0])
    n = record.normalized
    assert n["speed"] == 368            # int, not "368"
    assert n["altitude_ft"] == 16000    # simpleAltitude 160 -> hundreds of feet
    assert n["lat"] == 28.75
    assert n["lon"] == -81.233333
    assert n["eta"] == "2026-07-03T22:03:34Z"
    assert n["eta_type"] == "ESTIMATED"
    assert n["arrival_fix"] == "RAYZZ"
    assert n["next_lat"] == 28.640748


def test_normalize_is_safe_on_sparse_track():
    # a minimal track message (speed only) must not raise, just lift what's there
    doc = TRACK_WITH_POSITION.replace(
        '<nxcm:position>', '<!--').replace('</nxcm:position>', '-->')
    record = normalize(parse_payload(doc)[0])
    assert record.normalized["speed"] == 368
    assert "lat" not in record.normalized
    assert not record.errors


def test_normalize_leaves_unknown_types_untouched():
    # a type with no normalizer registered must pass through with empty normalized
    doc = FLIGHT_MODIFY_DOC.replace('msgType="FlightModify"', 'msgType="FlightSectors"')
    record = normalize(parse_payload(doc)[0])
    assert record.normalized == {}
    assert record.parse_status == ParseStatus.OK


# --- airline schedule normalizer + canonical projection --------------------

from aeroflux_parser import to_canonical, looks_like_registration

FLIGHT_MODIFY_FULL = """<?xml version="1.0"?>
<ds:tfmDataService xmlns:ds="urn:us:gov:dot:faa:atm:tfm:tfmdataservice"
 xmlns:fdm="urn:us:gov:dot:faa:atm:tfm:flightdata"
 xmlns:nxce="urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements"
 xmlns:nxcm="urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages">
<fltdOutput>
 <fdm:fltdMessage acid="AAL2033" airline="AAL" arrArpt="KLGA" depArpt="KCLT"
  flightRef="150976233" major="AAL" msgType="FlightModify" sourceFacility="AAL"
  sourceTimeStamp="2026-07-03T21:38:27Z">
  <fdm:ncsmFlightModify>
   <nxcm:qualifiedAircraftId><nxce:aircraftId>AAL2033</nxce:aircraftId>
    <nxce:igtd>2026-07-04T15:39:00Z</nxce:igtd></nxcm:qualifiedAircraftId>
   <nxcm:airlineData>
    <nxcm:flightStatusAndSpec>
     <nxcm:flightStatus>PLANNED</nxcm:flightStatus>
     <nxcm:aircraftModel>A320</nxcm:aircraftModel>
    </nxcm:flightStatusAndSpec>
    <nxcm:eta etaType="SCHEDULED" timeValue="2026-07-04T17:27:00Z"/>
    <nxcm:etd etdType="SCHEDULED" timeValue="2026-07-04T15:59:00Z"/>
    <nxcm:flightTimeData airlineInTime="2026-07-04T17:35:00Z"
      airlineOffTime="2026-07-04T15:59:00Z" airlineOnTime="2026-07-04T17:27:00Z"
      airlineOutTime="2026-07-04T15:39:00Z" originalArrival="2026-07-04T17:27:00Z"
      originalDeparture="2026-07-04T15:59:00Z"/>
   </nxcm:airlineData>
  </fdm:ncsmFlightModify>
 </fdm:fltdMessage>
</fltdOutput>
</ds:tfmDataService>"""


def test_normalize_flight_modify_extracts_oooi_and_status():
    n = normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]).normalized
    assert n["flight_status"] == "PLANNED"
    assert n["aircraft_model"] == "A320"
    assert n["gate_out"] == "2026-07-04T15:39:00Z"
    assert n["gate_in"] == "2026-07-04T17:35:00Z"
    assert n["etd_type"] == "SCHEDULED"


def test_canonical_from_flight_modify():
    rec = normalize(parse_payload(FLIGHT_MODIFY_FULL)[0])
    c = to_canonical(rec)
    assert c["flight_instance_id"] == "150976233"     # no gufi on airline msg -> flight_ref
    assert c["tail_number"] is None                   # AAL2033 is a callsign, not a reg
    assert c["callsign"] == "AAL2033"
    assert c["operating_carrier"] == "AAL"
    assert c["origin"] == "KCLT"
    assert c["destination"] == "KLGA"
    assert c["scheduled_gate_departure"] == "2026-07-04T15:39:00Z"   # airlineOutTime
    assert c["scheduled_gate_arrival"] == "2026-07-04T17:35:00Z"     # airlineInTime
    assert c["estimated_arrival"] is None        # PLANNED: no genuine estimate yet
    assert c["flight_status"] == "PLANNED"


def test_looks_like_registration():
    assert looks_like_registration("N649QS")     # GA: callsign IS the tail number
    assert not looks_like_registration("AAL2033")
    assert not looks_like_registration("SWA4340")


def test_canonical_ga_flight_gets_tail_from_callsign():
    doc = FLIGHT_MODIFY_FULL.replace('acid="AAL2033"', 'acid="N649QS"').replace(
        "<nxce:aircraftId>AAL2033", "<nxce:aircraftId>N649QS").replace(
        'airline="AAL"', 'airline="XXX"').replace('major="AAL"', 'major="EJA"')
    c = to_canonical(normalize(parse_payload(doc)[0]))
    assert c["tail_number"] == "N649QS"
    assert c["operating_carrier"] == "EJA"   # falls back off the 'XXX' placeholder


# --- fusion layer (GUFI/flight_ref merge + source-priority mediation) -------

from aeroflux_parser import FlightInstanceReducer, MemorySink, JsonlSink, make_sink

# Same flight_ref across a FlightModify (schedule+status, no gufi) and a later
# trackInformation (live ETA + gufi). They must fuse into ONE record.
TRACK_SAME_FLIGHT = """<?xml version="1.0"?>
<ds:tfmDataService xmlns:ds="urn:us:gov:dot:faa:atm:tfm:tfmdataservice"
 xmlns:fdm="urn:us:gov:dot:faa:atm:tfm:flightdata"
 xmlns:nxce="urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements"
 xmlns:nxcm="urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages">
<fltdOutput>
 <fdm:fltdMessage acid="AAL2033" airline="AAL" arrArpt="KLGA" depArpt="KCLT"
  flightRef="150976233" major="AAL" msgType="trackInformation" sourceFacility="KZDC"
  sourceTimeStamp="2026-07-04T16:30:00Z">
  <fdm:trackInformation>
   <nxcm:qualifiedAircraftId><nxce:gufi>KN6770564K</nxce:gufi></nxcm:qualifiedAircraftId>
   <nxcm:ncsmTrackData>
    <nxcm:eta etaType="ESTIMATED" timeValue="2026-07-04T17:41:00Z"/>
   </nxcm:ncsmTrackData>
  </fdm:trackInformation>
 </fdm:fltdMessage>
</fltdOutput>
</ds:tfmDataService>"""


def test_fusion_merges_across_message_types():
    reducer = FlightInstanceReducer()
    reducer.add(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]))   # schedule + status
    reducer.add(normalize(parse_payload(TRACK_SAME_FLIGHT)[0]))    # live ETA + gufi

    recs = reducer.records()
    assert len(recs) == 1                        # one fused flight, not two
    r = recs[0]
    assert r["flight_instance_id"] == "KN6770564K"     # gufi preferred once seen
    assert r["scheduled_gate_departure"] == "2026-07-04T15:39:00Z"  # from FlightModify
    assert r["scheduled_gate_arrival"] == "2026-07-04T17:35:00Z"    # from FlightModify
    assert r["estimated_arrival"] == "2026-07-04T17:41:00Z"    # from trackInformation
    assert r["flight_status"] == "PLANNED"


def test_fusion_source_priority_track_wins_estimate():
    # even if the track message is processed FIRST, the live ETA must own the
    # estimate field over an airline-planned value.
    reducer = FlightInstanceReducer()
    reducer.add(normalize(parse_payload(TRACK_SAME_FLIGHT)[0]))
    reducer.add(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]))
    r = reducer.records()[0]
    assert r["estimated_arrival"] == "2026-07-04T17:41:00Z"   # track, not airline


def test_memory_sink_roundtrip():
    reducer = FlightInstanceReducer()
    reducer.add(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]))
    sink = MemorySink()
    n = sink.write(reducer.records())
    assert n == 1 and sink.records[0]["callsign"] == "AAL2033"


def test_jsonl_sink_writes(tmp_path):
    reducer = FlightInstanceReducer()
    reducer.add(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]))
    path = tmp_path / "out.jsonl"
    n = make_sink("jsonl", path=str(path)).write(reducer.records())
    assert n == 1
    line = json.loads(path.read_text().strip())
    assert line["origin"] == "KCLT"


# --- identity resolution: airline crosswalk, callsign, flight number, ADS-B --

from aeroflux_parser import (
    AirlineTable, parse_callsign, resolve_flight_number,
    callsign_to_flight_number, parse_adsb_response,
)

TABLE = AirlineTable()


def test_airline_crosswalk_bridges_icao_iata():
    assert TABLE.icao_to_iata("AAL") == "AA"
    assert TABLE.icao_to_iata("SWA") == "WN"
    assert TABLE.iata_to_icao("DL") == "DAL"
    assert TABLE.by_icao("UAL").name == "United Airlines"


def test_parse_callsign_airline():
    p = parse_callsign("AAL2033")
    assert p.resolved and p.airline_icao == "AAL"
    assert p.airline_iata == "AA"
    assert p.airline_name == "American Airlines"
    assert p.flight_number == "2033"
    assert not p.is_registration


def test_parse_callsign_general_aviation_is_registration():
    p = parse_callsign("N649QS")
    assert p.is_registration and p.resolved
    assert p.airline_icao is None      # the callsign IS the tail; no airline


def test_parse_callsign_unknown_prefix_still_splits():
    p = parse_callsign("QXQ123")               # QXQ is not a real airline code
    assert p.airline_icao == "QXQ" and p.flight_number == "123"
    assert not p.resolved              # prefix not a known airline


def test_resolve_flight_number_iata_to_callsign():
    r = resolve_flight_number("AA2033")     # what a passenger types
    assert r.airline_icao == "AAL"
    assert "AAL2033" in r.callsign_candidates
    assert r.assumption                      # flags the callsign!=flightno caveat


def test_resolve_flight_number_accepts_icao_input_too():
    r = resolve_flight_number("DAL100")
    assert r.callsign_candidates == ["DAL100"]


def test_callsign_to_passenger_flight_number():
    assert callsign_to_flight_number("AAL2033") == "AA2033"
    assert callsign_to_flight_number("SWA5103") == "WN5103"


# Captured ADSBExchange-v2-shaped response (airplanes.live / adsb.lol).
_ADSB_SAMPLE = {
    "ac": [
        {"hex": "a1b2c3", "type": "adsb_icao", "flight": "AAL2033 ",
         "r": "N826AA", "t": "A321", "alt_baro": 35000},
        {"hex": "d4e5f6", "type": "adsb_icao", "flight": "AAL9999 ",
         "r": "N999AA", "t": "B738"},
    ],
    "total": 2,
}


def test_parse_adsb_extracts_airframe():
    frames = parse_adsb_response(_ADSB_SAMPLE, want_callsign="AAL2033")
    assert len(frames) == 1                       # filtered to exact callsign
    f = frames[0]
    assert f.hex == "a1b2c3"
    assert f.registration == "N826AA"             # the tail SWIM lacked
    assert f.aircraft_type == "A321"              # from 't', not 'type'
    assert f.callsign == "AAL2033"                # whitespace stripped


def test_parse_adsb_empty_is_safe():
    assert parse_adsb_response({"ac": [], "total": 0}) == []


# --- dataset enrichment + labeling + actual times --------------------------

from aeroflux_parser import enrich_record, classify, ResolutionStatus, DATASET_FIELDS


def test_actual_off_from_departure_information():
    doc = FLIGHT_MODIFY_DOC.replace(
        'msgType="FlightModify"', 'msgType="departureInformation"'
    ).replace("ncsmFlightModify", "departureInformation").replace(
        "<nxcm:airlineData>",
        '<nxcm:timeOfDeparture estimated="false">2026-07-04T16:05:00Z</nxcm:timeOfDeparture><nxcm:airlineData>',
    )
    n = normalize(parse_payload(doc)[0]).normalized
    assert n.get("actual_off") == "2026-07-04T16:05:00Z"


def test_enrich_labels_and_resolves_airline():
    rec = to_canonical(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]))
    e = enrich_record(rec)
    assert e["resolution_status"] == ResolutionStatus.AIRLINE
    assert e["flight_number"] == "AA2033"
    assert e["carrier_name"] == "American Airlines"
    assert e["tail_source"] == "none"          # airline tail unresolved offline
    assert e["tail_number"] is None


def test_enrich_ga_gets_tail_from_callsign():
    doc = FLIGHT_MODIFY_FULL.replace('acid="AAL2033"', 'acid="N649QS"')
    e = enrich_record(to_canonical(normalize(parse_payload(doc)[0])))
    assert e["resolution_status"] == ResolutionStatus.GA_TAIL
    assert e["tail_number"] == "N649QS"
    assert e["tail_source"] == "swim_ga"


def test_enrich_labels_unknown_airline():
    doc = FLIGHT_MODIFY_FULL.replace('acid="AAL2033"', 'acid="BBQ8256"').replace(
        'airline="AAL"', 'airline="BBQ"').replace('major="AAL"', 'major="BBQ"')
    e = enrich_record(to_canonical(normalize(parse_payload(doc)[0])))
    assert e["resolution_status"] == ResolutionStatus.UNKNOWN_AIRLINE


def test_enrich_can_resolve_tail_via_injected_adsb():
    class FakeFrame:
        hex = "a1b2c3"; registration = "N826AA"; aircraft_type = "A321"
    rec = to_canonical(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0]))
    e = enrich_record(rec, adsb_resolver=lambda cs: FakeFrame())
    assert e["hex"] == "a1b2c3"
    assert e["tail_number"] == "N826AA"
    assert e["tail_source"] == "adsb"


def test_enriched_record_has_every_dataset_field():
    e = enrich_record(to_canonical(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0])))
    for field in DATASET_FIELDS:
        assert field in e


# --- reducer keys on GUFI (flight_ref is unstable) -------------------------

def _msg(msg_type, gufi=None, flight_ref=None, dest="KLGA", extra=""):
    g = f"<nxce:gufi>{gufi}</nxce:gufi>" if gufi else ""
    return f"""<?xml version="1.0"?>
<ds:tfmDataService xmlns:ds="urn:us:gov:dot:faa:atm:tfm:tfmdataservice"
 xmlns:fdm="urn:us:gov:dot:faa:atm:tfm:flightdata"
 xmlns:nxce="urn:us:gov:dot:faa:atm:tfm:tfmdatacoreelements"
 xmlns:nxcm="urn:us:gov:dot:faa:atm:tfm:flightdatacommonmessages">
<fltdOutput><fdm:fltdMessage acid="ERU854" airline="ERU" arrArpt="{dest}" depArpt="KDAB"
 flightRef="{flight_ref or ''}" major="ERU" msgType="{msg_type}" sourceFacility="KZJX"
 sourceTimeStamp="2026-07-03T21:45:00Z">
 <fdm:{msg_type}><nxcm:qualifiedAircraftId>{g}</nxcm:qualifiedAircraftId>{extra}</fdm:{msg_type}>
</fdm:fltdMessage></fltdOutput></ds:tfmDataService>"""


def test_reducer_merges_same_gufi_across_changed_flight_ref():
    # the real collision: one flight, two flight_refs (refile), one GUFI
    r = FlightInstanceReducer()
    r.add(normalize(parse_payload(_msg("trackInformation", gufi="KJ767036Lr", flight_ref="151000265", dest="KOCF"))[0]))
    r.add(normalize(parse_payload(_msg("trackInformation", gufi="KJ767036Lr", flight_ref="151002836", dest="SFB"))[0]))
    recs = r.records()
    assert len(recs) == 1                                  # one flight, not two
    assert recs[0]["flight_instance_id"] == "KJ767036Lr"


def test_reducer_bridges_airline_message_via_flight_ref():
    # airline msg (no gufi) must still join the gufi record via shared flight_ref,
    # regardless of arrival order
    r = FlightInstanceReducer()
    r.add(normalize(parse_payload(_msg("FlightTimes", flight_ref="151000265"))[0]))   # no gufi, first
    r.add(normalize(parse_payload(_msg("trackInformation", gufi="KJ767036Lr", flight_ref="151000265"))[0]))
    recs = r.records()
    assert len(recs) == 1
    assert recs[0]["flight_instance_id"] == "KJ767036Lr"


# --- schema contract (validation gate) -------------------------------------

from aeroflux_parser.schema import (
    FlightInstance, validate_record, validate_batch, SCHEMA_VERSION,
)
import pytest
from pydantic import ValidationError


def _good_record():
    return enrich_record(to_canonical(normalize(parse_payload(FLIGHT_MODIFY_FULL)[0])))


def test_valid_record_passes_and_is_stamped():
    m = validate_record(_good_record())
    assert m.flight_instance_id
    assert m.schema_version == SCHEMA_VERSION
    assert m.resolution_status == "airline_resolved"


def test_missing_flight_instance_id_fails():
    rec = _good_record()
    rec["flight_instance_id"] = ""
    with pytest.raises(ValidationError):
        validate_record(rec)


def test_bad_latitude_fails():
    rec = _good_record()
    rec["last_latitude"] = 999.0
    with pytest.raises(ValidationError):
        validate_record(rec)


def test_bad_timestamp_fails():
    rec = _good_record()
    rec["scheduled_gate_departure"] = "not-a-date"
    with pytest.raises(ValidationError):
        validate_record(rec)


def test_unknown_resolution_status_fails():
    rec = _good_record()
    rec["resolution_status"] = "banana"
    with pytest.raises(ValidationError):
        validate_record(rec)


def test_unexpected_field_is_rejected():
    rec = _good_record()
    rec["surprise_column"] = "x"
    with pytest.raises(ValidationError):
        validate_record(rec)


def test_validate_batch_splits_valid_and_invalid():
    good = _good_record()
    bad = _good_record()
    bad["last_longitude"] = 500.0
    valid, invalid = validate_batch([good, bad])
    assert len(valid) == 1 and len(invalid) == 1
    assert valid[0]["schema_version"] == SCHEMA_VERSION
    assert invalid[0]["_errors"]        # carries the reason


# --- gold layer: feature/label table ---------------------------------------

from aeroflux_parser import flight_features, build_feature_table, ALL_COLUMNS


def _silver(**over):
    base = {
        "flight_instance_id": "G1", "callsign": "AAL2033", "flight_number": "AA2033",
        "carrier_icao": "AAL", "origin": "KCLT", "destination": "KLGA",
        "aircraft_type": "A320", "aircraft_category": "COMMERCIAL",
        "scheduled_gate_departure": "2026-07-04T15:39:00Z",   # a Saturday
        "scheduled_gate_arrival": "2026-07-04T17:35:00Z",
        "actual_off": "2026-07-04T15:57:00Z",   # 18 min after sched gate dep
        "actual_on": None,
    }
    base.update(over)
    return base


def test_gold_computes_delay_and_features():
    row = flight_features(_silver())
    assert row["dep_delay_min"] == 18.0
    assert row["dep_delay_15"] == 1              # 18 >= 15
    assert row["carrier"] == "AAL"
    assert row["sched_dep_hour"] == 15
    assert row["sched_dep_dow"] == 5             # Saturday
    assert row["is_weekend"] == 1
    assert row["sched_block_min"] == 116.0       # 15:39 -> 17:35


def test_gold_arrival_label_when_present():
    row = flight_features(_silver(actual_on="2026-07-04T17:50:00Z"))  # 15 min late
    assert row["arr_delay_min"] == 15.0
    assert row["arr_delay_15"] == 1


def test_gold_skips_rows_with_no_label():
    # planned flight: schedule but no actuals -> not trainable -> dropped
    assert flight_features(_silver(actual_off=None, actual_on=None)) is None


def test_gold_early_departure_negative_delay():
    row = flight_features(_silver(actual_off="2026-07-04T15:34:00Z"))  # 5 min early
    assert row["dep_delay_min"] == -5.0
    assert row["dep_delay_15"] == 0


def test_build_feature_table_filters_and_shapes():
    silver = [_silver(), _silver(actual_off=None, actual_on=None), _silver(flight_instance_id="G2")]
    rows = build_feature_table(silver)
    assert len(rows) == 2                         # planned one dropped
    assert set(rows[0].keys()) == set(ALL_COLUMNS)


# --- ADS-B rolling store + bulk poller ------------------------------------

from datetime import datetime, timedelta, timezone
from aeroflux_parser.adsb import parse_adsb_response, Airframe
from aeroflux_parser.adsb_store import InMemoryAirframeStore


def test_by_point_response_parses_many_airframes():
    # one /point request returns many aircraft (ADSBExchange v2 shape)
    payload = {"ac": [
        {"hex": "a1b2c3", "flight": "AAL2033 ", "r": "N826AA", "t": "A321"},
        {"hex": "d4e5f6", "flight": "DAL1150 ", "r": "N901DL", "t": "B738"},
        {"hex": "0a0b0c", "flight": "", "r": "", "t": ""},  # no callsign -> unusable
    ]}
    frames = parse_adsb_response(payload)
    assert len(frames) == 3
    assert frames[0].hex == "a1b2c3" and frames[0].callsign == "AAL2033"
    assert frames[0].registration == "N826AA"


def test_store_upsert_resolve_and_latest_wins():
    store = InMemoryAirframeStore()
    t0 = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)
    store.upsert([Airframe(hex="a1b2c3", registration="N826AA", aircraft_type="A321", callsign="AAL2033")], seen_at=t0)
    # a later sweep sees the same callsign on a different airframe -> newest wins
    store.upsert([Airframe(hex="ffffff", registration="N999AA", callsign="AAL2033")], seen_at=t0 + timedelta(minutes=30))
    frame = store.resolve("aal2033")           # case-insensitive
    assert frame.hex == "ffffff"
    assert store.resolve("UNKNOWN") is None


def test_store_skips_callsignless_or_empty():
    store = InMemoryAirframeStore()
    n = store.upsert([
        Airframe(hex="a1b2c3", callsign="AAL1"),   # ok
        Airframe(hex="", registration="", callsign="AAL2"),  # no id -> skip
        Airframe(hex="d4e5f6", callsign=""),        # no callsign -> skip
    ])
    assert n == 1 and len(store) == 1


def test_store_purge_drops_stale_rows():
    store = InMemoryAirframeStore()
    old = datetime.now(timezone.utc) - timedelta(hours=72)
    store.upsert([Airframe(hex="a1", callsign="OLD1")], seen_at=old)
    store.upsert([Airframe(hex="b2", callsign="NEW1")])   # now
    assert store.purge(older_than_hours=48) == 1
    assert store.resolve("OLD1") is None and store.resolve("NEW1") is not None


def test_store_resolve_is_a_valid_enrich_resolver():
    # the store's .resolve is a drop-in for enrich_record's adsb_resolver
    from aeroflux_parser import enrich_record
    store = InMemoryAirframeStore()
    store.upsert([Airframe(hex="a1b2c3", registration="N826AA", aircraft_type="A321", callsign="AAL2033")])
    rec = enrich_record({"flight_instance_id": "F1", "callsign": "AAL2033"},
                        adsb_resolver=store.resolve)
    assert rec["hex"] == "a1b2c3"
    assert rec["tail_number"] == "N826AA"
    assert rec["tail_source"] == "adsb"


# --- airport dimension: normalization, geo, tz -----------------------------

from aeroflux_parser.airports import AirportTable, DEFAULT_AIRPORTS


def test_airport_normalizes_iata_and_icao_to_one_canonical():
    at = DEFAULT_AIRPORTS
    assert at.to_icao("DFW") == "KDFW"      # IATA -> ICAO
    assert at.to_icao("KDFW") == "KDFW"     # already ICAO
    assert at.to_icao("dfw") == "KDFW"      # case-insensitive
    assert at.to_icao("MMMY") == "MMMY"     # intl ICAO passes through
    # the exact break we saw in the ENY3347 trace now unifies:
    assert at.to_icao("DFW") == at.to_icao("KDFW")


def test_airport_geo_and_tz_lookup():
    at = DEFAULT_AIRPORTS
    lat, lon = at.latlon("KDFW")
    assert 32 < lat < 33 and -98 < lon < -97
    assert at.tz("KDFW") == "America/Chicago"
    assert at.tz("MMMY") == "America/Mexico_City"   # via ICAO
    assert at.tz("ZZZZ") is None                    # unknown -> None


def test_enrich_normalizes_airport_codes():
    from aeroflux_parser import enrich_record
    rec = enrich_record({"flight_instance_id": "F1", "callsign": "ENY3347",
                         "origin": "DFW", "destination": "MTY"})
    assert rec["origin"] == "KDFW"          # IATA normalized to ICAO
    assert rec["destination"] == "MMMY"


def test_dedup_collapses_flight_ref_amendment_duplicates():
    from build_dataset import _dedup
    # the ENY3347 case: same callsign/route/sched_dep, amended arrival -> 2 refs
    rows = [
        {"flight_ref": "1", "gufi": None, "callsign": "ENY3347", "origin": "MMMY",
         "destination": "KDFW", "scheduled_gate_departure": "2026-07-31T00:11:00Z",
         "scheduled_gate_arrival": "2026-07-31T01:57:00Z", "flight_status": "PLANNED"},
        {"flight_ref": "2", "gufi": None, "callsign": "ENY3347", "origin": "MMMY",
         "destination": "KDFW", "scheduled_gate_departure": "2026-07-31T00:11:00Z",
         "scheduled_gate_arrival": "2026-07-31T01:56:00Z", "flight_status": "ACTIVE",
         "actual_off": "2026-07-31T00:20:00Z"},  # more complete -> kept
    ]
    out = _dedup(rows)
    assert len(out) == 1
    assert out[0]["flight_status"] == "ACTIVE"      # kept the richer row