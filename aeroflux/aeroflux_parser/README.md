# aeroflux-parser

A small, decoupled parser for FAA SWIM XML. It turns raw payloads into flat,
database-ready records without knowing anything about Kafka, Postgres, or files.

## Design in one breath

- **Unit of output is the message, not the document.** One `tfmDataService`
  payload contains many `fltdMessage` elements; each becomes one `ParsedMessage`.
- **Generic flatten, not 57 hand-written extractors.** Any `fltdMessage` is
  converted to a nested, namespace-free dict regardless of `msgType`. Unknown
  roots fall back to a generic flattener so nothing is ever dropped.
- **Never raises.** Malformed XML returns a `FAILED` record with `raw_xml`
  preserved. That record *is* your retry queue.
- **No infra in the core.** Pure `str | bytes -> list[ParsedMessage]`.

## Usage

```python
from aeroflux_parser import parse_payload, from_kafka_value

records = parse_payload(xml_string)          # raw XML
records = from_kafka_value(kafka_message)     # the JSON envelope from swim_to_kafka.py

for r in records:
    print(r.msg_type, r.identity["acid"], r.parse_status)
    row = r.to_dict()   # JSON-serializable; ready for a DB insert
```

## Output shape

```
ParsedMessage(
    parse_status  # "ok" | "partial" | "failed"
    parser        # "tfms" | "generic-xml" | "none"
    root_type     # "tfmDataService"
    msg_type      # "trackInformation", "flightPlanAmendmentInformation", ...
    identity      # lifted scalars -> SQL columns / indexes
    body          # nested dict -> JSONB column or NoSQL document
    raw_xml       # verbatim, for reprocessing
    errors        # list[str]
    message_id / source / ingested_at
)
```

`identity` gives you clean columns (`acid`, `dep_arpt`, `arr_arpt`,
`flight_ref`, `msg_type`, `gufi`, `igtd`, ...). `body` holds the full nested
structure. Store both: query on the columns, keep `body` as JSONB.

## Wiring it to your existing prototype (optional, later)

The parser stays pure. Adapters call into it:

```python
# In your Kafka consumer loop:
from aeroflux_parser import from_kafka_value

for msg in consumer:                     # confluent_kafka message
    for record in from_kafka_value(msg.value()):
        cur.execute(
            "INSERT INTO swim_messages "
            "(message_id, msg_type, acid, dep_arpt, arr_arpt, source_time, "
            " parse_status, body, raw_xml) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (record.message_id, record.msg_type,
             record.identity.get("acid"), record.identity.get("dep_arpt"),
             record.identity.get("arr_arpt"), record.identity.get("source_time"),
             record.parse_status, Json(record.body), record.raw_xml),
        )
```

A single Postgres table with typed identity columns + a `jsonb body` + a
`raw_xml text` handles all 57 message types today. Split into per-type tables
later only if query patterns demand it.

## Normalization layer (optional, additive)

Parsing captures everything into `body`. Normalizing lifts a small, typed,
model-ready set of fields for the message types you care about — without
touching `body` or `raw_xml`.

```python
from aeroflux_parser import parse_payload, normalize

for r in map(normalize, parse_payload(xml)):
    print(r.normalized)   # {} unless a normalizer is registered for r.msg_type
```

`trackInformation` is done: `speed` (int, kts), `altitude_ft` (int),
`lat`/`lon` (decimal, converted from DMS), `eta`/`eta_type`, `arrival_fix`,
`time_at_position`. A missing or malformed field is skipped, never raised.

Add a type by writing one function and registering it in
`normalizers.NORMALIZERS`. In the harness, add `--normalize` to any command.

## Canonical flight-instance record

`canonical.to_canonical(record)` projects a message onto the business schema
(flight_instance_id, callsign, operating_carrier, origin/destination, scheduled
+ estimated gate times, flight_status). Opinionated mapping choices live here,
separate from faithful extraction in `normalizers.py`.

```python
from aeroflux_parser import parse_payload, normalize, to_canonical
c = to_canonical(normalize(parse_payload(xml)[0]))
```

In the harness: add `--canonical` to any command.

Two things to know:
- **Tail number is not in TFMS for airline flights.** `acid` is the ATC
  callsign; it equals the registration only for GA (e.g. N649QS). For airline
  tails you must join on ADS-B (ICAO 24-bit hex -> FAA registry) or a fleet DB.
- **One message rarely fills every field.** FlightModify gives status + gate
  times; trackInformation gives the live ETA; departureInformation gives
  actual-off. The complete record comes from merging messages that share a GUFI
  over time -- the fusion layer (next increment). `to_canonical` projects from a
  single message.

## Fusion: one record per flight (configurable sink)

`FlightInstanceReducer` merges many messages into one evolving record per
flight, keyed on `flightRef` (present on every message; GUFI is preferred for
the displayed id but absent on airline messages). Conflicts resolve by **source
priority**, not latest-wins -- e.g. a live `trackInformation` ETA owns
`estimated_gate_arrival` even against a later airline message. Priorities live
in `fusion.FIELD_SOURCE_PRIORITY` and are easy to edit.

```python
from aeroflux_parser import parse_payload, normalize, FlightInstanceReducer, make_sink

reducer = FlightInstanceReducer()
for raw in batch:
    for rec in map(normalize, parse_payload(raw)):
        reducer.add(rec)

make_sink("memory").write(reducer.records())        # in-process
make_sink("jsonl", path="out.jsonl").write(...)      # file, zero infra
make_sink("postgres", dsn="...", table="flight_instance").write(...)   # upsert
```

In the harness: `--fuse --sink {memory,jsonl,postgres}` (with `--sink-path` or
`--sink-table`/`--dsn`). Start with `--sink memory` or `jsonl` to validate the
merge on your batch, then switch to `postgres` (DDL is in `sinks.py`).

### Design credit / prior art

The GUFI-keyed, source-priority-mediation approach follows NASA's ATD-2 **Fuser**
(github.com/nasa/atd2-fuser, NASA Open Source Agreement). That project is a
heavyweight Java/Spring/JMS app partly wrapping proprietary libraries, so this
is a lightweight Python re-implementation of the *idea*. Its public **Data
Dictionary** and **Database Input Mapping Table** (aviationsystems.arc.nasa.gov)
are the authoritative SWIM->field references worth mining as you add types.

## Identity resolution (SWIM <-> passenger <-> ADS-B)

Closes the identity chain needed for real-time inference: a passenger's flight
number <-> SWIM's ICAO callsign <-> the airframe (tail) that SWIM lacks.

```python
from aeroflux_parser import (
    resolve_flight_number, parse_callsign, callsign_to_flight_number, AdsbClient,
)

resolve_flight_number("AA2033").callsign_candidates   # -> ["AAL2033"]  (user -> SWIM)
callsign_to_flight_number("AAL2033")                  # -> "AA2033"     (SWIM -> user)
parse_callsign("N649QS").is_registration              # -> True (GA: callsign IS the tail)

AdsbClient().resolve_tail("AAL2033")   # live: -> Airframe(hex, registration, type) | None
```

- `airlines.py` — ICAO<->IATA<->name crosswalk (bundled OpenFlights data,
  offline). This is the bridge: SWIM speaks ICAO (AAL), passengers + BTS speak
  IATA (AA).
- `identity.py` — callsign parsing and flight-number resolution.
- `adsb.py` — live airframe lookup via airplanes.live / adsb.lol (free,
  non-commercial, real-time). Returns hex + registration + type. Never raises on
  a coverage miss -- returns None.

**Report** how resolvable your real SWIM data is:

```bash
run_parser.py <source> ... --fuse --sink jsonl --sink-path flights.jsonl
python resolve_report.py flights.jsonl --live 5
```

Two honest limits: (1) the ATC callsign is not always the marketed flight number
-- airlines sometimes fly AA2033 under a different ATC callsign, so
`resolve_flight_number` returns *candidates* and flags the assumption; a schedule
crosswalk or ADS-B match resolves the general case. (2) ADS-B tail resolution is
coverage-limited -- some flights won't be visible, so treat tail as nullable.

## The canonical dataset (build_dataset.py)

The end-to-end product: SWIM messages in, one clean labeled row per flight out.
Ties the whole pipeline together (parse -> normalize -> fuse -> enrich) and
writes JSONL + CSV plus a readable summary.

```bash
python build_dataset.py postgres --dsn "..." --table T --column C --limit 5000
python build_dataset.py postgres --dsn "..." --table T --column C --live 25   # ADS-B tails
```

Each row carries: identity (flight_instance_id, gufi, flight_ref, callsign,
flight_number, carrier icao/iata/name), a plain-language **resolution_status**
(`airline_resolved` / `ga_tail_from_callsign` / `unknown_airline` /
`unparseable` -- so the oddballs are labeled, never dropped), airframe
(tail_number, tail_source, hex, aircraft_type, aircraft_category), route,
scheduled/estimated/actual times, flight_status, and latest live state
(lat/lon/altitude/speed). Missing fields are filled by fusion across message
types; airline tails fill from ADS-B when `--live` is set, else stay null with
`tail_source` recording why. Full column list: `enrich.DATASET_FIELDS`.

## Schema contract — the validation gate (schema.py)

Every fused, enriched record must pass a typed contract before it lands, so
"reliable data" is enforced on write rather than hoped-for. Requires pydantic:

```bash
pip install "aeroflux-parser[validate]"     # or: pip install pydantic
```

```python
from aeroflux_parser.schema import validate_record, validate_batch, SCHEMA_VERSION
valid, invalid = validate_batch(records)     # invalid rows carry _errors
```

`flight_instance_id` is the only required field; everything else is nullable
(a flight may not have observed every field yet). The contract checks enum
membership (resolution_status, tail_source), coordinate ranges, ISO-8601
timestamps (kept as strings, so the DB flow is unchanged), and forbids
unexpected columns (catches schema drift). `build_dataset.py` validates by
default: valid rows -> dataset.jsonl/csv (stamped with schema_version), invalid
rows -> dataset.invalid.jsonl with reasons. Skip with `--no-validate`.

## Gold layer — the ML-ready table (gold.py / build_features.py)

Turns validated silver flight instances into a flat feature/label table for
pandas/scikit-learn — one row per flight, only rows with a computable delay
label.

```bash
python build_features.py --in dataset.jsonl                 # from silver JSONL
python build_features.py --dsn "postgresql://..." --table flight_instance
# -> gold.csv + gold.parquet, plus a readiness summary
```

Features: carrier, origin, destination, aircraft_type/category, scheduled
hour/day-of-week/month/weekend, scheduled block minutes. Labels:
`dep_delay_min`, `arr_delay_min`, and their 15-minute binaries.

**Label caveat (documented in `gold.py`):** live SWIM gives wheels-off/on
actuals but *gate* schedules, so these delays are proxies that include taxi
time. The same transform run over BTS historical yields true gate-to-gate
delays with identical columns — so a BTS-trained model lines up with live
inference features.

## Growth path (deliberately not built yet)

1. **Now:** capture everything, typed identity + JSONB body. (this)
2. **Next:** per-`msg_type` normalizers registered in `parsers.py` that coerce
   types (altitudes to int, timestamps to datetime, DMS lat/long to decimal)
   and emit tidy typed rows for the message types your model actually needs.
3. **Later:** a second SWIM feed = one new `Parser` class appended to
   `REGISTRY`. Dispatch and the public API don't change.

## Tests

```bash
pip install -e ".[dev]"
pytest
```
