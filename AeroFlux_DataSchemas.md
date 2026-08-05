# AeroFlux — Data Schemas & Samples (for the Intelligence / Reasoning Layer)

This is the data contract for the RAG / agent / reasoning work. It lists every
dataset the platform produces, where it lives, its schema, and 2–3 sample
records. The layers most relevant to the reasoning team are marked **[AGENT]** —
those are the ones the analyst tools will read.

Quick map of what to query for what:

| You want… | Read from |
|---|---|
| Current state of a flight (route, status, times, tail) | `flight_instance` (silver) **[AGENT]** |
| A delay prediction for a flight | `predictions` **[AGENT]** |
| The features a prediction was made from | `gold_features.parquet` **[AGENT]** |
| Airport name / city / timezone / coordinates | `airports` dim **[AGENT]** |
| Carrier name from a code | `airlines` dim **[AGENT]** |
| Weather at an airport/time | weather observations **[AGENT]** |
| Which airframe flew a callsign | `adsb_airframe` |
| Raw source message (audit/debug) | `swim.raw_messages` (bronze) |
| Historical labels for training | BTS On-Time Performance |
| Aviation documents for retrieval | RAG corpus / vector store (you build) **[AGENT]** |

Identifiers that tie it together: **`flight_instance_id`** (a.k.a. `flight_key`
in gold/predictions) joins silver ↔ gold ↔ predictions. **`callsign`** joins to
`adsb_airframe`. **airport ICAO** (`origin`/`destination`) joins to `airports`
and weather `station`. **`carrier_icao`** joins to `airlines`.

---

## 1. `flight_instance` — Silver (current per-flight state) **[AGENT]**

Postgres table `public.flight_instance`. One row per flight, fused from many SWIM
messages. Rolling 48-hour window. **This is the main "flight state" table the
agent queries.** All times are UTC (`timestamptz`).

| Column | Type | Notes |
|---|---|---|
| schema_version | text | contract version, e.g. "1.0" |
| flight_instance_id | text (PK) | stable id (GUFI, or flight_ref fallback) = `flight_key` in gold |
| gufi | text | Globally Unique Flight Identifier (null ~60% of live flights) |
| flight_ref | text | SWIM ref; changes on plan amendment (bridge only) |
| callsign | text | ATC callsign (ICAO), e.g. "SWA2606" — joins `adsb_airframe` |
| flight_number | text | marketed number, e.g. "WN2606" |
| carrier_icao | text | e.g. "SWA" — joins `airlines` |
| carrier_iata | text | e.g. "WN" |
| carrier_name | text | e.g. "Southwest Airlines" |
| resolution_status | text | airline_resolved · ga_tail_from_callsign · unknown_airline · unparseable |
| tail_number | text | registration, e.g. "N826AA" (null when unresolved) |
| tail_source | text | none · adsb · adsb_hex_only · swim_ga |
| hex | text | ADS-B ICAO 24-bit airframe id (null when unresolved) |
| aircraft_type | text | ICAO type, e.g. "B38M" |
| aircraft_category | text | COMMERCIAL · GA · … |
| origin | text | departure airport ICAO — joins `airports` |
| destination | text | arrival airport ICAO |
| scheduled_gate_departure | timestamptz | UTC |
| scheduled_gate_arrival | timestamptz | UTC |
| estimated_arrival | timestamptz | UTC |
| actual_off | timestamptz | wheels-off (runway proxy), UTC |
| actual_on | timestamptz | wheels-on, UTC |
| flight_status | text | PLANNED · ACTIVE · COMPLETED |
| last_latitude | double precision | most recent position |
| last_longitude | double precision | |
| last_altitude_ft | integer | |
| last_ground_speed | integer | knots |
| last_position_time | timestamptz | UTC |
| updated_at | timestamptz | row last updated |

**Samples:**
```json
{ "schema_version":"1.0","flight_instance_id":"KJ5957868p","gufi":"KJ5957868p",
  "flight_ref":"153371795","callsign":"SWA2606","flight_number":"WN2606",
  "carrier_icao":"SWA","carrier_iata":"WN","carrier_name":"Southwest Airlines",
  "resolution_status":"airline_resolved","tail_number":null,"tail_source":"none",
  "hex":null,"aircraft_type":"B38M","aircraft_category":"COMMERCIAL",
  "origin":"KMCO","destination":"KBNA",
  "scheduled_gate_departure":"2026-07-31T19:45:00Z",
  "scheduled_gate_arrival":"2026-07-31T21:44:00Z",
  "estimated_arrival":"2026-07-31T21:31:02Z","actual_off":"2026-07-31T20:02:00Z",
  "actual_on":null,"flight_status":"ACTIVE","last_latitude":34.178333,
  "last_longitude":-84.655556,"last_altitude_ft":35000,"last_ground_speed":456,
  "last_position_time":"2026-07-31T21:02:45Z" }
```
```json
{ "schema_version":"1.0","flight_instance_id":"KH4168263w","gufi":"KH4168263w",
  "flight_ref":"153335984","callsign":"SWA4252","flight_number":"WN4252",
  "carrier_icao":"SWA","carrier_iata":"WN","carrier_name":"Southwest Airlines",
  "resolution_status":"airline_resolved","tail_number":"N8712L","tail_source":"adsb",
  "hex":"a12b3c","aircraft_type":"B38M","aircraft_category":"COMMERCIAL",
  "origin":"KAUS","destination":"KBWI",
  "scheduled_gate_departure":"2026-07-31T13:55:00Z",
  "scheduled_gate_arrival":"2026-07-31T17:14:00Z",
  "estimated_arrival":"2026-07-31T17:08:15Z","actual_off":null,"actual_on":null,
  "flight_status":"COMPLETED","last_latitude":39.161389,"last_longitude":-76.66,
  "last_altitude_ft":200,"last_ground_speed":160,
  "last_position_time":"2026-07-31T17:07:53Z" }
```

---

## 2. `predictions` — Model output (delay predictions) **[AGENT]**

Written by the inference stage; served from the current-state store. One row per
flight per model/feature version. **This is what answers "is flight X delayed?".**

| Column | Type | Notes |
|---|---|---|
| flight_key | text | = `flight_instance_id` |
| delay_probability | float64 | P(arrival delayed ≥ 15 min), 0–1 |
| predicted_delayed | int8 | 1 if probability ≥ threshold (0.5) else 0 |
| model_version | text | e.g. "xgb_v2" |
| feature_version | text | e.g. "fe_v1" |
| scored_at | timestamptz | when scored (UTC) |
| prediction_key | text | `flight_key:feature_version:model_version` (idempotent id) |

**Samples:**
```json
{ "flight_key":"KJ5957868p","delay_probability":0.82,"predicted_delayed":1,
  "model_version":"xgb_v2","feature_version":"fe_v1",
  "scored_at":"2026-07-31T21:05:00Z","prediction_key":"KJ5957868p:fe_v1:xgb_v2" }
{ "flight_key":"KH4168263w","delay_probability":0.14,"predicted_delayed":0,
  "model_version":"xgb_v2","feature_version":"fe_v1",
  "scored_at":"2026-07-31T21:05:00Z","prediction_key":"KH4168263w:fe_v1:xgb_v2" }
```

---

## 3. `gold_features.parquet` — Model input features **[AGENT]**

Parquet in the data lake (`out/gold_features.parquet` locally, `s3://…/gold/`
in cloud). The feature matrix the model consumes; useful to the agent for
*explaining* a prediction (alongside SHAP). One row per flight, `flight_key`
joins to silver/predictions. Many columns are null when their inputs are absent
(sparse live data) — that is expected, not an error.

| Column | Type | Meaning |
|---|---|---|
| flight_key | str | = flight_instance_id |
| sched_dep_hour / _dow / _month | int8 | scheduled departure hour / day-of-week / month (UTC) |
| is_weekend | int8 | 1 if Sat/Sun |
| sched_block_min | int64 | scheduled gate-to-gate minutes |
| prev_leg_arr_delay_min | int64 | previous leg's arrival delay (propagation) |
| turnaround_buffer_min | int64 | scheduled ground time before this leg |
| legs_into_day | int64 | 0-based leg index for the airframe that day |
| inbound_resolved | int8 | 1 if the airframe (rotation) was resolved, else 0 |
| origin_dep_demand | uint32 | departures from origin in the rolling window |
| origin_recent_dep_delay | float64 | mean recent departure delay at origin |
| dest_arr_demand | uint32 | arrivals into destination in the window |
| dest_recent_arr_delay | float64 | mean recent arrival delay at destination |
| dep_delay_min / arr_delay_min | int64 | base delays (labels/derived) |
| origin_wx_wind_kt / _vis_mi / _ifr | f64/f64/i8 | origin weather (wind kt, vis mi, IFR flag) |
| origin_wx_temp_c / _ceiling_ft | float64 | origin temp / ceiling (from NCEI path) |
| dest_wx_wind_kt / _vis_mi / _ifr / _temp_c / _ceiling_ft | … | destination weather (same set) |

**Sample (one populated, one sparse):**
```json
{ "flight_key":"KJ5957868p","sched_dep_hour":19,"sched_dep_dow":4,"sched_dep_month":7,
  "is_weekend":0,"sched_block_min":119,"prev_leg_arr_delay_min":22,
  "turnaround_buffer_min":41,"legs_into_day":3,"inbound_resolved":1,
  "origin_dep_demand":18,"origin_recent_dep_delay":12.5,"dest_arr_demand":9,
  "dest_recent_arr_delay":6.0,"dep_delay_min":17,"arr_delay_min":null,
  "origin_wx_wind_kt":11.0,"origin_wx_vis_mi":10.0,"origin_wx_ifr":0,
  "origin_wx_temp_c":31.0,"origin_wx_ceiling_ft":null,
  "dest_wx_wind_kt":7.0,"dest_wx_vis_mi":10.0,"dest_wx_ifr":0,
  "dest_wx_temp_c":29.0,"dest_wx_ceiling_ft":null }
{ "flight_key":"KM569926RF","sched_dep_hour":null,"inbound_resolved":0,
  "origin_dep_demand":null,"origin_wx_wind_kt":null }
```

---

## 4. `airports` — Reference dimension **[AGENT]**

Bundled CSV `aeroflux_parser/data/airports.csv` (28,426 airports). Use for
enrichment: turn `KMCO` into "Orlando Intl", get its timezone and coordinates.

| Column | Type | Notes |
|---|---|---|
| icao | str | 4-letter, e.g. "KMCO" (join key) |
| iata | str | 3-letter, e.g. "MCO" |
| name | str | "Orlando International Airport" |
| lat | float64 | decimal degrees |
| lon | float64 | decimal degrees |
| tz | str | IANA, e.g. "America/New_York" |
| country | str | ISO country, e.g. "US" |

**Samples:**
```
icao,iata,name,lat,lon,tz,country
KMCO,MCO,Orlando International Airport,28.429399,-81.308998,America/New_York,US
KDFW,DFW,Dallas Fort Worth International Airport,32.896801,-97.038002,America/Chicago,US
MMMY,MTY,General Mariano Escobedo Intl,25.778681,-100.106926,America/Monterrey,MX
```

---

## 5. `airlines` — Reference dimension **[AGENT]**

Bundled CSV `aeroflux_parser/data/airlines.csv` (~5,800 carriers). Use to resolve
`carrier_icao` → carrier name.

| Column | Type | Notes |
|---|---|---|
| icao | str | e.g. "SWA" (join key) |
| iata | str | e.g. "WN" |
| name | str | "Southwest Airlines" |
| callsign | str | radio callsign, e.g. "SOUTHWEST" |
| country | str | "United States" |
| active | str | "Y" / "N" |

**Samples:**
```
icao,iata,name,callsign,country,active
SWA,WN,Southwest Airlines,SOUTHWEST,United States,Y
AAL,AA,American Airlines,AMERICAN,United States,Y
DAL,DL,Delta Air Lines,DELTA,United States,Y
```

---

## 6. Weather observations **[AGENT]**

The observation frame produced by the weather fetchers and joined into gold. Not
a standing table by default (fetched per run), but this is the shape if you
persist or query it. Times UTC. METAR fills wind/vis/ifr; NCEI adds temp/ceiling.

| Column | Type | Notes |
|---|---|---|
| station | str | airport ICAO, e.g. "KMCO" |
| obs_time | datetime | observation time (UTC) |
| wind_kt | float64 | wind speed, knots |
| vis_mi | float64 | visibility, statute miles |
| ifr | int8 | 1 if IFR/LIFR conditions else 0 |
| temp_c | float64 | temperature °C (NCEI) |
| ceiling_ft | float64 | cloud ceiling, feet (NCEI) |

**Samples:**
```json
{ "station":"KMCO","obs_time":"2026-07-31T19:53:00Z","wind_kt":11.0,"vis_mi":10.0,
  "ifr":0,"temp_c":31.0,"ceiling_ft":null }
{ "station":"KBNA","obs_time":"2026-07-31T21:53:00Z","wind_kt":6.0,"vis_mi":3.0,
  "ifr":1,"temp_c":24.0,"ceiling_ft":900.0 }
```

---

## 7. `adsb_airframe` — Rolling airframe store

Postgres table. Maps a live callsign to its airframe (hex/tail/type), rolling
48h. Used internally by fusion; handy if the agent needs "what aircraft is
flying callsign X right now".

| Column | Type | Notes |
|---|---|---|
| callsign | text (PK) | e.g. "AAL2033" |
| hex | text | ICAO 24-bit, e.g. "a1b2c3" |
| registration | text | tail, e.g. "N826AA" |
| aircraft_type | text | e.g. "A321" |
| last_seen | timestamptz | last sighting (UTC) |

**Samples:**
```json
{ "callsign":"AAL2033","hex":"a1b2c3","registration":"N826AA","aircraft_type":"A321","last_seen":"2026-07-31T21:04:00Z" }
{ "callsign":"DAL1150","hex":"a9f012","registration":"N901DL","aircraft_type":"B738","last_seen":"2026-07-31T21:03:30Z" }
```

---

## 8. `swim.raw_messages` — Bronze (raw source, audit/debug)

Postgres table. Raw SWIM XML + Kafka lineage. The reasoning layer normally won't
read this, but it's the source of truth for audit.

| Column | Type | Notes |
|---|---|---|
| id | bigserial (PK) | |
| kafka_topic / kafka_partition / kafka_offset | text / int / bigint | lineage / dedup key |
| swim_received_at | timestamptz | when received from SWIM |
| stored_at | timestamptz | when written (drives 48h retention) |
| solace_destination | text | source queue/topic |
| xml_root_tag | text | root element |
| flight_message_count | int | flights in this message |
| message_types | text[] | e.g. {trackInformation, departureInformation} |
| payload_size_bytes | int | |
| raw_xml | text | the raw SWIM TFMS XML |

**Sample (raw_xml abridged):**
```json
{ "id":90422133,"kafka_topic":"swim.raw.flight","kafka_partition":0,
  "kafka_offset":1783051,"swim_received_at":"2026-07-31T21:02:50Z",
  "stored_at":"2026-07-31T21:02:51Z","xml_root_tag":"MessageCollection",
  "flight_message_count":1,"message_types":["trackInformation"],
  "payload_size_bytes":2230,
  "raw_xml":"<fdm:fltdMessage acid=\"SWA2606\" airline=\"SWA\" depArpt=\"KMCO\" arrArpt=\"KBNA\" flightRef=\"153371795\" msgType=\"trackInformation\">…</fdm:fltdMessage>" }
```

---

## 9. BTS On-Time Performance — Historical training labels

Public CSV/Parquet (Bureau of Transportation Statistics). Not live; used to train
the model (has true gate times + tail numbers). Key columns we use:

| Column | Type | Notes |
|---|---|---|
| FL_DATE | date | flight date (local) |
| OP_UNIQUE_CARRIER | str | carrier (IATA-style) |
| OP_CARRIER_FL_NUM | int | flight number |
| TAIL_NUM | str | registration (airframe key for rotation) |
| ORIGIN / DEST | str | airport IATA |
| CRS_DEP_TIME / DEP_TIME | int (HHMM) | scheduled / actual departure (local) |
| DEP_DELAY | float | departure delay minutes |
| CRS_ARR_TIME / ARR_TIME | int (HHMM) | scheduled / actual arrival (local) |
| ARR_DELAY | float | arrival delay minutes (label source) |
| CANCELLED / DIVERTED | float | 1/0 flags |

**Sample:**
```
FL_DATE,OP_UNIQUE_CARRIER,OP_CARRIER_FL_NUM,TAIL_NUM,ORIGIN,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED
2026-07-30,WN,2606,N8712L,MCO,BNA,1545,1602,17,1744,1749,5,0.00
2026-07-30,AA,2033,N826AA,CLT,LGA,0800,0805,5,1000,0958,-2,0.00
```
> Note: BTS times are **local**; the pipeline converts them to UTC via the
> airport timezone so they align with live SWIM.

---

## 10. RAG document corpus / vector store — **[AGENT] — your layer to build**

Not produced by the pipeline; this is the reasoning layer's own store (pgvector).
Suggested schema so it integrates cleanly with the platform (Postgres + pgvector):

| Column | Type | Notes |
|---|---|---|
| doc_id | text | source document id |
| chunk_id | text | chunk within the document |
| source_uri | text | file path / URL |
| title | text | document title |
| section | text | heading / page |
| text | text | the chunk text (what's retrieved) |
| embedding | vector(N) | pgvector embedding (N = model dim) |
| metadata | jsonb | {source_type, published, tags, …} |
| ingested_at | timestamptz | |

**Sample:**
```json
{ "doc_id":"faa_swim_dict_2024","chunk_id":"faa_swim_dict_2024#p42",
  "source_uri":"corpus/faa/swim_data_dictionary.pdf","title":"SWIM Data Dictionary",
  "section":"TFMS Flight Data — fltdMessage","text":"The fltdMessage element carries…",
  "embedding":[0.0123,-0.0456, "…"],"metadata":{"source_type":"faa_manual","published":"2024"},
  "ingested_at":"2026-08-01T00:00:00Z" }
```

---

## Access patterns for the agent tools

- **flight_query** → `SELECT … FROM flight_instance WHERE callsign = :cs` (join
  `airports` for names/tz, `airlines` for carrier). Read-only.
- **prediction** → `SELECT … FROM predictions WHERE flight_key = :id` (or invoke
  the model); pair with `gold_features` + SHAP for the explanation.
- **weather** → weather observations for the flight's origin/dest around its
  scheduled times.
- **document_search** → pgvector similarity over the RAG corpus (§10).

All agent access is **read-only**; the analyst produces evidence-grounded briefs,
not operational actions.
