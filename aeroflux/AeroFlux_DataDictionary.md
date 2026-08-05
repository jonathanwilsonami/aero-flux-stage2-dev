# AeroFlux — Feature Data Dictionaries & Parity Review

Covers the model's gold feature frame in both forms: **training** (from BTS) and
**live** (from SWIM/ADS-B). Same schema by construction; the differences are in
*missingness* and a few *sources*, documented below. All times are UTC.

> `sched_dep_dow` = ISO day-of-week of the scheduled departure, **1 = Monday …
> 7 = Sunday** (e.g. 2015-01-01 was a Thursday → `4`). `sched_dep_hour` is the
> UTC hour 0–23. `is_weekend` = 1 if dow ∈ {6,7}.

---

## 1. Training data dictionary (BTS gold)

| Field | Type | Description | Example | Units/Format | Nullable | Source / Derivation |
|---|---|---|---|---|---|---|
| flight_key | str | Unique flight id | `2015-01-02_AA1659` | date_flightno | no | BTS date + carrier + flight no |
| sched_dep_hour | int8 | Scheduled departure hour | `13` | 0–23 UTC | no* | `from_bts` CRSDepTime → UTC |
| sched_dep_dow | int8 | Day of week | `4` | 1=Mon…7=Sun | no* | derived from sched_dep |
| sched_dep_month | int8 | Month | `1` | 1–12 | no* | derived from sched_dep |
| is_weekend | int8 | Weekend flag | `0` | 0/1 | no* | dow ∈ {6,7} |
| sched_block_min | int64 | Scheduled gate-to-gate time | `180` | minutes | no* | sched_arr − sched_dep (UTC) |
| prev_leg_arr_delay_min | int64 | Inbound aircraft's arrival delay | `27` | minutes | yes→0 | prior leg of same tail |
| turnaround_buffer_min | int64 | Scheduled ground time before this leg | `41` | minutes | yes→0 | sched_dep − prev leg sched_arr |
| legs_into_day | int64 | 0-based leg index for the airframe | `3` | count | yes→0 | rotation order by tail/day |
| inbound_resolved | int8 | Was the inbound leg found? | `1` | 0/1 | no | 1 if tail rotation linked |
| origin_dep_demand | uint32 | Departures from origin in window | `18` | count | yes→0 | rolling count |
| origin_recent_dep_delay | f64 | Mean recent dep delay at origin | `12.5` | minutes | yes | rolling mean |
| dest_arr_demand | uint32 | Arrivals into dest in window | `9` | count | yes→0 | rolling count |
| dest_recent_arr_delay | f64 | Mean recent arr delay at dest | `6.0` | minutes | yes | rolling mean |
| origin_wx_wind_kt | f64 | Origin wind speed | `15.9` | knots | yes | NCEI cache (WND) |
| origin_wx_vis_mi | f64 | Origin visibility | *(null)* | miles | yes | not in NCEI TMP/WND/CIG |
| origin_wx_ifr | int8 | Origin IFR conditions | `0` | 0/1 | yes | ceiling < 1000 ft |
| origin_wx_temp_c | f64 | Origin temperature | `-9.4` | °C | yes | NCEI cache (TMP) |
| origin_wx_ceiling_ft | f64 | Origin cloud ceiling | `498` | feet | yes | NCEI cache (CIG); null=clear |
| dest_wx_* | — | Same five, destination | — | — | yes | as-of scheduled departure |
| dep_delay_min | int64 | Actual departure delay | `17` | minutes | yes | diagnostic only (not a feature) |
| arr_delay_min | int64 | Actual arrival delay | `22` | minutes | yes | label source |
| label_delayed | int8 | Arrival delayed ≥ 15 min | `1` | 0/1 | yes | `arr_delay_min ≥ 15` |

\* Structural: if null, the flight has no schedule and is dropped by `scoreable()`.
"yes→0" = null filled with 0 by the fill policy (absence genuinely = 0).

---

## 2. Live data dictionary (SWIM/ADS-B gold)

Same fields; differences are **missingness** and **weather source** (live METAR).
No `arr_delay_min`/`label_delayed` (flight hasn't arrived).

| Field | Type | Description | Example | Nullable | Live source / note |
|---|---|---|---|---|---|
| flight_key | str | Flight id (GUFI or flight_ref) | `KC817311ru` | no | silver `flight_instance_id` |
| sched_dep_hour…is_weekend | int8 | as training | `23,7,8,1` | ~38% present | SWIM schedule (often absent) |
| sched_block_min | int64 | as training | `79` | ~34% | SWIM sched times |
| prev_leg_arr_delay_min | int64 | inbound delay | *(null→0)* | sparse | needs resolved airframe (ADS-B hex) |
| turnaround_buffer_min | int64 | ground buffer | *(null→0)* | sparse | needs resolved airframe |
| legs_into_day | int64 | leg index | *(null→0)* | sparse | needs resolved airframe |
| inbound_resolved | int8 | rotation known? | `0` | no | **mostly 0 live** (~38% hex) |
| origin_dep_demand / dest_arr_demand | uint32 | demand | `6 / 36` | present when scheduled | rolling count |
| origin_recent_dep_delay / dest_recent_arr_delay | f64 | recent delay | *(often null)* | sparse | needs recent actuals |
| origin_wx_wind_kt / dest_wx_wind_kt | f64 | wind | `14 / 13` | present | live METAR |
| origin_wx_ifr / dest_wx_ifr | int8 | IFR | `0 / 0` | present | live METAR flight-category |
| origin_wx_vis_mi | f64 | visibility | *(null)* | mostly null | METAR sometimes |
| origin_wx_temp_c / ceiling_ft | f64 | temp/ceiling | *(null)* | **null live** | METAR path doesn't fill these |
| dep_delay_min | int64 | dep delay | `16` | only if departed | not a feature |

---

## 3. Side-by-side parity comparison

Whether each **model feature** is available and comparable in live serving.

| Feature | Train type | Train miss | Live miss | In model? | Parity notes |
|---|---|---|---|---|---|
| sched_dep_hour/dow/month, is_weekend | int8 | ~0% | ~62% | ✅ | same derivation; live sparse but identical when present |
| sched_block_min | int64 | ~0% | ~66% | ✅ | same |
| prev_leg_arr_delay_min | int64 | low | high | ✅ | **filled 0** both sides; `inbound_resolved` flags real vs unknown |
| turnaround_buffer_min | int64 | low | high | ✅ | filled 0 both sides |
| legs_into_day | int64 | ~0% | high | ✅ | filled 0 both sides |
| inbound_resolved | int8 | 0% | 0% | ✅ | **the key flag**: ~1 often in train, mostly 0 live |
| propagation_pressure_min | int64 | derived | derived | ✅ | new; `max(0, prev_leg − turnaround)`; 0 when no inbound |
| origin/dest_dep/arr_demand | uint32 | ~0% | present-if-scheduled | ✅ | filled 0 |
| origin/dest_recent_*_delay | f64 | low | high | ✅ | **left null** both sides (missing≠0); XGBoost routes NaN |
| origin/dest_wx_wind_kt | f64 | ~2% | low | ✅ | both sources have wind |
| origin/dest_wx_ifr | int8 | ~0% | low | ✅ | both have IFR |
| origin/dest_wx_temp_c | f64 | ~0% | **~100%** | ⚠️ off by default | **parity gap**: dense train, null live → excluded (or opt-in + indicator) |
| origin/dest_wx_ceiling_ft | f64 | ~55% | **~100%** | ⚠️ off by default | parity gap |
| origin/dest_wx_vis_mi | f64 | **100%** | ~100% | ⚠️ off by default | absent both sides; not useful |
| dep_delay_min | int64 | ~0% | if departed | ❌ excluded | unknown pre-departure → leakage if used |
| arr_delay_min / label_delayed | int64/int8 | — | — | ❌ label | training only |

Legend: ✅ in model, parity-safe · ⚠️ excluded by default (parity gap) · ❌ never a feature.

---

## 4. Missing-value policy (encoded in `feature_prep.py`)

- **Fill 0 (absence == 0):** `prev_leg_arr_delay_min`, `turnaround_buffer_min`,
  `legs_into_day`, `origin_dep_demand`, `dest_arr_demand`. Rotation nulls mean
  "no inbound leg"; `inbound_resolved` + `legs_into_day` let the model tell a
  real 0 from an unknown.
- **Leave null (missing != 0):** all weather, `origin/dest_recent_*_delay`.
  Filling would inject false readings (0 °C, 0 kt). XGBoost handles NaN natively.
- **Structural (drop if null):** the `sched_dep_*` group — no schedule means the
  flight can't be scored; `scoreable()` filters it in **both** train and live.
- **Never a feature:** `dep_delay_min` (unknown pre-departure), `arr_delay_min`
  and `label_delayed` (labels).

The same `prepare()` runs on both frames, so the policy can't drift.

---

## 5. Propagation feature (2-week scope)

`propagation_pressure_min = max(0, prev_leg_arr_delay_min − turnaround_buffer_min)`
— the inbound delay that eats the scheduled turnaround. Simple, interpretable,
and computable identically in training and live. Zero when there's slack or no
inbound leg; active only when rotation is resolved (`inbound_resolved = 1`).

Later (out of scope now): airframe-graph cascade simulation across the day,
queuing/Bayesian propagation, or a graph neural net over the rotation network.
