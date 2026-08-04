"""Live map — flights as a geo network, arcs colored by delay risk (deck.gl/WebGL)."""
from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st

from data_access import load_flights, HUBS

st.set_page_config(page_title="AeroFlux · Live Map", page_icon="🗺️", layout="wide")
st.title("🗺️ Live Network Map")
st.caption("Each arc is a flight, origin → destination, colored by predicted delay risk.")

df = load_flights().copy()

# --- controls --------------------------------------------------------------
f1, f2, f3 = st.columns([0.4, 0.3, 0.3])
statuses = f1.multiselect("Status", ["ACTIVE", "PLANNED", "COMPLETED"],
                          default=["ACTIVE", "PLANNED"])
min_risk = f2.slider("Min delay risk", 0.0, 1.0, 0.0, 0.05)
carriers = f3.multiselect("Carrier", sorted(df["carrier_name"].unique().tolist()),
                          default=[])

if statuses:
    df = df[df["flight_status"].isin(statuses)]
df = df[df["delay_prob"] >= min_risk]
if carriers:
    df = df[df["carrier_name"].isin(carriers)]

# --- color by risk: green -> amber -> red ----------------------------------
def risk_color(p: float) -> list[int]:
    p = max(0.0, min(1.0, float(p)))
    if p < 0.5:
        r, g = int(2 * p * 245), 200
    else:
        r, g = 245, int((1 - (p - 0.5) * 2) * 200)
    return [r, g, 90, 170]

df["src_color"] = df["delay_prob"].map(risk_color)
df["tgt_color"] = df["delay_prob"].map(lambda p: risk_color(p)[:3] + [220])
df["width"] = 1 + df["delay_prob"] * 4

airports = pd.DataFrame(
    [{"name": n, "iata": i, "lat": la, "lon": lo} for _, i, n, la, lo in HUBS])

arc = pdk.Layer(
    "ArcLayer", data=df,
    get_source_position=["o_lon", "o_lat"],
    get_target_position=["d_lon", "d_lat"],
    get_source_color="src_color", get_target_color="tgt_color",
    get_width="width", pickable=True, auto_highlight=True,
)
dots = pdk.Layer(
    "ScatterplotLayer", data=airports,
    get_position=["lon", "lat"], get_fill_color=[34, 211, 238, 200],
    get_radius=26000, pickable=True, radius_min_pixels=3,
)
labels = pdk.Layer(
    "TextLayer", data=airports, get_position=["lon", "lat"],
    get_text="iata", get_size=12, get_color=[226, 232, 240, 220],
    get_alignment_baseline="'bottom'",
)

view = pdk.ViewState(latitude=39.5, longitude=-98, zoom=3.3, pitch=45, bearing=0)
deck = pdk.Deck(
    layers=[arc, dots, labels], initial_view_state=view,
    map_style="dark_no_labels",
    tooltip={"html": "<b>{callsign}</b> · {carrier_name}<br/>"
                     "{origin} → {destination}<br/>risk: {delay_prob}",
             "style": {"backgroundColor": "#111a2e", "color": "#e2e8f0"}},
)

st.pydeck_chart(deck, use_container_width=True)

l, m, r = st.columns(3)
l.metric("Flights shown", f"{len(df):,}")
m.metric("At-risk (≥50%)", f"{int((df['delay_prob']>=0.5).sum()):,}")
r.metric("Airborne", f"{int((df['flight_status']=='ACTIVE').sum()):,}")

st.caption("🟢 low risk → 🟡 → 🔴 high risk · thicker arc = higher predicted delay")
