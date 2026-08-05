"""Live map — Plotly Scattergeo (renders reliably where pydeck went blank).

Draws each flight as a faint origin->destination line plus an origin point
colored by predicted delay risk, on a US map. No Mapbox token needed.
"""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data_access import load_flights

st.set_page_config(page_title="AeroFlux · Live Map", page_icon="🗺️", layout="wide")
st.title("🗺️ Live Network Map")
st.caption("Recent flights, origin -> destination, colored by predicted delay risk.")

df = load_flights().copy()
df = df.dropna(subset=["o_lat", "o_lon", "d_lat", "d_lon"])
df["flight_status"] = df["flight_status"].fillna("UNKNOWN")

f1, f2, f3 = st.columns([0.4, 0.3, 0.3])
all_status = ["ACTIVE", "PLANNED", "COMPLETED", "UNKNOWN"]
statuses = f1.multiselect("Status", all_status, default=all_status)
min_risk = f2.slider("Min delay risk", 0.0, 1.0, 0.0, 0.05)
carrier_opts = sorted(c for c in df["carrier_name"].dropna().unique() if isinstance(c, str))
carriers = f3.multiselect("Carrier", carrier_opts, default=[])

if statuses:
    df = df[df["flight_status"].isin(statuses)]
df = df[df["delay_prob"].fillna(0.0) >= min_risk]
if carriers:
    df = df[df["carrier_name"].isin(carriers)]
if df.empty:
    st.warning("No flights match the current filters."); st.stop()

lines_df = df.head(1500)   # cap lines for performance
lon_seg, lat_seg = [], []
for _, r in lines_df.iterrows():
    lon_seg += [r["o_lon"], r["d_lon"], None]
    lat_seg += [r["o_lat"], r["d_lat"], None]

fig = go.Figure()
fig.add_trace(go.Scattergeo(lon=lon_seg, lat=lat_seg, mode="lines",
    line=dict(width=0.5, color="rgba(120,140,180,0.35)"),
    hoverinfo="skip", showlegend=False))
fig.add_trace(go.Scattergeo(lon=df["o_lon"], lat=df["o_lat"], mode="markers",
    marker=dict(size=6, color=df["delay_prob"], colorscale="RdYlGn_r",
                cmin=0, cmax=1, showscale=True, colorbar=dict(title="risk"),
                line=dict(width=0)),
    text=df["callsign"] + " - " + df["carrier_name"].astype(str) + "<br>"
         + df["origin"].astype(str) + " -> " + df["destination"].astype(str)
         + "<br>risk: " + (df["delay_prob"] * 100).round(0).astype(str) + "%",
    hoverinfo="text", showlegend=False))
fig.update_layout(
    geo=dict(scope="north america", projection_type="albers usa",
             bgcolor="rgba(0,0,0,0)", landcolor="#111a2e", lakecolor="#0b1120",
             countrycolor="#33415c", subunitcolor="#22304a",
             showlakes=True, showland=True, showcountries=True),
    paper_bgcolor="rgba(0,0,0,0)", height=560, margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig, use_container_width=True)

l, m, r = st.columns(3)
l.metric("Flights shown", f"{len(df):,}")
m.metric("At-risk (>=50%)", f"{int((df['delay_prob'] >= 0.5).sum()):,}")
r.metric("Airborne", f"{int((df['flight_status'] == 'ACTIVE').sum()):,}")
st.caption("green low risk -> red high risk. Lines show origin->destination for recent flights.")
