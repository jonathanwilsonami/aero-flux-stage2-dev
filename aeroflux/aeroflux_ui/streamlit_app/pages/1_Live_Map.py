"""Live map — Plotly Scattergeo (renders reliably where pydeck went blank).

Draws each flight as a faint origin->destination line plus an origin point
colored by predicted delay risk, on a US map. No Mapbox token needed.
"""
from __future__ import annotations

import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data_access import load_flights, current_flights, RECENT_HOURS

# The real constraint here is the browser's Plotly render, not the box or the
# DynamoDB read (both measured fast well past this size) — thousands of geo
# arcs get sluggish client-side regardless of server headroom. Metrics/KPIs
# below use the full (post-filter) tracked set; only the map trace itself is
# capped, and capped honestly (labeled), not silently.
MAP_LIMIT = int(os.getenv("MAP_LIMIT", "1000"))

st.set_page_config(page_title="AeroFlux · Live Map", page_icon="🗺️", layout="wide")
st.title("🗺️ Live Network Map")
st.caption("Recent flights, origin -> destination, colored by predicted delay risk.")

tracked_n = len(load_flights())
df = current_flights().copy()  # airborne now, or near sched/actual departure-arrival
df = df.dropna(subset=["o_lat", "o_lon", "d_lat", "d_lon"])
df["flight_status"] = df["flight_status"].fillna("UNKNOWN")
st.caption(f"Tracked (48h window): {tracked_n:,} · shown here: flights currently active or "
           f"within {RECENT_HOURS:.0f}h of scheduled/actual departure-arrival "
           f"(tune with `RECENT_HOURS`).")

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

map_df = df.head(MAP_LIMIT)   # only the render is capped — metrics below use full `df`
lon_seg, lat_seg = [], []
for _, r in map_df.iterrows():
    lon_seg += [r["o_lon"], r["d_lon"], None]
    lat_seg += [r["o_lat"], r["d_lat"], None]

fig = go.Figure()
fig.add_trace(go.Scattergeo(lon=lon_seg, lat=lat_seg, mode="lines",
    line=dict(width=0.5, color="rgba(120,140,180,0.35)"),
    hoverinfo="skip", showlegend=False))
fig.add_trace(go.Scattergeo(lon=map_df["o_lon"], lat=map_df["o_lat"], mode="markers",
    marker=dict(size=6, color=map_df["delay_prob"], colorscale="RdYlGn_r",
                cmin=0, cmax=1, showscale=True, colorbar=dict(title="risk"),
                line=dict(width=0)),
    text=map_df["callsign"] + " - " + map_df["carrier_name"].astype(str) + "<br>"
         + map_df["origin"].astype(str) + " -> " + map_df["destination"].astype(str)
         + "<br>risk: " + (map_df["delay_prob"] * 100).round(0).astype(str) + "%",
    hoverinfo="text", showlegend=False))
fig.update_layout(
    geo=dict(scope="north america", projection_type="albers usa",
             bgcolor="rgba(0,0,0,0)", landcolor="#111a2e", lakecolor="#0b1120",
             countrycolor="#33415c", subunitcolor="#22304a",
             showlakes=True, showland=True, showcountries=True),
    paper_bgcolor="rgba(0,0,0,0)", height=560, margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(fig, use_container_width=True)

if len(df) > len(map_df):
    st.caption(f"🗺️ Map showing {len(map_df):,} of {len(df):,} tracked flights "
               f"(a representative subset — raise MAP_LIMIT to show more; "
               f"metrics below reflect all {len(df):,}).")

l, m, r = st.columns(3)
l.metric("Current flights", f"{len(df):,}")
m.metric("At-risk (>=50%)", f"{int((df['delay_prob'] >= 0.5).sum()):,}")
r.metric("Airborne", f"{int((df['flight_status'] == 'ACTIVE').sum()):,}")
st.caption("green low risk -> red high risk. Lines show origin->destination for recent flights.")
