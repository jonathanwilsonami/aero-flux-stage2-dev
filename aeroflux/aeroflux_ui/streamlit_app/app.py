"""AeroFlux — demo control tower (Home).

Run:  streamlit run app.py
Live data:  set AEROFLUX_DSN=postgresql://... (else realistic sample data)
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from data_access import load_flights, kpis, is_live

st.set_page_config(page_title="AeroFlux", page_icon="✈️", layout="wide")

# --- header ----------------------------------------------------------------
left, right = st.columns([0.75, 0.25])
with left:
    st.title("✈️ AeroFlux — Delay Intelligence")
    st.caption("Real-time flight-delay prediction over FAA SWIM · ADS-B · Weather")
with right:
    k = kpis()
    badge = "🟢 LIVE" if k["mode"] == "LIVE" else "🟡 SAMPLE"
    st.markdown(f"### {badge}")
    st.caption("Set `AEROFLUX_DSN` for live data" if k["mode"] == "SAMPLE" else "Connected to Postgres")

st.divider()

# --- KPI row ---------------------------------------------------------------
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Flights tracked", f"{k['total']:,}")
c2.metric("Airborne now", f"{k['active']:,}")
c3.metric("Airframe (hex) coverage", f"{k['hex_cov']*100:.0f}%")
c4.metric("At-risk (≥50%)", f"{k['at_risk']:,}")
c5.metric("Mean delay risk", f"{k['avg_prob']*100:.0f}%")

st.divider()

df = load_flights()

# --- charts ----------------------------------------------------------------
a, b = st.columns([0.55, 0.45])

with a:
    st.subheader("Delay-risk distribution")
    fig = px.histogram(df, x="delay_prob", nbins=25,
                       color_discrete_sequence=["#22d3ee"])
    fig.update_layout(
        template="plotly_dark", height=320, margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        xaxis_title="P(delay ≥ 15 min)", yaxis_title="flights")
    fig.add_vline(x=0.5, line_dash="dash", line_color="#f59e0b")
    st.plotly_chart(fig, use_container_width=True)

with b:
    st.subheader("Busiest routes")
    routes = (df.assign(route=df["origin"] + " → " + df["destination"])
                .groupby("route").agg(flights=("route", "size"),
                                      risk=("delay_prob", "mean"))
                .sort_values("flights", ascending=False).head(10).reset_index())
    fig2 = px.bar(routes, x="flights", y="route", orientation="h",
                  color="risk", color_continuous_scale="turbo",
                  range_color=[0, 1])
    fig2.update_layout(
        template="plotly_dark", height=320, margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        yaxis=dict(autorange="reversed"), coloraxis_colorbar_title="risk")
    st.plotly_chart(fig2, use_container_width=True)

# --- carrier breakdown -----------------------------------------------------
st.subheader("By carrier")
car = (df.groupby("carrier_name").agg(flights=("carrier_name", "size"),
                                      at_risk=("delay_prob", lambda s: (s >= 0.5).sum()))
         .sort_values("flights", ascending=False).reset_index())
st.dataframe(car, use_container_width=True, hide_index=True)

st.divider()
st.caption("Use the pages in the sidebar → Live Map · Analyst · Live Inference")
