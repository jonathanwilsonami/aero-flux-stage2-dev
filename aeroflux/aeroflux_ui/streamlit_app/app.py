"""AeroFlux — demo control tower (Home).

Run:  streamlit run app.py
Live data:  set AEROFLUX_DSN=postgresql://... (else realistic sample data)
"""
from __future__ import annotations

import os

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data_access import (
    load_flights, kpis, is_live,
    RISK_TIER_COLORS, live_overview_metrics, carrier_risk_breakdown,
    risk_distribution, live_overview_timeseries,
)

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
    if k["mode"] == "SAMPLE":
        st.caption("Set `AEROFLUX_DSN` (Postgres) or `STATE_BACKEND=dynamodb` for live data")
    else:
        # was hardcoded to "Connected to Postgres" regardless of backend —
        # wrong once STATE_BACKEND=dynamodb became a real live path too.
        backend = os.getenv("STATE_BACKEND", "postgres").lower()
        label = "DynamoDB (cloud)" if backend == "dynamodb" else "Postgres (local)"
        st.caption(f"Connected to {label}")

st.divider()

# --- Live Overview (additive hero section) ----------------------------------
# Everything below reads through data_access's live_overview_* helpers, each
# @st.cache_data(ttl=600) and each sourced from load_flights() -- the same
# full tracked-population call the KPI row/charts further down already make
# -- or from the local out/predictions/ snapshot dir for the time series. No
# new DynamoDB/S3 reads: this section rides entirely on data already being
# pulled for the rest of the page.
st.subheader("📡 Live Network Overview")
st.caption("Full tracked population (not just the current map view) · refreshes ~every 10 min")

try:
    ov = live_overview_metrics()
    with st.container(border=True):
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("✈️ Flights tracked", f"{ov['total']:,}")
        m2.metric("🛫 Airborne now", f"{ov['active']:,}")
        m3.metric("⚠️ High-risk", f"{ov['pct_high_risk']:.1f}%")
        m4.metric("📈 Avg delay probability", f"{ov['avg_prob']*100:.1f}%")

    st.markdown("##### Flights by carrier — risk breakdown")
    car_risk = carrier_risk_breakdown(top_n=15)
    if car_risk.empty:
        st.info("No live flight data yet.")
    else:
        order = (car_risk.groupby("carrier_name")["flights"].sum()
                 .sort_values(ascending=False).index.tolist())
        fig_car = px.bar(
            car_risk, x="flights", y="carrier_name", color="risk_tier",
            orientation="h", color_discrete_map=RISK_TIER_COLORS,
            category_orders={"carrier_name": order, "risk_tier": ["Low", "Medium", "High"]},
        )
        fig_car.update_layout(
            template="plotly_dark", height=max(320, 24 * len(order)),
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(autorange="reversed", title=""), xaxis_title="flights",
            legend_title="risk tier",
        )
        st.plotly_chart(fig_car, use_container_width=True)

    st.markdown("##### Delay-risk distribution")
    prob = risk_distribution()
    if prob.empty:
        st.info("No live flight data yet.")
    else:
        counts, edges = np.histogram(prob, bins=20, range=(0, 1))
        centers = (edges[:-1] + edges[1:]) / 2
        fig_hist = go.Figure(go.Bar(
            x=centers, y=counts, width=(edges[1] - edges[0]) * 0.95,
            marker=dict(color=centers, colorscale=[[0, "#22c55e"], [0.5, "#f59e0b"], [1, "#ef4444"]],
                        cmin=0, cmax=1),
        ))
        fig_hist.update_layout(
            template="plotly_dark", height=420, margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="P(delay ≥ 15 min)", yaxis_title="flights",
        )
        fig_hist.add_vline(x=0.5, line_dash="dash", line_color="#94a3b8")
        st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown("##### Delay risk over time")
    ts = live_overview_timeseries(hours=24)
    if ts.empty:
        st.info("No prediction snapshot history yet — this fills in as out/predictions/ archives accumulate.")
    else:
        fig_ts = go.Figure()
        fig_ts.add_trace(go.Scatter(
            x=ts["hour"], y=ts["avg_delay_prob"], name="avg delay probability",
            mode="lines", fill="tozeroy", line=dict(color="#f59e0b"), yaxis="y1",
        ))
        fig_ts.add_trace(go.Scatter(
            x=ts["hour"], y=ts["flight_count"], name="flights scored",
            mode="lines", line=dict(color="#38bdf8", width=1.5, dash="dot"), yaxis="y2",
        ))
        fig_ts.update_layout(
            template="plotly_dark", height=340, margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            xaxis_title="hour",
            yaxis=dict(title="avg delay probability", range=[0, 1]),
            yaxis2=dict(title="flights scored", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", y=1.15),
        )
        st.plotly_chart(fig_ts, use_container_width=True)
except Exception as e:  # this section is additive polish -- never break the page
    st.warning(f"Live overview section unavailable ({e}).")

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
