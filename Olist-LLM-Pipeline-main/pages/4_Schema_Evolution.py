"""Page 4 — Schema Evolution Tracker"""
import json
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Schema Evolution", page_icon="🌊", layout="wide")
st.markdown("# 🌊 Schema Evolution Tracker")
st.caption("Track schema changes, column additions, type modifications across batches")

hist = json.load(open("metadata/batch_history.json")) if Path("metadata/batch_history.json").exists() else []
if not hist: st.info("No data yet."); st.stop()

# ── Batch Selector ──────────────────────────────────────────────
opts = [f"Batch #{i+1} — {b['batch_id']} ({b['status']})" for i,b in enumerate(hist)]
idx = st.selectbox("Select batch:", range(len(opts)), format_func=lambda i: opts[i], index=len(opts)-1)
sel = hist[idx]
drifts = sel.get("schema_drift_events", [])

# ── Summary ─────────────────────────────────────────────────────
high_d = [d for d in drifts if d.get("severity")=="HIGH"]
med_d = [d for d in drifts if d.get("severity")=="MEDIUM"]

c1,c2,c3 = st.columns(3)
c1.metric("Total Drift Events", len(drifts))
c2.metric("🔴 HIGH Severity", len(high_d))
c3.metric("🟡 MEDIUM Severity", len(med_d))

# ── Drift Events Table ──────────────────────────────────────────
st.divider()
if drifts:
    st.markdown("### 📋 Drift Events Detail")
    rows = []
    for d in drifts:
        sev = d.get("severity","?")
        rows.append({
            "Severity": f"{'🔴' if sev=='HIGH' else '🟡'} {sev}",
            "Drift Type": d.get("drift_type","?"),
            "Column": d.get("column_name","?"),
            "Expected": d.get("expected_type", "—"),
            "Actual": d.get("actual_type", "—"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── Drift Type Distribution ─────────────────────────────────
    st.markdown("### 📊 Drift Type Distribution")
    types = {}
    for d in drifts:
        t = d.get("drift_type","?")
        types[t] = types.get(t,0)+1
    fig = px.bar(x=list(types.keys()), y=list(types.values()),
                  color=list(types.keys()),
                  color_discrete_sequence=["#fb7185","#fbbf24","#818cf8","#34d399"],
                  labels={"x":"Drift Type","y":"Count"})
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      height=280, font=dict(family="Inter",color="#94a3b8"), showlegend=False)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.success("No schema drift detected in this batch.")

# ── Drift Trend ─────────────────────────────────────────────────
st.divider()
st.markdown("### 📈 Schema Drift Trend Across Batches")
hdf = pd.DataFrame(hist)
hdf["label"] = [f"#{i+1}" for i in range(len(hdf))]
fig2 = px.bar(hdf, x="label", y="schema_drifts", color="schema_drifts",
              color_continuous_scale=["#818cf8","#fb7185"], labels={"label":"Batch","schema_drifts":"Drift Events"})
fig2.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                   height=280, font=dict(family="Inter",color="#94a3b8"), coloraxis_showscale=False)
st.plotly_chart(fig2, use_container_width=True)
