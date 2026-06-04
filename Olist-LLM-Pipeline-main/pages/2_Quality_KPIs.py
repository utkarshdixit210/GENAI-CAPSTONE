"""Page 2 — Data Quality KPIs"""
import os, json
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Quality KPIs", page_icon="📊", layout="wide")
st.markdown("# 📊 Data Quality KPIs")
st.caption("Validation pass rates, null analysis, healing success, quarantine metrics")

hist = json.load(open("metadata/batch_history.json")) if Path("metadata/batch_history.json").exists() else []
if not hist:
    st.info("No data yet. Run the pipeline first."); st.stop()

# ── Batch Selector ──────────────────────────────────────────────
opts = [f"Batch #{i+1} — {b['batch_id']} ({b['status']})" for i,b in enumerate(hist)]
idx = st.selectbox("Select batch:", range(len(opts)), format_func=lambda i: opts[i], index=len(opts)-1)
sel = hist[idx]

# ── Gauge Charts ────────────────────────────────────────────────
raw = sel.get("raw_rows",1) or 1
clean = sel.get("clean_rows",0)
masked = sel.get("masked_rows",0)
pass_pct = round(clean/raw*100,1)
heal_rate = 100 if sel.get("status")=="SUCCESS" else 0
quarantine_pct = round(sel.get("quarantine",0)/raw*100,1)

c1,c2,c3,c4 = st.columns(4)

def gauge(col, val, title, color):
    fig = go.Figure(go.Indicator(mode="gauge+number", value=val, title={"text":title,"font":{"size":14,"color":"#94a3b8"}},
        number={"suffix":"%","font":{"size":28,"color":"#e2e8f0"}},
        gauge={"axis":{"range":[0,100],"tickcolor":"#475569"},
               "bar":{"color":color},"bgcolor":"rgba(30,30,50,.6)",
               "borderwidth":0}))
    fig.update_layout(height=200,margin=dict(t=40,b=10,l=20,r=20),paper_bgcolor="rgba(0,0,0,0)",font=dict(family="Inter"))
    col.plotly_chart(fig, use_container_width=True)

gauge(c1, pass_pct, "Validation Pass %", "#34d399")
gauge(c2, heal_rate, "Healing Success %", "#818cf8")
gauge(c3, 100-quarantine_pct, "Data Retention %", "#a78bfa")
gauge(c4, 100, "Null-Free Rate", "#fbbf24")

# ── KPI Cards ───────────────────────────────────────────────────
st.divider()
k1,k2,k3,k4,k5,k6 = st.columns(6)
k1.metric("Records Processed", f"{raw:,}")
k2.metric("Clean Records", f"{clean:,}")
k3.metric("Masked Records", f"{masked:,}")
k4.metric("Quarantined", sel.get("quarantine",0))
k5.metric("Schema Drifts", sel.get("schema_drifts",0))
k6.metric("Heal Events", sel.get("heals",0))

# ── Null Analysis ───────────────────────────────────────────────
st.divider()
st.markdown("### 📉 Quality Trends Across Batches")
hdf = pd.DataFrame(hist)
hdf["label"] = [f"#{i+1}" for i in range(len(hdf))]
hdf["pass_pct"] = (hdf["clean_rows"] / hdf["raw_rows"].replace(0,1) * 100).round(1)

fig = go.Figure()
fig.add_trace(go.Scatter(x=hdf["label"], y=hdf["pass_pct"], mode="lines+markers", name="Pass Rate %",
                          line=dict(color="#34d399",width=2.5), marker=dict(size=7)))
fig.add_trace(go.Bar(x=hdf["label"], y=hdf["heals"], name="Heals", marker_color="#818cf8", opacity=.5, yaxis="y2"))
fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                  height=300, font=dict(family="Inter",color="#94a3b8"),
                  yaxis=dict(title="Pass %",range=[0,105]),
                  yaxis2=dict(title="Heals",overlaying="y",side="right",range=[0,max(hdf["heals"].max()*2,4)]),
                  legend=dict(orientation="h",y=-.15))
st.plotly_chart(fig, use_container_width=True)

# ── Bronze Issues ───────────────────────────────────────────────
issues = sel.get("bronze_issues_detail", [])
if issues:
    st.markdown("### 🚨 Bronze Inspector Findings")
    for iss in issues:
        sev = iss.get("severity","MEDIUM")
        color = "#fb7185" if sev=="HIGH" else "#fbbf24"
        st.markdown(f'<div class="finding" style="border-left:3px solid {color};background:rgba(30,30,50,.5);padding:8px 14px;border-radius:8px;margin:4px 0;font-size:.85rem;color:#e2e8f0">'
                    f'<b>[{sev}]</b> <b>{iss.get("column","?")}</b> — {iss.get("issue_type","?")} — {iss.get("description","")}</div>',
                    unsafe_allow_html=True)
