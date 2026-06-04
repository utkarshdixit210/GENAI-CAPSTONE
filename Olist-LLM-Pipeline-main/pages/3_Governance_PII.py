"""Page 3 — Governance & PII Monitoring"""
import os, json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Governance & PII", page_icon="🔐", layout="wide")
st.markdown("# 🔐 Governance & PII Monitoring")
st.caption("PII detection, masking audit, data classification, governance compliance")

hist = json.load(open("metadata/batch_history.json")) if Path("metadata/batch_history.json").exists() else []
if not hist: st.info("No data yet."); st.stop()

opts = [f"Batch #{i+1} — {b['batch_id']} ({b['status']})" for i,b in enumerate(hist)]
idx = st.selectbox("Select batch:", range(len(opts)), format_func=lambda i: opts[i], index=len(opts)-1)
sel = hist[idx]

# ── PII Summary ─────────────────────────────────────────────────
masking = sel.get("masking_log", [])
high = [m for m in masking if m.get("level")=="HIGH"]
med = [m for m in masking if m.get("level")=="MEDIUM"]
low = [m for m in masking if m.get("level")=="LOW"]
none = [m for m in masking if m.get("level")=="NONE"]

c1,c2,c3,c4 = st.columns(4)
c1.metric("🔴 HIGH Risk", len(high))
c2.metric("🟡 MEDIUM Risk", len(med))
c3.metric("🟢 LOW Risk", len(low))
c4.metric("⚪ No PII", len(none))

# ── Classification Pie Chart ────────────────────────────────────
st.divider()
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 📊 Data Classification")
    class_data = {"RESTRICTED (HIGH PII)": len(high), "CONFIDENTIAL (MEDIUM)": len(med),
                  "INTERNAL (LOW)": len(low), "PUBLIC (NONE)": len(none)}
    fig = px.pie(values=list(class_data.values()), names=list(class_data.keys()),
                 color_discrete_sequence=["#fb7185","#fbbf24","#34d399","#818cf8"],
                 hole=0.45)
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=300,
                      font=dict(family="Inter", color="#94a3b8"), showlegend=True,
                      legend=dict(font=dict(size=11)))
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### 🔒 Masking Actions")
    if masking:
        actions = {}
        for m in masking:
            a = m.get("action","NONE")
            actions[a] = actions.get(a,0)+1
        fig2 = px.bar(x=list(actions.keys()), y=list(actions.values()),
                       color=list(actions.keys()),
                       color_discrete_map={"SHA256":"#fb7185","PARTIAL_MASK":"#fbbf24","NONE":"#818cf8"},
                       labels={"x":"Action","y":"Columns"})
        fig2.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=300,
                           font=dict(family="Inter",color="#94a3b8"), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

# ── PII Detail Table ────────────────────────────────────────────
st.divider()
st.markdown("### 📋 PII Column Details")
if masking:
    rows = []
    for m in masking:
        act = m.get("action","NONE")
        icon = "🔴" if act=="SHA256" else ("🟡" if act=="PARTIAL_MASK" else "✅")
        rows.append({
            "Column": m.get("column","?"),
            "PII Level": m.get("level","?"),
            "Action": f"{icon} {act}",
            "Reason": m.get("reason",""),
            "Detection": "LLM (Groq)",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ── Governance Compliance ───────────────────────────────────────
st.divider()
st.markdown("### ✅ Governance Compliance")
total_cols = len(masking) if masking else 1
pii_detected = len(high) + len(med)
masked_count = len([m for m in masking if m.get("action") in ("SHA256","PARTIAL_MASK")])
compliance = round(masked_count/max(pii_detected,1)*100,1)

g1,g2,g3 = st.columns(3)
g1.metric("PII Columns Detected", pii_detected)
g2.metric("Columns Masked", masked_count)
g3.metric("Compliance Rate", f"{compliance}%")
st.progress(compliance/100, text=f"PII Masking Compliance: {compliance}%")
