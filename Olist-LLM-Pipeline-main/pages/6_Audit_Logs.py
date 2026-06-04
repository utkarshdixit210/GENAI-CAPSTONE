"""Page 6 — Audit & Logs"""
import json, os
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Audit & Logs", page_icon="📋", layout="wide")
st.markdown("# 📋 Audit & Logs Panel")
st.caption("Run history, heal logs, error summaries, full observability")

hist = json.load(open("metadata/batch_history.json")) if Path("metadata/batch_history.json").exists() else []
if not hist: st.info("No data yet."); st.stop()

# ── Run History Table ───────────────────────────────────────────
st.markdown("### 📦 Run History")
hdf = pd.DataFrame(hist)
display_cols = [c for c in ["batch_id","timestamp","dataset","status","raw_rows","clean_rows","masked_rows","heals","schema_drifts","bronze_issues","duration_s"] if c in hdf.columns]
st.dataframe(hdf[display_cols], use_container_width=True, hide_index=True, height=300)

# ── Status Distribution ────────────────────────────────────────
st.divider()
c1,c2 = st.columns(2)

with c1:
    st.markdown("### ✅ Status Distribution")
    status_counts = hdf["status"].value_counts()
    fig = px.pie(values=status_counts.values, names=status_counts.index,
                 color=status_counts.index,
                 color_discrete_map={"SUCCESS":"#34d399","ESCALATED":"#fb7185"},
                 hole=0.45)
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", height=280,
                      font=dict(family="Inter",color="#94a3b8"))
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.markdown("### ⏱️ Duration Trend")
    hdf["label"] = [f"#{i+1}" for i in range(len(hdf))]
    colors = ["#34d399" if s=="SUCCESS" else "#fb7185" for s in hdf["status"]]
    fig2 = px.bar(hdf, x="label", y="duration_s", color="status",
                   color_discrete_map={"SUCCESS":"#34d399","ESCALATED":"#fb7185"},
                   labels={"label":"Batch","duration_s":"Duration (s)"})
    fig2.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                       height=280, font=dict(family="Inter",color="#94a3b8"), showlegend=False)
    st.plotly_chart(fig2, use_container_width=True)

# ── Heal Log Detail ─────────────────────────────────────────────
st.divider()
st.markdown("### 🔧 Heal Agent Log (All Batches)")
all_heals = []
for i,b in enumerate(hist):
    for h in b.get("heal_log",[]):
        all_heals.append({
            "Batch": f"#{i+1}",
            "Batch ID": b["batch_id"],
            "Retry": h.get("retry_num","?"),
            "Node": h.get("node","?"),
            "Error Type": h.get("error_type","?"),
            "Fix Applied": str(h.get("fix",""))[:100],
        })
if all_heals:
    st.dataframe(pd.DataFrame(all_heals), use_container_width=True, hide_index=True, height=300)
else:
    st.success("No heal events recorded.")

# ── Error Summary ───────────────────────────────────────────────
st.divider()
st.markdown("### 🚨 Error Summary")
if all_heals:
    edf = pd.DataFrame(all_heals)
    error_counts = edf["Error Type"].value_counts()
    fig3 = px.bar(x=error_counts.index, y=error_counts.values,
                   color=error_counts.index,
                   color_discrete_sequence=["#fb7185","#fbbf24","#818cf8","#34d399"],
                   labels={"x":"Error Type","y":"Occurrences"})
    fig3.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                       height=250, font=dict(family="Inter",color="#94a3b8"), showlegend=False)
    st.plotly_chart(fig3, use_container_width=True)

# ── Log Files ───────────────────────────────────────────────────
st.divider()
st.markdown("### 📄 Audit Report Files")
log_dir = Path("logs")
if log_dir.exists():
    logs = sorted(log_dir.glob("audit_*.txt"), reverse=True)[:10]
    if logs:
        sel_log = st.selectbox("View log file:", [l.name for l in logs])
        log_path = log_dir / sel_log
        st.code(log_path.read_text()[:3000], language="text")
    else:
        st.info("No log files found.")
