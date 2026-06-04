"""
streamlit_app.py — Main Landing Page
Olist LLM Pipeline — Enterprise Dashboard
"""
import os, json, datetime, subprocess, time
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Olist Pipeline", page_icon="🔮", layout="wide", initial_sidebar_state="expanded")

# ── Shared CSS ──────────────────────────────────────────────────
st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
html, body, [data-testid="stAppViewContainer"], [data-testid="stSidebar"] {
    font-family: 'Inter', sans-serif !important;
}
h1, h2, h3, h4, h5, h6, p, label, .stMetric, .stMarkdown, button {
    font-family: 'Inter', sans-serif !important;
}
[data-testid="stIconMaterial"], 
button[data-testid="stSidebarCollapseButton"] *, 
span[class*="Icon"], 
i[class*="Icon"],
[class*="material-symbols"] {
    font-family: 'Material Symbols Rounded', 'Material Icons' !important;
}
.stApp{background:#0b0b14}
section[data-testid="stSidebar"]{background:#10101f!important}
h1,h2,h3{color:#e2e8f0!important}
.hero{text-align:center;padding:2rem 0 1rem}
.hero h1{font-size:2.6rem;font-weight:900;background:linear-gradient(135deg,#818cf8,#a78bfa,#c084fc);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin:0}
.hero p{color:#64748b;font-size:.9rem;margin-top:.3rem}
.kpi-row{display:flex;gap:12px;margin:1rem 0}
.kpi{flex:1;background:rgba(30,30,50,.6);border:1px solid rgba(99,102,241,.12);border-radius:14px;padding:18px 16px;text-align:center;transition:border-color .2s}
.kpi:hover{border-color:rgba(99,102,241,.35)}
.kpi .v{font-size:2rem;font-weight:800;line-height:1.1}
.kpi .l{font-size:.68rem;color:#64748b;text-transform:uppercase;letter-spacing:1.2px;margin-top:4px}
.purple{color:#a78bfa}.green{color:#34d399}.amber{color:#fbbf24}.rose{color:#fb7185}.white{color:#e2e8f0}
.sec{font-size:1.05rem;font-weight:700;color:#e2e8f0;border-left:3px solid #818cf8;padding-left:12px;margin:1.8rem 0 .8rem}
.chip{display:inline-block;padding:3px 12px;border-radius:99px;font-size:.72rem;font-weight:700}
.chip-ok{background:rgba(52,211,153,.12);color:#34d399}
.chip-fail{background:rgba(251,113,133,.12);color:#fb7185}
.live-bar{text-align:center;padding:6px;margin:0 0 1.5rem;background:rgba(99,102,241,.06);border-radius:8px;color:#818cf8;font-size:.78rem}
.card{background:rgba(30,30,50,.5);border:1px solid rgba(99,102,241,.1);border-radius:12px;padding:16px;margin-bottom:10px}
.finding{padding:8px 14px;border-radius:8px;margin:4px 0;font-size:.82rem;background:rgba(30,30,50,.5);border-left:3px solid}
.finding-high{border-color:#fb7185;color:#fda4af}
.finding-med{border-color:#fbbf24;color:#fde68a}
.finding-low{border-color:#34d399;color:#6ee7b7}
.finding-info{border-color:#818cf8;color:#a5b4fc}

/* Glassmorphic Flowchart Styles */
.pipeline-container {display:flex;justify-content:space-between;align-items:stretch;gap:14px;margin:1.5rem 0 2rem;flex-wrap:wrap}
.pipeline-card {flex:1;min-width:270px;background:rgba(20,20,35,0.45);border-radius:16px;padding:18px;position:relative;transition:all .3s cubic-bezier(0.4, 0, 0.2, 1);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px)}
.pipeline-arrow {display:flex;align-items:center;justify-content:center;position:relative;min-width:40px}
.pipeline-arrow::before {content:'';width:100%;height:2px;background:linear-gradient(90deg,#818cf8,#a78bfa);box-shadow:0 0 8px rgba(129,140,248,.6)}
.pipeline-arrow::after {content:'➔';position:absolute;color:#a78bfa;font-size:20px;right:-5px;text-shadow:0 0 8px rgba(167,139,250,.6);animation:floatArrow 1.5s infinite ease-in-out}
@keyframes floatArrow { 0% { transform: translateX(0); } 50% { transform: translateX(5px); } 100% { transform: translateX(0); } }

@media (max-width:900px){
  .pipeline-container {flex-direction:column}
  .pipeline-arrow {height:40px;min-width:auto;margin:10px 0}
  .pipeline-arrow::before {width:2px;height:100%;background:linear-gradient(180deg,#818cf8,#a78bfa)}
  .pipeline-arrow::after {content:'➔';transform:rotate(90deg);bottom:-5px;right:auto;animation:floatArrowVertical 1.5s infinite ease-in-out}
}
@keyframes floatArrowVertical { 0% { transform: rotate(90deg) translateY(0); } 50% { transform: rotate(90deg) translateY(5px); } 100% { transform: rotate(90deg) translateY(0); } }

.bronze-card {border:1px solid rgba(217,119,6,.18);box-shadow:0 8px 32px rgba(217,119,6,.03)}
.bronze-card:hover {border-color:rgba(217,119,6,.45);box-shadow:0 8px 32px rgba(217,119,6,.2);background:rgba(217,119,6,.02);transform:translateY(-2px)}
.silver-card {border:1px solid rgba(99,102,241,.2);box-shadow:0 8px 32px rgba(99,102,241,.03)}
.silver-card:hover {border-color:rgba(99,102,241,.45);box-shadow:0 8px 32px rgba(99,102,241,.2);background:rgba(99,102,241,.02);transform:translateY(-2px)}
.gold-card {border:1px solid rgba(234,179,8,.18);box-shadow:0 8px 32px rgba(234,179,8,.03)}
.gold-card:hover {border-color:rgba(234,179,8,.45);box-shadow:0 8px 32px rgba(234,179,8,.2);background:rgba(234,179,8,.02);transform:translateY(-2px)}
.card-hdr {font-size:1.02rem;font-weight:900;margin-bottom:2px;letter-spacing:.5px}
.bronze-card .card-hdr {color:#f59e0b}
.silver-card .card-hdr {color:#818cf8}
.gold-card .card-hdr {color:#eab308}
.card-sub {font-size:.65rem;color:#64748b;text-transform:uppercase;letter-spacing:1px;margin-bottom:16px}
.step-box {display:flex;flex-direction:column;gap:9px}
.step-pill {background:rgba(30,30,50,.35);border:1px solid rgba(255,255,255,.02);border-radius:10px;padding:9px 12px;transition:all .2s ease}
.step-pill:hover {transform:translateX(3px);background:rgba(30,30,50,.65)}
.bronze-card .step-pill:hover {border-color:rgba(217,119,6,.22)}
.silver-card .step-pill:hover {border-color:rgba(99,102,241,.25)}
.gold-card .step-pill:hover {border-color:rgba(234,179,8,.22)}
.step-pill b {color:#e2e8f0;font-size:.82rem}
.step-desc {font-size:.7rem;color:#64748b;margin-top:1px}
.heal-loop-box {border:1px dashed rgba(244,63,94,.3);background:rgba(244,63,94,.02);position:relative;overflow:hidden}
.heal-loop-box:hover {background:rgba(244,63,94,.06);border-color:rgba(244,63,94,.5)}
.heal-badge {position:absolute;top:5px;right:6px;font-size:.5rem;font-weight:800;background:rgba(244,63,94,.12);color:#fb7185;padding:1px 5px;border-radius:3px;letter-spacing:.3px}
</style>""", unsafe_allow_html=True)

# ── Helpers ─────────────────────────────────────────────────────
@st.cache_resource(ttl=30)
def get_sf():
    try:
        import snowflake.connector
        return snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"), user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"), warehouse=os.getenv("SNOWFLAKE_WAREHOUSE","COMPUTE_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE","MY_DB"), schema=os.getenv("SNOWFLAKE_SCHEMA","PUBLIC"),
            role=os.getenv("SNOWFLAKE_ROLE","SYSADMIN"))
    except: return None

def qry(sql):
    conn = get_sf()
    if not conn: return pd.DataFrame()
    try:
        c=conn.cursor();c.execute(sql);cols=[d[0] for d in c.description];data=c.fetchall();c.close()
        return pd.DataFrame(data,columns=cols)
    except: return pd.DataFrame()

def load_hist():
    p=Path("metadata/batch_history.json")
    return json.load(open(p)) if p.exists() else []

# ── Data ────────────────────────────────────────────────────────
hist = load_hist()
raw_df = qry("SELECT COUNT(*) AS CNT FROM RAW_OLIST_CUSTOMERS")
silver_df = qry("SELECT COUNT(*) AS CNT FROM SILVER_CUSTOMERS_CLEAN")
masked_df = qry("SELECT COUNT(*) AS CNT FROM SILVER_CUSTOMERS_MASKED")
gold_df = qry("SELECT COUNT(*) AS CNT FROM GOLD_CUSTOMERS_KPIS")

raw_n = int(raw_df.iloc[0,0]) if not raw_df.empty else 0
silver_n = int(silver_df.iloc[0,0]) if not silver_df.empty else 0
masked_n = int(masked_df.iloc[0,0]) if not masked_df.empty else 0
gold_n = int(gold_df.iloc[0,0]) if not gold_df.empty else 0

# ── Hero ────────────────────────────────────────────────────────
st.markdown("""<div class="hero">
<h1>🔮 Olist LLM Pipeline</h1>
<p>Autonomous Self-Healing Data Pipeline &mdash; LLM on Every Agent &mdash; Snowflake</p>
</div>""", unsafe_allow_html=True)

now = datetime.datetime.now().strftime("%H:%M:%S")
bid = hist[-1].get("batch_id","—") if hist else "—"
st.markdown(f'<div class="live-bar">🟢 Live · {now} · {len(hist)} batches processed · Latest: {bid}</div>', unsafe_allow_html=True)

# ── Top KPIs ────────────────────────────────────────────────────
latest = hist[-1] if hist else {}
status = latest.get("status","—")
chip = "chip-ok" if status=="SUCCESS" else "chip-fail"

st.markdown(f"""<div class="kpi-row">
<div class="kpi"><div class="v amber">{raw_n:,}</div><div class="l">🟤 Bronze Rows</div></div>
<div class="kpi"><div class="v white">{silver_n:,}</div><div class="l">⚪ Silver Clean</div></div>
<div class="kpi"><div class="v purple">{masked_n:,}</div><div class="l">🔒 PII Masked</div></div>
<div class="kpi"><div class="v green">{gold_n}</div><div class="l">🥇 Gold KPIs</div></div>
<div class="kpi"><div class="v"><span class="chip {chip}">{status}</span></div><div class="l">Pipeline Status</div></div>
<div class="kpi"><div class="v rose">{latest.get('heals',0)}</div><div class="l">🔧 Self-Heals</div></div>
</div>""", unsafe_allow_html=True)

# ── Pipeline Flow ───────────────────────────────────────────────
st.markdown('<div class="sec">🔄 Pipeline Architecture Flow</div>', unsafe_allow_html=True)

pipeline_html = """
<div class="pipeline-container">
  <div class="pipeline-card bronze-card">
    <div class="card-hdr">🟤 BRONZE LAYER</div>
    <div class="card-sub">Ingestion & Scanning</div>
    <div class="step-box">
      <div class="step-pill"><b>📊 Profile Node</b><div class="step-desc">Extracts schemas & records raw stats</div></div>
      <div class="step-pill"><b>🔍 LLM Inspector</b><div class="step-desc">Groq scans batch for quality issues</div></div>
      <div class="step-pill"><b>🌊 Schema Drift</b><div class="step-desc">Flags column additions/type changes</div></div>
      <div class="step-pill"><b>🕵️‍♂️ PII Detector</b><div class="step-desc">Classifies column privacy sensitivity</div></div>
    </div>
  </div>
  
  <div class="pipeline-arrow">➔</div>
  
  <div class="pipeline-card silver-card">
    <div class="card-hdr">⚪ SILVER LAYER</div>
    <div class="card-sub">Auto-Healing & Masking</div>
    <div class="step-box">
      <div class="step-pill"><b>📜 Rule Gen & Validator</b><div class="step-desc">Generates GE rules & validates tables</div></div>
      <div class="step-pill heal-loop-box">
        <span class="heal-badge">AUTO-HEAL</span>
        <b>🩺 Self-Healing Agent</b>
        <div class="step-desc">Intercepts error & executes DB fix SQL</div>
      </div>
      <div class="step-pill"><b>🔒 PII Masker</b><div class="step-desc">Hashes HIGH / masks MEDIUM columns</div></div>
    </div>
  </div>
  
  <div class="pipeline-arrow">➔</div>
  
  <div class="pipeline-card gold-card">
    <div class="card-hdr">🟡 GOLD LAYER</div>
    <div class="card-sub">Analytics & Audit Reports</div>
    <div class="step-box">
      <div class="step-pill"><b>🥇 Gold KPI Generator</b><div class="step-desc">Aggregates cumulative gold metrics</div></div>
      <div class="step-pill"><b>💡 LLM Insights Node</b><div class="step-desc">Drafts business recommendations</div></div>
      <div class="step-pill"><b>🔗 Lineage & Audit Logs</b><div class="step-desc">Records run details & audit report</div></div>
    </div>
  </div>
</div>
"""
st.markdown(pipeline_html, unsafe_allow_html=True)

# ── Batch History ───────────────────────────────────────────────
if hist:
    st.markdown('<div class="sec">📈 Batch History Overview</div>', unsafe_allow_html=True)
    hdf = pd.DataFrame(hist)
    hdf["label"] = [f"#{i+1}" for i in range(len(hdf))]

    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=hdf["label"], y=hdf["raw_rows"], name="Bronze", marker_color="#d97706", opacity=.8))
        fig.add_trace(go.Bar(x=hdf["label"], y=hdf["clean_rows"], name="Silver", marker_color="#94a3b8", opacity=.8))
        fig.add_trace(go.Bar(x=hdf["label"], y=hdf["masked_rows"], name="Masked", marker_color="#818cf8", opacity=.8))
        fig.update_layout(title="Rows per Batch", barmode="group", height=300, template="plotly_dark",
                          paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font=dict(family="Inter", color="#94a3b8"), legend=dict(orientation="h", y=-.15))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        colors = ["#34d399" if s=="SUCCESS" else "#fb7185" for s in hdf["status"]]
        fig2 = go.Figure(go.Bar(x=hdf["label"], y=hdf["duration_s"], marker_color=colors,
                                 text=hdf["status"], textposition="auto", opacity=.85))
        fig2.update_layout(title="Duration & Status", height=300, template="plotly_dark",
                           paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                           font=dict(family="Inter", color="#94a3b8"))
        st.plotly_chart(fig2, use_container_width=True)

# ── Navigation ──────────────────────────────────────────────────
st.markdown('<div class="sec">📂 Dashboard Pages</div>', unsafe_allow_html=True)

pages = [
    ("🎛️", "Pipeline Control", "Generate data, trigger pipeline, control batch size"),
    ("📊", "Quality KPIs", "Validation pass %, null rates, healing success, quarantine rate"),
    ("🔐", "Governance & PII", "PII detection, masking audit, data classification"),
    ("🌊", "Schema Evolution", "Schema drift tracking, column changes, type modifications"),
    ("🔗", "Lineage", "Data flow tracing across Bronze → Silver → Gold"),
    ("📋", "Audit & Logs", "Run history, heal logs, error summaries"),
    ("🔍", "Data Explorer", "Browse Snowflake tables: Bronze, Silver, Masked, Gold"),
    ("🛡️", "Enterprise Ops", "Agent tracing, approvals queue, token costs, locks, and memory"),
]

cols = st.columns(4)
for i, (icon, name, desc) in enumerate(pages):
    with cols[i % 4]:
        st.markdown(f"""<div class="card">
        <div style="font-size:1.5rem;margin-bottom:4px">{icon}</div>
        <div style="font-weight:700;color:#e2e8f0;font-size:.9rem">{name}</div>
        <div style="color:#64748b;font-size:.75rem;margin-top:2px">{desc}</div>
        </div>""", unsafe_allow_html=True)

st.caption("👈 Use the sidebar to navigate between pages")
st.markdown("---")
st.markdown('<div style="text-align:center;color:#475569;font-size:.7rem">GEN AI Capstone-2 · LangGraph · Groq · Snowflake</div>', unsafe_allow_html=True)
