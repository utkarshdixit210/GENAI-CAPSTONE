"""Page 1 — Pipeline Control Panel"""
import os, sys, subprocess, json, time
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
from config.settings import PipelineConfig
from pipeline.batch_state import read_batch_state, write_pipeline_control
load_dotenv()

st.set_page_config(page_title="Pipeline Control", page_icon="🎛️", layout="wide")
st.markdown("# 🎛️ Pipeline Control Panel")
st.caption("Generate data, trigger pipeline runs, control batch parameters")

# ── Controls ────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)
with c1:
    rows = st.slider("Batch Size (customers)", 100, 1000, 200, 50)
with c2:
    interval = st.slider("Loop Interval (seconds)", 30, 120, 45, 15)
with c3:
    mode = st.radio("Run Mode", ["Single Batch", "Continuous Loop"], horizontal=True)

st.divider()
col1, col2, col3 = st.columns(3)

PROJECT_DIR = PipelineConfig.ROOT_DIR

state = read_batch_state()
status_val = state.get("status", "idle")
is_running = status_val == "running"

with col1:
    if st.button("📊 Generate Data Only", use_container_width=True, type="secondary", disabled=is_running):
        with st.spinner("Generating data..."):
            result = subprocess.run([sys.executable, str(PROJECT_DIR / "generate_data.py"), "--rows", str(rows)],
                                     capture_output=True, text=True, cwd=str(PROJECT_DIR), timeout=60)
            if result.returncode == 0:
                st.success("✅ Data generated and pushed to Snowflake!")
                st.code(result.stdout[-500:], language="text")
            else:
                st.error(f"❌ Error: {result.stderr[-300:]}")

with col2:
    if is_running:
        if st.button("⏹ Stop Pipeline", use_container_width=True, type="secondary"):
            write_pipeline_control(running=False)
            st.warning("⏹ Stop signal sent! The pipeline will halt after the current node finishes.")
            time.sleep(1.5)
            st.rerun()
    else:
        if st.button("🚀 Run Full Pipeline", use_container_width=True, type="primary"):
            flag = "--once" if mode == "Single Batch" else "--loop"
            write_pipeline_control(running=True, batch_interval_sec=interval, dataset="all")
            subprocess.Popen(
                [sys.executable, str(PROJECT_DIR / "run_continuous.py"), flag, "--rows", str(rows), "--interval", str(interval)],
                cwd=str(PROJECT_DIR)
            )
            st.success("🚀 Pipeline started in the background! Navigate to the other pages to approve queries and view logs.")
            time.sleep(1.5)
            st.rerun()

with col3:
    if st.button("🔄 Refresh Dashboard", use_container_width=True):
        st.cache_resource.clear()
        st.cache_data.clear()
        st.rerun()

# ── Config Display ──────────────────────────────────────────────
st.divider()
st.markdown("### ⚙️ Current Configuration")
c1, c2, c3 = st.columns(3)
c1.metric("LLM Provider", os.getenv("LLM_PROVIDER","—"))
c1.metric("LLM Model", os.getenv("LLM_MODEL","—"))
c2.metric("Snowflake DB", os.getenv("SNOWFLAKE_DATABASE","—"))
c2.metric("Schema", os.getenv("SNOWFLAKE_SCHEMA","—"))
c3.metric("Warehouse", os.getenv("SNOWFLAKE_WAREHOUSE","—"))
c3.metric("Role", os.getenv("SNOWFLAKE_ROLE","—"))

# ── Recent Batches ──────────────────────────────────────────────
st.divider()
st.markdown("### 📦 Recent Batches")
p = Path(PipelineConfig.ROOT_DIR) / "metadata" / "batch_history.json"
if p.exists():
    hist = json.load(open(p))
    import pandas as pd
    df = pd.DataFrame(hist[-10:])
    display_cols = [c for c in ["batch_id","timestamp","status","raw_rows","clean_rows","masked_rows","heals","duration_s"] if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
else:
    st.info("No batch history yet.")
