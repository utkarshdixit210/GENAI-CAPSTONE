"""Page 1 — Pipeline Control Panel"""
import os, subprocess, json, time
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv
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

with col1:
    if st.button("📊 Generate Data Only", use_container_width=True, type="secondary"):
        with st.spinner("Generating data..."):
            result = subprocess.run(["python3", "generate_data.py", "--rows", str(rows)],
                                     capture_output=True, text=True, cwd=os.getcwd(), timeout=60)
            if result.returncode == 0:
                st.success("✅ Data generated and pushed to Snowflake!")
                st.code(result.stdout[-500:], language="text")
            else:
                st.error(f"❌ Error: {result.stderr[-300:]}")

with col2:
    if st.button("🚀 Run Full Pipeline", use_container_width=True, type="primary"):
        flag = "--once" if mode == "Single Batch" else "--loop"
        with st.status("Running pipeline...", expanded=True) as status:
            st.write(f"Mode: {mode} | Rows: {rows}")
            result = subprocess.run(
                ["python3", "run_continuous.py", flag, "--rows", str(rows), "--interval", str(interval)],
                capture_output=True, text=True, cwd=os.getcwd(), timeout=300)
            if result.returncode == 0:
                status.update(label="✅ Pipeline Complete!", state="complete")
                st.code(result.stdout[-1500:], language="text")
            else:
                status.update(label="❌ Pipeline Failed", state="error")
                st.code(result.stderr[-500:], language="text")

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
p = Path("metadata/batch_history.json")
if p.exists():
    hist = json.load(open(p))
    import pandas as pd
    df = pd.DataFrame(hist[-10:])
    display_cols = [c for c in ["batch_id","timestamp","status","raw_rows","clean_rows","masked_rows","heals","duration_s"] if c in df.columns]
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True)
else:
    st.info("No batch history yet.")
