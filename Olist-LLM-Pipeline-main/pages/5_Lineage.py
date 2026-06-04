"""Page 5 — Data Lineage"""
import json
import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="Lineage", page_icon="🔗", layout="wide")
st.markdown("# 🔗 Data Lineage Visualization")
st.caption("Trace data flow from Bronze → Silver → Gold across pipeline nodes")

hist = json.load(open("metadata/batch_history.json")) if Path("metadata/batch_history.json").exists() else []
if not hist: st.info("No data yet."); st.stop()

opts = [f"Batch #{i+1} — {b['batch_id']} ({b['status']})" for i,b in enumerate(hist)]
idx = st.selectbox("Select batch:", range(len(opts)), format_func=lambda i: opts[i], index=len(opts)-1)
sel = hist[idx]

# ── Lineage Flow Diagram ───────────────────────────────────────
st.markdown("### 🗺️ Table-Level Lineage")
raw = sel.get("raw_rows",0); clean = sel.get("clean_rows",0); masked = sel.get("masked_rows",0)

st.markdown(f"""
```mermaid
graph LR
    RAW["RAW_OLIST_CUSTOMERS<br/>{raw} rows"] -->|Transform + Dedup| SILVER["SILVER_CUSTOMERS_CLEAN<br/>{clean} rows"]
    SILVER -->|PII Masking| MASKED["SILVER_CUSTOMERS_MASKED<br/>{masked} rows"]
    MASKED -->|KPI Aggregation| GOLD["GOLD_CUSTOMERS_KPIS<br/>7 KPIs"]
    RAW -->|Audit Logging| AUDIT["PIPELINE_AUDIT_LOG"]
    
    style RAW fill:#92400e,color:#fef3c7
    style SILVER fill:#374151,color:#e5e7eb
    style MASKED fill:#4c1d95,color:#ddd6fe
    style GOLD fill:#854d0e,color:#fef9c3
    style AUDIT fill:#1e3a5f,color:#bfdbfe
```
""")

# ── Column-Level Lineage ───────────────────────────────────────
st.divider()
st.markdown("### 📊 Column-Level Transformations")

col_lineage = [
    {"Source Column":"customer_email", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"SHA-256 Hash", "Target Column":"CUSTOMER_EMAIL", "Target Table":"SILVER_CUSTOMERS_MASKED"},
    {"Source Column":"customer_phone", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"SHA-256 Hash", "Target Column":"CUSTOMER_PHONE", "Target Table":"SILVER_CUSTOMERS_MASKED"},
    {"Source Column":"customer_id", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"Partial Mask", "Target Column":"CUSTOMER_ID", "Target Table":"SILVER_CUSTOMERS_MASKED"},
    {"Source Column":"customer_unique_id", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"Partial Mask", "Target Column":"CUSTOMER_UNIQUE_ID", "Target Table":"SILVER_CUSTOMERS_MASKED"},
    {"Source Column":"customer_zip_code_prefix", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"Partial Mask", "Target Column":"CUSTOMER_ZIP_CODE_PREFIX", "Target Table":"SILVER_CUSTOMERS_MASKED"},
    {"Source Column":"customer_city", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"Pass-through", "Target Column":"CUSTOMER_CITY", "Target Table":"SILVER_CUSTOMERS_MASKED"},
    {"Source Column":"customer_state", "Source Table":"RAW_OLIST_CUSTOMERS", "Transform":"Null → UNKNOWN", "Target Column":"CUSTOMER_STATE", "Target Table":"SILVER_CUSTOMERS_MASKED"},
]
st.dataframe(pd.DataFrame(col_lineage), use_container_width=True, hide_index=True)

# ── Node Pipeline Lineage ──────────────────────────────────────
st.divider()
st.markdown("### 🔄 Node Execution Lineage")
ns = sel.get("node_status",{})
nodes = ["profile","bronze_inspector","schema_drift","pii_detector","rule_gen","validator","transform","pii_masker","gold_kpi","lineage_tracker","audit_writer"]
node_data = []
for n in nodes:
    s = ns.get(n,"—")
    icon = "✅" if s=="pass" else ("❌" if s=="fail" else "⏭")
    node_data.append({"Node":n, "Status":f"{icon} {s}", "Layer":"Bronze" if nodes.index(n)<6 else ("Silver" if nodes.index(n)<8 else ("Gold" if nodes.index(n)==8 else "Audit"))})
st.dataframe(pd.DataFrame(node_data), use_container_width=True, hide_index=True)
