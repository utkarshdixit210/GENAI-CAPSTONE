"""Page 7 — Snowflake Data Explorer"""
import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="Data Explorer", page_icon="🔍", layout="wide")
st.markdown("# 🔍 Snowflake Data Explorer")
st.caption("Browse raw, clean, masked, and gold tables directly from Snowflake")

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

# ── Table Selector ──────────────────────────────────────────────
tables = {
    "🟤 RAW_OLIST_CUSTOMERS": "RAW_OLIST_CUSTOMERS",
    "🟤 RAW_OLIST_ORDERS": "RAW_OLIST_ORDERS",
    "🟤 RAW_OLIST_PAYMENTS": "RAW_OLIST_PAYMENTS",
    "🟤 RAW_OLIST_PRODUCTS": "RAW_OLIST_PRODUCTS",
    "⚪ SILVER_CUSTOMERS_CLEAN": "SILVER_CUSTOMERS_CLEAN",
    "🔒 SILVER_CUSTOMERS_MASKED": "SILVER_CUSTOMERS_MASKED",
    "🥇 GOLD_CUSTOMERS_KPIS": "GOLD_CUSTOMERS_KPIS",
    "📋 PIPELINE_AUDIT_LOG": "PIPELINE_AUDIT_LOG",
}

selected = st.selectbox("Select table:", list(tables.keys()))
table = tables[selected]
limit = st.slider("Row limit", 10, 500, 100, 10)

if st.button("🔄 Refresh", type="primary"):
    st.cache_resource.clear(); st.rerun()

# ── Query & Display ─────────────────────────────────────────────
with st.spinner(f"Loading {table}..."):
    df = qry(f"SELECT * FROM {table} LIMIT {limit}")

if not df.empty:
    c1,c2,c3 = st.columns(3)
    c1.metric("Rows Loaded", f"{len(df):,}")
    c2.metric("Columns", len(df.columns))
    c3.metric("Table", table)

    st.divider()

    # Schema info
    with st.expander("📐 Schema Info", expanded=False):
        schema_rows = [{"Column": col, "Type": str(df[col].dtype), "Non-Null": int(df[col].notna().sum()),
                        "Null %": f"{df[col].isna().mean()*100:.1f}%"} for col in df.columns]
        st.dataframe(pd.DataFrame(schema_rows), use_container_width=True, hide_index=True)

    # Data
    st.dataframe(df, use_container_width=True, height=450)

    # Download
    csv = df.to_csv(index=False)
    st.download_button(f"📥 Download {table}.csv", csv, f"{table}.csv", "text/csv")
else:
    st.warning(f"Table `{table}` is empty or doesn't exist.")
