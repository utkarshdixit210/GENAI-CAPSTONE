"""
etl-pipeline/plugins/self_healing_agent/mcp_tools.py
MCP tool wrappers used by the Airflow self-healing agent plugin.
Mirrors tools/snowflake_mcp_tool.py but optimised for Airflow task context.
Supports both Snowflake and local CSV (USE_LOCAL_CSV=true) modes.
"""
import os
import json
import pandas as pd
from pathlib import Path
from loguru import logger


USE_LOCAL_CSV = os.getenv("USE_LOCAL_CSV", "true").lower() == "true"
DATA_DIR      = os.getenv("DATA_DIR",    "data")
OUTPUTS_DIR   = os.getenv("OUTPUTS_DIR", "outputs")

CSV_MAP = {
    "RAW_OLIST_CUSTOMERS": f"{DATA_DIR}/olist_customers_dataset.csv",
    "RAW_OLIST_ORDERS":    f"{DATA_DIR}/olist_orders_dataset.csv",
    "RAW_OLIST_PAYMENTS":  f"{DATA_DIR}/olist_order_payments_dataset.csv",
    "RAW_OLIST_PRODUCTS":  f"{DATA_DIR}/olist_products_dataset.csv",
}


def get_snowflake_conn():
    """Return a live Snowflake connection (production mode only)."""
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE",  "OLIST_DB"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA",    "RAW"),
        role      = os.getenv("SNOWFLAKE_ROLE",      "SYSADMIN"),
    )


def mcp_fetch_table(table_name: str) -> pd.DataFrame:
    """Fetch a table as a DataFrame (local CSV or Snowflake)."""
    if USE_LOCAL_CSV:
        path = CSV_MAP.get(table_name)
        if path and Path(path).exists():
            return pd.read_csv(path)
        for layer in ["bronze", "silver", "gold"]:
            p = f"{OUTPUTS_DIR}/{layer}/{table_name}.csv"
            if Path(p).exists():
                return pd.read_csv(p)
        raise FileNotFoundError(f"Table '{table_name}' not found locally.")
    else:
        conn   = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        cols   = [d[0] for d in cursor.description]
        rows   = cursor.fetchall()
        cursor.close(); conn.close()
        return pd.DataFrame(rows, columns=cols)


def mcp_write_table(df: pd.DataFrame, table_name: str, layer: str = "silver"):
    """Write DataFrame to local CSV or Snowflake."""
    if USE_LOCAL_CSV:
        os.makedirs(f"{OUTPUTS_DIR}/{layer}", exist_ok=True)
        path = f"{OUTPUTS_DIR}/{layer}/{table_name}.csv"
        df.to_csv(path, index=False)
        logger.info(f"MCP | Written {len(df):,} rows → {path}")
        return {"status": "written", "path": path}
    else:
        from snowflake.connector.pandas_tools import write_pandas
        conn = get_snowflake_conn()
        write_pandas(conn, df, table_name, auto_create_table=True)
        conn.close()
        logger.info(f"MCP | Written {len(df):,} rows → Snowflake:{table_name}")
        return {"status": "written"}


def mcp_execute_sql(sql: str):
    """Execute SQL (Snowflake mode only; in local mode returns hint)."""
    if USE_LOCAL_CSV:
        logger.info(f"MCP | [LOCAL] SQL hint received (not executed): {sql[:120]}")
        return {"status": "local_mode", "sql": sql}
    else:
        conn   = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close(); conn.close()
        logger.info(f"MCP | SQL executed: {sql[:80]}")
        return {"status": "executed"}


def mcp_append_audit(record: dict, audit_file: str = "pipeline_audit_log"):
    """Append an audit record to the local JSON audit log."""
    os.makedirs("metadata", exist_ok=True)
    path    = f"metadata/{audit_file}.json"
    records = []
    if Path(path).exists():
        with open(path) as f:
            try: records = json.load(f)
            except json.JSONDecodeError: records = []
    records.append(record)
    with open(path, "w") as f:
        json.dump(records, f, indent=2, default=str)
    return {"status": "appended", "path": path}
