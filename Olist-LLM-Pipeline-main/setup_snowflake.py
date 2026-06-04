"""
setup_snowflake.py
Creates all required Snowflake tables (raw input + pipeline output)
and loads generated CSV data into the raw tables.
"""
import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
cur = conn.cursor()
print(f"Connected to Snowflake: {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA')}\n")

# ── DDL for all tables ────────────────────────────────────────────────────────

DDL = [
    # Raw input tables (Bronze layer source)
    """CREATE OR REPLACE TABLE RAW_OLIST_CUSTOMERS (
        customer_id              VARCHAR,
        customer_unique_id       VARCHAR,
        customer_zip_code_prefix VARCHAR,
        customer_city            VARCHAR,
        customer_state           VARCHAR
    )""",
    """CREATE OR REPLACE TABLE RAW_OLIST_ORDERS (
        order_id                        VARCHAR,
        customer_id                     VARCHAR,
        order_status                    VARCHAR,
        order_purchase_timestamp        VARCHAR,
        order_approved_at               VARCHAR,
        order_delivered_carrier_date    VARCHAR,
        order_delivered_customer_date   VARCHAR,
        order_estimated_delivery_date   VARCHAR
    )""",
    """CREATE OR REPLACE TABLE RAW_OLIST_PAYMENTS (
        order_id              VARCHAR,
        payment_sequential    INTEGER,
        payment_type          VARCHAR,
        payment_installments  INTEGER,
        payment_value         FLOAT
    )""",
    """CREATE OR REPLACE TABLE RAW_OLIST_PRODUCTS (
        product_id                  VARCHAR,
        product_category_name       VARCHAR,
        product_name_lenght         FLOAT,
        product_description_lenght  FLOAT,
        product_photos_qty          FLOAT,
        product_weight_g            FLOAT,
        product_length_cm           FLOAT,
        product_height_cm           FLOAT,
        product_width_cm            FLOAT
    )""",

    # Pipeline output tables (Silver / Gold / Meta)
    """CREATE TABLE IF NOT EXISTS SILVER_OLIST_CLEAN (
        dataset       VARCHAR,
        row_data      VARIANT,
        processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS SILVER_OLIST_MASKED (
        dataset       VARCHAR,
        row_data      VARIANT,
        processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS BRONZE_QUARANTINE (
        dataset        VARCHAR,
        row_data       VARIANT,
        reason         VARCHAR,
        quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS GOLD_OLIST_KPIS (
        dataset      VARCHAR,
        kpi_data     VARIANT,
        computed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE OR REPLACE TABLE PIPELINE_LINEAGE (
        RECORD_JSON  VARIANT,
        RECORDED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )""",
    """CREATE OR REPLACE TABLE PIPELINE_AUDIT_LOG (
        RECORD_JSON  VARIANT,
        RECORDED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )""",
    """CREATE OR REPLACE TABLE SCHEMA_DRIFT_LOG (
        RECORD_JSON  VARIANT,
        RECORDED_AT  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )""",
]

print("Creating tables...")
for ddl in DDL:
    name = ddl.split("TABLE")[1].strip().split("(")[0].strip().replace("IF NOT EXISTS ", "").replace("OR REPLACE ", "")
    cur.execute(ddl)
    print(f"  ✓ {name}")

# ── Load CSV data into raw tables ────────────────────────────────────────────

LOAD_MAP = {
    "RAW_OLIST_CUSTOMERS": "data/olist_customers_dataset.csv",
    "RAW_OLIST_ORDERS":    "data/olist_orders_dataset.csv",
    "RAW_OLIST_PAYMENTS":  "data/olist_order_payments_dataset.csv",
    "RAW_OLIST_PRODUCTS":  "data/olist_products_dataset.csv",
}

print("\nLoading data into Snowflake raw tables...")
for table, csv_path in LOAD_MAP.items():
    df = pd.read_csv(csv_path)
    # Ensure column names are uppercase to match Snowflake
    df.columns = [c.upper() for c in df.columns]
    cur.execute(f"TRUNCATE TABLE {table}")
    success, nchunks, nrows, _ = write_pandas(conn, df, table)
    print(f"  ✓ {table}: {nrows:,} rows loaded ({csv_path})")

cur.close()
conn.close()
print("\nSetup complete! Run the pipeline with: python3 main.py")
