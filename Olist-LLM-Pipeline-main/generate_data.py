"""
generate_data.py
Generates a fresh BATCH of synthetic Olist data and APPENDS it directly
into Snowflake RAW tables (Bronze layer).

Each call produces a unique batch_id so the pipeline always sees NEW rows.

Usage:
    python generate_data.py                # ~200 customers per batch
    python generate_data.py --rows 500     # custom batch size
    python generate_data.py --preview      # print sample without writing to Snowflake
"""
import os
import uuid
import random
import argparse
import datetime
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

random.seed()           # fully random each run
np.random.seed()        # fully random each run

BR_STATES = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
]
BR_CITIES = [
    "sao paulo","rio de janeiro","belo horizonte","curitiba","fortaleza",
    "manaus","salvador","recife","porto alegre","belem","goiania","florianopolis",
    "maceio","natal","teresina","campo grande","joao pessoa","aracaju","macapa","porto velho",
]
PAYMENT_TYPES   = ["credit_card","boleto","voucher","debit_card"]
ORDER_STATUSES  = ["delivered","shipped","canceled","processing","invoiced","approved","unavailable"]


def _nullify(series: pd.Series, rate: float = 0.05) -> pd.Series:
    mask = np.random.random(len(series)) < rate
    series = series.astype(object)
    series[mask] = np.nan
    return series


def _inject_bad_categorical(series: pd.Series, bad_values: list, rate: float = 0.03) -> pd.Series:
    mask = np.random.random(len(series)) < rate
    series = series.copy()
    series[mask] = np.random.choice(bad_values, mask.sum())
    return series


# ── Data generators ────────────────────────────────────────────────

def generate_customers(n: int, batch_id: str) -> pd.DataFrame:
    ids = [str(uuid.uuid4()) for _ in range(n)]
    uid = [str(uuid.uuid4()) for _ in range(n)]

    df = pd.DataFrame({
        "customer_id":              ids,
        "customer_unique_id":       uid,
        "customer_zip_code_prefix": np.random.randint(1000, 99999, n),
        "customer_city":            np.random.choice(BR_CITIES, n),
        "customer_state":           np.random.choice(BR_STATES, n),
        "batch_id":                 batch_id,
        "ingested_at":              datetime.datetime.utcnow().isoformat(),
    })

    # Inject quality issues intentionally
    df["customer_id"]    = _nullify(df["customer_id"],    rate=0.02)
    df["customer_state"] = _nullify(df["customer_state"], rate=0.04)
    df["customer_state"] = _inject_bad_categorical(
        df["customer_state"], bad_values=["XX","UNKNOWN","sp","São Paulo"], rate=0.03
    )
    # ~1% duplicates
    dup_rows = df.sample(frac=0.01)
    df = pd.concat([df, dup_rows], ignore_index=True)
    return df


def generate_orders(customer_ids: list, n: int, batch_id: str) -> pd.DataFrame:
    base_date = datetime.datetime(2017, 1, 1)
    def rand_ts():
        d = base_date + datetime.timedelta(days=random.randint(0, 730))
        return d.strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame({
        "order_id":                       [str(uuid.uuid4()) for _ in range(n)],
        "customer_id":                    np.random.choice(customer_ids, n),
        "order_status":                   np.random.choice(ORDER_STATUSES, n, p=[0.65,0.12,0.08,0.05,0.04,0.04,0.02]),
        "order_purchase_timestamp":       [rand_ts() for _ in range(n)],
        "order_approved_at":              [rand_ts() for _ in range(n)],
        "order_delivered_carrier_date":   [rand_ts() for _ in range(n)],
        "order_delivered_customer_date":  [rand_ts() for _ in range(n)],
        "order_estimated_delivery_date":  [rand_ts() for _ in range(n)],
        "batch_id":                       batch_id,
        "ingested_at":                    datetime.datetime.utcnow().isoformat(),
    })
    df["customer_id"]  = _nullify(df["customer_id"],  rate=0.03)
    df["order_status"] = _inject_bad_categorical(df["order_status"], bad_values=["pending","UNKNOWN","null"], rate=0.03)
    df["order_approved_at"]             = _nullify(df["order_approved_at"],             rate=0.08)
    df["order_delivered_carrier_date"]  = _nullify(df["order_delivered_carrier_date"],  rate=0.12)
    df["order_delivered_customer_date"] = _nullify(df["order_delivered_customer_date"], rate=0.15)
    return df


def generate_payments(order_ids: list, n: int, batch_id: str) -> pd.DataFrame:
    df = pd.DataFrame({
        "order_id":             np.random.choice(order_ids, n),
        "payment_sequential":   np.random.randint(1, 5, n),
        "payment_type":         np.random.choice(PAYMENT_TYPES, n, p=[0.74,0.19,0.05,0.02]),
        "payment_installments": np.random.randint(1, 12, n),
        "payment_value":        np.round(np.random.exponential(scale=150, size=n), 2),
        "batch_id":             batch_id,
        "ingested_at":          datetime.datetime.utcnow().isoformat(),
    })
    df["payment_value"] = _nullify(df["payment_value"], rate=0.02)
    df["payment_type"]  = _inject_bad_categorical(df["payment_type"], bad_values=["cash","check","UNKNOWN"], rate=0.03)
    neg_mask = np.random.random(n) < 0.03
    df.loc[neg_mask, "payment_value"] = -abs(df.loc[neg_mask, "payment_value"])
    return df


def generate_products(n: int, batch_id: str) -> pd.DataFrame:
    PRODUCT_CATEGORIES = [
        "cama_mesa_banho","beleza_saude","esporte_lazer","informatica_acessorios",
        "moveis_decoracao","utilidades_domesticas","auto","brinquedos",
    ]
    df = pd.DataFrame({
        "product_id":                 [str(uuid.uuid4()) for _ in range(n)],
        "product_category_name":      np.random.choice(PRODUCT_CATEGORIES, n),
        "product_name_lenght":        np.random.randint(10, 80, n).astype(float),
        "product_description_lenght": np.random.randint(50, 3000, n).astype(float),
        "product_photos_qty":         np.random.randint(1, 10, n).astype(float),
        "product_weight_g":           np.random.randint(100, 30000, n).astype(float),
        "product_length_cm":          np.random.randint(10, 100, n).astype(float),
        "product_height_cm":          np.random.randint(5, 80, n).astype(float),
        "product_width_cm":           np.random.randint(10, 80, n).astype(float),
        "batch_id":                   batch_id,
        "ingested_at":                datetime.datetime.utcnow().isoformat(),
    })
    df["product_category_name"] = _nullify(df["product_category_name"], rate=0.06)
    df["product_weight_g"]      = _nullify(df["product_weight_g"],      rate=0.04)
    return df


# ── Snowflake writer ───────────────────────────────────────────────

def write_to_snowflake(df: pd.DataFrame, table: str, conn) -> int:
    """Drop + recreate a Snowflake table with fresh batch data."""
    from snowflake.connector.pandas_tools import write_pandas
    df = df.copy()
    # Snowflake requires UPPERCASE column names
    df.columns = [c.upper() for c in df.columns]
    # Drop existing table so schema always matches the fresh batch
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table.upper()}")
        conn.commit()
    except Exception:
        pass
    finally:
        cur.close()
    success, nchunks, nrows, _ = write_pandas(
        conn, df, table.upper(), auto_create_table=True, overwrite=False
    )
    return nrows



def get_snowflake_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database  = os.getenv("SNOWFLAKE_DATABASE"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA"),
        role      = os.getenv("SNOWFLAKE_ROLE"),
    )


def run_batch(n_customers: int = 200, preview: bool = False) -> str:
    """
    Generate one fresh batch of data and push it to Snowflake.
    Returns the batch_id for the pipeline to process.
    """
    batch_id  = str(uuid.uuid4())[:8]
    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"\n{'='*65}")
    print(f"  BATCH {batch_id} | {timestamp}")
    print(f"  Generating {n_customers} customers + proportional orders/payments/products")
    print(f"{'='*65}")

    n_orders   = int(n_customers * 1.2)
    n_payments = int(n_customers * 1.4)
    n_products = int(n_customers * 0.8)

    customers = generate_customers(n_customers, batch_id)
    customer_ids = customers["customer_id"].dropna().tolist()

    orders   = generate_orders(customer_ids, n_orders, batch_id)
    order_ids = orders["order_id"].dropna().tolist()

    payments = generate_payments(order_ids, n_payments, batch_id)
    products = generate_products(n_products, batch_id)

    print(f"\n  Injected quality issues:")
    print(f"    customers : {len(customers):>5} rows  "
          f"(~{int(n_customers*0.02)} null IDs, ~{int(n_customers*0.04)} null states, "
          f"~{int(n_customers*0.03)} bad states, ~{int(n_customers*0.01)} dupes)")
    print(f"    orders    : {len(orders):>5} rows  "
          f"(~{int(n_orders*0.03)} null customer IDs, ~{int(n_orders*0.03)} bad statuses)")
    print(f"    payments  : {len(payments):>5} rows  "
          f"(~{int(n_payments*0.02)} null values, ~{int(n_payments*0.03)} bad types, "
          f"~{int(n_payments*0.03)} negative values)")
    print(f"    products  : {len(products):>5} rows  "
          f"(~{int(n_products*0.06)} null categories, ~{int(n_products*0.04)} null weights)")

    if preview:
        print("\n  ── Sample Customers ──")
        print(customers.head(3).to_string(index=False))
        print("\n  [PREVIEW MODE] — Not writing to Snowflake.")
        return batch_id

    # Push to Snowflake
    print(f"\n  Pushing to Snowflake...")
    conn = get_snowflake_conn()
    try:
        datasets = {
            "RAW_OLIST_CUSTOMERS": customers,
            "RAW_OLIST_ORDERS":    orders,
            "RAW_OLIST_PAYMENTS":  payments,
            "RAW_OLIST_PRODUCTS":  products,
        }
        for table, df in datasets.items():
            n = write_to_snowflake(df, table, conn)
            print(f"    ✓ {table}: {n} rows appended")
    finally:
        conn.close()

    print(f"\n  Batch {batch_id} ready for pipeline. Run: python main.py")
    return batch_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a fresh batch of Olist data → Snowflake")
    parser.add_argument("--rows",    type=int,  default=200, help="Number of customers per batch")
    parser.add_argument("--preview", action="store_true",    help="Preview data without writing to Snowflake")
    args = parser.parse_args()
    run_batch(n_customers=args.rows, preview=args.preview)
