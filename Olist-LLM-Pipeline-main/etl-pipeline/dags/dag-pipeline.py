"""
etl-pipeline/dags/dag-pipeline.py
Airflow DAG — Olist Bronze → Silver → Gold ETL Pipeline

Orchestrates the full medallion architecture with a self-healing agent plugin:
  BRONZE  → load raw CSVs → validate quality → quarantine bad rows
  SILVER  → clean + transform → PII mask
  GOLD    → aggregate KPIs

The self_healing_agent plugin is called whenever a task fails,
providing automatic retry with LLM-guided fixes (mirrors the
LangGraph pipeline's Heal Agent in an Airflow context).

Schedules daily at 02:00 UTC.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Default DAG args ──────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
}

DATASETS = ["customers", "orders", "payments", "products"]


# ── Task functions ────────────────────────────────────────────────

def _load_bronze(dataset: str, **ctx):
    """Load raw Olist CSV into Bronze layer (Snowflake or local)."""
    import pandas as pd
    import os
    from pathlib import Path

    csv_map = {
        "customers": "data/olist_customers_dataset.csv",
        "orders":    "data/olist_orders_dataset.csv",
        "payments":  "data/olist_order_payments_dataset.csv",
        "products":  "data/olist_products_dataset.csv",
    }
    path = csv_map[dataset]
    if not Path(path).exists():
        raise FileNotFoundError(
            f"CSV not found: {path}. Run: python generate_data.py"
        )
    df = pd.read_csv(path)
    os.makedirs(f"outputs/bronze", exist_ok=True)
    out = f"outputs/bronze/RAW_OLIST_{dataset.upper()}.csv"
    df.to_csv(out, index=False)
    print(f"BRONZE LOAD | {dataset} | {len(df):,} rows → {out}")
    ctx["ti"].xcom_push(key=f"{dataset}_bronze_rows", value=len(df))


def _validate_quality(dataset: str, **ctx):
    """
    Run GE-style validation on Bronze data.
    Push failed_checks to XCom for the self-healing agent.
    """
    import pandas as pd
    from pathlib import Path

    path = f"outputs/bronze/RAW_OLIST_{dataset.upper()}.csv"
    if not Path(path).exists():
        raise FileNotFoundError(f"Bronze file not found: {path}")

    df = pd.read_csv(path)
    failed = []

    # Null checks by dataset
    null_cols = {
        "customers": ["customer_id", "customer_state"],
        "orders":    ["order_id", "customer_id", "order_status"],
        "payments":  ["order_id", "payment_value"],
        "products":  ["product_id"],
    }.get(dataset, [])

    for col in null_cols:
        if col in df.columns:
            n = int(df[col].isnull().sum())
            if n > 0:
                failed.append({"column": col, "issue": f"{n} null values", "failed_count": n})

    # Value-set checks
    if dataset == "payments" and "payment_type" in df.columns:
        valid = {"credit_card","boleto","voucher","debit_card"}
        invalid = df[~df["payment_type"].astype(str).isin(valid)]
        if len(invalid) > 0:
            failed.append({"column": "payment_type",
                           "issue": f"{len(invalid)} invalid payment types",
                           "failed_count": len(invalid)})

    if dataset == "customers" and "customer_state" in df.columns:
        BR_STATES = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
                     "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
                     "RS","RO","RR","SC","SP","SE","TO"}
        invalid = df[~df["customer_state"].astype(str).isin(BR_STATES)]
        if len(invalid) > 0:
            failed.append({"column": "customer_state",
                           "issue": f"{len(invalid)} invalid state codes",
                           "failed_count": len(invalid)})

    # Range checks
    if dataset == "payments" and "payment_value" in df.columns:
        neg = int((pd.to_numeric(df["payment_value"], errors="coerce") < 0).sum())
        if neg > 0:
            failed.append({"column": "payment_value",
                           "issue": f"{neg} negative values",
                           "failed_count": neg})

    ctx["ti"].xcom_push(key=f"{dataset}_failed_checks", value=failed)
    print(f"VALIDATE | {dataset} | {len(failed)} check(s) failed")

    if failed:
        raise ValueError(
            f"Quality checks failed for {dataset}: {len(failed)} issue(s). "
            "Self-healing agent will handle."
        )


def _transform_to_silver(dataset: str, **ctx):
    """Apply cleaning transformations and write Silver layer."""
    import pandas as pd
    import os

    path = f"outputs/bronze/RAW_OLIST_{dataset.upper()}.csv"
    df   = pd.read_csv(path)

    # Remove duplicates
    id_cols = {"customers": "customer_id", "orders": "order_id",
                "payments": "order_id",    "products": "product_id"}
    id_col = id_cols.get(dataset)
    if id_col and id_col in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=[id_col])
        print(f"SILVER | {dataset} | Removed {before - len(df)} duplicates")

    # Fill nulls
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("UNKNOWN")
        else:
            df[col] = df[col].fillna(0)

    # State normalisation for customers
    if "customer_state" in df.columns:
        BR_STATES = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
                     "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
                     "RS","RO","RR","SC","SP","SE","TO"}
        df["customer_state"] = df["customer_state"].str.upper().str.strip()
        df.loc[~df["customer_state"].isin(BR_STATES), "customer_state"] = "UNKNOWN"

    # Payment value cleanup
    if "payment_value" in df.columns:
        df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0)
        df.loc[df["payment_value"] < 0, "payment_value"] = 0

    os.makedirs("outputs/silver", exist_ok=True)
    out = f"outputs/silver/SILVER_{dataset.upper()}_CLEAN.csv"
    df.to_csv(out, index=False)
    print(f"SILVER | {dataset} | {len(df):,} clean rows → {out}")
    ctx["ti"].xcom_push(key=f"{dataset}_silver_rows", value=len(df))


def _mask_pii(dataset: str, **ctx):
    """Apply PII masking (SHA-256 HIGH, partial mask MEDIUM) to Silver."""
    import pandas as pd
    import hashlib
    import os

    path = f"outputs/silver/SILVER_{dataset.upper()}_CLEAN.csv"
    df   = pd.read_csv(path)

    PII_LEVELS = {
        "customers": {
            "customer_id":        "MEDIUM",
            "customer_unique_id": "MEDIUM",
        },
        "orders": {
            "order_id":    "MEDIUM",
            "customer_id": "MEDIUM",
        },
        "payments": {
            "order_id": "MEDIUM",
        },
        "products": {},
    }

    def sha256(v):
        return hashlib.sha256(str(v).encode()).hexdigest()

    def partial(v, n=4):
        s = str(v)
        return s[:n] + "*" * max(0, len(s) - n)

    for col, level in PII_LEVELS.get(dataset, {}).items():
        if col in df.columns:
            if level == "HIGH":
                df[col] = df[col].astype(str).apply(sha256)
            elif level == "MEDIUM":
                df[col] = df[col].astype(str).apply(partial)

    out = f"outputs/silver/SILVER_{dataset.upper()}_MASKED.csv"
    df.to_csv(out, index=False)
    print(f"PII MASK | {dataset} | {len(df):,} rows masked → {out}")


def _compute_gold_kpis(dataset: str, **ctx):
    """Aggregate Silver clean data into Gold KPI table."""
    import pandas as pd
    import json
    import os

    path = f"outputs/silver/SILVER_{dataset.upper()}_CLEAN.csv"
    df   = pd.read_csv(path)

    kpis = {"dataset": dataset, "total_rows": len(df)}

    if dataset == "customers":
        kpis["unique_states"] = int(df["customer_state"].nunique()) if "customer_state" in df.columns else 0
        kpis["unique_cities"] = int(df["customer_city"].nunique()) if "customer_city" in df.columns else 0
        if "customer_state" in df.columns:
            kpis["top_state"] = df["customer_state"].value_counts().idxmax()

    elif dataset == "payments":
        if "payment_value" in df.columns:
            v = pd.to_numeric(df["payment_value"], errors="coerce")
            kpis["total_revenue"]     = round(float(v.sum()), 2)
            kpis["avg_payment"]       = round(float(v.mean()), 2)
            kpis["max_payment"]       = round(float(v.max()), 2)
        if "payment_type" in df.columns:
            kpis["top_payment_type"]  = df["payment_type"].value_counts().idxmax()

    elif dataset == "orders":
        if "order_status" in df.columns:
            kpis["delivered_pct"] = round(
                (df["order_status"] == "delivered").sum() / len(df) * 100, 1
            )
            kpis["top_status"] = df["order_status"].value_counts().idxmax()

    elif dataset == "products":
        if "product_category_name" in df.columns:
            kpis["unique_categories"] = int(df["product_category_name"].nunique())
            kpis["top_category"] = df["product_category_name"].value_counts().idxmax()

    os.makedirs("outputs/gold", exist_ok=True)
    gold_df = pd.DataFrame([{"kpi_key": k, "kpi_value": str(v)} for k, v in kpis.items()])
    out = f"outputs/gold/GOLD_{dataset.upper()}_KPIS.csv"
    gold_df.to_csv(out, index=False)

    # Also save JSON for easy reading
    with open(f"outputs/gold/GOLD_{dataset.upper()}_KPIS.json", "w") as f:
        json.dump(kpis, f, indent=2, default=str)

    print(f"GOLD KPI | {dataset} | {len(kpis)} KPIs → {out}")
    print(f"GOLD KPI | {dataset} | {json.dumps(kpis, default=str)}")


def _self_healing_task(dataset: str, **ctx):
    """
    Self-healing agent task — called on validate_quality failure.
    Reads failed_checks from XCom, applies pandas fixes, clears issues.
    """
    import pandas as pd
    import os

    ti           = ctx["ti"]
    failed_checks = ti.xcom_pull(key=f"{dataset}_failed_checks", task_ids=f"validate_{dataset}") or []

    print(f"HEAL AGENT | {dataset} | Fixing {len(failed_checks)} issue(s)...")

    path = f"outputs/bronze/RAW_OLIST_{dataset.upper()}.csv"
    df   = pd.read_csv(path)

    for check in failed_checks:
        col   = check.get("column", "")
        issue = check.get("issue", "")
        print(f"  Fixing: {col} — {issue}")

        if "null" in issue.lower():
            if col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].fillna("UNKNOWN")
                else:
                    df[col] = df[col].fillna(0)

        elif "state" in col.lower():
            BR_STATES = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
                         "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
                         "RS","RO","RR","SC","SP","SE","TO"}
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper().str.strip()
                df.loc[~df[col].isin(BR_STATES), col] = "UNKNOWN"

        elif "payment_type" in col.lower():
            valid = {"credit_card","boleto","voucher","debit_card"}
            if col in df.columns:
                df.loc[~df[col].astype(str).isin(valid), col] = "boleto"

        elif "negative" in issue.lower() and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").abs()

    df.to_csv(path, index=False)
    print(f"HEAL AGENT | {dataset} | Fixed and saved → {path}")


# ── Build DAG ─────────────────────────────────────────────────────
with DAG(
    dag_id          = "olist_bronze_silver_gold_pipeline",
    default_args    = DEFAULT_ARGS,
    description     = "Olist E-Commerce Bronze→Silver→Gold ETL with Self-Healing Agent",
    schedule_interval = "0 2 * * *",  # Daily at 02:00 UTC
    catchup         = False,
    max_active_runs = 1,
    tags            = ["olist", "etl", "data-quality", "self-healing", "medallion"],
) as dag:

    start = EmptyOperator(task_id="pipeline_start")
    end   = EmptyOperator(task_id="pipeline_end", trigger_rule=TriggerRule.ALL_DONE)

    for dataset in DATASETS:
        load = PythonOperator(
            task_id       = f"load_bronze_{dataset}",
            python_callable = _load_bronze,
            op_kwargs     = {"dataset": dataset},
        )

        validate = PythonOperator(
            task_id         = f"validate_{dataset}",
            python_callable = _validate_quality,
            op_kwargs       = {"dataset": dataset},
        )

        heal = PythonOperator(
            task_id         = f"self_heal_{dataset}",
            python_callable = _self_healing_task,
            op_kwargs       = {"dataset": dataset},
            trigger_rule    = TriggerRule.ONE_FAILED,
        )

        silver = PythonOperator(
            task_id         = f"transform_silver_{dataset}",
            python_callable = _transform_to_silver,
            op_kwargs       = {"dataset": dataset},
            trigger_rule    = TriggerRule.ONE_SUCCESS,
        )

        mask = PythonOperator(
            task_id         = f"mask_pii_{dataset}",
            python_callable = _mask_pii,
            op_kwargs       = {"dataset": dataset},
        )

        gold = PythonOperator(
            task_id         = f"compute_gold_{dataset}",
            python_callable = _compute_gold_kpis,
            op_kwargs       = {"dataset": dataset},
        )

        # DAG edges: load → validate → [heal →] silver → mask → gold → end
        start >> load >> validate
        validate >> silver           # happy path
        validate >> heal >> silver   # heal path (trigger_rule=ONE_FAILED)
        silver >> mask >> gold >> end
