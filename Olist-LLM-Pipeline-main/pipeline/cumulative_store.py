"""
pipeline/cumulative_store.py
Cumulative data store — merges each new processed batch into the
running totals for Bronze, Silver, Gold layers.

Design:
  - Bronze  = ALL raw rows ever seen (append)
  - Silver  = ALL clean rows ever healed (append, dedup by ID)
  - Gold    = Recomputed KPIs from the FULL Silver history each batch
  - Quarantine = ALL bad rows ever seen (append)

This gives the "previous data + new data → updated KPIs" behaviour requested.
"""
import os
import pandas as pd
from pathlib import Path
from loguru import logger

STORE_DIR = "outputs/cumulative"
os.makedirs(f"{STORE_DIR}/bronze", exist_ok=True)
os.makedirs(f"{STORE_DIR}/silver", exist_ok=True)
os.makedirs(f"{STORE_DIR}/gold",   exist_ok=True)


def _path(layer: str, dataset: str) -> str:
    return f"{STORE_DIR}/{layer}/{dataset}.csv"


def _load(layer: str, dataset: str) -> pd.DataFrame:
    p = _path(layer, dataset)
    if Path(p).exists():
        try:
            return pd.read_csv(p)
        except Exception:
            pass
    return pd.DataFrame()


def _save(df: pd.DataFrame, layer: str, dataset: str):
    df.to_csv(_path(layer, dataset), index=False)


# ── Bronze layer: append all raw rows ────────────────────────────
def append_bronze(dataset: str, new_df: pd.DataFrame) -> pd.DataFrame:
    """Append new raw batch to cumulative Bronze store."""
    existing = _load("bronze", dataset)
    combined = pd.concat([existing, new_df], ignore_index=True) if not existing.empty else new_df.copy()
    _save(combined, "bronze", dataset)
    logger.debug(f"STORE | Bronze/{dataset} | total={len(combined):,}")
    return combined


# ── Silver layer: append clean rows, deduplicate by primary key ──
ID_COLS = {
    "customers": "customer_id",
    "orders":    "order_id",
    "payments":  "order_id",
    "products":  "product_id",
}

def append_silver(dataset: str, clean_df: pd.DataFrame, quarantine_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge new clean rows into cumulative Silver.
    Dedup by primary key — newer batch wins on conflict.
    """
    existing = _load("silver", dataset)
    id_col   = ID_COLS.get(dataset)

    if not existing.empty and not clean_df.empty:
        if id_col and id_col in clean_df.columns and id_col in existing.columns:
            # New batch wins: remove existing rows whose IDs appear in new batch
            existing = existing[~existing[id_col].isin(clean_df[id_col].dropna())]
        combined = pd.concat([existing, clean_df], ignore_index=True)
    elif not clean_df.empty:
        combined = clean_df.copy()
    else:
        combined = existing.copy() if not existing.empty else pd.DataFrame()

    if not combined.empty:
        _save(combined, "silver", dataset)
    logger.debug(f"STORE | Silver/{dataset} | total={len(combined):,}")

    # Append quarantine rows too
    if not quarantine_df.empty:
        existing_q = _load("bronze", f"{dataset}_quarantine")
        combined_q = pd.concat([existing_q, quarantine_df], ignore_index=True) \
                     if not existing_q.empty else quarantine_df.copy()
        _save(combined_q, "bronze", f"{dataset}_quarantine")

    return combined


# ── Gold layer: recompute KPIs from full Silver history ───────────
def recompute_gold(dataset: str) -> dict:
    """
    Read the FULL cumulative Silver for this dataset and recompute KPIs.
    This is called after every batch so Gold always reflects ALL history.
    """
    df = _load("silver", dataset)
    if df.empty:
        return {}

    kpis: dict = {"dataset": dataset, "total_silver_rows": len(df)}

    if dataset == "customers":
        kpis["unique_customers"]  = int(df["customer_unique_id"].nunique()) if "customer_unique_id" in df.columns else 0
        kpis["states_covered"]    = int(df["customer_state"].nunique())     if "customer_state"     in df.columns else 0
        kpis["cities_covered"]    = int(df["customer_city"].nunique())      if "customer_city"      in df.columns else 0
        if "customer_state" in df.columns:
            top = df["customer_state"].value_counts().head(5).to_dict()
            kpis["top_states"] = top

    elif dataset == "orders":
        if "order_status" in df.columns:
            kpis["delivered_pct"]  = round((df["order_status"] == "delivered").sum() / len(df) * 100, 1)
            kpis["canceled_pct"]   = round((df["order_status"] == "canceled").sum()  / len(df) * 100, 1)
            kpis["status_counts"]  = df["order_status"].value_counts().to_dict()
        kpis["unique_customers"]   = int(df["customer_id"].nunique()) if "customer_id" in df.columns else 0

    elif dataset == "payments":
        if "payment_value" in df.columns:
            v = pd.to_numeric(df["payment_value"], errors="coerce")
            kpis["total_revenue"]  = round(float(v.sum()), 2)
            kpis["avg_payment"]    = round(float(v.mean()), 2)
            kpis["max_payment"]    = round(float(v.max()), 2)
        if "payment_type" in df.columns:
            kpis["type_breakdown"] = df["payment_type"].value_counts().to_dict()

    elif dataset == "products":
        if "product_category_name" in df.columns:
            kpis["unique_categories"] = int(df["product_category_name"].nunique())
            kpis["top_categories"]    = df["product_category_name"].value_counts().head(10).to_dict()
        if "product_weight_g" in df.columns:
            kpis["avg_weight_g"] = round(float(pd.to_numeric(df["product_weight_g"], errors="coerce").mean()), 2)

    # Save Gold CSV
    gold_df = pd.DataFrame([{"kpi_key": k, "kpi_value": str(v)} for k, v in kpis.items()])
    os.makedirs(f"{STORE_DIR}/gold", exist_ok=True)
    gold_df.to_csv(_path("gold", dataset), index=False)
    logger.debug(f"STORE | Gold/{dataset} | {len(kpis)} KPIs from {len(df):,} Silver rows")
    return kpis


# ── Counts for dashboard ──────────────────────────────────────────
def get_layer_counts() -> dict:
    """Return row counts for all datasets across all layers."""
    counts = {}
    for dataset in ["customers", "orders", "payments", "products"]:
        counts[dataset] = {
            "bronze":     len(_load("bronze", dataset)),
            "silver":     len(_load("silver", dataset)),
            "quarantine": len(_load("bronze", f"{dataset}_quarantine")),
        }
    return counts


def reset_store():
    """Clear all cumulative data — fresh start."""
    import shutil
    if Path(STORE_DIR).exists():
        shutil.rmtree(STORE_DIR)
    os.makedirs(f"{STORE_DIR}/bronze", exist_ok=True)
    os.makedirs(f"{STORE_DIR}/silver", exist_ok=True)
    os.makedirs(f"{STORE_DIR}/gold",   exist_ok=True)
    logger.info("STORE | Cumulative store reset.")
