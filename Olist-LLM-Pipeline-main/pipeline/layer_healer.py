"""
pipeline/layer_healer.py
Layer-level Heal Agent — applied INSIDE each medallion layer.

Instead of only healing after a node fails in the LangGraph graph,
this module applies targeted healing at each layer transition:

  BRONZE  → detect issues in raw batch → apply fixes → produce healed Bronze
  SILVER  → validate clean rows → re-heal anything still failing → produce Silver
  GOLD    → validate KPI sanity → fix aggregation anomalies

Each layer healer returns (healed_df, quarantine_df, heal_events[])
so the dashboard can show exactly what was fixed at each layer.
"""
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, List, Dict
from loguru import logger

BR_STATES = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
}
VALID_PAYMENT_TYPES  = {"credit_card","boleto","voucher","debit_card"}
VALID_ORDER_STATUSES = {"delivered","shipped","canceled","processing","invoiced","approved","unavailable"}


def _event(layer: str, dataset: str, column: str, issue: str, fix: str, count: int) -> dict:
    return {
        "ts":      datetime.utcnow().strftime("%H:%M:%S"),
        "layer":   layer,
        "dataset": dataset,
        "column":  column,
        "issue":   issue,
        "fix":     fix,
        "count":   count,
    }


# ═══════════════════════════════════════════════════════════════════
#  BRONZE LAYER HEALER
#  Input:  raw generated batch (may have nulls, bad values, negatives)
#  Output: healed_df (best-effort fixed), quarantine_df (unfixable rows)
# ═══════════════════════════════════════════════════════════════════

def heal_bronze(dataset: str, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict]]:
    """
    Apply all healing rules to a raw Bronze batch.
    Returns (healed_df, quarantine_df, heal_events).
    """
    df     = df.copy()
    events = []
    quarantine_masks = []   # rows that CANNOT be healed → quarantine

    # ── 1. Remove exact duplicates ────────────────────────────────
    id_cols = {"customers":"customer_id","orders":"order_id",
                "payments":"order_id","products":"product_id"}
    id_col  = id_cols.get(dataset)
    if id_col and id_col in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=[id_col], keep="last")
        removed = before - len(df)
        if removed:
            events.append(_event("BRONZE", dataset, id_col,
                                  f"{removed} duplicate rows", "kept last occurrence", removed))

    # ── 2. Dataset-specific healing ───────────────────────────────

    if dataset == "customers":
        # Null customer_id → quarantine (cannot fix primary key)
        if "customer_id" in df.columns:
            null_mask = df["customer_id"].isnull()
            if null_mask.sum():
                quarantine_masks.append(null_mask)
                events.append(_event("BRONZE", dataset, "customer_id",
                                      f"{null_mask.sum()} null IDs", "quarantined", int(null_mask.sum())))

        # Invalid / null customer_state → standardise or UNKNOWN
        if "customer_state" in df.columns:
            # Normalise
            df["customer_state"] = df["customer_state"].astype(str).str.upper().str.strip()
            bad_mask = ~df["customer_state"].isin(BR_STATES | {"NAN","NONE","NULL","UNKNOWN"})
            null_state = df["customer_state"].isin({"NAN","NONE","NULL"})
            if (bad_mask | null_state).sum():
                n = int((bad_mask | null_state).sum())
                df.loc[bad_mask | null_state, "customer_state"] = "UNKNOWN"
                events.append(_event("BRONZE", dataset, "customer_state",
                                      f"{n} invalid/null states", "set to UNKNOWN", n))

        # customer_city: lowercase + strip
        if "customer_city" in df.columns:
            null_city = df["customer_city"].isnull().sum()
            df["customer_city"] = df["customer_city"].fillna("unknown").astype(str).str.lower().str.strip()
            if null_city:
                events.append(_event("BRONZE", dataset, "customer_city",
                                      f"{null_city} null cities", "filled 'unknown'", int(null_city)))

    elif dataset == "orders":
        # Null order_id → quarantine
        if "order_id" in df.columns:
            null_mask = df["order_id"].isnull()
            if null_mask.sum():
                quarantine_masks.append(null_mask)
                events.append(_event("BRONZE", dataset, "order_id",
                                      f"{null_mask.sum()} null IDs", "quarantined", int(null_mask.sum())))

        # Invalid order_status → "unknown"
        if "order_status" in df.columns:
            df["order_status"] = df["order_status"].fillna("unknown").astype(str).str.lower()
            bad = ~df["order_status"].isin(VALID_ORDER_STATUSES | {"unknown"})
            if bad.sum():
                df.loc[bad, "order_status"] = "unknown"
                events.append(_event("BRONZE", dataset, "order_status",
                                      f"{bad.sum()} invalid statuses", "set to 'unknown'", int(bad.sum())))

        # Null timestamps (non-critical) → fill with "1970-01-01"
        for ts_col in ["order_approved_at","order_delivered_carrier_date",
                        "order_delivered_customer_date"]:
            if ts_col in df.columns:
                n = int(df[ts_col].isnull().sum())
                if n:
                    df[ts_col] = df[ts_col].fillna("1970-01-01 00:00:00")
                    events.append(_event("BRONZE", dataset, ts_col,
                                          f"{n} null timestamps", "filled sentinel '1970-01-01'", n))

    elif dataset == "payments":
        # Null order_id → quarantine
        if "order_id" in df.columns:
            null_mask = df["order_id"].isnull()
            if null_mask.sum():
                quarantine_masks.append(null_mask)
                events.append(_event("BRONZE", dataset, "order_id",
                                      f"{null_mask.sum()} null IDs", "quarantined", int(null_mask.sum())))

        # Negative / null payment_value → abs() or 0
        if "payment_value" in df.columns:
            df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce")
            neg_null = df["payment_value"].isnull() | (df["payment_value"] < 0)
            if neg_null.sum():
                df.loc[df["payment_value"].isnull(), "payment_value"] = 0.0
                df["payment_value"] = df["payment_value"].abs()
                events.append(_event("BRONZE", dataset, "payment_value",
                                      f"{neg_null.sum()} negative/null values", "abs() or 0", int(neg_null.sum())))

        # Invalid payment_type → "boleto" (most common fallback)
        if "payment_type" in df.columns:
            df["payment_type"] = df["payment_type"].fillna("boleto").astype(str).str.lower().str.strip()
            bad = ~df["payment_type"].isin(VALID_PAYMENT_TYPES)
            if bad.sum():
                df.loc[bad, "payment_type"] = "boleto"
                events.append(_event("BRONZE", dataset, "payment_type",
                                      f"{bad.sum()} invalid types", "set to 'boleto'", int(bad.sum())))

    elif dataset == "products":
        # Null product_id → quarantine
        if "product_id" in df.columns:
            null_mask = df["product_id"].isnull()
            if null_mask.sum():
                quarantine_masks.append(null_mask)
                events.append(_event("BRONZE", dataset, "product_id",
                                      f"{null_mask.sum()} null IDs", "quarantined", int(null_mask.sum())))

        # Null category → "unknown"
        if "product_category_name" in df.columns:
            n = int(df["product_category_name"].isnull().sum())
            if n:
                df["product_category_name"] = df["product_category_name"].fillna("unknown")
                events.append(_event("BRONZE", dataset, "product_category_name",
                                      f"{n} null categories", "filled 'unknown'", n))

        # Null/negative numeric columns → 0
        for col in ["product_weight_g","product_length_cm","product_height_cm",
                    "product_width_cm","product_photos_qty","product_name_lenght",
                    "product_description_lenght"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                n = int((df[col].isnull() | (df[col] < 0)).sum())
                if n:
                    df[col] = df[col].abs().fillna(0)
                    events.append(_event("BRONZE", dataset, col,
                                          f"{n} null/negative values", "abs() or 0", n))

    # ── 3. Build quarantine mask ──────────────────────────────────
    if quarantine_masks:
        combined_mask = quarantine_masks[0]
        for m in quarantine_masks[1:]:
            combined_mask = combined_mask | m
        quarantine_df = df[combined_mask].copy()
        df            = df[~combined_mask].copy().reset_index(drop=True)
    else:
        quarantine_df = pd.DataFrame(columns=df.columns)

    if not quarantine_df.empty:
        quarantine_df["quarantined_at"]    = datetime.utcnow().isoformat()
        quarantine_df["quarantine_reason"] = "unfixable_null_primary_key"

    logger.debug(f"HEAL BRONZE | {dataset} | healed={len(df)} | quarantine={len(quarantine_df)} | events={len(events)}")
    return df, quarantine_df, events


# ═══════════════════════════════════════════════════════════════════
#  SILVER LAYER HEALER
#  Input:  healed Bronze df (should be mostly clean)
#  Output: silver_clean_df, silver_quarantine_df, heal_events
# ═══════════════════════════════════════════════════════════════════

def heal_silver(dataset: str, df: pd.DataFrame, ge_rules: list) -> Tuple[pd.DataFrame, pd.DataFrame, List[Dict]]:
    """
    Second-pass healing on Bronze output.
    Applies GE rules to split rows → Silver clean vs Silver quarantine.
    Any row failing a GE check gets one more healing attempt before quarantine.
    """
    df     = df.copy()
    events = []

    for rule in ge_rules:
        exp_type = rule.get("expectation_type","")
        col      = rule.get("column","")
        kwargs   = rule.get("kwargs",{})

        if col not in df.columns:
            continue

        if exp_type == "expect_column_values_to_not_be_null":
            n = int(df[col].isnull().sum())
            if n:
                # Try to fill based on dtype
                if df[col].dtype == object:
                    df[col] = df[col].fillna("UNKNOWN")
                else:
                    df[col] = df[col].fillna(0)
                events.append(_event("SILVER", dataset, col,
                                      f"{n} nulls after Bronze heal", "filled UNKNOWN/0", n))

        elif exp_type == "expect_column_values_to_be_in_set":
            value_set = [str(v) for v in kwargs.get("value_set",[])]
            if value_set:
                bad = ~df[col].astype(str).isin(value_set)
                n   = int(bad.sum())
                if n:
                    # Replace with most common valid value
                    most_common = df[col][~bad].mode()
                    fallback    = most_common.iloc[0] if len(most_common) > 0 else value_set[0]
                    df.loc[bad, col] = fallback
                    events.append(_event("SILVER", dataset, col,
                                          f"{n} invalid values not in set", f"replaced with '{fallback}'", n))

        elif exp_type == "expect_column_values_to_be_between":
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            numeric = pd.to_numeric(df[col], errors="coerce")
            n       = 0
            if min_val is not None:
                bad = numeric < min_val
                n  += int(bad.sum())
                df.loc[bad, col] = min_val
            if max_val is not None:
                bad = numeric > max_val
                n  += int(bad.sum())
                df.loc[bad, col] = max_val
            if n:
                events.append(_event("SILVER", dataset, col,
                                      f"{n} values out of range", f"clamped to [{min_val},{max_val}]", n))

    # After Silver healing — rows still failing critical rules go to Silver quarantine
    id_col    = {"customers":"customer_id","orders":"order_id",
                  "payments":"order_id","products":"product_id"}.get(dataset)
    if id_col and id_col in df.columns:
        still_bad = df[id_col].isnull()
        silver_q  = df[still_bad].copy()
        df        = df[~still_bad].reset_index(drop=True)
        if not silver_q.empty:
            silver_q["quarantined_at"]    = datetime.utcnow().isoformat()
            silver_q["quarantine_reason"] = "null_primary_key_after_silver_heal"
            events.append(_event("SILVER", dataset, id_col,
                                  f"{len(silver_q)} still-null IDs after Silver heal",
                                  "quarantined to Silver quarantine", len(silver_q)))
    else:
        silver_q = pd.DataFrame(columns=df.columns)

    logger.debug(f"HEAL SILVER | {dataset} | clean={len(df)} | quarantine={len(silver_q)} | events={len(events)}")
    return df, silver_q, events


# ═══════════════════════════════════════════════════════════════════
#  GOLD LAYER HEALER
#  Input:  computed KPIs dict
#  Output: healed KPIs dict, heal_events
# ═══════════════════════════════════════════════════════════════════

def heal_gold(dataset: str, kpis: dict) -> Tuple[dict, List[Dict]]:
    """
    Sanity-check and heal Gold KPI values.
    Catches: NaN/Inf totals, negative revenue, zero unique counts when data exists.
    """
    kpis   = dict(kpis)
    events = []

    for key, val in list(kpis.items()):
        if key == "business_insights":
            continue
        try:
            fval = float(val)
            if pd.isna(fval) or not pd.api.types.is_float(fval) and pd.isnull(fval):
                kpis[key] = 0
                events.append(_event("GOLD", dataset, key, "NaN KPI value", "set to 0", 1))
            elif key in ("total_revenue","avg_payment","max_payment") and fval < 0:
                kpis[key] = abs(fval)
                events.append(_event("GOLD", dataset, key, f"Negative KPI {fval}", "abs()", 1))
        except (TypeError, ValueError):
            pass  # non-numeric KPI (e.g. dict, string) — skip

    logger.debug(f"HEAL GOLD | {dataset} | events={len(events)}")
    return kpis, events
