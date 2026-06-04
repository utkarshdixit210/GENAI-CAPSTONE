"""
agents/nodes/transform.py
Node 6 — Transform (Bronze → Silver)
Splits data into CLEAN (Silver) and QUARANTINE tables based on GE rules.
Applies pandas-native cleaning using the validated rules.
Saves outputs via MCP to outputs/silver/ and outputs/bronze/.
Dataset: Olist Brazilian E-Commerce
"""
import pandas as pd
from agents.state import AgentState
from tools.snowflake_mcp_tool import mcp_tool
from config.settings import PipelineConfig
from loguru import logger

DATASET_TABLE_MAP = {
    "customers": PipelineConfig.RAW_TABLE_CUSTOMERS,
    "orders":    PipelineConfig.RAW_TABLE_ORDERS,
    "payments":  PipelineConfig.RAW_TABLE_PAYMENTS,
    "products":  PipelineConfig.RAW_TABLE_PRODUCTS,
}


def _build_clean_mask(df: pd.DataFrame, ge_rules: list) -> pd.Series:
    """
    Build a boolean mask: True = row passes all rules, False = quarantine.
    Only applies row-level filters for rules that can be expressed that way.
    """
    mask = pd.Series([True] * len(df), index=df.index)

    for rule in ge_rules:
        exp_type = rule.get("expectation_type", "")
        col      = rule.get("column", "")
        kwargs   = rule.get("kwargs", {})

        if col not in df.columns:
            continue

        try:
            if exp_type == "expect_column_values_to_not_be_null":
                mask &= df[col].notna()

            elif exp_type == "expect_column_values_to_be_in_set":
                value_set = [str(v) for v in kwargs.get("value_set", [])]
                if value_set:
                    mask &= df[col].astype(str).isin(value_set)

            elif exp_type == "expect_column_values_to_be_between":
                min_val = kwargs.get("min_value")
                max_val = kwargs.get("max_value")
                numeric = pd.to_numeric(df[col], errors="coerce")
                if min_val is not None:
                    mask &= (numeric >= min_val) | numeric.isna()
                if max_val is not None:
                    mask &= (numeric <= max_val) | numeric.isna()

            elif exp_type == "expect_column_values_to_match_regex":
                pattern = kwargs.get("regex", kwargs.get("pattern", ".*"))
                mask &= df[col].astype(str).str.match(pattern, na=False)

        except Exception as rule_err:
            logger.warning(f"TRANSFORM | Mask error for {exp_type}/{col}: {rule_err}")

    return mask


def _apply_olist_healing(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    """
    Apply dataset-specific basic healing before splitting.
    This covers the healer.py logic from the original project,
    now applied in a structured way per dataset.
    """
    df = df.copy()

    # Remove full duplicates
    before = len(df)
    if "customer_id" in df.columns:
        df = df.drop_duplicates(subset=["customer_id"])
    elif "order_id" in df.columns:
        df = df.drop_duplicates(subset=["order_id"])
    elif "product_id" in df.columns:
        df = df.drop_duplicates(subset=["product_id"])
    else:
        df = df.drop_duplicates()
    removed = before - len(df)
    if removed:
        logger.info(f"TRANSFORM | Removed {removed} duplicate rows")

    # Fill nulls
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("UNKNOWN")
        else:
            df[col] = df[col].fillna(0)

    # Customers — state standardisation
    if "customer_state" in df.columns:
        VALID_STATES = [
            "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
            "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
            "RS","RO","RR","SC","SP","SE","TO",
        ]
        STATE_MAP = {"SAOPAULO": "SP", "S": "SP", "sp": "SP", "rj": "RJ", "mg": "MG"}
        df["customer_state"] = (
            df["customer_state"].astype(str).str.upper().replace(STATE_MAP)
        )
        df.loc[~df["customer_state"].isin(VALID_STATES), "customer_state"] = "UNKNOWN"

    if "customer_city" in df.columns:
        df["customer_city"] = df["customer_city"].astype(str).str.lower().str.strip()

    # Payments — numeric cleanup
    if "payment_value" in df.columns:
        df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0.0)
        df.loc[df["payment_value"] < 0, "payment_value"] = 0.0

    # Products — numeric cleanup
    for numeric_col in ["product_weight_g", "product_length_cm", "product_height_cm",
                         "product_width_cm", "product_photos_qty"]:
        if numeric_col in df.columns:
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce").fillna(0.0)

    return df


def run(state: AgentState) -> AgentState:
    state["current_node"] = "transform"
    dataset  = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    table    = DATASET_TABLE_MAP.get(dataset, PipelineConfig.RAW_TABLE_CUSTOMERS)
    ge_rules = state.get("ge_rules", [])

    logger.info(f"TRANSFORM | Starting | dataset={dataset}")

    try:
        # Fetch raw data via MCP
        df = mcp_tool.call("snowflake_fetch_data", {"table": table})
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        logger.info(f"TRANSFORM | Loaded {len(df):,} raw rows")

        # Apply basic dataset-specific healing
        df = _apply_olist_healing(df, dataset)

        # Build clean/quarantine split from GE rules
        if ge_rules:
            clean_mask      = _build_clean_mask(df, ge_rules)
            clean_df        = df[clean_mask].copy().reset_index(drop=True)
            quarantine_df   = df[~clean_mask].copy().reset_index(drop=True)
        else:
            # No rules — treat everything as clean
            clean_df      = df.copy()
            quarantine_df = pd.DataFrame(columns=df.columns)

        clean_count      = len(clean_df)
        quarantine_count = len(quarantine_df)

        logger.info(
            f"TRANSFORM | Split complete | clean={clean_count:,} | "
            f"quarantine={quarantine_count:,}"
        )

        # Write Silver (clean) via MCP
        clean_result = mcp_tool.call("snowflake_write_df", {
            "df":    clean_df,
            "table": f"SILVER_{dataset.upper()}_CLEAN",
            "layer": "silver",
        })

        # Write Quarantine (bronze bad rows) via MCP
        if quarantine_count > 0:
            quarantine_df["quarantined_at"]    = pd.Timestamp.utcnow().isoformat()
            quarantine_df["quarantine_reason"] = "failed_quality_checks"
            mcp_tool.call("snowflake_write_df", {
                "df":    quarantine_df,
                "table": f"BRONZE_{dataset.upper()}_QUARANTINE",
                "layer": "bronze",
            })

        state["clean_row_count"]    = clean_count
        state["quarantine_count"]   = quarantine_count
        state["clean_df_path"]      = clean_result.get("path", "")
        state["quarantine_df_path"] = f"outputs/bronze/BRONZE_{dataset.upper()}_QUARANTINE.csv"

        logger.success(
            f"TRANSFORM | Done | clean={clean_count:,} rows → Silver | "
            f"quarantine={quarantine_count:,} rows → Bronze"
        )
        state["node_status"]["transform"] = "pass"

    except Exception as e:
        logger.error(f"TRANSFORM | FAILED: {e}")
        state["node_errors"]["transform"] = str(e)
        state["node_status"]["transform"] = "fail"

    return state
