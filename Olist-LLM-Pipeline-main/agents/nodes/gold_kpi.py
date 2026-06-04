"""
agents/nodes/gold_kpi.py
Node 8 — Gold KPI Generator  ★ NEW NODE (not in B1)
Computes business KPIs from Silver clean data → Gold layer.
Also uses LLM to generate business insights from KPIs.
This implements the Bronze → Silver → Gold medallion architecture
that was present in GEN_AI_Capstone but missing as a proper node.
Dataset: Olist Brazilian E-Commerce
"""
import json
import pandas as pd
from pathlib import Path
from agents.state import AgentState
from tools.snowflake_mcp_tool import mcp_tool
from tools.llm_client import llm_client
from config.settings import PipelineConfig
from loguru import logger


INSIGHTS_PROMPT = """
You are a senior data analyst reviewing Olist Brazilian E-Commerce metrics.

Dataset: {dataset}
KPI Summary:
{kpis}

Generate exactly 3 concise, actionable business insights from these metrics.
Focus on: customer behaviour, data quality, revenue patterns, or geographic trends.
Keep each insight to 1-2 sentences. Be specific and use the numbers provided.

Format as JSON array:
[
  {{"insight": "...", "category": "quality|revenue|customers|geography"}},
  ...
]
Respond ONLY with JSON. No markdown.
"""


def _compute_customers_kpis(df: pd.DataFrame) -> dict:
    kpis = {
        "total_customers":     int(df.shape[0]),
        "unique_customers":    int(df["customer_unique_id"].nunique()) if "customer_unique_id" in df.columns else 0,
        "states_covered":      int(df["customer_state"].nunique()) if "customer_state" in df.columns else 0,
        "cities_covered":      int(df["customer_city"].nunique()) if "customer_city" in df.columns else 0,
        "top_states":          df["customer_state"].value_counts().head(5).to_dict() if "customer_state" in df.columns else {},
        "top_cities":          df["customer_city"].value_counts().head(5).to_dict() if "customer_city" in df.columns else {},
        "null_rate_pct":       round(df.isnull().mean().mean() * 100, 2),
    }
    return kpis


def _compute_orders_kpis(df: pd.DataFrame) -> dict:
    kpis = {
        "total_orders":        int(df.shape[0]),
        "unique_customers":    int(df["customer_id"].nunique()) if "customer_id" in df.columns else 0,
        "status_breakdown":    df["order_status"].value_counts().to_dict() if "order_status" in df.columns else {},
        "null_rate_pct":       round(df.isnull().mean().mean() * 100, 2),
    }
    return kpis


def _compute_payments_kpis(df: pd.DataFrame) -> dict:
    kpis: dict = {"total_payments": int(df.shape[0])}
    if "payment_value" in df.columns:
        vals = pd.to_numeric(df["payment_value"], errors="coerce")
        kpis["total_revenue"]      = round(float(vals.sum()), 2)
        kpis["avg_payment_value"]  = round(float(vals.mean()), 2)
        kpis["max_payment_value"]  = round(float(vals.max()), 2)
        kpis["min_payment_value"]  = round(float(vals.min()), 2)
    if "payment_type" in df.columns:
        kpis["payment_type_breakdown"] = df["payment_type"].value_counts().to_dict()
    if "payment_installments" in df.columns:
        inst = pd.to_numeric(df["payment_installments"], errors="coerce")
        kpis["avg_installments"] = round(float(inst.mean()), 2)
    kpis["null_rate_pct"] = round(df.isnull().mean().mean() * 100, 2)
    return kpis


def _compute_products_kpis(df: pd.DataFrame) -> dict:
    kpis = {
        "total_products":      int(df.shape[0]),
        "unique_categories":   int(df["product_category_name"].nunique()) if "product_category_name" in df.columns else 0,
        "top_categories":      df["product_category_name"].value_counts().head(10).to_dict() if "product_category_name" in df.columns else {},
        "null_rate_pct":       round(df.isnull().mean().mean() * 100, 2),
    }
    if "product_weight_g" in df.columns:
        w = pd.to_numeric(df["product_weight_g"], errors="coerce")
        kpis["avg_weight_g"] = round(float(w.mean()), 2)
    return kpis


KPI_COMPUTERS = {
    "customers": _compute_customers_kpis,
    "orders":    _compute_orders_kpis,
    "payments":  _compute_payments_kpis,
    "products":  _compute_products_kpis,
}


def run(state: AgentState) -> AgentState:
    state["current_node"] = "gold_kpi"
    dataset = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)

    logger.info(f"GOLD_KPI | Starting | dataset={dataset}")

    try:
        # Load Silver clean dataset
        clean_table = f"SILVER_{dataset.upper()}_CLEAN"
        df = mcp_tool.call("snowflake_read_table", {
            "table": clean_table,
            "layer": "silver",
        })

        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        logger.info(f"GOLD_KPI | Loaded {len(df):,} Silver rows")

        # Compute KPIs
        compute_fn = KPI_COMPUTERS.get(dataset, _compute_customers_kpis)
        kpis = compute_fn(df)

        logger.info(f"GOLD_KPI | KPIs computed: {list(kpis.keys())}")

        # LLM generates business insights
        try:
            insight_prompt = INSIGHTS_PROMPT.format(
                dataset = dataset,
                kpis    = json.dumps(kpis, indent=2, default=str),
            )
            raw_insights = llm_client.invoke(insight_prompt)
            raw_insights = raw_insights.strip()
            if raw_insights.startswith("```"):
                raw_insights = raw_insights.split("```")[1]
                if raw_insights.startswith("json"):
                    raw_insights = raw_insights[4:]
            raw_insights = raw_insights.strip()
            start = raw_insights.find("[")
            end   = raw_insights.rfind("]") + 1
            insights = json.loads(raw_insights[start:end]) if start != -1 else []
        except Exception as ins_err:
            logger.warning(f"GOLD_KPI | LLM insights failed (non-critical): {ins_err}")
            insights = []

        kpis["business_insights"] = insights

        # Write Gold KPI summary
        gold_df = pd.DataFrame([{"dataset": dataset, "kpi_key": k, "kpi_value": str(v)}
                                  for k, v in kpis.items() if k != "business_insights"])
        gold_result = mcp_tool.call("snowflake_write_df", {
            "df":    gold_df,
            "table": f"GOLD_{dataset.upper()}_KPIS",
            "layer": "gold",
        })

        state["gold_kpis"]    = kpis
        state["gold_df_path"] = gold_result.get("path", "")

        logger.success(
            f"GOLD_KPI | Done | {len(gold_df)} KPIs written to Gold | "
            f"{len(insights)} business insights generated"
        )
        state["node_status"]["gold_kpi"] = "pass"

    except Exception as e:
        logger.error(f"GOLD_KPI | FAILED: {e}")
        state["node_errors"]["gold_kpi"] = str(e)
        state["node_status"]["gold_kpi"] = "fail"

    return state
