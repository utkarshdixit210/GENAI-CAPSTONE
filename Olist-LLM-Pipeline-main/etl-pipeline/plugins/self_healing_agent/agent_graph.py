"""
etl-pipeline/plugins/self_healing_agent/agent_graph.py
LangGraph mini-graph embedded inside the Airflow self-healing plugin.
Called when an Airflow task fails — classifies the error, applies a fix,
and returns a result dict that the DAG uses to decide whether to retry.

This is the ETL-context version of the main pipeline's Heal Agent.
"""
import json
import os
from typing import TypedDict, List, Dict
from langgraph.graph import StateGraph
from loguru import logger


class HealState(TypedDict):
    task_id:      str
    dataset:      str
    error_msg:    str
    error_type:   str
    fix_applied:  str
    retry:        bool
    heal_log:     List[Dict]


CLASSIFY_PROMPT = """
An Airflow ETL task '{task_id}' failed with this error:
{error_msg}

Classify into ONE of:
- connection_error
- data_quality_error
- sql_error
- file_not_found
- timeout
- invalid_llm_output

Respond with ONLY the error type string.
"""

FIX_PROMPT = """
Airflow task '{task_id}' (dataset: {dataset}) failed.
Error type: {error_type}
Error message: {error_msg}

Write a concise description (1-2 sentences) of the fix that should be applied.
Be specific to the Olist Brazilian E-Commerce dataset context.
"""


def _classify_node(state: HealState) -> HealState:
    from tools.llm_client import llm_client
    try:
        prompt = CLASSIFY_PROMPT.format(
            task_id  = state["task_id"],
            error_msg= state["error_msg"][:600],
        )
        error_type = llm_client.invoke(prompt).strip().lower()
    except Exception as e:
        logger.warning(f"ETL HEAL | LLM classify failed: {e}")
        error_type = "unknown"
    state["error_type"] = error_type
    logger.info(f"ETL HEAL | Classified: {error_type}")
    return state


def _fix_node(state: HealState) -> HealState:
    from tools.llm_client import llm_client
    from etl_pipeline.plugins.self_healing_agent.mcp_tools import (
        mcp_fetch_table, mcp_write_table
    )
    import pandas as pd

    error_type = state["error_type"]
    dataset    = state["dataset"]
    fix        = "No specific fix available — manual review required."

    try:
        if error_type == "data_quality_error":
            table_name = f"RAW_OLIST_{dataset.upper()}"
            df = mcp_fetch_table(table_name)

            # Apply generic healing
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].fillna("UNKNOWN")
                else:
                    df[col] = df[col].fillna(0)

            # Fix negative payment_value
            if "payment_value" in df.columns:
                df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").abs()

            # Fix invalid customer_state
            if "customer_state" in df.columns:
                BR_STATES = {"AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
                             "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
                             "RS","RO","RR","SC","SP","SE","TO"}
                df["customer_state"] = df["customer_state"].str.upper().str.strip()
                df.loc[~df["customer_state"].isin(BR_STATES), "customer_state"] = "UNKNOWN"

            mcp_write_table(df, table_name, layer="bronze")
            fix = f"Data quality fixes applied to {table_name}: nulls filled, negatives removed, invalid states standardised."

        elif error_type == "connection_error":
            fix = "Connection error — MCP will reconnect on next retry."

        elif error_type == "file_not_found":
            fix = "File not found — run python generate_data.py to create CSV files."

        elif error_type == "timeout":
            import time
            time.sleep(10)
            fix = "Waited 10s after timeout before retry."

        else:
            fix = f"Error type '{error_type}' — retrying task as-is."

    except Exception as e:
        fix = f"Fix attempt failed: {e}"
        logger.error(f"ETL HEAL | Fix failed: {e}")

    state["fix_applied"] = fix
    state["retry"]       = error_type not in ("file_not_found",)
    state["heal_log"]    = state.get("heal_log", []) + [{
        "task_id":    state["task_id"],
        "dataset":    dataset,
        "error_type": error_type,
        "fix":        fix,
        "retry":      state["retry"],
    }]
    logger.info(f"ETL HEAL | Fix: {fix[:100]} | retry={state['retry']}")
    return state


def build_heal_graph():
    graph = StateGraph(HealState)
    graph.add_node("classify", _classify_node)
    graph.add_node("fix",      _fix_node)
    graph.set_entry_point("classify")
    graph.add_edge("classify", "fix")
    graph.add_edge("fix", "__end__")
    return graph.compile()


heal_pipeline = build_heal_graph()


def run_healing(task_id: str, dataset: str, error_msg: str) -> dict:
    """
    Main entry point called by the Airflow on_failure_callback.
    Returns a dict with fix_applied, error_type, retry.
    """
    initial = {
        "task_id":    task_id,
        "dataset":    dataset,
        "error_msg":  error_msg,
        "error_type": "",
        "fix_applied":"",
        "retry":      True,
        "heal_log":   [],
    }
    result = heal_pipeline.invoke(initial)
    logger.info(f"ETL HEAL | Complete | task={task_id} | fix={result['fix_applied'][:80]}")
    return result
