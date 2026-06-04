"""
agents/nodes/profile.py
Node 1 — Profile
Reads schema, sample rows, and row count from the active Olist dataset via MCP.
"""
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


def run(state: AgentState) -> AgentState:
    state["current_node"] = "profile"
    dataset = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    table   = DATASET_TABLE_MAP.get(dataset, PipelineConfig.RAW_TABLE_CUSTOMERS)

    logger.info(f"PROFILE | Starting | dataset={dataset} | table={table}")

    try:
        state["schema"]       = mcp_tool.call("snowflake_get_schema",  {"table": table})
        state["sample_rows"]  = mcp_tool.call("snowflake_sample_rows", {"table": table, "limit": 5})
        state["row_count"]    = mcp_tool.call("snowflake_row_count",   {"table": table})
        state["dataset_name"] = dataset

        logger.success(
            f"PROFILE | Done | columns={list(state['schema'].keys())} | "
            f"rows={state['row_count']:,}"
        )
        state["node_status"]["profile"] = "pass"

    except Exception as e:
        logger.error(f"PROFILE | FAILED: {e}")
        state["node_errors"]["profile"] = str(e)
        state["node_status"]["profile"] = "fail"

    return state
