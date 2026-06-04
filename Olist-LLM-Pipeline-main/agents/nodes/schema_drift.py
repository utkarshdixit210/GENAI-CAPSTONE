"""
agents/nodes/schema_drift.py
Node 2 — Schema Drift Detector
Compares incoming schema vs known-good schema per Olist dataset.
LLM detects renamed columns. Drift events written to metadata/schema_drift_log.json.
"""
import json
import uuid
from datetime import datetime
from agents.state import AgentState
from tools.snowflake_mcp_tool import mcp_tool
from tools.llm_client import llm_client
from config.settings import PipelineConfig
from loguru import logger

# Known-good schemas for each Olist dataset
KNOWN_SCHEMAS = {
    "customers": {
        "customer_id": "object", "customer_unique_id": "object",
        "customer_zip_code_prefix": "int64", "customer_city": "object", "customer_state": "object",
    },
    "orders": {
        "order_id": "object", "customer_id": "object", "order_status": "object",
        "order_purchase_timestamp": "object", "order_approved_at": "object",
        "order_delivered_carrier_date": "object", "order_delivered_customer_date": "object",
        "order_estimated_delivery_date": "object",
    },
    "payments": {
        "order_id": "object", "payment_sequential": "int64",
        "payment_type": "object", "payment_installments": "int64", "payment_value": "float64",
    },
    "products": {
        "product_id": "object", "product_category_name": "object",
        "product_name_lenght": "float64", "product_description_lenght": "float64",
        "product_photos_qty": "float64", "product_weight_g": "float64",
        "product_length_cm": "float64", "product_height_cm": "float64", "product_width_cm": "float64",
    },
}

RENAME_PROMPT = """
You are a schema comparison expert.
Old schema columns (missing from new): {old_cols}
New schema columns (not in old): {new_cols}

Identify columns that appear RENAMED (disappeared from old, similar one appeared in new).
Respond ONLY with valid JSON:
{{"renamed": [{{"old_name": "col_a", "new_name": "col_b"}}]}}
If none: {{"renamed": []}}
No explanation. No markdown. Only JSON.
"""


def _norm_type(t: str) -> str:
    t = str(t).lower()
    if "int" in t:   return "int"
    if "float" in t: return "float"
    if "object" in t or "str" in t: return "str"
    return t


def run(state: AgentState) -> AgentState:
    state["current_node"] = "schema_drift"
    dataset  = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    run_id   = str(uuid.uuid4())[:8]
    incoming = state.get("schema", {})
    expected = KNOWN_SCHEMAS.get(dataset, {})
    events   = []

    logger.info(f"SCHEMA_DRIFT | Starting | dataset={dataset} | run_id={run_id}")

    try:
        old_cols = set(expected.keys())
        new_cols = set(incoming.keys())
        now      = datetime.utcnow().isoformat()

        for col in old_cols - new_cols:
            events.append({"run_id": run_id, "dataset": dataset, "drift_type": "COLUMN_REMOVED",
                           "column_name": col, "severity": "HIGH", "detected_at": now})
            logger.warning(f"SCHEMA_DRIFT | COLUMN_REMOVED | HIGH | {col}")

        for col in new_cols - old_cols:
            events.append({"run_id": run_id, "dataset": dataset, "drift_type": "COLUMN_ADDED",
                           "column_name": col, "severity": "MEDIUM", "detected_at": now})
            logger.info(f"SCHEMA_DRIFT | COLUMN_ADDED | MEDIUM | {col}")

        for col in old_cols & new_cols:
            if _norm_type(expected[col]) != _norm_type(incoming[col]):
                events.append({"run_id": run_id, "dataset": dataset, "drift_type": "TYPE_CHANGED",
                               "column_name": col, "old_type": str(expected[col]),
                               "new_type": str(incoming[col]), "severity": "HIGH", "detected_at": now})
                logger.warning(f"SCHEMA_DRIFT | TYPE_CHANGED | HIGH | {col}")

        if old_cols != new_cols:
            try:
                raw = llm_client.invoke(RENAME_PROMPT.format(
                    old_cols=sorted(old_cols - new_cols),
                    new_cols=sorted(new_cols - old_cols),
                ))
                raw = raw.strip()
                if raw.startswith("```"):
                    raw = raw.split("```")[1].lstrip("json").strip()
                parsed = json.loads(raw)
                for r in parsed.get("renamed", []):
                    events.append({"run_id": run_id, "dataset": dataset,
                                   "drift_type": "COLUMN_RENAMED",
                                   "column_name": r.get("old_name", "?"),
                                   "new_name": r.get("new_name", "?"),
                                   "severity": "HIGH", "detected_at": now})
                    logger.warning(f"SCHEMA_DRIFT | COLUMN_RENAMED | {r.get('old_name')} → {r.get('new_name')}")
            except Exception as llm_err:
                logger.warning(f"SCHEMA_DRIFT | LLM rename detection skipped: {llm_err}")

        for event in events:
            mcp_tool.call("snowflake_append_json", {
                "file": "schema_drift_log", "record": event,
                "table": PipelineConfig.DRIFT_LOG_TABLE,
            })

        state["schema_drift_events"] = events
        state["schema_drift_run_id"] = run_id
        logger.success(f"SCHEMA_DRIFT | Done | {len(events)} event(s) detected")
        state["node_status"]["schema_drift"] = "pass"

    except Exception as e:
        logger.error(f"SCHEMA_DRIFT | FAILED: {e}")
        state["node_errors"]["schema_drift"] = str(e)
        state["node_status"]["schema_drift"] = "fail"

    return state
