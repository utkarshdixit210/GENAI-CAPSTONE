"""
agents/nodes/audit_writer.py
Node 10 — Audit Writer
Writes every heal event from heal_log plus a pipeline summary row
into PIPELINE_AUDIT_LOG. Provides a permanent, queryable record of every run.
Dataset: Olist Brazilian E-Commerce
"""
import uuid
from datetime import datetime
from agents.state import AgentState
from tools.snowflake_mcp_tool import mcp_tool
from config.settings import PipelineConfig
from loguru import logger


def run(state: AgentState) -> AgentState:
    state["current_node"] = "audit_writer"
    dataset    = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    run_id     = state.get("lineage_run_id", str(uuid.uuid4())[:12])
    now        = datetime.utcnow().isoformat()
    heal_log   = state.get("heal_log", [])

    logger.info(f"AUDIT_WRITER | Starting | dataset={dataset} | heals={len(heal_log)}")

    try:
        # Write individual heal events
        for i, heal in enumerate(heal_log):
            audit_record = {
                "audit_id":        f"{run_id}-heal-{i+1}",
                "run_id":          run_id,
                "dataset":         dataset,
                "recorded_at":     now,
                "node_name":       heal.get("node", "unknown"),
                "error_type":      heal.get("error_type", "unknown"),
                "error_message":   heal.get("error_msg", "")[:500],
                "fix_applied":     heal.get("fix", "")[:500],
                "retry_number":    heal.get("retry_num", 0),
                "pipeline_status": "HEALED",
                "clean_rows":      state.get("clean_row_count", 0),
                "quarantine_rows": state.get("quarantine_count", 0),
                "raw_row_count":   state.get("row_count", 0),
                "schema_drifts":   len(state.get("schema_drift_events", [])),
                "heal_count":      len(heal_log),
            }
            mcp_tool.call("snowflake_append_json", {
                "file":   "pipeline_audit_log",
                "record": audit_record,
                "table":  PipelineConfig.AUDIT_TABLE,
            })

        # Write pipeline summary row
        overall_status = "ESCALATED" if state.get("give_up") else "SUCCESS"
        summary_record = {
            "audit_id":        f"{run_id}-summary",
            "run_id":          run_id,
            "dataset":         dataset,
            "recorded_at":     now,
            "node_name":       "PIPELINE_SUMMARY",
            "error_type":      "none",
            "error_message":   "",
            "fix_applied":     f"Pipeline completed | status={overall_status}",
            "retry_number":    0,
            "pipeline_status": overall_status,
            "clean_rows":      state.get("clean_row_count", 0),
            "quarantine_rows": state.get("quarantine_count", 0),
            "raw_row_count":   state.get("row_count", 0),
            "schema_drifts":   len(state.get("schema_drift_events", [])),
            "heal_count":      len(heal_log),
            "ge_rules_count":  len(state.get("ge_rules", [])),
            "pii_columns":     [
                col for col, v in state.get("pii_map", {}).items()
                if v.get("pii_level") in ("HIGH", "MEDIUM")
            ],
            "gold_kpi_keys":   list(state.get("gold_kpis", {}).keys()),
        }
        mcp_tool.call("snowflake_append_json", {
            "file":   "pipeline_audit_log",
            "record": summary_record,
            "table":  PipelineConfig.AUDIT_TABLE,
        })

        total_written = len(heal_log) + 1
        logger.success(
            f"AUDIT_WRITER | Done | {total_written} records written to audit log | "
            f"status={overall_status}"
        )
        state["audit_written"] = True
        state["node_status"]["audit_writer"] = "pass"

    except Exception as e:
        logger.error(f"AUDIT_WRITER | FAILED: {e}")
        state["node_errors"]["audit_writer"] = str(e)
        state["node_status"]["audit_writer"] = "fail"

    return state
