"""
agents/nodes/lineage_tracker.py
Node 9 — Lineage Tracker
Records every transformation the pipeline applied into PIPELINE_LINEAGE.
Creates a full queryable audit trail of how data flowed through each layer.
Dataset: Olist Brazilian E-Commerce
"""
import uuid
from datetime import datetime
from agents.state import AgentState
from tools.snowflake_mcp_tool import mcp_tool
from config.settings import PipelineConfig
from loguru import logger


def run(state: AgentState) -> AgentState:
    state["current_node"] = "lineage_tracker"
    dataset    = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    run_id     = str(uuid.uuid4())[:12]
    now        = datetime.utcnow().isoformat()

    logger.info(f"LINEAGE_TRACKER | Starting | dataset={dataset} | run_id={run_id}")

    try:
        raw_table       = f"RAW_OLIST_{dataset.upper()}"
        silver_clean    = f"SILVER_{dataset.upper()}_CLEAN"
        silver_masked   = f"SILVER_{dataset.upper()}_MASKED"
        bronze_quarant  = f"BRONZE_{dataset.upper()}_QUARANTINE"
        gold_kpis       = f"GOLD_{dataset.upper()}_KPIS"

        lineage_entries = [
            {
                "run_id":          run_id,
                "dataset":         dataset,
                "source_table":    "EXTERNAL_SOURCE",
                "target_table":    raw_table,
                "transformation":  "raw_ingest",
                "description":     "Initial raw CSV load — Bronze layer",
                "rows_in":         state.get("row_count", 0),
                "rows_out":        state.get("row_count", 0),
                "recorded_at":     now,
            },
            {
                "run_id":          run_id,
                "dataset":         dataset,
                "source_table":    raw_table,
                "target_table":    raw_table,
                "transformation":  "ge_validation_and_self_heal",
                "description":     f"GE validation ({len(state.get('ge_rules', []))} rules) + Heal Agent fixes",
                "rows_in":         state.get("row_count", 0),
                "rows_out":        state.get("row_count", 0),
                "recorded_at":     now,
                "heals_applied":   len(state.get("heal_log", [])),
            },
            {
                "run_id":          run_id,
                "dataset":         dataset,
                "source_table":    raw_table,
                "target_table":    silver_clean,
                "transformation":  "quality_filter_to_silver",
                "description":     "Rows passing all GE quality checks → Silver clean layer",
                "rows_in":         state.get("row_count", 0),
                "rows_out":        state.get("clean_row_count", 0),
                "recorded_at":     now,
            },
            {
                "run_id":          run_id,
                "dataset":         dataset,
                "source_table":    raw_table,
                "target_table":    bronze_quarant,
                "transformation":  "quarantine_bad_rows",
                "description":     "Rows failing GE quality checks → Bronze quarantine",
                "rows_in":         state.get("row_count", 0),
                "rows_out":        state.get("quarantine_count", 0),
                "recorded_at":     now,
            },
            {
                "run_id":          run_id,
                "dataset":         dataset,
                "source_table":    silver_clean,
                "target_table":    silver_masked,
                "transformation":  "pii_masking",
                "description":     "PII masking: SHA-256 (HIGH) + partial mask (MEDIUM) → Silver masked",
                "rows_in":         state.get("clean_row_count", 0),
                "rows_out":        state.get("masked_row_count", 0),
                "recorded_at":     now,
                "pii_map_summary": {
                    k: v.get("pii_level")
                    for k, v in state.get("pii_map", {}).items()
                },
            },
            {
                "run_id":          run_id,
                "dataset":         dataset,
                "source_table":    silver_clean,
                "target_table":    gold_kpis,
                "transformation":  "gold_kpi_aggregation",
                "description":     "KPI aggregation + LLM business insights → Gold layer",
                "rows_in":         state.get("clean_row_count", 0),
                "rows_out":        len(state.get("gold_kpis", {})),
                "recorded_at":     now,
            },
        ]

        # Write all lineage entries to metadata JSON
        for entry in lineage_entries:
            mcp_tool.call("snowflake_append_json", {
                "file":   "pipeline_lineage",
                "record": entry,
                "table":  PipelineConfig.LINEAGE_TABLE,
            })

        state["lineage_run_id"] = run_id
        logger.success(
            f"LINEAGE_TRACKER | Done | {len(lineage_entries)} lineage entries written | run_id={run_id}"
        )
        state["node_status"]["lineage_tracker"] = "pass"

    except Exception as e:
        logger.error(f"LINEAGE_TRACKER | FAILED: {e}")
        state["node_errors"]["lineage_tracker"] = str(e)
        state["node_status"]["lineage_tracker"] = "fail"

    return state
