"""
main.py
Entry point for the GEN_AI Capstone — Olist Data Quality Pipeline.

Usage:
    python main.py                          # default: customers dataset
    ACTIVE_DATASET=payments python main.py  # run on payments dataset
    ACTIVE_DATASET=orders python main.py    # run on orders dataset
    ACTIVE_DATASET=products python main.py  # run on products dataset

The pipeline runs fully autonomously — no manual steps required.
Bronze → Silver → Gold medallion architecture with full self-healing.
"""
import json
from datetime import datetime
from config.logger import logger   # initialises loguru sinks
from pipeline.graph import pipeline
from config.settings import PipelineConfig


INITIAL_STATE = {
    # Pipeline tracking
    "current_node":        "",
    "node_status":         {},
    "node_errors":         {},

    # Heal Agent
    "error_type":          "",
    "fix_applied":         "",
    "retry_count":         {},
    "heal_log":            [],
    "give_up":             False,

    # Dataset
    "dataset_name":        PipelineConfig.ACTIVE_DATASET,

    # Node 1 — Profile
    "schema":              {},
    "sample_rows":         [],
    "row_count":           0,

    # Node 1b — Bronze Inspector (LLM)
    "bronze_report":       {},
    "bronze_issues":       [],

    # Node 2 — Schema Drift
    "schema_drift_events": [],
    "schema_drift_run_id": "",

    # Node 3 — PII Detector
    "pii_map":             {},

    # Node 4 — Rule Generator
    "ge_rules":            [],
    "llm_retry_strict":    False,

    # Node 5 — Validator
    "validation_passed":   False,
    "failed_checks":       [],

    # Node 6 — Transform
    "clean_row_count":     0,
    "quarantine_count":    0,
    "fix_sql":             "",
    "clean_df_path":       "",
    "quarantine_df_path":  "",

    # Node 7 — PII Masker
    "masked_table":        "",
    "masked_row_count":    0,
    "masked_df_path":      "",
    "masking_log":         [],

    # Node 8 — Gold KPI
    "gold_kpis":           {},
    "gold_df_path":        "",

    # Node 9 — Lineage Tracker
    "lineage_run_id":      "",

    # Node 10 — Audit Writer
    "audit_written":       False,

    # Node 11 — Alert
    "audit_report":        "",
}


def print_summary(state: dict, duration: float):
    dataset = state.get("dataset_name", "?")
    status  = "ESCALATED" if state.get("give_up") else "SUCCESS"

    logger.info("=" * 70)
    logger.info("  GEN_AI CAPSTONE — OLIST DATA PIPELINE — RUN COMPLETE")
    logger.info("=" * 70)
    logger.info(f"  Dataset           : {dataset.upper()}")
    logger.info(f"  Duration          : {duration:.1f}s")
    logger.info(f"  Pipeline status   : {status}")
    logger.info(f"  Raw rows (Bronze) : {state.get('row_count', 0):,}")
    logger.info(f"  Clean rows (Silver): {state.get('clean_row_count', 0):,}")
    logger.info(f"  Quarantine rows   : {state.get('quarantine_count', 0):,}")
    logger.info(f"  Masked rows       : {state.get('masked_row_count', 0):,}")
    logger.info(f"  GE rules applied  : {len(state.get('ge_rules', []))}")
    logger.info(f"  Schema drifts     : {len(state.get('schema_drift_events', []))}")
    logger.info(f"  Total heals       : {len(state.get('heal_log', []))}")
    logger.info(f"  Retries per node  : {state.get('retry_count', {})}")
    logger.info(f"  Audit written     : {state.get('audit_written', False)}")
    logger.info("=" * 70)

    # Bronze Inspector report
    bronze_report = state.get("bronze_report", {})
    bronze_issues = state.get("bronze_issues", [])
    if bronze_report:
        logger.info(f"\n  🔍 BRONZE LLM INSPECTION:")
        logger.info(f"     Summary : {bronze_report.get('batch_summary','')}")
        logger.info(f"     Issues  : {len(bronze_issues)} detected")
        for iss in bronze_issues:
            logger.info(
                f"     ⚠ [{iss.get('severity','?')}] {iss.get('column','?')} "
                f"[{iss.get('issue_type','?')}]: {iss.get('description','')}"
            )
        clean_cols = bronze_report.get("columns_look_clean", [])
        logger.info(f"     Clean   : {clean_cols}")
        logger.info(f"     Advice  : {bronze_report.get('recommendation','')}")

    # Masking log
    masking_log = state.get("masking_log", [])
    if masking_log:
        logger.info("\n  🔒 PII MASKING REPORT (LLM Detected → Masked):")
        for m in masking_log:
            action = m.get("action", "?")
            col    = m.get("column", "?")
            level  = m.get("level", m.get("action", "?"))
            reason = m.get("reason", "")
            icon   = "🔴" if action == "SHA256" else ("🟡" if action == "PARTIAL_MASK" else "✅")
            logger.info(f"     {icon} {col:35s} | {action:14s} | {level} | {reason}")

    # Heal log
    if state.get("heal_log"):
        logger.info("\n  HEAL LOG:")
        for i, h in enumerate(state["heal_log"], 1):
            logger.info(
                f"    [{i}] Node: {h.get('node','?'):20s} | "
                f"Type: {h.get('error_type','?'):22s} | "
                f"Retry: {h.get('retry_num','?')}"
            )

    # Schema drift
    if state.get("schema_drift_events"):
        logger.info("\n  SCHEMA DRIFT EVENTS:")
        for d in state["schema_drift_events"]:
            logger.info(
                f"    {d.get('severity','?'):6s} | "
                f"{d.get('drift_type','?'):20s} | "
                f"Column: {d.get('column_name','?')}"
            )

    # PII map
    if state.get("pii_map"):
        high   = [c for c, v in state["pii_map"].items() if v.get("pii_level") == "HIGH"]
        medium = [c for c, v in state["pii_map"].items() if v.get("pii_level") == "MEDIUM"]
        logger.info(f"\n  PII MAP → HIGH: {high}  |  MEDIUM: {medium}")

    # Gold KPIs
    if state.get("gold_kpis"):
        kpis = {k: v for k, v in state["gold_kpis"].items() if k != "business_insights"}
        logger.info(f"\n  GOLD KPIs: {json.dumps(kpis, indent=4, default=str)}")

    # Business insights
    insights = state.get("gold_kpis", {}).get("business_insights", [])
    if insights:
        logger.info("\n  BUSINESS INSIGHTS (LLM):")
        for i, ins in enumerate(insights, 1):
            logger.info(f"    [{i}] [{ins.get('category','?').upper()}] {ins.get('insight','')}")

    # Output paths
    logger.info("\n  OUTPUT FILES:")
    for key in ["clean_df_path", "quarantine_df_path", "masked_df_path", "gold_df_path"]:
        path = state.get(key, "")
        if path:
            logger.info(f"    {key}: {path}")

    logger.info("=" * 70)


def main():
    start = datetime.now()
    dataset = PipelineConfig.ACTIVE_DATASET

    logger.info("=" * 70)
    logger.info("  GEN_AI CAPSTONE — OLIST DATA PIPELINE — STARTING")
    logger.info(f"  Dataset: {dataset.upper()} | {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("  Architecture: Bronze → Silver → Gold")
    logger.info("=" * 70)

    initial = dict(INITIAL_STATE)
    initial["dataset_name"] = dataset

    final_state = pipeline.invoke(initial)
    duration    = (datetime.now() - start).total_seconds()

    print_summary(final_state, duration)
    return final_state


if __name__ == "__main__":
    main()
