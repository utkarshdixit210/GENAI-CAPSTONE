"""
agents/nodes/alert.py
Node 11 — Alert (Terminal Node)
LLM reads full heal_log, schema drift events, PII map, Gold KPIs, and
pipeline stats to generate a professional audit report.
Saves to logs/audit_YYYYMMDD_HHMMSS.txt.
Dataset: Olist Brazilian E-Commerce
"""
import json
import os
from datetime import datetime
from agents.state import AgentState
from tools.llm_client import llm_client
from loguru import logger


ALERT_PROMPT = """
You are a Senior Data Engineering Team Lead writing a post-run audit report.

Pipeline Run Summary:
- Dataset:          {dataset}
- Total raw rows:   {raw_rows:,}
- Clean rows:       {clean_rows:,} (Silver layer)
- Quarantine rows:  {quarantine_rows:,} (Bronze bad rows)
- Masked rows:      {masked_rows:,} (PII-safe Silver)
- Schema drifts:    {schema_drifts}
- Total heals:      {heal_count}
- Pipeline status:  {status}

Schema Drift Events:
{drift_events}

PII Map:
{pii_map}

Heal Log:
{heal_log}

Gold KPIs Generated:
{gold_kpis}

Write a professional audit report with these sections:
1. EXECUTIVE SUMMARY (2-3 sentences on overall outcome)
2. DATA QUALITY FINDINGS (what issues were found, how many rows affected)
3. SELF-HEALING ACTIONS (what the Heal Agent fixed and how)
4. PII & COMPLIANCE (what columns were masked and why)
5. BRONZE → SILVER → GOLD SUMMARY (data flow through medallion layers)
6. SCHEMA DRIFT (any schema changes detected)
7. RECOMMENDATIONS (2-3 actionable next steps)

Be specific, use the numbers provided, keep it concise and professional.
"""


def run(state: AgentState) -> AgentState:
    state["current_node"] = "alert"
    dataset = state.get("dataset_name", "olist")
    status  = "ESCALATED" if state.get("give_up") else "SUCCESS"

    logger.info(f"ALERT | Starting | dataset={dataset} | status={status}")

    try:
        drift_events = state.get("schema_drift_events", [])
        heal_log     = state.get("heal_log", [])
        pii_map      = state.get("pii_map", {})
        gold_kpis    = state.get("gold_kpis", {})

        prompt = ALERT_PROMPT.format(
            dataset        = dataset,
            raw_rows       = state.get("row_count", 0),
            clean_rows     = state.get("clean_row_count", 0),
            quarantine_rows= state.get("quarantine_count", 0),
            masked_rows    = state.get("masked_row_count", 0),
            schema_drifts  = len(drift_events),
            heal_count     = len(heal_log),
            status         = status,
            drift_events   = json.dumps(drift_events, indent=2, default=str) if drift_events else "None",
            pii_map        = json.dumps(
                {k: v.get("pii_level") for k, v in pii_map.items()}, indent=2
            ),
            heal_log       = json.dumps(
                [{"node": h.get("node"), "type": h.get("error_type"), "fix": h.get("fix", "")[:100]}
                 for h in heal_log], indent=2
            ) if heal_log else "No heals required",
            gold_kpis      = json.dumps(
                {k: v for k, v in gold_kpis.items() if k != "business_insights"},
                indent=2, default=str
            ) if gold_kpis else "Not computed",
        )

        audit_report = llm_client.invoke(prompt)
        state["audit_report"] = audit_report

        # Save to logs/
        os.makedirs("logs", exist_ok=True)
        timestamp   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_path = f"logs/audit_{dataset}_{timestamp}.txt"

        with open(report_path, "w") as f:
            f.write(f"GEN_AI CAPSTONE — PIPELINE AUDIT REPORT\n")
            f.write(f"Dataset: {dataset.upper()} | Generated: {timestamp}\n")
            f.write("=" * 70 + "\n\n")
            f.write(audit_report)
            f.write("\n\n" + "=" * 70 + "\n")
            f.write(f"Pipeline Status: {status}\n")
            f.write(f"Heals Applied:   {len(heal_log)}\n")
            f.write(f"Schema Drifts:   {len(drift_events)}\n")
            f.write(f"Clean Rows:      {state.get('clean_row_count', 0):,}\n")
            f.write(f"Quarantine Rows: {state.get('quarantine_count', 0):,}\n")

        logger.success(f"ALERT | Audit report saved → {report_path}")

        # Log business insights if available
        insights = gold_kpis.get("business_insights", [])
        if insights:
            logger.info("ALERT | Business Insights from Gold KPIs:")
            for idx, ins in enumerate(insights, 1):
                logger.info(f"  [{idx}] [{ins.get('category','?').upper()}] {ins.get('insight','')}")

        state["node_status"]["alert"] = "pass"

    except Exception as e:
        logger.error(f"ALERT | FAILED: {e}")
        state["node_errors"]["alert"] = str(e)
        state["node_status"]["alert"] = "fail"

    return state
