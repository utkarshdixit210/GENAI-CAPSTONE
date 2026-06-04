"""
agents/nodes/bronze_inspector.py
Node 1b — Bronze LLM Inspector

After profiling, LLM scans the raw batch and produces a structured
quality report covering:
  - Null columns and their rates
  - Columns with invalid/bad categorical values
  - Duplicate key columns
  - Anything that looks anomalous

The report is stored in state["bronze_report"] and logged to Snowflake.
This is the first LLM "eye" on the raw data.
"""
import json
from agents.state import AgentState
from tools.llm_client import llm_client
from tools.snowflake_mcp_tool import mcp_tool
from config.settings import PipelineConfig
from loguru import logger


BRONZE_INSPECT_PROMPT = """
You are a senior data quality engineer inspecting a RAW data batch from the Olist
Brazilian E-Commerce platform.

Dataset   : {dataset}
Batch rows: {row_count}
Schema (column → data_type):
{schema}

Sample rows (first 5):
{sample_rows}

Your job is to inspect this batch and report EVERY data quality problem you detect.
Be specific and quantify where possible based on the samples.

Respond ONLY with valid JSON in this exact format:
{{
  "batch_summary": "one-line summary of overall data health",
  "issues_found": [
    {{
      "column": "column_name",
      "issue_type": "NULL_VALUES | BAD_CATEGORICAL | DUPLICATE_KEY | OUT_OF_RANGE | FORMAT_ERROR | SUSPICIOUS_VALUE",
      "severity": "HIGH | MEDIUM | LOW",
      "description": "specific description of what was found",
      "sample_bad_values": ["val1", "val2"]
    }}
  ],
  "columns_look_clean": ["col1", "col2"],
  "recommendation": "one-line recommendation for the Silver layer"
}}

If no issues found, issues_found should be an empty list.
No markdown. No explanation. Only JSON.
"""


def run(state: AgentState) -> AgentState:
    state["current_node"] = "bronze_inspector"
    dataset     = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    schema      = state.get("schema", {})
    sample_rows = state.get("sample_rows", [])
    row_count   = state.get("row_count", 0)

    logger.info(f"BRONZE_INSPECTOR | Starting | dataset={dataset} | rows={row_count:,}")

    try:
        prompt = BRONZE_INSPECT_PROMPT.format(
            dataset     = dataset,
            row_count   = row_count,
            schema      = json.dumps(schema, indent=2),
            sample_rows = json.dumps(sample_rows[:5], indent=2, default=str),
        )

        raw = llm_client.invoke(prompt).strip()
        # Strip markdown if LLM added it
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        report = json.loads(raw)
        issues = report.get("issues_found", [])
        clean  = report.get("columns_look_clean", [])

        logger.info(f"BRONZE_INSPECTOR | Summary: {report.get('batch_summary','')}")
        logger.info(f"BRONZE_INSPECTOR | Issues found: {len(issues)} | Clean columns: {clean}")

        for issue in issues:
            sev = issue.get("severity", "?")
            col = issue.get("column", "?")
            typ = issue.get("issue_type", "?")
            desc = issue.get("description", "")
            bads = issue.get("sample_bad_values", [])
            if sev == "HIGH":
                logger.warning(f"  ⚠ [{sev}] {col} [{typ}]: {desc} | Samples: {bads}")
            else:
                logger.info(f"  ℹ [{sev}] {col} [{typ}]: {desc} | Samples: {bads}")

        if not issues:
            logger.success("BRONZE_INSPECTOR | Batch looks clean — no issues detected")
        else:
            logger.warning(
                f"BRONZE_INSPECTOR | {len(issues)} issue(s) flagged — "
                f"sending to Silver healing layer"
            )

        logger.info(f"BRONZE_INSPECTOR | Recommendation: {report.get('recommendation','')}")

        # Append to Snowflake audit log
        try:
            mcp_tool.call("snowflake_append_json", {
                "table": "PIPELINE_AUDIT_LOG",
                "record": {
                    "node":       "bronze_inspector",
                    "dataset":    dataset,
                    "row_count":  row_count,
                    "issues":     issues,
                    "summary":    report.get("batch_summary", ""),
                    "clean_cols": clean,
                },
            })
        except Exception:
            pass  # Non-fatal — audit failure doesn't stop pipeline

        state["bronze_report"]  = report
        state["bronze_issues"]  = issues
        state["node_status"]["bronze_inspector"] = "pass"

    except Exception as e:
        logger.error(f"BRONZE_INSPECTOR | FAILED: {e}")
        state["bronze_report"] = {}
        state["bronze_issues"] = []
        state["node_status"]["bronze_inspector"] = "fail"
        state["node_errors"]["bronze_inspector"]  = str(e)

    return state
