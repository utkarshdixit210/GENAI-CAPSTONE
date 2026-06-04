"""
agents/nodes/pii_masker.py
Node 7 — PII Masker

Reads pii_map from State (set by the LLM PII Detector).
For every column classified as HIGH or MEDIUM:
  - Logs a BEFORE/AFTER sample so you can see exactly what was masked
  - Applies SHA-256 hash (HIGH) or partial mask (MEDIUM)
  - Reports any column NOT masked and why

Also logs a detailed masking report to Snowflake PIPELINE_AUDIT_LOG.
"""
import hashlib
import pandas as pd
from agents.state import AgentState
from tools.snowflake_mcp_tool import mcp_tool
from config.settings import PipelineConfig
from loguru import logger


def _sha256(value: str) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()


def _partial_mask(value: str, visible_chars: int = 4) -> str:
    s = str(value)
    if len(s) <= visible_chars:
        return "*" * len(s)
    return s[:visible_chars] + "*" * (len(s) - visible_chars)


def _sample_before_after(series_before: pd.Series, series_after: pd.Series, n: int = 3) -> list:
    """Return a list of {before, after} dicts for the first n non-null rows."""
    results = []
    for i, (b, a) in enumerate(zip(series_before, series_after)):
        if i >= n:
            break
        if pd.notna(b):
            results.append({"before": str(b), "after": str(a)})
    return results


def run(state: AgentState) -> AgentState:
    state["current_node"] = "pii_masker"
    dataset    = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)
    pii_map    = state.get("pii_map", {})

    logger.info(f"PII_MASKER | Starting | dataset={dataset}")

    try:
        clean_table = f"SILVER_{dataset.upper()}_CLEAN"
        df = mcp_tool.call("snowflake_read_table", {"table": clean_table, "layer": "silver"})
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        logger.info(f"PII_MASKER | Loaded {len(df):,} clean rows from {clean_table}")

        masked_df    = df.copy()
        masking_log  = []

        # ── Print header ───────────────────────────────────────────
        logger.info("PII_MASKER | ─── Column-by-Column PII Masking Report ───")

        for col, meta in pii_map.items():
            level  = meta.get("pii_level", "NONE")
            reason = meta.get("reason", "")

            if col not in masked_df.columns:
                logger.info(f"  ⏭  {col:35s} | SKIP   | column not in Silver table")
                masking_log.append({"column": col, "action": "SKIP", "reason": "not in Silver"})
                continue

            if level == "HIGH":
                before_sample = [str(v) for v in masked_df[col].dropna().head(3).tolist()]
                masked_df[col] = masked_df[col].astype(str).apply(_sha256)
                after_sample  = masked_df[col].head(3).tolist()
                logger.warning(
                    f"  🔴 {col:35s} | HIGH   | SHA-256 hash applied | "
                    f"Reason: {reason}"
                )
                for b, a in zip(before_sample, after_sample):
                    logger.info(f"      BEFORE: {str(b)[:40]:<42} → AFTER: {a[:20]}...")
                masking_log.append({
                    "column": col, "action": "SHA256", "level": "HIGH",
                    "reason": reason, "rows_masked": int(masked_df[col].notna().sum()),
                    "sample_before": before_sample[:2],
                    "sample_after":  [a[:20]+"..." for a in after_sample[:2]],
                })

            elif level == "MEDIUM":
                before_sample = [str(v) for v in masked_df[col].dropna().head(3).tolist()]
                masked_df[col] = masked_df[col].astype(str).apply(_partial_mask)
                after_sample  = masked_df[col].head(3).tolist()
                logger.warning(
                    f"  🟡 {col:35s} | MEDIUM | Partial mask applied | "
                    f"Reason: {reason}"
                )
                for b, a in zip(before_sample, after_sample):
                    logger.info(f"      BEFORE: {str(b)[:40]:<42} → AFTER: {a}")
                masking_log.append({
                    "column": col, "action": "PARTIAL_MASK", "level": "MEDIUM",
                    "reason": reason, "rows_masked": int(masked_df[col].notna().sum()),
                    "sample_before": before_sample[:2],
                    "sample_after":  after_sample[:2],
                })

            elif level == "LOW":
                logger.info(
                    f"  🟢 {col:35s} | LOW    | NOT masked — low risk | Reason: {reason}"
                )
                masking_log.append({"column": col, "action": "NOT_MASKED", "level": "LOW", "reason": reason})

            else:  # NONE
                logger.info(
                    f"  ✅ {col:35s} | NONE   | NOT masked — no PII risk | Reason: {reason}"
                )
                masking_log.append({"column": col, "action": "NOT_MASKED", "level": "NONE", "reason": reason})

        logger.info("PII_MASKER | ─── End of Masking Report ───")

        # Summary
        high_count   = sum(1 for m in masking_log if m.get("action") == "SHA256")
        medium_count = sum(1 for m in masking_log if m.get("action") == "PARTIAL_MASK")
        skip_count   = sum(1 for m in masking_log if m.get("action") in ("NOT_MASKED","SKIP"))

        logger.success(
            f"PII_MASKER | Done | {high_count} HIGH-masked | "
            f"{medium_count} MEDIUM-masked | {skip_count} not masked"
        )

        # Write masked Silver table to Snowflake
        masked_table = f"SILVER_{dataset.upper()}_MASKED"
        mcp_tool.call("snowflake_write_df", {"df": masked_df, "table": masked_table, "layer": "silver"})

        # Append masking audit record to Snowflake
        try:
            mcp_tool.call("snowflake_append_json", {
                "table": "PIPELINE_AUDIT_LOG",
                "record": {
                    "node":        "pii_masker",
                    "dataset":     dataset,
                    "rows_masked": len(masked_df),
                    "masking_log": masking_log,
                },
            })
        except Exception:
            pass

        state["masked_table"]     = masked_table
        state["masked_row_count"] = len(masked_df)
        state["masking_log"]      = masking_log
        state["node_status"]["pii_masker"] = "pass"

    except Exception as e:
        logger.error(f"PII_MASKER | FAILED: {e}")
        state["node_errors"]["pii_masker"] = str(e)
        state["node_status"]["pii_masker"] = "fail"

    return state
