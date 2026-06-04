"""
agents/nodes/heal_agent.py
Universal Heal Agent — handles failures from ANY node in the pipeline.

Workflow:
  1. Read which node failed + error message from State
  2. LLM classifies the error type
  3. Apply the appropriate fix
  4. Update heal_log and retry_count
  5. Return state — LangGraph routes back to retry the failed node

Dataset: Olist Brazilian E-Commerce
"""
import json
import time
import pandas as pd
from agents.state import AgentState
from tools.llm_client import llm_client
from tools.snowflake_mcp_tool import mcp_tool
from agents.nodes.validator import DATASET_TABLE_MAP
from config.settings import PipelineConfig
from loguru import logger
from tools.pattern_memory import pattern_memory

def send_slack_alert(sql_query: str, dataset: str, node: str, run_id: str):
    import os, json, urllib.request
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("Slack Alert | No SLACK_WEBHOOK_URL found in environment. Skipping alert.")
        return
        
    payload = {
        "text": f"🚨 *HIGH-RISK PIPELINE ACTION PENDING APPROVAL* 🚨\nDataset: `{dataset}` | Node: `{node}` | Run ID: `{run_id}`",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"🚨 *HIGH-RISK ACTION PENDING* 🚨\nAn autonomous fix requires your approval to proceed.\n\n*Run ID:* `{run_id}`\n*Dataset:* `{dataset}`\n*Node:* `{node}`"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Proposed SQL Fix:*\n```sql\n{sql_query}\n```"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "👉 Please review and approve this action in your Olist Pipeline Dashboard."
                    }
                ]
            }
        ]
    }
    
    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req) as res:
            logger.info("Slack Alert | Webhook alert sent successfully.")
    except Exception as e:
        logger.error(f"Slack Alert | Failed to send alert: {e}")


def wait_for_human_approval(sql_query: str, dataset: str, node: str) -> bool:
    import os, time, sqlite3
    run_id = os.environ.get("PIPELINE_RUN_ID", "local_run")
    
    # 1. Insert request into SQLite
    conn = sqlite3.connect(PipelineConfig.DB_PATH, timeout=30.0)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pending_approvals (run_id, dataset, node, sql_query, status)
        VALUES (?, ?, ?, ?, 'PENDING')
    """, (run_id, dataset, node, sql_query))
    conn.commit()
    conn.close()
    
    # 2. Send Slack Notification
    send_slack_alert(sql_query, dataset, node, run_id)
    logger.warning(f"HITL | Paused. Waiting for manual approval of SQL query: {sql_query}")
    
    # 3. Poll database for approval
    timeout_sec = 300  # 5 minutes timeout
    start_time = time.time()
    while time.time() - start_time < timeout_sec:
        conn = sqlite3.connect(PipelineConfig.DB_PATH, timeout=30.0)
        cur = conn.cursor()
        cur.execute("SELECT status FROM pending_approvals WHERE run_id = ? AND dataset = ? AND node = ? ORDER BY id DESC LIMIT 1",
                    (run_id, dataset, node))
        row = cur.fetchone()
        conn.close()
        
        if row:
            status = row[0]
            if status == "APPROVED":
                logger.success("HITL | SQL query APPROVED by human. Executing...")
                return True
            elif status == "REJECTED":
                logger.error("HITL | SQL query REJECTED by human. Aborting...")
                return False
                
        time.sleep(2)
        
    logger.error("HITL | Approval request timed out. Aborting...")
    return False

MAX_RETRIES = PipelineConfig.MAX_RETRIES

CLASSIFY_PROMPT = """
A LangGraph pipeline node called '{node}' failed with this error:

{error}

Classify this into EXACTLY ONE of these error types:
- connection_error      (Snowflake/MCP connection timeout or authentication failure)
- invalid_llm_output    (LLM returned malformed JSON or wrong output format)
- data_quality_error    (data validation failed — null values, bad values, out-of-range)
- sql_error             (SQL syntax error or Snowflake execution failure)
- ge_config_error       (Great Expectations configuration or setup issue)
- file_not_found        (CSV or output file not found on disk)
- timeout               (operation exceeded the allowed time limit)

Respond with ONLY the error type string. No explanation. No punctuation.
"""

SQL_FIX_PROMPT = """
You are a Snowflake SQL expert working with the Olist Brazilian E-Commerce dataset.
These validation checks failed on dataset '{dataset}' in table '{table}':

{failed_checks}

Write a single Snowflake SQL `UPDATE` statement that can be applied to the table `{table}` to fix these issues. 
CRITICAL: Do NOT use analytic functions like `ROW_NUMBER()`, `RANK()`, etc. in the `UPDATE` statement.
Use simple `SET` and `WHERE` clauses.
Examples:
  UPDATE {table} SET customer_state = 'UNKNOWN' WHERE customer_state IS NULL;
  DELETE FROM {table} WHERE payment_value < 0;

Respond with ONLY the Snowflake SQL string. No explanation. No markdown.
"""

PANDAS_FIX_PROMPT = """
You are a data cleaning expert working with the Olist Brazilian E-Commerce dataset.
These validation checks failed on dataset '{dataset}':

{failed_checks}

Write Python pandas code (as a string) that can be applied to a DataFrame named `df`
to fix these issues. Use only pandas operations.
Examples:
  df['customer_state'] = df['customer_state'].fillna('UNKNOWN')
  df = df[df['payment_value'] >= 0]
  df = df.drop_duplicates(subset=['customer_id'])

Respond with ONLY the Python code string. No explanation. No markdown.
"""


def _apply_pandas_fix(fix_code: str, dataset: str) -> bool:
    """
    Apply a pandas fix to the raw CSV in local mode.
    Reads the CSV, applies the fix code, writes it back.
    """
    table_map = {
        "customers": f"{PipelineConfig.DATA_DIR}/olist_customers_dataset.csv",
        "orders":    f"{PipelineConfig.DATA_DIR}/olist_orders_dataset.csv",
        "payments":  f"{PipelineConfig.DATA_DIR}/olist_order_payments_dataset.csv",
        "products":  f"{PipelineConfig.DATA_DIR}/olist_products_dataset.csv",
    }
    csv_path = table_map.get(dataset)
    if not csv_path:
        return False

    try:
        df = pd.read_csv(csv_path)
        exec(fix_code, {"df": df, "pd": pd})  # noqa: S102
        df.to_csv(csv_path, index=False)
        logger.info(f"HEAL AGENT | Pandas fix applied to {csv_path}")
        return True
    except Exception as e:
        logger.warning(f"HEAL AGENT | Pandas fix exec failed: {e}")
        return False


def run(state: AgentState) -> AgentState:
    failed_node = state["current_node"]
    error_msg   = state["node_errors"].get(failed_node, "unknown error")
    dataset     = state.get("dataset_name", PipelineConfig.ACTIVE_DATASET)

    logger.warning(
        f"HEAL AGENT | Activated | node={failed_node} | error={error_msg[:120]}"
    )

    # ── Safety: enforce max retries per node ──────────────────────
    retries = state.get("retry_count", {})
    retries[failed_node] = retries.get(failed_node, 0) + 1

    if retries[failed_node] > MAX_RETRIES:
        logger.error(
            f"HEAL AGENT | MAX RETRIES ({MAX_RETRIES}) reached for {failed_node}. "
            f"Escalating to alert."
        )
        state["give_up"]     = True
        state["retry_count"] = retries
        state["heal_log"]    = state.get("heal_log", []) + [{
            "node":       failed_node,
            "error_type": "max_retries_exceeded",
            "error_msg":  error_msg[:300],
            "fix":        f"Max retries ({MAX_RETRIES}) exceeded — escalated for manual review",
            "retry_num":  retries[failed_node],
        }]
        return state

    # ── Step 1: LLM classifies error type ─────────────────────────
    classify_prompt = CLASSIFY_PROMPT.format(
        node  = failed_node,
        error = error_msg[:600],
    )
    try:
        error_type = llm_client.invoke(classify_prompt).strip().lower()
    except Exception:
        error_type = "unknown"
    state["error_type"] = error_type
    logger.info(f"HEAL AGENT | Error classified as: '{error_type}'")

    # ── Step 2: Apply fix based on error type ─────────────────────
    fix_applied = ""

    # 2a. Connection / file error → reconnect MCP
    if error_type in ("connection_error", "file_not_found"):
        try:
            mcp_tool.reconnect()
            fix_applied = f"MCP reconnected (retry #{retries[failed_node]})"
            logger.success("HEAL AGENT | MCP reconnected successfully.")
        except Exception as e:
            fix_applied = f"Reconnect failed: {e}"
            logger.error(f"HEAL AGENT | Reconnect failed: {e}")

    # 2b. Invalid LLM output → set strict flag + clear bad rules
    elif error_type == "invalid_llm_output":
        state["ge_rules"]         = []
        state["llm_retry_strict"] = True
        fix_applied = (
            "Cleared invalid GE rules. Strict prompt mode enabled for retry."
        )
        logger.info("HEAL AGENT | Strict LLM prompt flag set.")

    # 2c. Data quality error → check memory first, then LLM fix → apply fix (with human approval if SQL)
    elif error_type == "data_quality_error":
        failed_checks = state.get("failed_checks", [])
        table = DATASET_TABLE_MAP.get(dataset, PipelineConfig.RAW_TABLE_CUSTOMERS)
        
        try:
            # B5: Attempt to recall similar past incident fix
            recalled_fix = pattern_memory.recall(error_msg)
            recalled = False
            if recalled_fix:
                logger.success("HEAL AGENT | Found similar past incident in memory! Reusing cached fix.")
                fix_code = recalled_fix
                recalled = True
                
            success = False
            if PipelineConfig.USE_LOCAL_CSV:
                if not recalled:
                    prompt = PANDAS_FIX_PROMPT.format(
                        dataset       = dataset,
                        failed_checks = json.dumps(failed_checks, indent=2),
                    )
                    fix_code = llm_client.invoke(prompt).strip()
                    fix_code = fix_code.replace("```python", "").replace("```", "").strip()
                
                logger.info(f"HEAL AGENT | Pandas fix code: {fix_code[:120]}")
                success = _apply_pandas_fix(fix_code, dataset)
                fix_applied = (
                    f"Pandas fix applied to CSV: {fix_code[:150]}"
                    if success else f"Pandas fix could not be applied: {fix_code[:80]}"
                )
            else:
                if not recalled:
                    prompt = SQL_FIX_PROMPT.format(
                        dataset       = dataset,
                        table         = table,
                        failed_checks = json.dumps(failed_checks, indent=2),
                    )
                    fix_code = llm_client.invoke(prompt).strip()
                    fix_code = fix_code.replace("```sql", "").replace("```", "").strip()
                
                logger.info(f"HEAL AGENT | SQL fix query: {fix_code[:120]}")
                
                # B3: Human-in-the-Loop approval check
                if wait_for_human_approval(fix_code, dataset, failed_node):
                    mcp_tool.call("snowflake_execute_sql", {"sql": fix_code})
                    fix_applied = f"SQL fix executed on Snowflake: {fix_code[:150]}"
                    success = True
                else:
                    fix_applied = "SQL fix query was REJECTED or timed out by human."
                    logger.error("HEAL AGENT | Fix rejected.")

            # B5: Remember the successful fix for learning
            if success and not recalled:
                pattern_memory.remember(error_msg, dataset, failed_node, fix_code)

            # Store for reference
            state["fix_sql"] = fix_code

            # Reset validation state so GE re-runs fresh
            state["validation_passed"] = False
            state["failed_checks"]     = []
            logger.success("HEAL AGENT | Data fix applied.")
        except Exception as e:
            fix_applied = f"Fix generation/application failed: {e}"
            logger.error(f"HEAL AGENT | Fix failed: {e}")

    # 2d. SQL / code error → LLM rewrites
    elif error_type == "sql_error":
        rewrite_prompt = f"""
        This code failed with error: {error_msg[:400]}
        Code: {state.get('fix_sql', 'not available')[:300]}
        Rewrite it to fix the error. Respond with ONLY the corrected code.
        """
        try:
            new_code = llm_client.invoke(rewrite_prompt).strip()
            state["fix_sql"] = new_code
            fix_applied = f"Code rewritten by LLM: {new_code[:150]}"
            logger.info(f"HEAL AGENT | Rewrote code: {new_code[:120]}")
        except Exception as e:
            fix_applied = f"Rewrite failed: {e}"

    # 2e. GE config error → reset GE rules
    elif error_type == "ge_config_error":
        state["ge_rules"] = []
        fix_applied = "GE rules cleared — will regenerate from rule_gen node."
        logger.info("HEAL AGENT | GE rules cleared for regeneration.")

    # 2f. Timeout → exponential backoff
    elif error_type == "timeout":
        wait_sec = 5 * retries[failed_node]
        logger.info(f"HEAL AGENT | Timeout — waiting {wait_sec}s before retry.")
        time.sleep(wait_sec)
        fix_applied = f"Waited {wait_sec}s after timeout (retry #{retries[failed_node]})."

    else:
        fix_applied = (
            f"Unrecognised error type '{error_type}' — retrying node as-is."
        )
        logger.warning(f"HEAL AGENT | Unknown error type: {error_type}")

    # ── Step 3: Update state ───────────────────────────────────────
    state["fix_applied"]  = fix_applied
    state["retry_count"]  = retries
    state["give_up"]      = False

    heal_entry = {
        "node":       failed_node,
        "error_type": error_type,
        "error_msg":  error_msg[:300],
        "fix":        fix_applied,
        "retry_num":  retries[failed_node],
    }
    state["heal_log"] = state.get("heal_log", []) + [heal_entry]

    logger.info(
        f"HEAL AGENT | Done | fix={fix_applied[:80]} | "
        f"retrying={failed_node} (attempt #{retries[failed_node]})"
    )

    return state
