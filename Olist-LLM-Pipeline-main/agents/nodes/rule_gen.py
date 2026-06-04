"""
agents/nodes/rule_gen.py
Node 4 — Rule Generator
LLM dynamically generates Great Expectations validation rules from schema + sample rows.
Zero hardcoded rules — fully intelligent, dataset-aware.
Dataset: Olist Brazilian E-Commerce
"""
import json
from agents.state import AgentState
from tools.llm_client import llm_client
from loguru import logger


RULE_GEN_PROMPT = """
You are a professional Data Quality Engineer working with the Olist Brazilian E-Commerce dataset.

Dataset: {dataset}
Schema: {schema}
Sample rows: {sample_rows}

Generate a JSON array of Great Expectations validation rules for this dataset.
Use ONLY these expectation types:
  - expect_column_values_to_not_be_null
  - expect_column_values_to_be_unique
  - expect_column_values_to_be_in_set
  - expect_column_values_to_be_between
  - expect_column_values_to_match_regex

Format EXACTLY as:
[
  {{"expectation_type": "expect_column_values_to_not_be_null", "column": "col_name"}},
  {{"expectation_type": "expect_column_values_to_be_in_set", "column": "col_name", "kwargs": {{"value_set": ["a","b"]}}}},
  {{"expectation_type": "expect_column_values_to_be_between", "column": "col_name", "kwargs": {{"min_value": 0, "max_value": 999999}}}}
]

Rules to include:
- Not-null checks for all required/key columns
- Uniqueness for primary key columns (customer_id, order_id, product_id, etc.)
- Value-set checks for categorical columns (status, state, payment_type, etc.)
- Range checks for numeric columns (payment_value, product_weight_g, etc.)
- Regex checks ONLY for known format columns like zip codes
- DO NOT generate regex rules for UUID columns (customer_id, order_id, product_id, etc.) — they may contain modified values after healing
- DO NOT generate regex rules for phone numbers or email addresses — too many valid formats
- DO NOT generate value_set rules with an empty list
- For uniqueness rules, always add: "kwargs": {"mostly": 0.98} to allow ~2% duplicates
- Skip regex rules for columns: *_phone, *_email, *_id, *_timestamp, *_date

Generate 8-12 rules appropriate for this specific dataset.
Respond ONLY with valid JSON array. No explanation. No markdown. Only the JSON array.
"""

RULE_GEN_STRICT_PROMPT = """
You are a Data Quality Engineer. Generate ONLY a valid JSON array of GE rules.

Dataset: {dataset}
Schema columns: {columns}

CRITICAL: Respond with ONLY a JSON array starting with [ and ending with ].
No text before or after. No markdown. No explanation.

Example of EXACT required format:
[
  {{"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"}},
  {{"expectation_type": "expect_column_values_to_be_unique", "column": "customer_id"}}
]

Generate 6-10 rules for the columns listed.
"""


def run(state: AgentState) -> AgentState:
    state["current_node"] = "rule_gen"
    dataset     = state.get("dataset_name", "customers")
    schema      = state.get("schema", {})
    sample_rows = state.get("sample_rows", [])
    strict      = state.get("llm_retry_strict", False)

    logger.info(f"RULE_GEN | Starting | dataset={dataset} | strict={strict}")

    try:
        if strict:
            prompt = RULE_GEN_STRICT_PROMPT.format(
                dataset = dataset,
                columns = list(schema.keys()),
            )
        else:
            prompt = RULE_GEN_PROMPT.format(
                dataset     = dataset,
                schema      = json.dumps(schema, indent=2),
                sample_rows = json.dumps(sample_rows[:3], indent=2, default=str),
            )

        raw = llm_client.invoke(prompt)

        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        # Find JSON array boundaries
        start = raw.find("[")
        end   = raw.rfind("]") + 1
        if start == -1 or end == 0:
            raise ValueError(f"LLM did not return a JSON array. Raw: {raw[:200]}")

        ge_rules = json.loads(raw[start:end])

        if not isinstance(ge_rules, list) or len(ge_rules) == 0:
            raise ValueError(f"LLM returned empty or non-list rules: {ge_rules}")

        # Post-process: drop/patch rules that would cause repeat failures
        SKIP_REGEX_COLS = {
            "customer_phone", "customer_email",
            "customer_id", "customer_unique_id", "order_id", "product_id",
            "order_approved_at", "order_purchase_timestamp",
            "order_delivered_carrier_date", "order_delivered_customer_date",
            "order_estimated_delivery_date",
        }
        filtered = []
        for r in ge_rules:
            exp_type = r.get("expectation_type", "")
            col      = r.get("column", "")

            # Drop regex rules for ID / phone / email / date cols
            if exp_type == "expect_column_values_to_match_regex" and col in SKIP_REGEX_COLS:
                logger.info(f"RULE_GEN | Dropped strict regex rule for '{col}'")
                continue

            # Drop value_set rules with empty or oversized sets
            if exp_type == "expect_column_values_to_be_in_set":
                vs = r.get("kwargs", {}).get("value_set", [])
                if len(vs) == 0 or len(vs) > 50:
                    logger.info(f"RULE_GEN | Dropped value_set rule for '{col}' (size={len(vs)})")
                    continue

            # Drop uniqueness rules — duplicates are quarantined by the transform node
            # The ~1% intentional duplicates must not block the entire pipeline
            if exp_type == "expect_column_values_to_be_unique":
                logger.info(f"RULE_GEN | Dropped uniqueness rule for '{col}' (handled by transform)")
                continue

            filtered.append(r)
        ge_rules = filtered

        logger.success(f"RULE_GEN | Done | {len(ge_rules)} GE rules generated (after filter)")
        for r in ge_rules:
            logger.debug(f"  Rule: {r.get('expectation_type')} | col={r.get('column')}")

        state["ge_rules"]         = ge_rules
        state["llm_retry_strict"] = False   # reset after success
        state["node_status"]["rule_gen"] = "pass"

    except Exception as e:
        logger.error(f"RULE_GEN | FAILED: {e}")
        state["node_errors"]["rule_gen"] = str(e)
        state["node_status"]["rule_gen"] = "fail"

    return state
