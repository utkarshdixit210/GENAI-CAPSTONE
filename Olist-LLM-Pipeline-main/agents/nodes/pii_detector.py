"""
agents/nodes/pii_detector.py
Node 3 — PII Detector
LLM reads schema + sample rows and classifies every column by PII sensitivity.
Result written to pii_map in State for the masking node.
Dataset: Olist Brazilian E-Commerce
"""
import json
from agents.state import AgentState
from tools.llm_client import llm_client
from loguru import logger


PII_DETECT_PROMPT = """
You are a privacy and data governance expert.

Dataset: {dataset}
Schema columns and their data types:
{schema}

Sample rows:
{sample_rows}

Classify EVERY column by PII sensitivity level:
- HIGH   → directly identifies a person (name, email, SSN, phone, address, account number)
- MEDIUM → indirectly identifies (customer_id, order_id, unique_id, zip_code)
- LOW    → contextual but not identifying (amount, city, state, category, status)
- NONE   → no privacy concern (timestamps, flags, system codes, counts)

Respond ONLY with valid JSON in this exact format:
{{
  "column_name": {{"pii_level": "HIGH|MEDIUM|LOW|NONE", "reason": "brief reason"}},
  ...
}}

Classify ALL columns. No explanation. No markdown. Only JSON.
"""


def run(state: AgentState) -> AgentState:
    state["current_node"] = "pii_detector"
    dataset     = state.get("dataset_name", "customers")
    schema      = state.get("schema", {})
    sample_rows = state.get("sample_rows", [])

    logger.info(f"PII_DETECTOR | Starting | dataset={dataset} | columns={list(schema.keys())}")

    try:
        prompt = PII_DETECT_PROMPT.format(
            dataset     = dataset,
            schema      = json.dumps(schema, indent=2),
            sample_rows = json.dumps(sample_rows[:3], indent=2, default=str),
        )

        raw = llm_client.invoke(prompt)

        # Strip markdown fences if present
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        pii_map = json.loads(raw)

        high   = [c for c, v in pii_map.items() if v.get("pii_level") == "HIGH"]
        medium = [c for c, v in pii_map.items() if v.get("pii_level") == "MEDIUM"]
        low    = [c for c, v in pii_map.items() if v.get("pii_level") == "LOW"]

        logger.success(
            f"PII_DETECTOR | Done | HIGH={high} | MEDIUM={medium} | LOW={low}"
        )

        state["pii_map"] = pii_map
        state["node_status"]["pii_detector"] = "pass"

    except Exception as e:
        logger.error(f"PII_DETECTOR | FAILED: {e}")
        state["node_errors"]["pii_detector"] = str(e)
        state["node_status"]["pii_detector"] = "fail"

    return state
