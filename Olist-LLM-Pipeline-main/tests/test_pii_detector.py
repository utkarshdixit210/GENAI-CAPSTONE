"""
tests/test_pii_detector.py
Unit tests for the PII Detector node.
"""
import pytest
from unittest.mock import patch


def _base_state(dataset="customers"):
    return {
        "current_node": "pii_detector",
        "node_status":  {},
        "node_errors":  {},
        "dataset_name": dataset,
        "schema": {
            "customer_id": "object",
            "customer_unique_id": "object",
            "customer_zip_code_prefix": "int64",
            "customer_city": "object",
            "customer_state": "object",
        },
        "sample_rows": [
            {"customer_id": "abc123", "customer_unique_id": "uid456",
             "customer_zip_code_prefix": "01310", "customer_city": "sao paulo",
             "customer_state": "SP"},
        ],
        "error_type": "", "fix_applied": "", "retry_count": {}, "heal_log": [],
        "give_up": False, "schema_drift_events": [], "schema_drift_run_id": "",
        "pii_map": {}, "ge_rules": [], "llm_retry_strict": False,
        "validation_passed": False, "failed_checks": [], "row_count": 0,
        "clean_row_count": 0, "quarantine_count": 0, "fix_sql": "",
        "clean_df_path": "", "quarantine_df_path": "", "masked_table": "",
        "masked_row_count": 0, "masked_df_path": "", "gold_kpis": {},
        "gold_df_path": "", "lineage_run_id": "", "audit_written": False, "audit_report": "",
    }


MOCK_PII_RESPONSE = """{
  "customer_id": {"pii_level": "MEDIUM", "reason": "indirect identifier"},
  "customer_unique_id": {"pii_level": "MEDIUM", "reason": "indirect identifier"},
  "customer_zip_code_prefix": {"pii_level": "LOW", "reason": "coarse location"},
  "customer_city": {"pii_level": "LOW", "reason": "coarse location"},
  "customer_state": {"pii_level": "NONE", "reason": "no privacy concern"}
}"""


class TestPIIDetector:
    def test_pii_map_populated(self):
        from agents.nodes.pii_detector import run
        state = _base_state()
        with patch("agents.nodes.pii_detector.llm_client") as mock_llm:
            mock_llm.invoke.return_value = MOCK_PII_RESPONSE
            result = run(state)

        assert result["node_status"]["pii_detector"] == "pass"
        assert "customer_id" in result["pii_map"]
        assert result["pii_map"]["customer_id"]["pii_level"] == "MEDIUM"

    def test_all_columns_classified(self):
        from agents.nodes.pii_detector import run
        state = _base_state()
        with patch("agents.nodes.pii_detector.llm_client") as mock_llm:
            mock_llm.invoke.return_value = MOCK_PII_RESPONSE
            result = run(state)

        schema_cols = set(state["schema"].keys())
        pii_cols    = set(result["pii_map"].keys())
        assert schema_cols == pii_cols

    def test_llm_failure_sets_fail(self):
        from agents.nodes.pii_detector import run
        state = _base_state()
        with patch("agents.nodes.pii_detector.llm_client") as mock_llm:
            mock_llm.invoke.side_effect = Exception("LLM timeout")
            result = run(state)

        assert result["node_status"]["pii_detector"] == "fail"
        assert "pii_detector" in result["node_errors"]

    def test_markdown_fence_stripped(self):
        from agents.nodes.pii_detector import run
        state = _base_state()
        fenced = f"```json\n{MOCK_PII_RESPONSE}\n```"
        with patch("agents.nodes.pii_detector.llm_client") as mock_llm:
            mock_llm.invoke.return_value = fenced
            result = run(state)

        assert result["node_status"]["pii_detector"] == "pass"
        assert len(result["pii_map"]) > 0
