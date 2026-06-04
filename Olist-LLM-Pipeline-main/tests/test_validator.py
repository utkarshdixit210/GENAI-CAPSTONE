"""
tests/test_validator.py
Unit tests for the Validator node — pandas-based GE rule execution.
"""
import pytest
import pandas as pd
from unittest.mock import patch


def _base_state(dataset="payments"):
    return {
        "current_node": "validator",
        "node_status":  {},
        "node_errors":  {},
        "dataset_name": dataset,
        "schema": {},
        "sample_rows": [],
        "row_count": 0,
        "schema_drift_events": [], "schema_drift_run_id": "",
        "pii_map": {}, "error_type": "", "fix_applied": "",
        "retry_count": {}, "heal_log": [], "give_up": False,
        "llm_retry_strict": False,
        "ge_rules": [
            {"expectation_type": "expect_column_values_to_not_be_null", "column": "order_id"},
            {"expectation_type": "expect_column_values_to_be_in_set",
             "column": "payment_type",
             "kwargs": {"value_set": ["credit_card", "boleto", "voucher", "debit_card"]}},
            {"expectation_type": "expect_column_values_to_be_between",
             "column": "payment_value",
             "kwargs": {"min_value": 0, "max_value": 999999}},
        ],
        "validation_passed": False,
        "failed_checks": [],
        "clean_row_count": 0, "quarantine_count": 0, "fix_sql": "",
        "clean_df_path": "", "quarantine_df_path": "",
        "masked_table": "", "masked_row_count": 0, "masked_df_path": "",
        "gold_kpis": {}, "gold_df_path": "",
        "lineage_run_id": "", "audit_written": False, "audit_report": "",
    }


CLEAN_DF = pd.DataFrame({
    "order_id":           ["o1", "o2", "o3"],
    "payment_type":       ["credit_card", "boleto", "voucher"],
    "payment_value":      [50.0, 120.0, 30.0],
    "payment_sequential": [1, 1, 1],
    "payment_installments": [1, 3, 1],
})

DIRTY_DF = pd.DataFrame({
    "order_id":     ["o1", None, "o3"],
    "payment_type": ["credit_card", "invalid_type", "voucher"],
    "payment_value": [50.0, -10.0, 30.0],
    "payment_sequential": [1, 1, 1],
    "payment_installments": [1, 3, 1],
})


class TestValidatorPass:
    def test_clean_data_passes(self):
        from agents.nodes.validator import run
        state = _base_state()
        with patch("agents.nodes.validator.mcp_tool") as mock_mcp:
            mock_mcp.call.return_value = CLEAN_DF.copy()
            result = run(state)

        assert result["node_status"]["validator"] == "pass"
        assert result["validation_passed"] is True
        assert result["failed_checks"] == []


class TestValidatorFail:
    def test_dirty_data_fails(self):
        from agents.nodes.validator import run
        state = _base_state()
        with patch("agents.nodes.validator.mcp_tool") as mock_mcp:
            mock_mcp.call.return_value = DIRTY_DF.copy()
            result = run(state)

        assert result["node_status"]["validator"] == "fail"
        assert result["validation_passed"] is False
        assert len(result["failed_checks"]) > 0

    def test_null_check_detected(self):
        from agents.nodes.validator import run
        state = _base_state()
        with patch("agents.nodes.validator.mcp_tool") as mock_mcp:
            mock_mcp.call.return_value = DIRTY_DF.copy()
            result = run(state)

        failed_cols = [f["column"] for f in result["failed_checks"]]
        assert "order_id" in failed_cols

    def test_value_set_violation_detected(self):
        from agents.nodes.validator import run
        state = _base_state()
        with patch("agents.nodes.validator.mcp_tool") as mock_mcp:
            mock_mcp.call.return_value = DIRTY_DF.copy()
            result = run(state)

        failed_cols = [f["column"] for f in result["failed_checks"]]
        assert "payment_type" in failed_cols

    def test_no_rules_returns_fail(self):
        from agents.nodes.validator import run
        state = _base_state()
        state["ge_rules"] = []
        with patch("agents.nodes.validator.mcp_tool") as mock_mcp:
            mock_mcp.call.return_value = CLEAN_DF.copy()
            result = run(state)

        assert result["node_status"]["validator"] == "fail"
