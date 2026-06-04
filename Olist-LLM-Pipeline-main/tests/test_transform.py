"""
tests/test_transform.py
Unit tests for the Transform node — Bronze to Silver split.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


def _state(dataset="customers", ge_rules=None):
    return {
        "current_node": "transform",
        "node_status":  {},
        "node_errors":  {},
        "dataset_name": dataset,
        "schema":       {},
        "sample_rows":  [],
        "row_count":    5,
        "schema_drift_events": [],
        "schema_drift_run_id": "",
        "pii_map": {},
        "ge_rules": ge_rules or [
            {"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"},
        ],
        "llm_retry_strict": False,
        "validation_passed": False,
        "failed_checks": [],
        "error_type": "", "fix_applied": "",
        "retry_count": {}, "heal_log": [], "give_up": False,
        "clean_row_count": 0, "quarantine_count": 0, "fix_sql": "",
        "clean_df_path": "", "quarantine_df_path": "",
        "masked_table": "", "masked_row_count": 0, "masked_df_path": "",
        "gold_kpis": {}, "gold_df_path": "",
        "lineage_run_id": "", "audit_written": False, "audit_report": "",
    }


CLEAN_DF = pd.DataFrame({
    "customer_id":              ["c1","c2","c3"],
    "customer_unique_id":       ["u1","u2","u3"],
    "customer_zip_code_prefix": [1310, 20040, 30130],
    "customer_city":            ["sao paulo","rio de janeiro","belo horizonte"],
    "customer_state":           ["SP","RJ","MG"],
})

DIRTY_DF = pd.DataFrame({
    "customer_id":              ["c1", None, "c3"],
    "customer_unique_id":       ["u1","u2","u3"],
    "customer_zip_code_prefix": [1310, 20040, 30130],
    "customer_city":            ["sao paulo","rio de janeiro","belo horizonte"],
    "customer_state":           ["SP","XX","MG"],
})


class TestTransformCleanData:
    def test_clean_data_all_to_silver(self):
        from agents.nodes.transform import run
        state = _state()
        with patch("agents.nodes.transform.mcp_tool") as mock_mcp:
            mock_mcp.call.side_effect = [CLEAN_DF.copy(),
                                          {"status":"written","path":"outputs/silver/x.csv"},
                                          {"status":"written","path":"outputs/bronze/x.csv"}]
            result = run(state)

        assert result["node_status"]["transform"] == "pass"
        assert result["clean_row_count"] == 3
        assert result["quarantine_count"] == 0

    def test_dirty_data_split(self):
        from agents.nodes.transform import run
        state = _state()
        with patch("agents.nodes.transform.mcp_tool") as mock_mcp:
            mock_mcp.call.side_effect = [DIRTY_DF.copy(),
                                          {"status":"written","path":"outputs/silver/x.csv"},
                                          {"status":"written","path":"outputs/bronze/x.csv"}]
            result = run(state)

        assert result["node_status"]["transform"] == "pass"
        assert result["clean_row_count"] < 3     # some quarantined
        assert result["quarantine_count"] > 0

    def test_no_rules_all_clean(self):
        from agents.nodes.transform import run
        state = _state(ge_rules=[])
        with patch("agents.nodes.transform.mcp_tool") as mock_mcp:
            mock_mcp.call.side_effect = [DIRTY_DF.copy(),
                                          {"status":"written","path":"x"}]
            result = run(state)

        # No rules → everything treated as clean (no quarantine)
        assert result["quarantine_count"] == 0

    def test_mcp_failure_sets_fail(self):
        from agents.nodes.transform import run
        state = _state()
        with patch("agents.nodes.transform.mcp_tool") as mock_mcp:
            mock_mcp.call.side_effect = Exception("connection lost")
            result = run(state)

        assert result["node_status"]["transform"] == "fail"
        assert "transform" in result["node_errors"]


class TestTransformOlistHealing:
    def test_state_normalisation(self):
        from agents.nodes.transform import _apply_olist_healing
        df = pd.DataFrame({
            "customer_id": ["c1","c2"],
            "customer_state": ["sp", "SAO PAULO"],
            "customer_city": ["  SÃO PAULO  ", "curitiba"],
            "customer_unique_id": ["u1","u2"],
            "customer_zip_code_prefix": [1310, 80010],
        })
        result = _apply_olist_healing(df, "customers")
        # All states should be upper-case 2-letter or UNKNOWN
        for state in result["customer_state"]:
            assert state == state.upper()

    def test_negative_payment_fixed(self):
        from agents.nodes.transform import _apply_olist_healing
        df = pd.DataFrame({
            "order_id": ["o1","o2"],
            "payment_sequential": [1,1],
            "payment_type": ["credit_card","boleto"],
            "payment_installments": [1,1],
            "payment_value": [-100.0, 50.0],
        })
        result = _apply_olist_healing(df, "payments")
        assert (result["payment_value"] >= 0).all()

    def test_duplicates_removed(self):
        from agents.nodes.transform import _apply_olist_healing
        df = pd.DataFrame({
            "customer_id":              ["c1","c1","c2"],
            "customer_unique_id":       ["u1","u1","u2"],
            "customer_zip_code_prefix": [1310,1310,20040],
            "customer_city":            ["sao paulo","sao paulo","rio"],
            "customer_state":           ["SP","SP","RJ"],
        })
        result = _apply_olist_healing(df, "customers")
        assert len(result) == 2
