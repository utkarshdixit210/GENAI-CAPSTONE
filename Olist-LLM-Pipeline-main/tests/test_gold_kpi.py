"""
tests/test_gold_kpi.py
Unit tests for the Gold KPI Generator node.
"""
import pytest
import pandas as pd
from unittest.mock import patch


def _state(dataset="payments"):
    return {
        "current_node": "gold_kpi",
        "node_status":  {},
        "node_errors":  {},
        "dataset_name": dataset,
        "schema": {}, "sample_rows": [], "row_count": 0,
        "schema_drift_events": [], "schema_drift_run_id": "",
        "pii_map": {}, "ge_rules": [], "llm_retry_strict": False,
        "validation_passed": True, "failed_checks": [],
        "error_type": "", "fix_applied": "", "retry_count": {},
        "heal_log": [], "give_up": False,
        "clean_row_count": 100, "quarantine_count": 5, "fix_sql": "",
        "clean_df_path": "outputs/silver/SILVER_PAYMENTS_CLEAN.csv",
        "quarantine_df_path": "", "masked_table": "",
        "masked_row_count": 0, "masked_df_path": "",
        "gold_kpis": {}, "gold_df_path": "",
        "lineage_run_id": "", "audit_written": False, "audit_report": "",
    }


PAYMENTS_DF = pd.DataFrame({
    "order_id":             ["o1","o2","o3","o4","o5"],
    "payment_sequential":   [1,1,1,1,2],
    "payment_type":         ["credit_card","boleto","credit_card","voucher","credit_card"],
    "payment_installments": [3,1,6,1,1],
    "payment_value":        [150.0, 89.90, 320.0, 45.00, 75.50],
})

CUSTOMERS_DF = pd.DataFrame({
    "customer_id":              ["c1","c2","c3","c4","c5"],
    "customer_unique_id":       ["u1","u2","u3","u4","u5"],
    "customer_zip_code_prefix": [1310,20040,30130,80010,90010],
    "customer_city":            ["sao paulo","rio de janeiro","belo horizonte","curitiba","porto alegre"],
    "customer_state":           ["SP","RJ","MG","PR","RS"],
})

MOCK_INSIGHTS = '[{"insight": "SP has the most customers.", "category": "geography"}]'


class TestGoldKPIPayments:
    def test_revenue_kpis_computed(self):
        from agents.nodes.gold_kpi import run
        state = _state("payments")
        with patch("agents.nodes.gold_kpi.mcp_tool") as mock_mcp, \
             patch("agents.nodes.gold_kpi.llm_client") as mock_llm:
            mock_mcp.call.side_effect = [PAYMENTS_DF.copy(), {"status":"written","path":"x"}]
            mock_llm.invoke.return_value = MOCK_INSIGHTS
            result = run(state)

        assert result["node_status"]["gold_kpi"] == "pass"
        kpis = result["gold_kpis"]
        assert "total_revenue" in kpis
        assert "avg_payment_value" in kpis
        assert abs(kpis["total_revenue"] - 680.40) < 0.01

    def test_payment_type_breakdown(self):
        from agents.nodes.gold_kpi import run
        state = _state("payments")
        with patch("agents.nodes.gold_kpi.mcp_tool") as mock_mcp, \
             patch("agents.nodes.gold_kpi.llm_client") as mock_llm:
            mock_mcp.call.side_effect = [PAYMENTS_DF.copy(), {"status":"written","path":"x"}]
            mock_llm.invoke.return_value = MOCK_INSIGHTS
            result = run(state)

        assert "payment_type_breakdown" in result["gold_kpis"]


class TestGoldKPICustomers:
    def test_customer_kpis_computed(self):
        from agents.nodes.gold_kpi import run
        state = _state("customers")
        with patch("agents.nodes.gold_kpi.mcp_tool") as mock_mcp, \
             patch("agents.nodes.gold_kpi.llm_client") as mock_llm:
            mock_mcp.call.side_effect = [CUSTOMERS_DF.copy(), {"status":"written","path":"x"}]
            mock_llm.invoke.return_value = MOCK_INSIGHTS
            result = run(state)

        assert result["node_status"]["gold_kpi"] == "pass"
        kpis = result["gold_kpis"]
        assert kpis["total_customers"] == 5
        assert kpis["states_covered"] == 5

    def test_business_insights_populated(self):
        from agents.nodes.gold_kpi import run
        state = _state("customers")
        with patch("agents.nodes.gold_kpi.mcp_tool") as mock_mcp, \
             patch("agents.nodes.gold_kpi.llm_client") as mock_llm:
            mock_mcp.call.side_effect = [CUSTOMERS_DF.copy(), {"status":"written","path":"x"}]
            mock_llm.invoke.return_value = MOCK_INSIGHTS
            result = run(state)

        assert "business_insights" in result["gold_kpis"]
        assert len(result["gold_kpis"]["business_insights"]) > 0


class TestGoldKPIFailure:
    def test_mcp_failure_sets_fail(self):
        from agents.nodes.gold_kpi import run
        state = _state("payments")
        with patch("agents.nodes.gold_kpi.mcp_tool") as mock_mcp:
            mock_mcp.call.side_effect = Exception("file not found")
            result = run(state)

        assert result["node_status"]["gold_kpi"] == "fail"
        assert "gold_kpi" in result["node_errors"]

    def test_llm_insight_failure_non_critical(self):
        """LLM insight failure should not fail the node — it's non-critical."""
        from agents.nodes.gold_kpi import run
        state = _state("payments")
        with patch("agents.nodes.gold_kpi.mcp_tool") as mock_mcp, \
             patch("agents.nodes.gold_kpi.llm_client") as mock_llm:
            mock_mcp.call.side_effect = [PAYMENTS_DF.copy(), {"status":"written","path":"x"}]
            mock_llm.invoke.side_effect = Exception("LLM timeout")
            result = run(state)

        # Node should still pass even if insights fail
        assert result["node_status"]["gold_kpi"] == "pass"
        assert result["gold_kpis"].get("business_insights", []) == []
