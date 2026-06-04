"""
tests/test_pipeline_integration.py
Integration tests — runs the full LangGraph pipeline with mocked MCP and LLM.
Validates state flows correctly through all 11 nodes.
"""
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock


SAMPLE_CUSTOMERS = pd.DataFrame({
    "customer_id":              ["c1","c2","c3"],
    "customer_unique_id":       ["u1","u2","u3"],
    "customer_zip_code_prefix": [1310, 20040, 30130],
    "customer_city":            ["sao paulo","rio","belo horizonte"],
    "customer_state":           ["SP","RJ","MG"],
})

MOCK_SCHEMA = {
    "customer_id": "object", "customer_unique_id": "object",
    "customer_zip_code_prefix": "int64", "customer_city": "object", "customer_state": "object",
}

MOCK_GE_RULES = [
    {"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"},
    {"expectation_type": "expect_column_values_to_be_unique",   "column": "customer_id"},
]

MOCK_PII_MAP = """{
  "customer_id": {"pii_level": "MEDIUM", "reason": "indirect"},
  "customer_unique_id": {"pii_level": "MEDIUM", "reason": "indirect"},
  "customer_zip_code_prefix": {"pii_level": "LOW", "reason": "coarse"},
  "customer_city": {"pii_level": "LOW", "reason": "coarse"},
  "customer_state": {"pii_level": "NONE", "reason": "aggregate"}
}"""

MOCK_GE_RULES_JSON = '[{"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"}, {"expectation_type": "expect_column_values_to_be_unique", "column": "customer_id"}]'

MOCK_INSIGHTS = '[{"insight": "SP leads with most customers.", "category": "geography"}]'

MOCK_AUDIT_REPORT = "Pipeline ran successfully with no major issues."


def _initial_state():
    return {
        "current_node":        "",
        "node_status":         {},
        "node_errors":         {},
        "error_type":          "",
        "fix_applied":         "",
        "retry_count":         {},
        "heal_log":            [],
        "give_up":             False,
        "dataset_name":        "customers",
        "schema":              {},
        "sample_rows":         [],
        "row_count":           0,
        "schema_drift_events": [],
        "schema_drift_run_id": "",
        "pii_map":             {},
        "ge_rules":            [],
        "llm_retry_strict":    False,
        "validation_passed":   False,
        "failed_checks":       [],
        "clean_row_count":     0,
        "quarantine_count":    0,
        "fix_sql":             "",
        "clean_df_path":       "",
        "quarantine_df_path":  "",
        "masked_table":        "",
        "masked_row_count":    0,
        "masked_df_path":      "",
        "gold_kpis":           {},
        "gold_df_path":        "",
        "lineage_run_id":      "",
        "audit_written":       False,
        "audit_report":        "",
    }


class TestFullPipelineHappyPath:
    @pytest.mark.integration
    def test_all_nodes_pass(self):
        """Full pipeline on clean data — all nodes should pass, no heals."""
        with patch("tools.snowflake_mcp_tool.mcp_tool") as mock_mcp, \
             patch("tools.llm_client.llm_client") as mock_llm:

            # MCP returns for each node
            mock_mcp.call.side_effect = [
                MOCK_SCHEMA,                            # profile: get_schema
                SAMPLE_CUSTOMERS.head(5).to_dict("records"), # profile: sample_rows
                3,                                      # profile: row_count
                None,                                   # schema_drift: append_json (no drift)
                {"status":"appended"},                  # lineage
                {"status":"written","path":"outputs/silver/SILVER_CUSTOMERS_CLEAN.csv"},  # transform write
                SAMPLE_CUSTOMERS.copy(),                # pii_masker: read_table
                {"status":"written","path":"outputs/silver/SILVER_CUSTOMERS_MASKED.csv"}, # pii_masker write
                SAMPLE_CUSTOMERS.copy(),                # gold_kpi: read_table
                {"status":"written","path":"outputs/gold/GOLD_CUSTOMERS_KPIS.csv"},       # gold write
            ]
            mock_mcp.call.return_value = {"status":"appended","path":"metadata/x.json"}

            # LLM returns
            mock_llm.invoke.side_effect = [
                '{"renamed":[]}',    # schema_drift LLM
                MOCK_PII_MAP,        # pii_detector
                MOCK_GE_RULES_JSON,  # rule_gen
                MOCK_INSIGHTS,       # gold_kpi insights
                MOCK_AUDIT_REPORT,   # alert
            ]

            # Run full pipeline
            from pipeline.graph import build_pipeline
            pipeline = build_pipeline()
            final = pipeline.invoke(_initial_state())

        assert final.get("give_up") is False
        assert len(final.get("heal_log", [])) == 0


class TestPipelineHealOnce:
    @pytest.mark.integration
    def test_rule_gen_fails_then_heals(self):
        """
        Simulate: rule_gen node fails once with invalid JSON → heal agent fixes →
        rule_gen succeeds on retry → rest of pipeline passes.
        """
        call_count = {"n": 0}
        llm_responses = [
            '{"renamed":[]}',           # schema_drift
            MOCK_PII_MAP,               # pii_detector
            "NOT VALID JSON AT ALL",    # rule_gen FAIL
            "invalid_llm_output",       # heal classify
            MOCK_GE_RULES_JSON,         # rule_gen RETRY (strict mode)
            MOCK_INSIGHTS,              # gold_kpi
            MOCK_AUDIT_REPORT,          # alert
        ]
        llm_iter = iter(llm_responses)

        def llm_invoke(prompt):
            return next(llm_iter)

        with patch("tools.snowflake_mcp_tool.mcp_tool") as mock_mcp, \
             patch("tools.llm_client.llm_client") as mock_llm:

            mock_mcp.call.return_value = {"status":"appended","path":"metadata/x.json"}
            mock_mcp.call.side_effect = None  # reset

            # Default returns for all MCP calls
            def mcp_side(*args, **kwargs):
                tool = args[0] if args else kwargs.get("tool_name","")
                params = args[1] if len(args)>1 else kwargs.get("params",{})
                if tool == "snowflake_get_schema":   return MOCK_SCHEMA
                if tool == "snowflake_sample_rows":  return SAMPLE_CUSTOMERS.head(3).to_dict("records")
                if tool == "snowflake_row_count":    return 3
                if tool == "snowflake_fetch_data":   return SAMPLE_CUSTOMERS.copy()
                if tool == "snowflake_read_table":   return SAMPLE_CUSTOMERS.copy()
                return {"status":"written","path":"outputs/x.csv"}

            mock_mcp.call.side_effect = mcp_side
            mock_llm.invoke.side_effect = llm_invoke

            from pipeline.graph import build_pipeline
            pipeline = build_pipeline()
            final = pipeline.invoke(_initial_state())

        # Should have healed once and completed
        assert final.get("give_up") is False
        heals = final.get("heal_log", [])
        assert len(heals) >= 1
        assert heals[0]["node"] == "rule_gen"
