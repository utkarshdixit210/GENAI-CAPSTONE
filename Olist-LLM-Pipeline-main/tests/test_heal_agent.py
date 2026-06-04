"""
tests/test_heal_agent.py
Unit tests for the Heal Agent — verifies all 7 error type handlers.
"""
import pytest
from unittest.mock import patch, MagicMock


def _base_state(node="validator", error="test error"):
    return {
        "current_node": node,
        "node_status":  {node: "fail"},
        "node_errors":  {node: error},
        "error_type":   "",
        "fix_applied":  "",
        "retry_count":  {},
        "heal_log":     [],
        "give_up":      False,
        "ge_rules":     [{"expectation_type": "expect_column_values_to_not_be_null", "column": "order_id"}],
        "failed_checks": [{"column": "order_id", "issue": "1200 null values", "failed_count": 1200}],
        "fix_sql":      "",
        "llm_retry_strict": False,
        "dataset_name": "orders",
        "schema":              {},
        "sample_rows":         [],
        "row_count":           0,
        "schema_drift_events": [],
        "schema_drift_run_id": "",
        "pii_map":             {},
        "llm_retry_strict":    False,
        "validation_passed":   False,
        "clean_row_count":     0,
        "quarantine_count":    0,
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


class TestHealAgentMaxRetries:
    def test_give_up_after_max_retries(self):
        from agents.nodes.heal_agent import run
        state = _base_state()
        state["retry_count"] = {"validator": 3}  # already at MAX

        with patch("agents.nodes.heal_agent.llm_client") as mock_llm:
            mock_llm.invoke.return_value = "data_quality_error"
            result = run(state)

        assert result["give_up"] is True
        assert len(result["heal_log"]) == 1
        assert result["heal_log"][0]["error_type"] == "max_retries_exceeded"

    def test_first_retry_allowed(self):
        from agents.nodes.heal_agent import run
        state = _base_state()

        with patch("agents.nodes.heal_agent.llm_client") as mock_llm, \
             patch("agents.nodes.heal_agent.mcp_tool") as mock_mcp:
            mock_llm.invoke.side_effect = ["connection_error"]
            mock_mcp.reconnect.return_value = None
            result = run(state)

        assert result["give_up"] is False
        assert result["retry_count"]["validator"] == 1
        assert len(result["heal_log"]) == 1


class TestHealAgentErrorTypes:
    def _run_with_error_type(self, error_type: str, extra_state=None):
        from agents.nodes.heal_agent import run
        state = _base_state()
        if extra_state:
            state.update(extra_state)

        with patch("agents.nodes.heal_agent.llm_client") as mock_llm, \
             patch("agents.nodes.heal_agent.mcp_tool") as mock_mcp, \
             patch("agents.nodes.heal_agent._apply_pandas_fix", return_value=True):
            mock_llm.invoke.return_value = error_type
            mock_mcp.reconnect.return_value = None
            mock_mcp.call.return_value = {"status": "executed"}
            result = run(state)

        return result

    def test_connection_error_reconnects(self):
        result = self._run_with_error_type("connection_error")
        assert "reconnect" in result["fix_applied"].lower() or "mcp" in result["fix_applied"].lower()
        assert result["give_up"] is False

    def test_invalid_llm_output_sets_strict(self):
        result = self._run_with_error_type("invalid_llm_output")
        assert result["llm_retry_strict"] is True
        assert result["ge_rules"] == []

    def test_data_quality_resets_validation(self):
        result = self._run_with_error_type("data_quality_error")
        assert result["validation_passed"] is False
        assert result["failed_checks"] == []

    def test_ge_config_clears_rules(self):
        result = self._run_with_error_type("ge_config_error")
        assert result["ge_rules"] == []

    def test_timeout_sets_fix(self):
        with patch("agents.nodes.heal_agent.time.sleep"):
            result = self._run_with_error_type("timeout")
        assert "waited" in result["fix_applied"].lower() or "timeout" in result["fix_applied"].lower()

    def test_unknown_error_still_logs(self):
        result = self._run_with_error_type("some_unknown_error_xyz")
        assert len(result["heal_log"]) == 1
        assert result["give_up"] is False


class TestHealAgentStateUpdates:
    def test_heal_log_appended(self):
        from agents.nodes.heal_agent import run
        state = _base_state()
        with patch("agents.nodes.heal_agent.llm_client") as mock_llm, \
             patch("agents.nodes.heal_agent.mcp_tool"):
            mock_llm.invoke.return_value = "timeout"
            with patch("agents.nodes.heal_agent.time.sleep"):
                result = run(state)

        assert len(result["heal_log"]) == 1
        entry = result["heal_log"][0]
        assert entry["node"] == "validator"
        assert entry["retry_num"] == 1
        assert "fix" in entry

    def test_retry_count_increments(self):
        from agents.nodes.heal_agent import run
        state = _base_state()
        state["retry_count"] = {"validator": 1}

        with patch("agents.nodes.heal_agent.llm_client") as mock_llm, \
             patch("agents.nodes.heal_agent.mcp_tool"):
            mock_llm.invoke.return_value = "connection_error"
            result = run(state)

        assert result["retry_count"]["validator"] == 2
