"""
tests/conftest.py
Shared pytest fixtures for all test modules.
"""
import pytest
import pandas as pd


@pytest.fixture
def base_state():
    """Minimal valid AgentState for any node test."""
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


@pytest.fixture
def customers_df():
    """Clean synthetic customers DataFrame."""
    return pd.DataFrame({
        "customer_id":              ["c1","c2","c3","c4","c5"],
        "customer_unique_id":       ["u1","u2","u3","u4","u5"],
        "customer_zip_code_prefix": [1310, 20040, 30130, 80010, 90010],
        "customer_city":            ["sao paulo","rio de janeiro","belo horizonte","curitiba","porto alegre"],
        "customer_state":           ["SP","RJ","MG","PR","RS"],
    })


@pytest.fixture
def dirty_customers_df():
    """Dirty customers DataFrame with known quality issues."""
    return pd.DataFrame({
        "customer_id":              ["c1", None, "c3", "c4", "c1"],  # null + duplicate
        "customer_unique_id":       ["u1","u2","u3","u4","u5"],
        "customer_zip_code_prefix": [1310, 20040, 30130, 80010, 90010],
        "customer_city":            ["sao paulo","rio de janeiro","belo horizonte","curitiba","porto alegre"],
        "customer_state":           ["SP","INVALID","MG","PR","RS"],   # invalid state
    })


@pytest.fixture
def payments_df():
    """Clean payments DataFrame."""
    return pd.DataFrame({
        "order_id":             ["o1","o2","o3"],
        "payment_sequential":   [1, 1, 2],
        "payment_type":         ["credit_card","boleto","voucher"],
        "payment_installments": [3, 1, 1],
        "payment_value":        [150.0, 89.90, 45.00],
    })


@pytest.fixture
def dirty_payments_df():
    """Dirty payments DataFrame with quality issues."""
    return pd.DataFrame({
        "order_id":             ["o1", None, "o3"],
        "payment_sequential":   [1, 1, 2],
        "payment_type":         ["credit_card","INVALID_TYPE","voucher"],
        "payment_installments": [3, 1, 1],
        "payment_value":        [150.0, -50.0, 45.00],
    })


@pytest.fixture
def sample_ge_rules():
    """Sample GE rules for customers dataset."""
    return [
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_be_unique",   "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "column": "customer_state",
         "kwargs": {"value_set": ["SP","RJ","MG","PR","RS","BA","GO","PE","CE","MA",
                                   "SC","MT","MS","PA","RN","PI","AL","PB","ES","TO",
                                   "AM","RO","AC","AP","RR","SE","DF","UNKNOWN"]}},
    ]


@pytest.fixture
def sample_pii_map():
    """Sample PII map for customers dataset."""
    return {
        "customer_id":              {"pii_level": "MEDIUM", "reason": "indirect identifier"},
        "customer_unique_id":       {"pii_level": "MEDIUM", "reason": "indirect identifier"},
        "customer_zip_code_prefix": {"pii_level": "LOW",    "reason": "coarse geographic"},
        "customer_city":            {"pii_level": "LOW",    "reason": "coarse location"},
        "customer_state":           {"pii_level": "NONE",   "reason": "aggregate geography"},
    }
