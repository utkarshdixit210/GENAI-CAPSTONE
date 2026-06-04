"""
agents/state.py
Shared State object — the single communication bus for ALL nodes.
Every node reads from and writes to this TypedDict.
No node ever calls another node directly.
Dataset: Olist Brazilian E-Commerce
"""
from typing import TypedDict, List, Dict, Any


class AgentState(TypedDict):

    # ── Pipeline tracking ─────────────────────────────────────────
    current_node:        str          # name of node currently running
    node_status:         Dict         # {node_name: 'pass'|'fail'|'healing'}
    node_errors:         Dict         # {node_name: error_message_string}

    # ── Heal Agent ────────────────────────────────────────────────
    error_type:          str          # LLM-classified error type
    fix_applied:         str          # description of fix applied
    retry_count:         Dict         # {node_name: int} per-node counter
    heal_log:            List         # full ordered list of all heals
    give_up:             bool         # True when max retries exceeded

    # ── Node 1 — Profile ──────────────────────────────────────────
    schema:              Dict         # {column_name: data_type}
    sample_rows:         List         # 5 row preview
    row_count:           int          # total rows in RAW table
    dataset_name:        str          # which Olist dataset is active

    # ── Node 1b — Bronze LLM Inspector ────────────────────────────
    bronze_report:       Dict         # structured quality report
    bronze_issues:       List         # list of data quality issues

    # ── Node 2 — Schema Drift ─────────────────────────────────────
    schema_drift_events: List         # list of drift event dicts
    schema_drift_run_id: str          # run identifier for drift log

    # ── Node 3 — PII Detector ─────────────────────────────────────
    pii_map:             Dict         # {col: {pii_level, reason}}

    # ── Node 4 — Rule Generator ───────────────────────────────────
    ge_rules:            List         # LLM-generated GE expectation configs
    llm_retry_strict:    bool         # True → stricter prompt on retry

    # ── Node 5 — Validator ────────────────────────────────────────
    validation_passed:   bool         # True = all GE checks passed
    failed_checks:       List         # list of failed GE expectations

    # ── Node 6 — Transform ────────────────────────────────────────
    clean_row_count:     int          # rows written to Silver CLEAN
    quarantine_count:    int          # rows written to Bronze QUARANTINE
    fix_sql:             str          # fix code written by Heal Agent
    clean_df_path:       str          # path to saved clean CSV (Silver)
    quarantine_df_path:  str          # path to saved quarantine CSV

    # ── Node 7 — PII Masker ───────────────────────────────────────
    masked_table:        str          # name of masked Silver table
    masked_row_count:    int          # rows in masked table
    masked_df_path:      str          # path to saved masked CSV
    masking_log:         List         # detail list of masked columns and actions

    # ── Node 8 — Gold KPI Generator ───────────────────────────────
    gold_kpis:           Dict         # computed KPI metrics dict
    gold_df_path:        str          # path to saved gold CSV

    # ── Node 9 — Lineage Tracker ──────────────────────────────────
    lineage_run_id:      str          # unique run ID for lineage entries

    # ── Node 10 — Audit Writer ────────────────────────────────────
    audit_written:       bool         # True once audit log written

    # ── Node 11 — Alert ───────────────────────────────────────────
    audit_report:        str          # LLM-generated final audit report
