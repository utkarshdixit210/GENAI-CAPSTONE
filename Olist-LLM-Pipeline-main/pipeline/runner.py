"""
pipeline/runner.py
Continuous Batch Pipeline Runner

Flow per batch:
  1. Generator produces a new dirty batch (customers / orders / payments / products)
  2. BRONZE LAYER  → store raw → heal_bronze (fix nulls, bad values, negatives)
  3. SILVER LAYER  → apply GE validation → heal_silver (second-pass fix) → clean + quarantine
  4. GOLD LAYER    → recompute KPIs from FULL cumulative Silver → heal_gold (sanity check)
  5. Write live state for dashboard
  6. Sleep batch_interval seconds → repeat

Run from terminal:
    python -m pipeline.runner              (all 4 datasets round-robin)
    python -m pipeline.runner customers    (one dataset only)

Controlled by:  metadata/pipeline_control.json  (written by dashboard)
"""
import sys
import os
import time
import random
import traceback
from datetime import datetime

import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.batch_generator  import generate_batch
from pipeline.layer_healer     import heal_bronze, heal_silver, heal_gold
from pipeline.cumulative_store  import (
    append_bronze, append_silver, recompute_gold, get_layer_counts, reset_store
)
from pipeline.batch_state import (
    write_batch_state, append_batch_log, push_live_log,
    read_pipeline_control, write_pipeline_control,
)
from config.settings import PipelineConfig

DATASETS = ["customers", "orders", "payments", "products"]

# LLM-generated GE rules per dataset (generated once, reused across batches)
# Falls back to hardcoded if LLM unavailable
DEFAULT_GE_RULES = {
    "customers": [
        {"expectation_type": "expect_column_values_to_not_be_null",  "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_not_be_null",  "column": "customer_state"},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "column": "customer_state",
         "kwargs": {"value_set": list({
             "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
             "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO","UNKNOWN"
         })}},
    ],
    "orders": [
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "order_id"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "customer_id"},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "column": "order_status",
         "kwargs": {"value_set": ["delivered","shipped","canceled","processing",
                                   "invoiced","approved","unavailable","unknown"]}},
    ],
    "payments": [
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "order_id"},
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "payment_value"},
        {"expectation_type": "expect_column_values_to_be_between",
         "column": "payment_value", "kwargs": {"min_value": 0, "max_value": 999999}},
        {"expectation_type": "expect_column_values_to_be_in_set",
         "column": "payment_type",
         "kwargs": {"value_set": ["credit_card","boleto","voucher","debit_card"]}},
    ],
    "products": [
        {"expectation_type": "expect_column_values_to_not_be_null", "column": "product_id"},
        {"expectation_type": "expect_column_values_to_be_between",
         "column": "product_weight_g", "kwargs": {"min_value": 0, "max_value": 100000}},
    ],
}


def _log(msg: str, level: str = "INFO", node: str = "", batch_id: int = 0):
    push_live_log(msg, level=level, node=node, batch_id=batch_id)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}")


def _get_ge_rules(dataset: str) -> list:
    """Try LLM-generated rules first; fall back to defaults."""
    try:
        from tools.llm_client import llm_client
        prompt = f"""
Generate 4-6 Great Expectations validation rules for Olist {dataset} dataset.
Respond ONLY with a JSON array. No markdown. Example:
[{{"expectation_type": "expect_column_values_to_not_be_null", "column": "col_name"}}]
"""
        raw = llm_client.invoke(prompt).strip()
        raw = raw.strip("```json").strip("```").strip()
        s, e = raw.find("["), raw.rfind("]") + 1
        if s != -1:
            import json
            rules = json.loads(raw[s:e])
            if isinstance(rules, list) and rules:
                return rules
    except Exception:
        pass
    return DEFAULT_GE_RULES.get(dataset, [])


def process_batch(
    batch_id:  int,
    dataset:   str,
    batch_size: int,
    ge_rules:  list,
) -> dict:
    """
    Full Bronze → Silver → Gold pipeline for one batch of one dataset.
    Returns a summary dict for the dashboard + audit log.
    """
    summary = {
        "batch_id":       batch_id,
        "dataset":        dataset,
        "batch_size":     batch_size,
        "bronze_events":  [],
        "silver_events":  [],
        "gold_events":    [],
        "bronze_rows":    0,
        "silver_rows":    0,
        "quarantine_rows":0,
        "gold_kpis":      {},
        "status":         "complete",
        "error":          None,
        "started_at":     datetime.utcnow().isoformat(),
    }

    try:
        # ── Step 1: Generate dirty batch ─────────────────────────
        write_batch_state(batch_id, dataset, "running", "generator",
                           bronze_rows=0, silver_rows=0)
        _log(f"Batch #{batch_id} | {dataset} | Generating {batch_size} rows...",
             node="GENERATOR", batch_id=batch_id)

        raw_df = generate_batch(dataset, batch_size=batch_size)
        _log(f"Batch #{batch_id} | {dataset} | Generated {len(raw_df)} raw rows "
             f"(nulls={raw_df.isnull().sum().sum()})",
             node="GENERATOR", batch_id=batch_id)

        # ── Step 2: BRONZE layer ──────────────────────────────────
        write_batch_state(batch_id, dataset, "running", "BRONZE",
                           bronze_rows=len(raw_df))
        _log(f"Batch #{batch_id} | {dataset} | → BRONZE healing...",
             node="BRONZE", batch_id=batch_id)

        healed_bronze, bronze_quarantine, bronze_events = heal_bronze(dataset, raw_df)
        summary["bronze_events"] = bronze_events

        for ev in bronze_events:
            _log(f"  🔧 BRONZE HEAL | {ev['column']}: {ev['issue']} → {ev['fix']} ({ev['count']} rows)",
                 level="WARN", node="BRONZE", batch_id=batch_id)

        # Append to cumulative Bronze store
        cumulative_bronze = append_bronze(dataset, healed_bronze)
        summary["bronze_rows"] = len(healed_bronze)

        _log(f"Batch #{batch_id} | {dataset} | BRONZE done | "
             f"healed={len(healed_bronze)} | quarantine={len(bronze_quarantine)} | "
             f"fixes={len(bronze_events)} | total_bronze={len(cumulative_bronze):,}",
             node="BRONZE", batch_id=batch_id)

        # ── Step 3: SILVER layer ──────────────────────────────────
        write_batch_state(batch_id, dataset, "running", "SILVER",
                           bronze_rows=len(cumulative_bronze))
        _log(f"Batch #{batch_id} | {dataset} | → SILVER healing ({len(ge_rules)} GE rules)...",
             node="SILVER", batch_id=batch_id)

        silver_clean, silver_quarantine, silver_events = heal_silver(
            dataset, healed_bronze, ge_rules
        )
        summary["silver_events"] = silver_events

        for ev in silver_events:
            _log(f"  🔧 SILVER HEAL | {ev['column']}: {ev['issue']} → {ev['fix']} ({ev['count']} rows)",
                 level="WARN", node="SILVER", batch_id=batch_id)

        # Merge into cumulative Silver (previous + new, deduped)
        cumulative_silver = append_silver(dataset, silver_clean,
                                           pd.concat([bronze_quarantine, silver_quarantine],
                                                      ignore_index=True))
        q_total = len(bronze_quarantine) + len(silver_quarantine)
        summary["silver_rows"]    = len(silver_clean)
        summary["quarantine_rows"]= q_total

        _log(f"Batch #{batch_id} | {dataset} | SILVER done | "
             f"clean={len(silver_clean)} | quarantine={q_total} | "
             f"fixes={len(silver_events)} | total_silver={len(cumulative_silver):,}",
             node="SILVER", batch_id=batch_id)

        # ── Step 4: GOLD layer ────────────────────────────────────
        write_batch_state(batch_id, dataset, "running", "GOLD",
                           bronze_rows=len(cumulative_bronze),
                           silver_rows=len(cumulative_silver),
                           quarantine_rows=q_total)
        _log(f"Batch #{batch_id} | {dataset} | → GOLD KPI recompute from {len(cumulative_silver):,} Silver rows...",
             node="GOLD", batch_id=batch_id)

        gold_kpis = recompute_gold(dataset)
        gold_kpis, gold_events = heal_gold(dataset, gold_kpis)
        summary["gold_events"] = gold_events
        summary["gold_kpis"]   = gold_kpis

        for ev in gold_events:
            _log(f"  🔧 GOLD HEAL | {ev['column']}: {ev['issue']} → {ev['fix']}",
                 level="WARN", node="GOLD", batch_id=batch_id)

        total_heals = len(bronze_events) + len(silver_events) + len(gold_events)
        counts      = get_layer_counts()

        _log(f"Batch #{batch_id} | {dataset} | GOLD done | "
             f"total_heals={total_heals} | "
             f"revenue={gold_kpis.get('total_revenue', gold_kpis.get('total_silver_rows','?'))}",
             node="GOLD", batch_id=batch_id)

        # ── Step 5: Write final state for dashboard ───────────────
        write_batch_state(
            batch_id      = batch_id,
            dataset       = dataset,
            status        = "complete",
            current_node  = "COMPLETE",
            bronze_rows   = counts.get(dataset,{}).get("bronze",0),
            silver_rows   = counts.get(dataset,{}).get("silver",0),
            quarantine_rows = counts.get(dataset,{}).get("quarantine",0),
            heal_count    = total_heals,
            schema_drifts = 0,
            gold_kpis     = gold_kpis,
        )

        summary["completed_at"] = datetime.utcnow().isoformat()

    except Exception as e:
        err = traceback.format_exc()
        summary["status"] = "error"
        summary["error"]  = str(e)
        _log(f"Batch #{batch_id} | {dataset} | ERROR: {e}", level="ERROR", batch_id=batch_id)
        write_batch_state(batch_id, dataset, "failed", "ERROR", error=str(e))

    return summary


def run_continuous(
    datasets:           list  = None,
    batch_size:         int   = 150,
    batch_interval_sec: int   = 12,
    reset:              bool  = False,
):
    """
    Main continuous loop.
    Runs forever until pipeline_control.json sets running=false.
    Cycles through datasets in round-robin order.
    """
    if reset:
        reset_store()
        _log("Store reset — fresh start.", node="RUNNER", batch_id=0)

    datasets = datasets or DATASETS
    ge_rules = {ds: _get_ge_rules(ds) for ds in datasets}

    _log(f"RUNNER | Starting continuous pipeline | datasets={datasets} | "
         f"batch_size={batch_size} | interval={batch_interval_sec}s",
         node="RUNNER", batch_id=0)

    # Write initial control
    write_pipeline_control(running=True,
                            batch_interval_sec=batch_interval_sec,
                            dataset=",".join(datasets))

    batch_id = 1
    ds_cycle = 0

    while True:
        # Check control file — dashboard can stop the pipeline
        ctrl = read_pipeline_control()
        if not ctrl.get("running", True):
            _log("RUNNER | Stop signal received from dashboard. Halting.", node="RUNNER")
            break

        dataset = datasets[ds_cycle % len(datasets)]
        ds_cycle += 1

        _log(f"═══ Batch #{batch_id} | Dataset: {dataset.upper()} ═══",
             node="RUNNER", batch_id=batch_id)

        summary = process_batch(
            batch_id   = batch_id,
            dataset    = dataset,
            batch_size = batch_size,
            ge_rules   = ge_rules[dataset],
        )
        append_batch_log(summary)

        total_heals = (len(summary.get("bronze_events",[])) +
                       len(summary.get("silver_events",[])) +
                       len(summary.get("gold_events",[])))

        _log(f"✓ Batch #{batch_id} | {dataset} | "
             f"bronze={summary.get('bronze_rows',0)} | "
             f"silver={summary.get('silver_rows',0)} | "
             f"quarantine={summary.get('quarantine_rows',0)} | "
             f"heals={total_heals} | "
             f"status={summary.get('status','?')}",
             node="RUNNER", batch_id=batch_id)

        batch_id += 1

        # Sleep between batches (check control every second)
        for _ in range(batch_interval_sec):
            ctrl = read_pipeline_control()
            if not ctrl.get("running", True):
                break
            time.sleep(1)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Continuous Olist Pipeline Runner")
    parser.add_argument("--datasets",  nargs="+", default=None,
                        choices=["customers","orders","payments","products"],
                        help="Which datasets to cycle through (default: all 4)")
    parser.add_argument("--batch-size",  type=int, default=150,
                        help="Rows per batch (default 150)")
    parser.add_argument("--interval",    type=int, default=12,
                        help="Seconds between batches (default 12)")
    parser.add_argument("--reset",       action="store_true",
                        help="Clear all cumulative data before starting")
    args = parser.parse_args()

    run_continuous(
        datasets           = args.datasets,
        batch_size         = args.batch_size,
        batch_interval_sec = args.interval,
        reset              = args.reset,
    )
