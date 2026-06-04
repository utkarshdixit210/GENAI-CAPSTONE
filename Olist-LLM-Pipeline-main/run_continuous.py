"""
run_continuous.py — GEN_AI_Capstone-2 (Snowflake mode)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Continuous Olist Data Pipeline: Generate → Bronze → Silver → Gold

LLM watches EVERY layer:
  🟤 BRONZE : LLM scans raw batch → reports every data quality issue
  ⚪ SILVER : LLM heals bad rows via Snowflake SQL + masks PII (before/after)
  🟡 GOLD   : LLM generates KPIs + business insights
  🔧 HEAL   : LLM auto-heals any failed node (max 3 retries)

Each run generates a FRESH batch of data with unique batch_id → Snowflake.
The pipeline processes only the latest batch via MERGE/INSERT logic.

Usage:
    python3 run_continuous.py --once              # one batch, then stop
    python3 run_continuous.py --batches 5         # run 5 batches
    python3 run_continuous.py --loop --interval 60 # run forever, every 60s
    python3 run_continuous.py --rows 500          # 500 customers per batch
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import os
import sys
import uuid
import time
import json
import random
import argparse
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── ANSI colour helpers ───────────────────────────────────────────
R = "\033[0m";  BOLD = "\033[1m";   DIM  = "\033[2m"
CY = "\033[96m"; GR = "\033[92m";  YL = "\033[93m"
RD = "\033[91m"; BL = "\033[94m";  MG = "\033[95m"

def hdr(txt, col=CY):
    pad = (66 - len(txt) - 2) // 2
    return f"{col}{BOLD}{'─'*pad} {txt} {'─'*pad}{R}"

def banner(num, bid, dataset, ts):
    print(f"\n{BOLD}{CY}{'═'*70}{R}")
    print(f"{BOLD}{CY}  BATCH #{num}  |  id={bid}  |  {ts}{R}")
    print(f"{BOLD}{CY}  Dataset : {dataset.upper()}  |  Snowflake mode{R}")
    print(f"{BOLD}{CY}{'═'*70}{R}")


# ── Data constants ────────────────────────────────────────────────
BR_STATES = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
             "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
             "RS","RO","RR","SC","SP","SE","TO"]
BR_CITIES = ["sao paulo","rio de janeiro","belo horizonte","curitiba","fortaleza",
             "manaus","salvador","recife","porto alegre","belem","goiania",
             "florianopolis","maceio","natal","teresina","campo grande"]
PAYMENT_TYPES  = ["credit_card","boleto","voucher","debit_card"]
ORDER_STATUSES = ["delivered","shipped","canceled","processing","invoiced","approved"]
PRODUCT_CATS   = ["cama_mesa_banho","beleza_saude","esporte_lazer",
                  "informatica_acessorios","moveis_decoracao","auto","brinquedos"]

def _nullify(s, rate=0.05):
    mask = np.random.random(len(s)) < rate
    s = s.astype(object); s[mask] = np.nan; return s

def _bad_cats(s, bads, rate=0.03):
    mask = np.random.random(len(s)) < rate
    s = s.copy(); s[mask] = np.random.choice(bads, mask.sum()); return s


# ── Snowflake connection ──────────────────────────────────────────
def _get_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE","COMPUTE_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE","MY_DB"),
        schema    = os.getenv("SNOWFLAKE_SCHEMA","PUBLIC"),
        role      = os.getenv("SNOWFLAKE_ROLE","SYSADMIN"),
    )

def _write_to_snowflake(df: pd.DataFrame, table: str, conn) -> int:
    """Replace RAW table contents with the fresh batch (truncate + load)."""
    from snowflake.connector.pandas_tools import write_pandas
    df = df.copy()
    # Snowflake write_pandas requires UPPERCASE column names
    df.columns = [c.upper() for c in df.columns]

    # Drop and recreate to ensure schema matches the fresh batch
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table.upper()}")
        conn.commit()
    except Exception:
        pass
    finally:
        cur.close()

    success, _, nrows, _ = write_pandas(conn, df, table.upper(),
                                        auto_create_table=True, overwrite=False)
    return nrows


# ── Data generators (Snowflake-ready) ─────────────────────────────
def _gen_customers(n, batch_id):
    try:
        from faker import Faker
        fk = Faker("pt_BR")
        emails = [fk.email() for _ in range(n)]
        phones = [fk.phone_number() for _ in range(n)]
    except ImportError:
        emails = [f"user{i}@email.com" for i in range(n)]
        phones = [f"+55-11-9{random.randint(10000000,99999999)}" for _ in range(n)]

    df = pd.DataFrame({
        "customer_id":              [str(uuid.uuid4()) for _ in range(n)],
        "customer_unique_id":       [str(uuid.uuid4()) for _ in range(n)],
        "customer_email":           emails,
        "customer_phone":           phones,
        "customer_zip_code_prefix": np.random.randint(10000, 99999, n),
        "customer_city":            np.random.choice(BR_CITIES, n),
        "customer_state":           np.random.choice(BR_STATES, n),
        "batch_id":                 batch_id,
        "ingested_at":              datetime.datetime.utcnow().isoformat(),
    })
    df["customer_id"]    = _nullify(df["customer_id"],    rate=0.02)
    df["customer_state"] = _nullify(df["customer_state"], rate=0.04)
    df["customer_email"] = _nullify(df["customer_email"], rate=0.03)
    df["customer_state"] = _bad_cats(df["customer_state"],
                                     ["XX","UNKNOWN","sp","São Paulo"], 0.03)
    dup = df.sample(frac=0.01)
    df = pd.concat([df, dup], ignore_index=True)
    return df


def _gen_orders(customer_ids, n, batch_id):
    base = datetime.datetime(2017, 1, 1)
    def rts(): return (base + datetime.timedelta(days=random.randint(0,730))).strftime("%Y-%m-%d %H:%M:%S")
    df = pd.DataFrame({
        "order_id":                       [str(uuid.uuid4()) for _ in range(n)],
        "customer_id":                    np.random.choice(customer_ids, n),
        "order_status":                   np.random.choice(ORDER_STATUSES, n,
                                              p=[0.65,0.12,0.08,0.05,0.04,0.06]),
        "order_purchase_timestamp":       [rts() for _ in range(n)],
        "order_approved_at":              [rts() for _ in range(n)],
        "order_delivered_carrier_date":   [rts() for _ in range(n)],
        "order_delivered_customer_date":  [rts() for _ in range(n)],
        "order_estimated_delivery_date":  [rts() for _ in range(n)],
        "batch_id":                       batch_id,
        "ingested_at":                    datetime.datetime.utcnow().isoformat(),
    })
    df["customer_id"]  = _nullify(df["customer_id"],  rate=0.03)
    df["order_status"] = _bad_cats(df["order_status"], ["pending","UNKNOWN","null"], 0.03)
    df["order_approved_at"]             = _nullify(df["order_approved_at"],             rate=0.08)
    df["order_delivered_customer_date"] = _nullify(df["order_delivered_customer_date"], rate=0.15)
    return df


def _gen_payments(order_ids, n, batch_id):
    df = pd.DataFrame({
        "order_id":             np.random.choice(order_ids, n),
        "payment_sequential":   np.random.randint(1, 5, n),
        "payment_type":         np.random.choice(PAYMENT_TYPES, n, p=[0.74,0.19,0.05,0.02]),
        "payment_installments": np.random.randint(1, 12, n),
        "payment_value":        np.round(np.random.exponential(scale=150, size=n), 2),
        "batch_id":             batch_id,
        "ingested_at":          datetime.datetime.utcnow().isoformat(),
    })
    df["payment_value"] = _nullify(df["payment_value"], rate=0.02)
    df["payment_type"]  = _bad_cats(df["payment_type"], ["cash","check","UNKNOWN"], 0.03)
    neg = np.random.random(n) < 0.03
    df.loc[neg, "payment_value"] = -abs(df.loc[neg, "payment_value"])
    return df


def _gen_products(n, batch_id):
    df = pd.DataFrame({
        "product_id":                 [str(uuid.uuid4()) for _ in range(n)],
        "product_category_name":      np.random.choice(PRODUCT_CATS, n),
        "product_name_lenght":        np.random.randint(10, 80, n).astype(float),
        "product_description_lenght": np.random.randint(50, 3000, n).astype(float),
        "product_photos_qty":         np.random.randint(1, 10, n).astype(float),
        "product_weight_g":           np.random.randint(100, 30000, n).astype(float),
        "product_length_cm":          np.random.randint(10, 100, n).astype(float),
        "product_height_cm":          np.random.randint(5, 80, n).astype(float),
        "product_width_cm":           np.random.randint(10, 80, n).astype(float),
        "batch_id":                   batch_id,
        "ingested_at":                datetime.datetime.utcnow().isoformat(),
    })
    df["product_category_name"] = _nullify(df["product_category_name"], rate=0.06)
    df["product_weight_g"]      = _nullify(df["product_weight_g"],      rate=0.04)
    return df


# ── Bronze ingestion print ────────────────────────────────────────
def print_bronze_report(batch_id, df, dataset):
    print(f"\n{hdr('BRONZE LAYER — Fresh Batch Ingested to Snowflake', YL)}")
    print(f"  {YL}batch_id :{R} {batch_id}")
    print(f"  {YL}dataset  :{R} {dataset}")
    print(f"  {YL}rows     :{R} {len(df):,}")
    print(f"  {YL}columns  :{R} {[c for c in df.columns if c not in ('batch_id','ingested_at')]}")

    null_counts = df.isnull().sum()
    null_cols   = null_counts[null_counts > 0]
    if not null_cols.empty:
        print(f"\n  {RD}⚠  Injected quality issues:{R}")
        for col, cnt in null_cols.items():
            print(f"    {RD}✗{R} {col:40s}  {cnt:4d} nulls  ({cnt/len(df)*100:.1f}%)")

    print(f"\n  {DIM}Sample raw rows (first 2):{R}")
    skip_cols = {"batch_id","ingested_at","BATCH_ID","INGESTED_AT"}
    for i, row in df.head(2).iterrows():
        parts = "  |  ".join(
            f"{k}={str(v)[:18]}" for k, v in row.items() if k not in skip_cols
        )
        print(f"  {DIM}  [{i}] {parts}{R}")


# ── Batch results printer ─────────────────────────────────────────
def print_results(state: dict, duration: float):
    print(f"\n{hdr('PIPELINE RESULTS', GR)}")

    # Bronze LLM inspection
    bronze = state.get("bronze_report", {})
    b_issues = state.get("bronze_issues", [])
    if bronze:
        print(f"\n  {YL}🔍 BRONZE LLM INSPECTION:{R}")
        print(f"     {bronze.get('batch_summary','')}")
        if b_issues:
            print(f"     {RD}Issues detected ({len(b_issues)}):{R}")
            for iss in b_issues:
                sev = iss.get("severity","?")
                col = f"{RD if sev=='HIGH' else YL}{iss.get('column','?')}{R}"
                print(f"     [{sev}] {col} [{iss.get('issue_type','?')}]: {iss.get('description','')}")
                bads = iss.get("sample_bad_values",[])
                if bads:
                    print(f"            {DIM}samples: {bads[:4]}{R}")
        clean = bronze.get("columns_look_clean",[])
        if clean:
            print(f"     {GR}✅ Clean: {', '.join(clean)}{R}")
        print(f"     {BL}💡 {bronze.get('recommendation','')}{R}")

    # PII masking before/after
    masking = state.get("masking_log", [])
    if masking:
        print(f"\n  {CY}🔒 PII MASKING (LLM Detected → Action):{R}")
        print(f"  {'Column':42s} {'Action':14s} {'Level':8s} Before → After")
        print(f"  {'─'*90}")
        for m in masking:
            act    = m.get("action","?")
            col    = m.get("column","?")
            lvl    = m.get("level","?")
            reason = m.get("reason","")
            icon   = "🔴" if act=="SHA256" else ("🟡" if act=="PARTIAL_MASK" else "✅")
            print(f"  {icon} {col:40s} {act:14s} {lvl:8s} {DIM}{reason[:30]}{R}")
            befores = m.get("sample_before",[])
            afters  = m.get("sample_after",[])
            for b, a in zip(befores, afters):
                if str(b) != str(a):
                    print(f"       {RD}BEFORE:{R} {str(b)[:35]:<37} {GR}→ AFTER:{R} {str(a)[:30]}")
                else:
                    print(f"       {DIM}(unchanged) {str(b)[:35]}{R}")

    # Heal log
    heals = state.get("heal_log", [])
    if heals:
        print(f"\n  {RD}🔧 HEAL LOG ({len(heals)} events):{R}")
        for h in heals:
            fix = str(h.get("fix",""))[:70]
            print(f"     [{h.get('retry_num','?')}] {h.get('node','?'):20s} | "
                  f"{h.get('error_type','?'):20s} | {DIM}{fix}{R}")

    # Schema drift
    drifts = state.get("schema_drift_events", [])
    if drifts:
        print(f"\n  {YL}🌊 SCHEMA DRIFT ({len(drifts)}):{R}")
        for d in drifts:
            print(f"     {d.get('severity','?'):6s} | {d.get('drift_type','?'):20s} | "
                  f"col={d.get('column_name','?')}")

    # Metrics
    print(f"\n  {CY}📊 LAYER METRICS:{R}")
    print(f"     🟤 Bronze raw rows   : {YL}{state.get('row_count',0):,}{R}")
    print(f"     ⚪ Silver clean rows  : {GR}{state.get('clean_row_count',0):,}{R}")
    print(f"     ⚠  Quarantined rows  : {RD}{state.get('quarantine_count',0):,}{R}")
    print(f"     🔒 Masked rows       : {CY}{state.get('masked_row_count',0):,}{R}")

    # Gold KPIs
    kpis = state.get("gold_kpis", {})
    if kpis:
        print(f"\n  {MG}🥇 GOLD KPIs:{R}")
        for k, v in kpis.items():
            if k != "business_insights":
                print(f"     {k:30s}: {v}")

    # LLM insights
    insights = kpis.get("business_insights", [])
    if insights:
        print(f"\n  {MG}💡 LLM BUSINESS INSIGHTS:{R}")
        for i, ins in enumerate(insights, 1):
            cat = ins.get("category","?").upper()
            print(f"     [{i}] [{cat}] {ins.get('insight','')}")

    status = "✅ SUCCESS" if not state.get("give_up") else "⚠  ESCALATED"
    col    = GR if not state.get("give_up") else YL
    print(f"\n  {col}{BOLD}Status: {status}  |  Duration: {duration:.1f}s{R}")


# ── Main batch run ────────────────────────────────────────────────
def run_one_batch(dataset: str, n_rows: int, batch_id: str, batch_num: int) -> dict:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    banner(batch_num, batch_id, dataset, ts)

    # ── Step 1: Generate ALL 4 datasets → Snowflake Bronze ─────────
    print(f"\n{hdr('STEP 1 — Generate ALL Datasets → Snowflake RAW Tables', YL)}")

    np.random.seed(); random.seed()   # truly random every batch

    # Use generate_data.py's run_batch to push ALL 4 tables
    from generate_data import run_batch as gen_full_batch
    gen_full_batch(n_customers=n_rows, preview=False)

    # Now read back the customers table to display Bronze report
    conn = _get_conn()
    try:
        df = pd.read_sql(f"SELECT * FROM RAW_OLIST_CUSTOMERS", conn)
        df.columns = [c.lower() for c in df.columns]
        print_bronze_report(batch_id, df, dataset)
        print(f"\n  {GR}✓ All 4 datasets loaded → Snowflake RAW tables{R}")
    finally:
        conn.close()


    # ── Step 2: Run LangGraph pipeline ────────────────────────────
    print(f"\n{hdr('STEP 2 — LLM Pipeline: Bronze → Silver → Gold', CY)}")

    node_map = [
        ("🟤 BRONZE", "profile",         "Profile raw table (schema, count, samples)"),
        ("🟤 BRONZE", "bronze_inspector","LLM scans batch → reports every issue"),
        ("🟤 BRONZE", "schema_drift",    "Detect schema drift vs reference"),
        ("🟤 BRONZE", "pii_detector",    "LLM classifies every column for PII"),
        ("🟤 BRONZE", "rule_gen",        "LLM generates GE validation rules"),
        ("🟤 BRONZE", "validator",       "Run GE rules (fail → Heal Agent)"),
        ("⚪ SILVER", "transform",       "Split clean/quarantine + apply SQL heals"),
        ("⚪ SILVER", "pii_masker",      "LLM-detected PII masked (before/after shown)"),
        ("🟡 GOLD",   "gold_kpi",        "LLM computes KPIs + business insights"),
        ("📋 AUDIT",  "lineage_tracker", "Record data lineage"),
        ("📋 AUDIT",  "audit_writer",    "Write audit log"),
    ]
    for layer, node, desc in node_map:
        print(f"  {layer} {BOLD}{node:20s}{R} {DIM}{desc}{R}")

    # Set active dataset and import pipeline
    os.environ["ACTIVE_DATASET"] = dataset
    os.environ["PIPELINE_RUN_ID"] = batch_id

    from pipeline.graph import pipeline

    initial = {
        "current_node": "", "node_status": {}, "node_errors": {},
        "error_type": "", "fix_applied": "", "retry_count": {}, "heal_log": [], "give_up": False,
        "dataset_name":        dataset,
        "schema":              {}, "sample_rows": [], "row_count": 0,
        "bronze_report":       {}, "bronze_issues": [],
        "schema_drift_events": [], "schema_drift_run_id": "",
        "pii_map":             {},
        "ge_rules":            [], "llm_retry_strict": False,
        "validation_passed":   False, "failed_checks": [],
        "clean_row_count":     0, "quarantine_count": 0,
        "fix_sql": "", "clean_df_path": "", "quarantine_df_path": "",
        "masked_table": "", "masked_row_count": 0, "masked_df_path": "", "masking_log": [],
        "gold_kpis":           {}, "gold_df_path": "",
        "lineage_run_id":      "",
        "audit_written":       False,
        "audit_report":        "",
    }

    print(f"\n  {DIM}Invoking pipeline...{R}\n")
    start  = datetime.datetime.now()
    final  = pipeline.invoke(initial)
    dur    = (datetime.datetime.now() - start).total_seconds()

    # ── Step 3: Results ───────────────────────────────────────────
    print(f"\n{hdr('STEP 3 — Results', GR)}")
    print_results(final, dur)

    # Save to batch history
    os.makedirs("metadata", exist_ok=True)
    hist_path = "metadata/batch_history.json"
    history = []
    if Path(hist_path).exists():
        try:
            with open(hist_path) as f:
                history = json.load(f)
        except Exception:
            pass
    history.append({
        "batch_num":     batch_num,
        "batch_id":      batch_id,
        "dataset":       dataset,
        "timestamp":     ts,
        "duration_s":    round(dur, 1),
        "status":        "ESCALATED" if final.get("give_up") else "SUCCESS",
        "raw_rows":      final.get("row_count", 0),
        "clean_rows":    final.get("clean_row_count", 0),
        "quarantine":    final.get("quarantine_count", 0),
        "masked_rows":   final.get("masked_row_count", 0),
        "heals":         len(final.get("heal_log", [])),
        "bronze_issues": len(final.get("bronze_issues", [])),
        "schema_drifts": len(final.get("schema_drift_events", [])),
        # ── Per-node details for dashboard ──
        "node_status":          final.get("node_status", {}),
        "heal_log":             final.get("heal_log", []),
        "schema_drift_events":  final.get("schema_drift_events", []),
        "bronze_issues_detail": final.get("bronze_issues", []),
        "masking_log":          final.get("masking_log", []),
        "gold_kpis":            final.get("gold_kpis", {}),
    })
    with open(hist_path, "w") as f:
        json.dump(history[-50:], f, indent=2, default=str)


    return final


# ── Entry point ───────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Continuous Olist LLM Pipeline (Snowflake)")
    parser.add_argument("--rows",     type=int,  default=200,           help="Customers per batch")
    parser.add_argument("--interval", type=int,  default=60,            help="Seconds between batches")
    parser.add_argument("--batches",  type=int,  default=0,             help="Max batches (0=infinite)")
    parser.add_argument("--once",     action="store_true",              help="Run one batch and exit")
    parser.add_argument("--loop",     action="store_true",              help="Loop indefinitely")
    parser.add_argument("--datasets", nargs="+",
                        default=["customers"],
                        choices=["customers","orders","payments","products"],
                        help="Datasets to cycle through")
    args = parser.parse_args()

    print(f"\n{BOLD}{CY}{'═'*70}{R}")
    print(f"{BOLD}{CY}  GEN AI CAPSTONE-2 — OLIST CONTINUOUS LLM PIPELINE{R}")
    print(f"{BOLD}{CY}  LLM on every node: Bronze → Silver → Gold → Snowflake{R}")
    print(f"{BOLD}{CY}{'═'*70}{R}")
    print(f"\n  {DIM}rows/batch  : {args.rows}")
    print(f"  interval    : {args.interval}s")
    print(f"  datasets    : {args.datasets}")
    print(f"  max batches : {'∞' if args.batches==0 else args.batches}")
    print(f"  LLM         : {os.getenv('LLM_PROVIDER','?')} / {os.getenv('LLM_MODEL','?')}")
    print(f"  Snowflake   : {os.getenv('SNOWFLAKE_DATABASE','?')}.{os.getenv('SNOWFLAKE_SCHEMA','?')}{R}\n")

    batch_num = 0
    ds_idx    = 0

    while True:
        batch_num += 1
        batch_id   = str(uuid.uuid4())[:8]
        dataset    = args.datasets[ds_idx % len(args.datasets)]
        ds_idx    += 1

        try:
            run_one_batch(dataset=dataset, n_rows=args.rows,
                          batch_id=batch_id, batch_num=batch_num)
        except KeyboardInterrupt:
            print(f"\n{YL}⛔ Stopped by user after batch #{batch_num}.{R}")
            sys.exit(0)
        except Exception as e:
            print(f"\n{RD}✗ Batch #{batch_num} failed: {e}{R}")
            import traceback; traceback.print_exc()

        if args.once or (args.batches > 0 and batch_num >= args.batches):
            print(f"\n{GR}✅ Completed {batch_num} batch(es). Done.{R}")
            break

        if not args.loop and args.batches == 0:
            # Default: run once unless --loop or --batches specified
            print(f"\n{GR}✅ Single batch complete. Use --loop to run continuously.{R}")
            break

        print(f"\n{DIM}  ⏱  Next batch in {args.interval}s  (Ctrl+C to stop){R}")
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print(f"\n{YL}⛔ Stopped by user.{R}")
            sys.exit(0)


if __name__ == "__main__":
    main()
