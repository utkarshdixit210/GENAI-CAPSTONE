# GEN_AI Capstone — Continuous Live Pipeline
## Quick Start (3 commands)

```bash
# 1. Install
pip install -r requirements.txt

# 2. Set API key
cp .env.example .env && nano .env   # set ANTHROPIC_API_KEY

# 3. Launch dashboard — it controls everything
streamlit run live_dashboard.py
```

Then in the dashboard:
- Click **▶ START** to begin continuous batch processing
- Watch Bronze → Silver → Gold counts update live
- See the Heal Agent fire at each layer in real time
- Click **■ STOP** anytime to pause

---

## How the Continuous Pipeline Works

```
┌─────────────────────────────────────────────────────────────────┐
│  GENERATOR (every N seconds)                                     │
│  Produces a new dirty batch (150 rows × 4 datasets)             │
│  — random nulls, invalid values, negative numbers, dupes        │
└──────────────┬──────────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER HEAL                                               │
│  ✦ Fix nulls in non-key cols (fill UNKNOWN/0)                   │
│  ✦ Standardise customer_state (SP/RJ/MG etc.)                   │
│  ✦ Fix negative payment_value (abs)                              │
│  ✦ Fix invalid payment_type → 'boleto'                          │
│  ✦ Quarantine rows with null primary keys (unfixable)           │
│  Output: healed_bronze_df + bronze_quarantine_df                │
└──────────────┬──────────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER HEAL (GE rules applied)                            │
│  ✦ expect_column_values_to_not_be_null  → fill UNKNOWN/0        │
│  ✦ expect_column_values_to_be_in_set   → replace with mode      │
│  ✦ expect_column_values_to_be_between  → clamp to [min,max]     │
│  ✦ Rows still failing → silver_quarantine_df                    │
│  Output: silver_clean_df + silver_quarantine_df                 │
└──────────────┬──────────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  CUMULATIVE MERGE                                                │
│  Silver clean MERGED with all previous batches (deduped by ID)  │
│  Gold KPIs RECOMPUTED from FULL cumulative Silver               │
│  → Revenue grows batch by batch                                 │
│  → Customer counts keep accumulating                            │
└──────────────┬──────────────────────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER HEAL                                                 │
│  ✦ Sanity-check KPI values (NaN, Inf, negative revenue)         │
│  Output: healed_kpis_dict                                       │
└──────────────┬──────────────────────────────────────────────────┘
               ▼
         Dashboard update → sleep N sec → repeat
```

---

## Running the Pipeline Manually (without dashboard)

```bash
# All 4 datasets, 150 rows/batch, 12s interval
python -m pipeline.runner

# Specific datasets only
python -m pipeline.runner --datasets customers payments

# Faster batches (50 rows, 5s interval)
python -m pipeline.runner --batch-size 50 --interval 5

# Reset all data and start fresh
python -m pipeline.runner --reset

# Then open dashboard separately
streamlit run live_dashboard.py
```

---

## File Structure (new files)

```
pipeline/
  runner.py           ← Continuous loop: generate → Bronze → Silver → Gold
  layer_healer.py     ← Healing logic at EACH layer (not just on node fail)
  batch_generator.py  ← Generates dirty batches with random quality issues
  cumulative_store.py ← Merges batches; Gold KPIs from full Silver history
  batch_state.py      ← Shared state JSON (pipeline writes, dashboard reads)

live_dashboard.py     ← Live Streamlit dashboard (auto-refreshes every 3s)

outputs/cumulative/
  bronze/{dataset}.csv          ← All raw rows ever
  silver/{dataset}.csv          ← All clean rows ever (deduped)
  bronze/{dataset}_quarantine.csv ← All bad rows ever
  gold/{dataset}.csv            ← Latest Gold KPIs

metadata/
  batch_state.json          ← Current batch status (live)
  batch_log.json            ← All batch summaries
  live_log.json             ← Live log feed (last 300 lines)
  pipeline_control.json     ← Start/stop signal
```
