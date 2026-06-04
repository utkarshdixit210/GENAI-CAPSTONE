# 🔮 Olist Agentic Data Engineering Operations System

> **Autonomous Self-Healing Data Quality Pipeline (Bronze → Silver → Gold) on Snowflake with Central Enterprise Spend, Concurrency Locks, and Observability Controls.**

[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)]()
[![LangGraph](https://img.shields.io/badge/LangGraph-StateGraph-green.svg)]()
[![Groq](https://img.shields.io/badge/LLM-Groq%20Llama%203.3-purple.svg)]()
[![Snowflake](https://img.shields.io/badge/DW-Snowflake-blue.svg)]()
[![ChromaDB](https://img.shields.io/badge/Vector%20Store-ChromaDB-red.svg)]()
[![Arize Phoenix](https://img.shields.io/badge/Observability-Arize%20Phoenix-cyan.svg)]()

---

## 🎯 System Capabilities

This system orchestrates an end-to-end Snowflake data engineering pipeline for Brazilian Olist e-commerce datasets using autonomous LLM agents:

1. **Synthetic Data Ingestion:** Generates e-commerce data with quality errors, loading them to Snowflake.
2. **Bronze Scanning & Profiling:** Profiles schema structures and uses LLMs to catalog data issues.
3. **Great Expectations Validation:** Runs validation rules on columns to enforce schema integrity.
4. **Self-Healing agent (Pipeline Fixer):** Intercepts database constraints validation failures, recalls cached updates from memory, or writes/executes Snowflake SQL repairs.
5. **Silver Transformation & Masking:** Normalizes columns and applies dynamic hashing/partial masking to PII.
6. **Gold Metrics & Insights:** Computes business dimensions, KPIs, and builds recommended recommendations.
7. **Governance Lineage & Audits:** Compiles full lineage data flows and logs audit trails to Snowflake.

---

## 🏛️ System Architecture Blueprint

For detailed system component descriptions and a full diagram workflow matching corporate standards, view [**`specifications.md`**](Olist-LLM-Pipeline-main/specifications.md) and [**`architecture_diagram.md`**](Olist-LLM-Pipeline-main/architecture_diagram.md).

```
Generate → Bronze (LLM) → Silver (LLM) → Gold (LLM) → Snowflake → Dashboard
 
  🟤 BRONZE : Profile → Inspector → Drift → PII Detect → Rules → Validate
       ↓ fail → 🔧 Heal Agent (LLM generates SQL fix) → retry (max 3)
  ⚪ SILVER : Transform → PII Masker
  🟡 GOLD   : KPI Engine + LLM Business Insights
  📋 AUDIT  : Lineage Tracker → Audit Writer → Alert
```

---

## ⚡ Enterprise Operations Layers (B1–B6)

* **B1 (Observability):** Streams tracing timelines, execution spans, latency percentiles, and LLM prompts directly to local **Arize Phoenix** on port `6006`.
* **B2 (Cost Guardrails):** Captures Groq prompt/completion tokens and estimates runs cost. Pipeline blocks execution if the calendar daily budget limit of `$2.00` is exceeded.
* **B3 (Human-in-the-Loop):** Pauses high-risk SQL operations, alerts operators, and holds execution waiting for authorization.
* **B4 (Multi-Agent Coordination):** Prevents dataset write hazards using an SQLite locking registry with deadlock breakers.
* **B5 (Incident Learning Cache):** Uses a local **ChromaDB** vector store to cache successful fixes. Similar exceptions recall cached scripts instantly, bypassing Groq API costs and reducing latencies.
* **B6 (Slack Alerting):** Dispatches Block Kit alerting payloads to your Slack channel on failures, budget breaches, or approval actions.

---

## 🚀 Quick Start (Local Setup)

### 1. Install Dependencies
```bash
# Clone the repository
git clone https://github.com/utkarshdixit210/GENAI-CAPSTONE.git
cd Olist-LLM-Pipeline-main

# Install dependencies from requirements
pip install -r requirements.txt
```

### 2. Configure Credentials (`.env`)
Create a `.env` file in the project root:
```env
LLM_PROVIDER=groq
LLM_MODEL=llama-3.3-70b-versatile
GROQ_API_KEY=gsk_...

SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=OLIST_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=SYSADMIN

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

### 3. Run Observability UI
Launch Arize Phoenix in the background to collect OpenTelemetry traces:
```bash
python -m phoenix.server.main serve
```
Open **[http://localhost:6006](http://localhost:6006)** in your browser.

### 4. Execute Pipeline
Run one test batch:
```bash
python run_continuous.py --once
```

### 5. Launch Streamlit Dashboard
```bash
streamlit run streamlit_app.py
```
Open **[http://localhost:8501](http://localhost:8501)** to view KPIs and approve pending fixes.

---

## 📊 Operations Dashboard Page Structure

* **1. Pipeline Control:** Run custom pipeline cycles and load mock Olist data batches.
* **2. Quality KPIs:** Inspect validation logs, pass percentages, and column failure rates.
* **3. Governance & PII:** Audit PII categories (High/Medium/Low) and examine masked columns.
* **4. Schema Evolution:** Inspect schema drift logs and column changes.
* **5. Lineage:** Explore data flow diagrams from Bronze raw source to Gold target aggregates.
* **6. Audit & Logs:** Fetch chronological execution history and self-heal counts.
* **7. Data Explorer:** Run interactive SQL queries against your Snowflake database.
* **8. Enterprise Ops:** Active budget gauge, nodes cost breakdown, concurrency locks monitor, ChromaDB vector records viewer, and the **Human Approval Queue** card actions.

---

## 📂 Repository File Structure

* `agents/nodes/` — 11 LangGraph pipeline nodes + Heal Agent.
* `pipeline/graph.py` — StateGraph compiler and concurrency lock wrapper.
* `tools/llm_client.py` — Singleton LLM client, OTEL register, and budget controller.
* `tools/pattern_memory.py` — ChromaDB semantic memory client.
* `config/settings.py` — Central configurations and DB path resolver.
* `generate_data.py` — Random Olist batch generator.
* `run_continuous.py` — Pipeline loop entry script.
* `streamlit_app.py` — Dashboard landing page with animated flowchart.
