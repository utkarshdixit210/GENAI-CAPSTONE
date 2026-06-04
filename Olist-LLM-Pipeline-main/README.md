# 🔮 GEN AI Capstone-2 — Olist LLM Data Pipeline

> **Autonomous Self-Healing Data Quality Pipeline with LLM on Every Node**

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)]()
[![LangGraph](https://img.shields.io/badge/LangGraph-StateGraph-green.svg)]()
[![Groq](https://img.shields.io/badge/LLM-Groq%20Llama%203.3-purple.svg)]()
[![Snowflake](https://img.shields.io/badge/DW-Snowflake-blue.svg)]()

---

## 🎯 What This Does

A fully autonomous data pipeline that:

1. **Generates** realistic Olist e-commerce data with intentional quality issues
2. **Inspects** raw data with LLM agents (Bronze layer)
3. **Heals** data quality issues automatically via LLM-generated SQL
4. **Transforms** clean data to Silver layer with PII masking
5. **Computes** business KPIs and LLM insights (Gold layer)
6. **Audits** everything with lineage tracking to Snowflake

**Zero human intervention.** LLM watches every layer.

---

## 🏗️ Architecture

```
Generate → Bronze (LLM) → Silver (LLM) → Gold (LLM) → Snowflake → Dashboard

  🟤 BRONZE : Profile → Inspector → Drift → PII Detect → Rules → Validate
       ↓ fail → 🔧 Heal Agent (LLM generates SQL fix) → retry (max 3)
  ⚪ SILVER : Transform → PII Masker
  🟡 GOLD   : KPI Engine + LLM Business Insights
  📋 AUDIT  : Lineage Tracker → Audit Writer → Alert
```

**11 pipeline nodes** orchestrated by LangGraph. **6 nodes powered by LLM.**

---

## ⚡ Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Configure .env with your Groq API key + Snowflake credentials
cp .env.example .env

# Run one batch
python3 run_continuous.py --once --rows 200

# Run continuously (generates + processes every 45s)
python3 run_continuous.py --loop --rows 200 --interval 45

# Start dashboard
streamlit run streamlit_app.py
```

---

## 📊 Dashboard

Live Streamlit dashboard at `http://localhost:8501`:

- **Batch selector** — inspect any historical batch
- **Per-agent sections** — see what each of the 11 agents found
- **Schema drift details** — HIGH/MEDIUM severity with column names
- **PII masking before/after** — SHA-256 vs partial mask comparison
- **Gold KPIs + LLM insights** — Top states chart + business recommendations
- **Batch history charts** — rows, duration, heal trend across all runs

---

## 📁 Project Structure

```
agents/nodes/     — 11 pipeline agents + heal agent
pipeline/graph.py — LangGraph StateGraph definition
tools/            — LLM client + Snowflake MCP tool
generate_data.py  — Data generator (4 datasets)
run_continuous.py — Main entry point (loop)
streamlit_app.py  — Dashboard
```

📖 **Full documentation:** [DOCUMENTATION.md](DOCUMENTATION.md)

---

## 🔧 Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | LangGraph |
| LLM | Groq (Llama 3.3 70B) |
| Data Warehouse | Snowflake |
| Dashboard | Streamlit + Plotly |
| Data Generation | Faker + NumPy |
| Logging | Loguru |

---

*GEN AI Capstone-2 — Sigmoid Data Engineering Program*
