"""Page 8 — Enterprise Data Engineering Operations"""
import os
import sqlite3
import datetime
import time
import streamlit as st
import pandas as pd
from pathlib import Path
from config.settings import PipelineConfig

st.set_page_config(page_title="Enterprise Data Ops", page_icon="🛡️", layout="wide")
st.markdown("# 🛡️ Enterprise Data Engineering Operations")
st.caption("Agent Observability, Token Budgets, Human-in-the-Loop Approvals, & Concurrency Locks")

DB_PATH = PipelineConfig.DB_PATH

def get_db_connection():
    return sqlite3.connect(DB_PATH)

# ── Shared Styling ────────────────────────────────────────────────
st.markdown("""<style>
.section-header {font-size:1.2rem;font-weight:700;color:#e2e8f0;margin:1.5rem 0 0.8rem;border-left:4px solid #818cf8;padding-left:10px}
.ops-card {background:rgba(30,30,50,0.5);border:1px solid rgba(129,140,248,0.15);border-radius:12px;padding:16px;margin-bottom:12px}
.approve-box {border:1px solid rgba(244,63,94,0.3);background:rgba(244,63,94,0.02)}
</style>""", unsafe_allow_html=True)

# ── Left Column: Cost & Observability | Right Column: Approvals ─────
left_col, right_col = st.columns([1, 1.2])

with left_col:
    # ── B1: Observability & Tracing ───────────────────────────────
    st.markdown('<div class="section-header">🔍 Agent Observability & Tracing</div>', unsafe_allow_html=True)
    with st.container(border=True):
        st.write("Every agent node execution is traced using **OpenTelemetry** and instrumented with **Arize Phoenix**.")
        st.markdown("[🚀 Open Local Arize Phoenix Server](http://localhost:6006)", unsafe_allow_html=True)
        st.caption("Inspect trace timelines, inputs, completions, execution graphs, and costs directly in the Phoenix UI.")

    # ── B2: Cost Guardrails & Budget Management ─────────────────
    st.markdown('<div class="section-header">💸 Cost Guardrails & Budgets</div>', unsafe_allow_html=True)
    
    # Calculate costs
    conn = get_db_connection()
    cur = conn.cursor()
    today_str = datetime.datetime.now().strftime("%Y-%m-%d")
    
    # Cumulative Cost
    cur.execute("SELECT SUM(prompt_tokens), SUM(completion_tokens), SUM(cost_usd) FROM pipeline_costs WHERE date(recorded_at) = ?", (today_str,))
    usage = cur.fetchone()
    total_prompt = usage[0] or 0
    total_comp = usage[1] or 0
    total_cost = usage[2] or 0.0
    
    # Model Costs list
    cur.execute("SELECT node, SUM(cost_usd), COUNT(*) FROM pipeline_costs WHERE date(recorded_at) = ? GROUP BY node", (today_str,))
    node_costs = cur.fetchall()
    conn.close()
    
    MAX_BUDGET = 2.00
    budget_pct = min(total_cost / MAX_BUDGET, 1.0)
    
    c1, c2 = st.columns(2)
    c1.metric("Today's Token Cost", f"${total_cost:.5f}", help="Calculated using Llama 3 70B Groq API rates")
    c2.metric("Daily Limit Ceiling", f"${MAX_BUDGET:.2f}")
    
    st.write("Daily Budget Usage:")
    st.progress(budget_pct, text=f"{budget_pct*100:.1f}% consumed (${total_cost:.4f} / ${MAX_BUDGET:.2f})")
    
    if total_cost > MAX_BUDGET:
        st.error("🚨 COST GUARDRAIL TRIGGERED: Budget Exceeded. Pipeline has been paused.")
        if st.button("🔓 Reset / Override Daily Budget", type="primary", use_container_width=True):
            conn = get_db_connection()
            cur = conn.cursor()
            # Simulate resetting cost by deleting today's entries or lowering their reported values
            cur.execute("DELETE FROM pipeline_costs WHERE date(recorded_at) = ?", (today_str,))
            conn.commit()
            conn.close()
            st.success("Daily budget counters reset. Pipeline unlocked.")
            time.sleep(1)
            st.rerun()
            
    # Breakdown by Node
    if node_costs:
        st.write("**Cost Breakdown by Agent Node:**")
        breakdown_df = pd.DataFrame(node_costs, columns=["Agent Node", "Cost (USD)", "LLM Calls"])
        st.dataframe(breakdown_df, use_container_width=True, hide_index=True)

with right_col:
    # ── B3: Human-in-the-Loop Approval Queue ─────────────────────
    st.markdown('<div class="section-header">⏳ Human-in-the-Loop Approvals</div>', unsafe_allow_html=True)
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, run_id, dataset, node, sql_query, created_at FROM pending_approvals WHERE status = 'PENDING' ORDER BY id DESC")
    pending = cur.fetchall()
    conn.close()
    
    if pending:
        st.warning(f"🚨 {len(pending)} high-risk actions are currently holding execution and require authorization:")
        for pid, rid, dataset, node, sql_query, created_at in pending:
            with st.container(border=True):
                st.markdown(f"### ⚡ SQL Mutation Request (ID: #{pid})")
                st.markdown(f"**Run ID:** `{rid}` | **Dataset:** `{dataset.upper()}` | **Requested By:** `{node.upper()}`")
                st.code(sql_query, language="sql")
                st.caption(f"Requested at {created_at} UTC")
                
                ac1, ac2 = st.columns(2)
                if ac1.button("✅ Approve Query", key=f"app_{pid}", use_container_width=True, type="primary"):
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("UPDATE pending_approvals SET status = 'APPROVED' WHERE id = ?", (pid,))
                    conn.commit()
                    conn.close()
                    st.success("Query approved! Pipeline resuming.")
                    time.sleep(1.5)
                    st.rerun()
                    
                if ac2.button("❌ Reject & Abort", key=f"rej_{pid}", use_container_width=True, type="secondary"):
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute("UPDATE pending_approvals SET status = 'REJECTED' WHERE id = ?", (pid,))
                    conn.commit()
                    conn.close()
                    st.error("Query rejected. Pipeline aborting.")
                    time.sleep(1.5)
                    st.rerun()
    else:
        st.success("✅ Approvals queue is clean. All active pipelines are running without interruption.")

    # ── B4: Concurrency Locks Monitor ────────────────────────────
    st.markdown('<div class="section-header">🔒 Multi-Agent Concurrency Locks</div>', unsafe_allow_html=True)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT dataset, node, lock_status, acquired_at FROM concurrency_locks ORDER BY acquired_at DESC")
    locks = cur.fetchall()
    conn.close()
    
    if locks:
        locks_df = pd.DataFrame(locks, columns=["Dataset", "Owner Node", "Status", "Acquired At"])
        st.dataframe(locks_df, use_container_width=True, hide_index=True)
    else:
        st.info("No active locks currently held.")

    # ── B5: Incident Memory cache monitor ───────────────────────
    st.markdown('<div class="section-header">🧠 Recall Memory Collections (ChromaDB)</div>', unsafe_allow_html=True)
    try:
        from tools.pattern_memory import pattern_memory
        if pattern_memory.collection:
            # Query all items in ChromaDB
            results = pattern_memory.collection.get()
            if results and results["ids"]:
                st.success(f"ChromaDB memory contains {len(results['ids'])} learned healing patterns:")
                memory_data = []
                for i in range(len(results["ids"])):
                    memory_data.append({
                        "Incident ID": results["ids"][i],
                        "Error Details": results["documents"][i][:120] + "...",
                        "Cached SQL Fix": results["metadatas"][i].get("sql_fix", "")
                    })
                st.dataframe(pd.DataFrame(memory_data), use_container_width=True, hide_index=True)
            else:
                st.info("ChromaDB vector memory is currently empty. Successful self-healing fixes will be cached here automatically.")
        else:
            st.warning("ChromaDB not initialized.")
    except Exception as e:
        st.error(f"Failed to query ChromaDB: {e}")

st.divider()
st.markdown('<div style="text-align:center;color:#475569;font-size:.7rem">Olist Medallion Data Engineering Portal · LangGraph · ChromaDB · SQLite</div>', unsafe_allow_html=True)
