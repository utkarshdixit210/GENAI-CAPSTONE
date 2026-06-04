"""
live_dashboard.py
Live Pipeline Dashboard — auto-refreshes every 3 seconds.

Shows in real time:
  ● Pipeline control (Start / Stop / Reset)
  ● Current batch status + active node indicator
  ● Bronze / Silver / Gold / Quarantine row counts (all datasets)
  ● Heal Agent events per layer (Bronze / Silver / Gold)
  ● Live log feed
  ● Gold KPI charts (update after every batch)
  ● Batch history table
  ● Per-dataset layer breakdown

Run:  streamlit run live_dashboard.py
"""
import os
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Must be first Streamlit call ──────────────────────────────────
st.set_page_config(
    page_title = "Olist Live Pipeline",
    page_icon  = "🔴",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

# ── Inject auto-refresh CSS + JS ──────────────────────────────────
REFRESH_SEC = 3
st.markdown(
    f"""
    <script>
    setTimeout(function(){{ window.location.reload(); }}, {REFRESH_SEC * 1000});
    </script>
    <style>
    .stMetric {{border: 1px solid #333; border-radius: 8px; padding: 6px;}}
    .heal-badge {{
        display:inline-block; padding:2px 10px; border-radius:12px;
        font-size:12px; font-weight:bold; margin:2px;
    }}
    .layer-bronze {{background:#cd7f3222; border:1px solid #cd7f32; color:#cd7f32;}}
    .layer-silver {{background:#c0c0c022; border:1px solid #aaa; color:#ccc;}}
    .layer-gold   {{background:#ffd70022; border:1px solid #ffd700; color:#ffd700;}}
    .node-active  {{background:#00ff0033; border:1px solid #0f0; color:#0f0; 
                    padding:4px 14px; border-radius:6px; font-weight:bold;}}
    .node-pass    {{background:#00800022; border:1px solid #080; color:#0a0;
                    padding:3px 10px; border-radius:4px; font-size:12px;}}
    .node-fail    {{background:#ff000022; border:1px solid #f00; color:#f55;
                    padding:3px 10px; border-radius:4px; font-size:12px;}}
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Helper loaders ────────────────────────────────────────────────
def _read_json(path: str, default):
    try:
        if Path(path).exists():
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return default

def read_ctrl():   return _read_json("metadata/pipeline_control.json",
                                     {"running":False,"batch_interval_sec":12,"dataset":"all"})
def read_state():  return _read_json("metadata/batch_state.json",
                                     {"batch_id":0,"dataset":"-","status":"idle",
                                      "current_node":"-","bronze_rows":0,"silver_rows":0,
                                      "quarantine_rows":0,"heal_count":0,"gold_kpis":{},
                                      "updated_at":"-"})
def read_log():    return _read_json("metadata/batch_log.json", [])
def read_live():   return _read_json("metadata/live_log.json", [])

def write_ctrl(running: bool, interval: int, dataset: str):
    os.makedirs("metadata", exist_ok=True)
    data = {"running": running, "batch_interval_sec": interval,
            "dataset": dataset, "updated_at": datetime.utcnow().isoformat()}
    with open("metadata/pipeline_control.json","w") as f:
        json.dump(data, f, indent=2)

def load_csv(path: str) -> pd.DataFrame:
    try:
        if Path(path).exists():
            return pd.read_csv(path)
    except Exception:
        pass
    return pd.DataFrame()

DATASETS = ["customers","orders","payments","products"]
LAYER_COLORS = {
    "BRONZE": "#cd7f32", "SILVER": "#aaaaaa", "GOLD": "#FFD700",
    "GENERATOR": "#4499ff", "RUNNER": "#44ffaa", "ERROR": "#ff4444"
}
STATUS_ICONS = {"complete":"✅","running":"🔄","failed":"❌","idle":"💤","error":"❌"}

# ── Sidebar: Pipeline Control ─────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔴 Live Pipeline Control")
    ctrl    = read_ctrl()
    running = ctrl.get("running", False)

    # Status badge
    if running:
        st.markdown(
            "<div style='background:#00ff0022;border:1px solid #0f0;color:#0f0;"
            "padding:8px;border-radius:8px;text-align:center;font-weight:bold'>"
            "● PIPELINE RUNNING</div>", unsafe_allow_html=True
        )
    else:
        st.markdown(
            "<div style='background:#ff000022;border:1px solid #f00;color:#f55;"
            "padding:8px;border-radius:8px;text-align:center;font-weight:bold'>"
            "■ PIPELINE STOPPED</div>", unsafe_allow_html=True
        )

    st.divider()

    # Controls
    interval = st.slider("Batch interval (sec)", 5, 60, int(ctrl.get("batch_interval_sec",12)), 5)
    batch_sz = st.select_slider("Batch size", [50,100,150,200,300,500], value=150)
    ds_choice = st.multiselect("Datasets to process",
                               DATASETS, default=DATASETS)

    col1, col2 = st.columns(2)
    if col1.button("▶ START", type="primary", use_container_width=True, disabled=running):
        write_ctrl(True, interval, ",".join(ds_choice or DATASETS))
        # Launch runner in background subprocess
        cmd = [sys.executable, "-m", "pipeline.runner",
               "--batch-size", str(batch_sz),
               "--interval",   str(interval)]
        if ds_choice:
            cmd += ["--datasets"] + ds_choice
        subprocess.Popen(cmd, cwd=os.path.dirname(os.path.abspath(__file__)))
        st.rerun()

    if col2.button("■ STOP", type="secondary", use_container_width=True, disabled=not running):
        write_ctrl(False, interval, ",".join(ds_choice or DATASETS))
        st.rerun()

    if st.button("🗑 RESET (clear all data)", use_container_width=True):
        write_ctrl(False, interval, "all")
        from pipeline.cumulative_store import reset_store
        from pipeline.batch_state import reset_state
        reset_store()
        reset_state()
        st.success("All data cleared.")
        st.rerun()

    st.divider()
    st.caption(f"Auto-refresh: every {REFRESH_SEC}s")
    st.caption(f"Last state update: {read_state().get('updated_at','-')[-8:]}")

# ── Load current state ────────────────────────────────────────────
state    = read_state()
batch_log= read_log()
live_log = read_live()

# ── Header ────────────────────────────────────────────────────────
st.markdown("# 🛒 Olist Data Quality Pipeline — Live Dashboard")
st.caption(
    f"Batch #{state.get('batch_id',0)} | "
    f"Dataset: **{state.get('dataset','-').upper()}** | "
    f"Node: **{state.get('current_node','-')}** | "
    f"Status: {STATUS_ICONS.get(state.get('status','idle'), '?')} {state.get('status','idle').upper()}"
)

# ── Tabs ──────────────────────────────────────────────────────────
tab_live, tab_layers, tab_healer, tab_kpis, tab_history = st.tabs([
    "🔴 Live Feed",
    "🏗 Layer Counts",
    "🔧 Heal Agent",
    "🥇 KPIs",
    "📋 Batch History",
])

# ═══════════════════════════════════════════════════════════════════
# TAB 1: LIVE FEED
# ═══════════════════════════════════════════════════════════════════
with tab_live:
    # Top metrics row
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    total_b = total_s = total_q = total_g = total_heals = 0
    for ds in DATASETS:
        b = load_csv(f"outputs/cumulative/bronze/{ds}.csv")
        s = load_csv(f"outputs/cumulative/silver/{ds}.csv")
        q = load_csv(f"outputs/cumulative/bronze/{ds}_quarantine.csv")
        total_b += len(b); total_s += len(s); total_q += len(q)

    # Count heals from batch log
    for batch in batch_log:
        total_heals += (len(batch.get("bronze_events",[])) +
                        len(batch.get("silver_events",[])) +
                        len(batch.get("gold_events",[])))

    c1.metric("🟤 Bronze Total",     f"{total_b:,}")
    c2.metric("⚪ Silver Total",     f"{total_s:,}")
    c3.metric("🔴 Quarantine Total", f"{total_q:,}")
    c4.metric("🥇 Batches Run",      f"{len(batch_log):,}")
    c5.metric("🔧 Total Heals",      f"{total_heals:,}")
    c6.metric("📊 Datasets Active",  f"{len(DATASETS)}")

    st.divider()

    # Current batch progress
    if state.get("status") == "running":
        node = state.get("current_node", "-")
        st.markdown(
            f"<div class='node-active'>⚡ Processing Batch #{state.get('batch_id')} "
            f"| Dataset: {state.get('dataset','?').upper()} | Node: {node}</div>",
            unsafe_allow_html=True
        )
        # Show pipeline node progress
        nodes = ["GENERATOR","BRONZE","SILVER","GOLD","COMPLETE"]
        cols  = st.columns(len(nodes))
        for i, n in enumerate(nodes):
            if n == node:
                cols[i].markdown(f"<div class='node-active'>{n}</div>", unsafe_allow_html=True)
            elif nodes.index(node) > i:
                cols[i].markdown(f"<div class='node-pass'>✓ {n}</div>", unsafe_allow_html=True)
            else:
                cols[i].markdown(f"<div style='color:#555;font-size:12px'>○ {n}</div>", unsafe_allow_html=True)
        st.divider()

    # Live log feed (last 40 lines, newest first)
    st.subheader("📡 Live Node Log")
    if live_log:
        recent = list(reversed(live_log[-40:]))
        log_lines = []
        for entry in recent:
            level  = entry.get("level","INFO")
            node   = entry.get("node","")
            ts     = entry.get("ts","")
            msg    = entry.get("msg","")
            bid    = entry.get("batch_id",0)
            color  = LAYER_COLORS.get(node, "#cccccc")
            icon   = "🔧" if level == "WARN" else ("❌" if level == "ERROR" else "▶")
            log_lines.append(
                f"<div style='font-family:monospace;font-size:12px;margin:1px 0;"
                f"padding:2px 6px;border-left:3px solid {color};'>"
                f"<span style='color:#666'>{ts}</span> "
                f"<span style='color:{color};font-weight:bold'>[{node or 'SYS'}]</span> "
                f"{icon} {msg}"
                f"</div>"
            )
        st.markdown(
            "<div style='background:#111;padding:10px;border-radius:8px;"
            "max-height:450px;overflow-y:auto'>"
            + "\n".join(log_lines) +
            "</div>",
            unsafe_allow_html=True
        )
    else:
        st.info("No log entries yet. Start the pipeline to see live activity here.")

# ═══════════════════════════════════════════════════════════════════
# TAB 2: LAYER COUNTS
# ═══════════════════════════════════════════════════════════════════
with tab_layers:
    st.subheader("Bronze → Silver → Gold Cumulative Row Counts")
    st.caption("Shows ALL data ever processed — previous batches + new batches combined")

    chart_data = []
    for ds in DATASETS:
        b = load_csv(f"outputs/cumulative/bronze/{ds}.csv")
        s = load_csv(f"outputs/cumulative/silver/{ds}.csv")
        q = load_csv(f"outputs/cumulative/bronze/{ds}_quarantine.csv")
        chart_data.append({"Dataset": ds.capitalize(), "Layer": "Bronze (Raw)",     "Rows": len(b)})
        chart_data.append({"Dataset": ds.capitalize(), "Layer": "Silver (Clean)",   "Rows": len(s)})
        chart_data.append({"Dataset": ds.capitalize(), "Layer": "Quarantine (Bad)", "Rows": len(q)})

    if any(r["Rows"] > 0 for r in chart_data):
        df_chart = pd.DataFrame(chart_data)

        # Grouped bar
        fig = px.bar(
            df_chart, x="Dataset", y="Rows", color="Layer",
            barmode="group",
            color_discrete_map={
                "Bronze (Raw)": "#cd7f32",
                "Silver (Clean)": "#aaaaaa",
                "Quarantine (Bad)": "#ff4444",
            },
            title="Cumulative Row Counts per Dataset per Layer",
        )
        fig.update_layout(height=400, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                          font_color="white")
        st.plotly_chart(fig, use_container_width=True)

        # Layer totals funnel
        totals = df_chart.groupby("Layer")["Rows"].sum().reset_index()
        fig2   = px.funnel(totals, x="Rows", y="Layer",
                            title="All Datasets Combined — Bronze→Silver Funnel",
                            color_discrete_sequence=["#cd7f32","#aaaaaa","#ff4444"])
        fig2.update_layout(height=300, paper_bgcolor="#0e1117", font_color="white")
        st.plotly_chart(fig2, use_container_width=True)

        # Per-dataset summary table
        st.subheader("Per-Dataset Summary")
        rows = []
        for ds in DATASETS:
            b = load_csv(f"outputs/cumulative/bronze/{ds}.csv")
            s = load_csv(f"outputs/cumulative/silver/{ds}.csv")
            q = load_csv(f"outputs/cumulative/bronze/{ds}_quarantine.csv")
            rows.append({
                "Dataset":       ds.capitalize(),
                "Bronze Rows":   len(b),
                "Silver Clean":  len(s),
                "Quarantine":    len(q),
                "Quality %":     f"{len(s)/(len(b)+0.001)*100:.1f}%" if len(b) else "—",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No data yet. Start the pipeline to see layer counts populate in real time.")

# ═══════════════════════════════════════════════════════════════════
# TAB 3: HEAL AGENT
# ═══════════════════════════════════════════════════════════════════
with tab_healer:
    st.subheader("🔧 Heal Agent Activity — per Layer")
    st.caption("Healing is applied at every layer: Bronze (raw fixes), Silver (GE rule fixes), Gold (KPI sanity)")

    if batch_log:
        # Flatten all heal events from all batches
        all_events = []
        for batch in batch_log:
            for ev in batch.get("bronze_events",[]):
                ev["batch_id"] = batch["batch_id"]
                ev["layer"]    = "BRONZE"
                all_events.append(ev)
            for ev in batch.get("silver_events",[]):
                ev["batch_id"] = batch["batch_id"]
                ev["layer"]    = "SILVER"
                all_events.append(ev)
            for ev in batch.get("gold_events",[]):
                ev["batch_id"] = batch["batch_id"]
                ev["layer"]    = "GOLD"
                all_events.append(ev)

        if all_events:
            df_ev = pd.DataFrame(all_events)

            # Summary metrics
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Heal Events",   len(df_ev))
            c2.metric("Bronze Heals",         len(df_ev[df_ev["layer"]=="BRONZE"]))
            c3.metric("Silver Heals",         len(df_ev[df_ev["layer"]=="SILVER"]))
            c4.metric("Gold Heals",           len(df_ev[df_ev["layer"]=="GOLD"]))

            # Heal events by layer pie
            fig = px.pie(df_ev, names="layer",
                          color="layer",
                          color_discrete_map={"BRONZE":"#cd7f32","SILVER":"#aaa","GOLD":"#FFD700"},
                          title="Heal Events by Layer")
            fig.update_layout(height=280, paper_bgcolor="#0e1117", font_color="white")
            st.plotly_chart(fig, use_container_width=True)

            # Heals over time (batches)
            df_time = df_ev.groupby(["batch_id","layer"]).size().reset_index(name="heals")
            fig2 = px.bar(df_time, x="batch_id", y="heals", color="layer",
                           barmode="stack",
                           color_discrete_map={"BRONZE":"#cd7f32","SILVER":"#aaa","GOLD":"#FFD700"},
                           title="Heal Events per Batch (stacked by layer)",
                           labels={"batch_id":"Batch #","heals":"Heal Events"})
            fig2.update_layout(height=300, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                                font_color="white")
            st.plotly_chart(fig2, use_container_width=True)

            # Most healed columns
            st.subheader("Most Frequently Healed Columns")
            col_counts = df_ev.groupby(["column","layer"])["count"].sum().reset_index()
            col_counts  = col_counts.sort_values("count", ascending=False).head(20)
            fig3 = px.bar(col_counts, x="column", y="count", color="layer",
                           barmode="stack",
                           color_discrete_map={"BRONZE":"#cd7f32","SILVER":"#aaa","GOLD":"#FFD700"},
                           title="Total Rows Healed per Column")
            fig3.update_layout(height=320, paper_bgcolor="#0e1117", plot_bgcolor="#0e1117",
                                font_color="white")
            st.plotly_chart(fig3, use_container_width=True)

            # Recent heal events table
            st.subheader("Recent Heal Events")
            recent_ev = df_ev.sort_values("batch_id", ascending=False).head(50)
            st.dataframe(recent_ev[["batch_id","layer","dataset","column","issue","fix","count"]],
                          use_container_width=True, hide_index=True)
        else:
            st.info("No heal events yet in current run.")
    else:
        st.info("No batch history yet. Start the pipeline to see heal agent activity.")

# ═══════════════════════════════════════════════════════════════════
# TAB 4: GOLD KPIs
# ═══════════════════════════════════════════════════════════════════
with tab_kpis:
    st.subheader("🥇 Gold KPIs — Recomputed from Full Cumulative Silver")
    st.caption("KPIs update after every batch using ALL previously processed data")

    # Read Gold KPIs per dataset
    kpi_data = {}
    for ds in DATASETS:
        df_g = load_csv(f"outputs/cumulative/gold/{ds}.csv")
        if not df_g.empty:
            kpi_data[ds] = dict(zip(df_g["kpi_key"], df_g["kpi_value"]))

    if kpi_data:
        # Payments — revenue metrics
        if "payments" in kpi_data:
            kpis = kpi_data["payments"]
            st.subheader("💰 Revenue KPIs (Payments)")
            c1,c2,c3 = st.columns(3)
            c1.metric("Total Revenue (BRL)", f"R$ {float(kpis.get('total_revenue',0)):,.2f}")
            c2.metric("Avg Payment",          f"R$ {float(kpis.get('avg_payment',0)):,.2f}")
            c3.metric("Max Payment",          f"R$ {float(kpis.get('max_payment',0)):,.2f}")

            # Payment type breakdown chart
            try:
                import ast
                bp = ast.literal_eval(kpis.get("type_breakdown","{}"))
                if bp:
                    fig = px.pie(
                        values=list(bp.values()), names=list(bp.keys()),
                        title="Payment Type Distribution (Cumulative)",
                        color_discrete_sequence=px.colors.qualitative.Set2,
                    )
                    fig.update_layout(height=300, paper_bgcolor="#0e1117", font_color="white")
                    st.plotly_chart(fig, use_container_width=True)
            except Exception:
                pass

        # Revenue over time
        if len(batch_log) > 1:
            payment_history = [
                {"batch_id": b["batch_id"],
                 "total_revenue": float(b.get("gold_kpis",{}).get("total_revenue", 0))}
                for b in batch_log if b.get("dataset") == "payments"
                and "total_revenue" in b.get("gold_kpis",{})
            ]
            if payment_history:
                df_rev = pd.DataFrame(payment_history)
                fig = px.area(df_rev, x="batch_id", y="total_revenue",
                               title="Cumulative Revenue Growth per Batch",
                               color_discrete_sequence=["#FFD700"],
                               labels={"batch_id":"Batch #","total_revenue":"Total Revenue (BRL)"})
                fig.update_layout(height=280, paper_bgcolor="#0e1117",
                                   plot_bgcolor="#111", font_color="white")
                st.plotly_chart(fig, use_container_width=True)

        # Customers — geographic KPIs
        if "customers" in kpi_data:
            kpis = kpi_data["customers"]
            st.subheader("👤 Customer KPIs")
            c1,c2,c3 = st.columns(3)
            c1.metric("Total Silver Customers", f"{int(kpis.get('total_silver_rows',0)):,}")
            c2.metric("States Covered",          kpis.get("states_covered","?"))
            c3.metric("Cities Covered",          kpis.get("cities_covered","?"))

            try:
                import ast
                top_states = ast.literal_eval(kpis.get("top_states","{}"))
                if top_states:
                    fig = px.bar(
                        x=list(top_states.keys()), y=list(top_states.values()),
                        title="Top States by Customer Count (Cumulative)",
                        color=list(top_states.values()),
                        color_continuous_scale="Blues",
                        labels={"x":"State","y":"Customers"},
                    )
                    fig.update_layout(height=300, paper_bgcolor="#0e1117",
                                       plot_bgcolor="#111", font_color="white")
                    st.plotly_chart(fig, use_container_width=True)
            except Exception:
                pass

        # Orders KPIs
        if "orders" in kpi_data:
            kpis = kpi_data["orders"]
            st.subheader("📦 Order KPIs")
            c1,c2,c3 = st.columns(3)
            c1.metric("Total Orders (Silver)", f"{int(kpis.get('total_silver_rows',0)):,}")
            c2.metric("Delivery Rate",          f"{kpis.get('delivered_pct','?')}%")
            c3.metric("Cancellation Rate",      f"{kpis.get('canceled_pct','?')}%")

            try:
                import ast
                status_c = ast.literal_eval(kpis.get("status_counts","{}"))
                if status_c:
                    fig = px.bar(
                        x=list(status_c.keys()), y=list(status_c.values()),
                        title="Order Status Distribution (Cumulative)",
                        color=list(status_c.values()),
                        color_continuous_scale="Viridis",
                    )
                    fig.update_layout(height=280, paper_bgcolor="#0e1117",
                                       plot_bgcolor="#111", font_color="white")
                    st.plotly_chart(fig, use_container_width=True)
            except Exception:
                pass

        # Products KPIs
        if "products" in kpi_data:
            kpis = kpi_data["products"]
            st.subheader("🛍 Product KPIs")
            c1,c2 = st.columns(2)
            c1.metric("Unique Categories", kpis.get("unique_categories","?"))
            c2.metric("Avg Weight (g)",    f"{float(kpis.get('avg_weight_g',0)):.0f}g")

            try:
                import ast
                top_cat = ast.literal_eval(kpis.get("top_categories","{}"))
                if top_cat:
                    fig = px.bar(
                        x=list(top_cat.values()), y=list(top_cat.keys()),
                        orientation="h",
                        title="Top Product Categories (Cumulative)",
                        color=list(top_cat.values()),
                        color_continuous_scale="Oranges",
                        labels={"x":"Product Count","y":"Category"},
                    )
                    fig.update_layout(height=380, paper_bgcolor="#0e1117",
                                       plot_bgcolor="#111", font_color="white")
                    st.plotly_chart(fig, use_container_width=True)
            except Exception:
                pass

    else:
        st.info("No Gold KPIs yet. Start the pipeline and let it run a few batches.")

# ═══════════════════════════════════════════════════════════════════
# TAB 5: BATCH HISTORY
# ═══════════════════════════════════════════════════════════════════
with tab_history:
    st.subheader("📋 Batch Run History")
    st.caption("One row per batch — click any column header to sort")

    if batch_log:
        rows = []
        for b in reversed(batch_log[-100:]):
            b_ev = len(b.get("bronze_events",[]))
            s_ev = len(b.get("silver_events",[]))
            g_ev = len(b.get("gold_events",[]))
            rows.append({
                "Batch #":      b.get("batch_id","?"),
                "Dataset":      b.get("dataset","?"),
                "Status":       STATUS_ICONS.get(b.get("status","?"),"?") + " " + b.get("status","?"),
                "Bronze Rows":  b.get("bronze_rows",0),
                "Silver Rows":  b.get("silver_rows",0),
                "Quarantine":   b.get("quarantine_rows",0),
                "Bronze Heals": b_ev,
                "Silver Heals": s_ev,
                "Gold Heals":   g_ev,
                "Total Heals":  b_ev + s_ev + g_ev,
                "Started":      str(b.get("started_at",""))[-8:],
            })
        df_hist = pd.DataFrame(rows)

        # Summary mini-charts
        col1, col2 = st.columns(2)
        with col1:
            fig = px.line(df_hist.head(50), x="Batch #", y="Silver Rows",
                           color="Dataset", title="Silver Rows per Batch",
                           color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=250, paper_bgcolor="#0e1117",
                               plot_bgcolor="#111", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(df_hist.head(50), x="Batch #", y="Total Heals",
                          color="Dataset", title="Total Heals per Batch",
                          color_discrete_sequence=px.colors.qualitative.Pastel)
            fig.update_layout(height=250, paper_bgcolor="#0e1117",
                               plot_bgcolor="#111", font_color="white")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df_hist, use_container_width=True, hide_index=True)
    else:
        st.info("No batch history yet. Start the pipeline to see history populate.")

# ── Footer ────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    f"GEN_AI Capstone | Olist Live Pipeline | "
    f"Bronze → Silver → Gold | Auto-refresh every {REFRESH_SEC}s | "
    f"{datetime.now().strftime('%H:%M:%S')}"
)
