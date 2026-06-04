"""
pipeline/graph.py — GEN_AI_Capstone-2
LangGraph StateGraph: 12 nodes + Heal Agent

LLM is on top of EVERY layer:
  BRONZE  1. profile          — schema, row count, sample rows
          2. bronze_inspector — LLM scans raw batch → reports every issue
          3. schema_drift     — detect schema drift vs reference
          4. pii_detector     — LLM classifies every column for PII
          5. rule_gen         — LLM generates GE validation rules
          6. validator        — runs GE rules (fail → Heal Agent)

  SILVER  7. transform        — clean / quarantine split + pandas healing
          8. pii_masker       — LLM-detected PII masked; before/after logged

  GOLD    9. gold_kpi         — LLM computes KPIs + business insights

  AUDIT  10. lineage_tracker
         11. audit_writer
         12. alert            — terminal (SUCCESS or ESCALATED)

Every node: pass → next | fail → heal_agent → LLM fix → retry (max 3) → alert
"""
from langgraph.graph import StateGraph
from agents.state import AgentState
from agents.nodes import (
    profile, bronze_inspector, schema_drift, pii_detector, rule_gen,
    validator, transform, pii_masker, gold_kpi,
    lineage_tracker, audit_writer, alert, heal_agent,
)
from loguru import logger
from config.settings import PipelineConfig


def route(state: AgentState) -> str:
    node   = state["current_node"]
    status = state["node_status"].get(node, "pass")
    logger.debug(f"ROUTER | '{node}' → '{status}'")
    return status


def route_after_heal(state: AgentState) -> str:
    if state.get("give_up", False):
        logger.error("ROUTER | give_up=True → escalating to alert")
        return "give_up"
    target = f"retry_{state['current_node']}"
    logger.info(f"ROUTER | Heal done → '{target}'")
    return target


def acquire_lock(dataset: str, node: str) -> bool:
    import sqlite3
    conn = sqlite3.connect(PipelineConfig.DB_PATH, timeout=30.0)
    cur = conn.cursor()
    try:
        # Check if another node has locked this dataset
        cur.execute("SELECT node, lock_status FROM concurrency_locks WHERE dataset = ? AND lock_status = 'locked'", (dataset,))
        row = cur.fetchone()
        if row:
            locked_node = row[0]
            if locked_node != node:
                return False
        # Otherwise, insert/update lock status to locked
        cur.execute("""
            INSERT OR REPLACE INTO concurrency_locks (dataset, node, lock_status)
            VALUES (?, ?, 'locked')
        """, (dataset, node))
        conn.commit()
        return True
    except Exception as e:
        logger.warning(f"Coordination | Lock acquisition failed: {e}")
        return False
    finally:
        conn.close()


def release_lock(dataset: str, node: str):
    import sqlite3
    conn = sqlite3.connect(PipelineConfig.DB_PATH, timeout=30.0)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT OR REPLACE INTO concurrency_locks (dataset, node, lock_status)
            VALUES (?, ?, 'resolved')
        """, (dataset, node))
        conn.commit()
    except Exception as e:
        logger.warning(f"Coordination | Lock release failed: {e}")
    finally:
        conn.close()


def clear_dataset_locks(dataset: str):
    import sqlite3
    conn = sqlite3.connect(PipelineConfig.DB_PATH, timeout=30.0)
    cur = conn.cursor()
    try:
        cur.execute("UPDATE concurrency_locks SET lock_status = 'resolved' WHERE dataset = ?", (dataset,))
        conn.commit()
        logger.info(f"Coordination | Cleared all locks on dataset '{dataset}'")
    except Exception as e:
        logger.warning(f"Coordination | Failed to clear dataset locks: {e}")
    finally:
        conn.close()


def wrap_node(node_func, node_name):
    def wrapper(state):
        import os, time
        # Set node context for telemetry billing
        os.environ["CURRENT_NODE"] = node_name
        dataset = state.get("dataset_name", "customers")
        
        # Concurrency Lock (B4)
        retries = 0
        while not acquire_lock(dataset, node_name):
            logger.warning(f"Coordination | Dataset '{dataset}' is locked by another node. Retrying in 1s...")
            time.sleep(1)
            retries += 1
            if retries > 10:  # Deadlock detection timeout
                logger.error(f"Coordination | Deadlock detected on '{dataset}'! Releasing stale locks...")
                clear_dataset_locks(dataset)
        try:
            res = node_func(state)
            return res
        finally:
            release_lock(dataset, node_name)
    return wrapper


def build_pipeline():
    graph = StateGraph(AgentState)

    # ── Register all nodes ────────────────────────────────────────
    graph.add_node("profile",          wrap_node(profile.run, "profile"))
    graph.add_node("bronze_inspector", wrap_node(bronze_inspector.run, "bronze_inspector"))   # NEW — LLM on Bronze
    graph.add_node("schema_drift",     wrap_node(schema_drift.run, "schema_drift"))
    graph.add_node("pii_detector",     wrap_node(pii_detector.run, "pii_detector"))       # LLM on Bronze
    graph.add_node("rule_gen",         wrap_node(rule_gen.run, "rule_gen"))           # LLM on Bronze
    graph.add_node("validator",        wrap_node(validator.run, "validator"))
    graph.add_node("transform",        wrap_node(transform.run, "transform"))
    graph.add_node("pii_masker",       wrap_node(pii_masker.run, "pii_masker"))         # LLM on Silver
    graph.add_node("gold_kpi",         wrap_node(gold_kpi.run, "gold_kpi"))           # LLM on Gold
    graph.add_node("lineage_tracker",  wrap_node(lineage_tracker.run, "lineage_tracker"))
    graph.add_node("audit_writer",     wrap_node(audit_writer.run, "audit_writer"))
    graph.add_node("alert",            wrap_node(alert.run, "alert"))
    graph.add_node("heal_agent",       wrap_node(heal_agent.run, "heal_agent"))         # LLM everywhere

    graph.set_entry_point("profile")

    # ── Pipeline flow: Bronze → Silver → Gold ─────────────────────
    pipeline_order = [
        # BRONZE
        ("profile",          "bronze_inspector"),
        ("bronze_inspector", "schema_drift"),
        ("schema_drift",     "pii_detector"),
        ("pii_detector",     "rule_gen"),
        ("rule_gen",         "validator"),
        ("validator",        "transform"),
        # SILVER
        ("transform",        "pii_masker"),
        ("pii_masker",       "gold_kpi"),
        # GOLD
        ("gold_kpi",         "lineage_tracker"),
        # AUDIT
        ("lineage_tracker",  "audit_writer"),
        ("audit_writer",     "alert"),
    ]

    for src, dst in pipeline_order:
        graph.add_conditional_edges(src, route, {"pass": dst, "fail": "heal_agent"})

    # ── Heal Agent router ─────────────────────────────────────────
    graph.add_conditional_edges(
        "heal_agent",
        route_after_heal,
        {
            "retry_profile":           "profile",
            "retry_bronze_inspector":  "bronze_inspector",
            "retry_schema_drift":      "schema_drift",
            "retry_pii_detector":      "pii_detector",
            "retry_rule_gen":          "rule_gen",
            "retry_validator":         "validator",
            "retry_transform":         "transform",
            "retry_pii_masker":        "pii_masker",
            "retry_gold_kpi":          "gold_kpi",
            "retry_lineage_tracker":   "lineage_tracker",
            "retry_audit_writer":      "audit_writer",
            "give_up":                 "alert",
        }
    )

    graph.add_edge("alert", "__end__")

    logger.info(
        "GRAPH | Pipeline compiled — 12 nodes + Heal Agent | "
        "LLM on every layer: Bronze → Silver → Gold"
    )
    return graph.compile()


pipeline = build_pipeline()
