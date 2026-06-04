"""
etl-pipeline/plugins/self_healing_agent/callbacks.py
Airflow on_failure_callback integration.
Attach heal_on_failure to any PythonOperator to auto-invoke the healing agent.

Usage in DAG:
    validate = PythonOperator(
        task_id         = "validate_customers",
        python_callable = _validate_quality,
        op_kwargs       = {"dataset": "customers"},
        on_failure_callback = heal_on_failure,
    )
"""
import logging
from typing import Any, Dict

log = logging.getLogger("airflow.task")


def heal_on_failure(context: Dict[str, Any]) -> None:
    """
    Airflow on_failure_callback — invoked automatically when a task fails.
    Runs the LangGraph self-healing agent and logs the fix applied.
    """
    task_instance = context.get("task_instance")
    exception     = context.get("exception")

    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id  = context.get("dag").dag_id if context.get("dag") else "unknown"

    # Extract dataset from task_id (e.g. "validate_customers" → "customers")
    dataset = "unknown"
    for ds in ["customers", "orders", "payments", "products"]:
        if ds in task_id:
            dataset = ds
            break

    error_msg = str(exception) if exception else "Unknown failure"
    log.warning(
        f"HEAL CALLBACK | task={task_id} | dag={dag_id} | "
        f"dataset={dataset} | error={error_msg[:120]}"
    )

    try:
        from etl_pipeline.plugins.self_healing_agent.agent_graph import run_healing
        from etl_pipeline.plugins.self_healing_agent.mcp_tools import mcp_append_audit
        from datetime import datetime

        result = run_healing(task_id=task_id, dataset=dataset, error_msg=error_msg)

        # Write audit record
        mcp_append_audit({
            "dag_id":    dag_id,
            "task_id":   task_id,
            "dataset":   dataset,
            "error_type":result.get("error_type","?"),
            "fix":       result.get("fix_applied","?"),
            "retry":     result.get("retry", False),
            "timestamp": datetime.utcnow().isoformat(),
        })

        log.info(
            f"HEAL CALLBACK | Fix applied: {result.get('fix_applied','?')[:120]} | "
            f"retry={result.get('retry', False)}"
        )

    except Exception as heal_err:
        log.error(f"HEAL CALLBACK | Healing agent itself failed: {heal_err}")
        log.error("Manual intervention required.")
