"""
pipeline/batch_state.py
Shared batch state manager.
The continuous pipeline writes state here after every batch.
The Streamlit dashboard reads it to update the live display.
Uses a JSON file as a simple message bus — no Redis/queue needed.
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

BATCH_STATE_FILE = "metadata/batch_state.json"
BATCH_LOG_FILE   = "metadata/batch_log.json"
LIVE_LOG_FILE    = "metadata/live_log.json"

os.makedirs("metadata", exist_ok=True)


def _read_json(path: str, default):
    try:
        if Path(path).exists():
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return default


def _write_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


# ── Pipeline writes these ─────────────────────────────────────────

def write_batch_state(
    batch_id:        int,
    dataset:         str,
    status:          str,          # "running" | "complete" | "healed" | "failed"
    current_node:    str,
    bronze_rows:     int  = 0,
    silver_rows:     int  = 0,
    quarantine_rows: int  = 0,
    gold_rows:       int  = 0,
    heal_count:      int  = 0,
    schema_drifts:   int  = 0,
    node_statuses:   dict = None,
    gold_kpis:       dict = None,
    error:           Optional[str] = None,
):
    """Write current batch status — dashboard reads this."""
    state = {
        "batch_id":        batch_id,
        "dataset":         dataset,
        "status":          status,
        "current_node":    current_node,
        "bronze_rows":     bronze_rows,
        "silver_rows":     silver_rows,
        "quarantine_rows": quarantine_rows,
        "gold_rows":       gold_rows,
        "heal_count":      heal_count,
        "schema_drifts":   schema_drifts,
        "node_statuses":   node_statuses or {},
        "gold_kpis":       gold_kpis or {},
        "error":           error,
        "updated_at":      datetime.utcnow().isoformat(),
    }
    _write_json(BATCH_STATE_FILE, state)


def append_batch_log(batch_summary: dict):
    """Append a completed batch summary to the cumulative log."""
    logs = _read_json(BATCH_LOG_FILE, [])
    logs.append(batch_summary)
    # Keep last 200 batches
    if len(logs) > 200:
        logs = logs[-200:]
    _write_json(BATCH_LOG_FILE, logs)


def push_live_log(message: str, level: str = "INFO", node: str = "", batch_id: int = 0):
    """Push a log line for the live feed in the dashboard."""
    logs = _read_json(LIVE_LOG_FILE, [])
    logs.append({
        "ts":       datetime.utcnow().strftime("%H:%M:%S"),
        "level":    level,
        "node":     node,
        "msg":      message,
        "batch_id": batch_id,
    })
    # Keep last 300 lines
    if len(logs) > 300:
        logs = logs[-300:]
    _write_json(LIVE_LOG_FILE, logs)


def write_pipeline_control(running: bool, batch_interval_sec: int = 15, dataset: str = "all"):
    """Dashboard writes this; pipeline runner reads it."""
    ctrl = _read_json("metadata/pipeline_control.json", {})
    ctrl["running"]             = running
    ctrl["batch_interval_sec"]  = batch_interval_sec
    ctrl["dataset"]             = dataset
    ctrl["updated_at"]          = datetime.utcnow().isoformat()
    _write_json("metadata/pipeline_control.json", ctrl)


def read_pipeline_control() -> dict:
    return _read_json("metadata/pipeline_control.json", {
        "running": False,
        "batch_interval_sec": 15,
        "dataset": "all",
    })


# ── Dashboard reads these ─────────────────────────────────────────

def read_batch_state() -> dict:
    return _read_json(BATCH_STATE_FILE, {
        "batch_id": 0, "dataset": "-", "status": "idle",
        "current_node": "-", "bronze_rows": 0, "silver_rows": 0,
        "quarantine_rows": 0, "gold_rows": 0, "heal_count": 0,
        "schema_drifts": 0, "node_statuses": {}, "gold_kpis": {},
        "error": None, "updated_at": "-",
    })


def read_batch_log() -> list:
    return _read_json(BATCH_LOG_FILE, [])


def read_live_log() -> list:
    return _read_json(LIVE_LOG_FILE, [])


def reset_state():
    """Clear all state files — called on fresh start."""
    for f in [BATCH_STATE_FILE, BATCH_LOG_FILE, LIVE_LOG_FILE,
              "metadata/pipeline_control.json"]:
        if Path(f).exists():
            os.remove(f)
