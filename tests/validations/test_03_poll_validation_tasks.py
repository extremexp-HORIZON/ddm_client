from __future__ import annotations

import json
import pytest
from helpers import OUT_DIR, safe_call, write_artifact, poll_task_until_ready, unwrap_task_value, get_task_value_fallback_from_status


def _read_task_ids() -> list[str]:
    p = OUT_DIR / "validations" / "validations_tasks.json"
    if not p.exists():
        pytest.skip("No validations_tasks.json (run test_02_validate_files_against_suite first, or set env to run it)")
    d = json.loads(p.read_text(encoding="utf-8"))
    return d.get("task_ids", [])


def test_03_validations_poll_tasks(client):
    task_ids = _read_task_ids()
    if not task_ids:
        pytest.skip("validations_tasks.json exists but contains no task_ids")

    statuses = {}
    values = {}

    for tid in task_ids:
        st = poll_task_until_ready(client, tid, timeout_s=300, interval_s=1.0)
        st_dump = st.model_dump(mode="json", exclude_none=False) if hasattr(st, "model_dump") else (
            st.__dict__ if hasattr(st, "__dict__") else st
        )
        statuses[tid] = st_dump

        if getattr(st, "is_success", None) and st.is_success():
            val = unwrap_task_value(get_task_value_fallback_from_status(client, tid))
            values[tid] = val
        else:
            values[tid] = {"error": "task not success", "status": st_dump}

    write_artifact("validations_task_statuses", statuses, subdir="validations")
    write_artifact("validations_task_values", values, subdir="validations")
