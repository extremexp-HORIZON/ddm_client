from __future__ import annotations

import pytest
from ddm_sdk.models.blockchain import PrepareValidationBody

from helpers import (
    getenv_bool,
    safe_call,
    task_id_from_taskref,
    poll_task_until_ready,
    get_task_value,
    unwrap_task_value,
    write_artifact,
)

def test_prepare_validation_frontend(client, network, dataset_fingerprint, uploader):
    if not getenv_bool("DDM_TEST_PREPARE_VALIDATION", False):
        pytest.skip("DDM_TEST_PREPARE_VALIDATION not enabled")

    payload = PrepareValidationBody(
        network=network,
        uploader=uploader,
        dataset_fingerprint=dataset_fingerprint,
        validation_json={"score": 100},
        include_report=True,
    )

    out = safe_call("blockchain.prepare_validation(frontend)", lambda: client.blockchain.prepare_validation(payload))
    write_artifact("prepare_validation_taskref", out if isinstance(out, dict) else getattr(out, "model_dump", lambda: out)(), subdir="blockchain")

    tid = task_id_from_taskref(out)
    assert tid, "prepare_validation returned no task_id"

    st = poll_task_until_ready(client, tid, timeout_s=240, interval_s=2.0)
    assert st is not None, "Timed out waiting for validation task"

    write_artifact("prepare_validation_status", st.model_dump() if hasattr(st, "model_dump") else getattr(st, "__dict__", str(st)), subdir="blockchain")

    if st.is_failure():
        pytest.fail(f"validation task failed: {st.error or st.message or st.result}")

    val = get_task_value(client, tid)
    write_artifact("prepare_validation_value_raw", val, subdir="blockchain")

    result = unwrap_task_value(val)
    write_artifact("prepare_validation_result", result, subdir="blockchain")
    assert isinstance(result, dict)
    assert "validation_hash" in result
    assert "report_uri" in result
