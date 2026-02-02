from __future__ import annotations

import pytest
from helpers import (
    getenv_bool,
    getenv_str,
    getenv_int,
    safe_call,
    task_id_from_taskref,
    poll_task_until_ready,
    get_task_value,
    unwrap_task_value,
    write_artifact,
    extract_suite_hash,
)

def test_prepare_suite(client, network, requester):
    if not getenv_bool("DDM_TEST_PREPARE_SUITE", False):
        pytest.skip("DDM_TEST_PREPARE_SUITE not enabled")

    if not requester:
        pytest.skip("Missing DDM_TEST_REQUESTER (0x...)")

    expectation_suite_id = getenv_str("DDM_TEST_SUITE_ID") or getenv_str("DDM_TEST_EXPECTATION_SUITE_ID")
    if not expectation_suite_id:
        pytest.skip("Missing DDM_TEST_SUITE_ID or DDM_TEST_EXPECTATION_SUITE_ID")

    deadline = getenv_int("DDM_TEST_DEADLINE") or 1893456000
    total_expected = getenv_int("DDM_TEST_TOTAL_EXPECTED") or 1
    category = getenv_str("DDM_TEST_CATEGORY", "tutorial")
    file_format = getenv_str("DDM_TEST_FILE_FORMAT", "csv")

    suite_payload = {
        "expectation_suite_id": expectation_suite_id,
        "name": getenv_str("DDM_TEST_SUITE_NAME", "new"),
        "description": getenv_str("DDM_TEST_SUITE_DESCRIPTION", ""),
        "expectations": [],
        "column_descriptions": {},
        "column_names": [],
        "file_types": [file_format],
        "category": category,
    }

    body = {
        "network": network,
        "requester": requester,
        "expectation_suite_id": expectation_suite_id,
        "suite": suite_payload,
        "category": category,
        "fileFormat": file_format,
        "deadline": deadline,
        "totalExpected": total_expected,
    }

    t = safe_call(
        "blockchain.prepare_suite",
        lambda: client.blockchain.prepare_suite(body),
    )
    write_artifact("prepare_suite_taskref", t if isinstance(t, dict) else getattr(t, "model_dump", lambda: t)(), subdir="blockchain")

    tid = task_id_from_taskref(t)
    assert tid, f"prepare_suite returned no task_id. resp={t}"

    st = poll_task_until_ready(client, tid, timeout_s=240, interval_s=2.0)
    assert st is not None, "Timed out waiting for suite task"

    write_artifact(
        "prepare_suite_status",
        st.model_dump() if hasattr(st, "model_dump") else getattr(st, "__dict__", str(st)),
        subdir="blockchain"
    )

    if st.is_failure():
        pytest.fail(f"suite task failed: {st.error or st.message or st.result}")

    val = get_task_value(client, tid)
    write_artifact("prepare_suite_value_raw", val, subdir="blockchain")

    result = unwrap_task_value(val)
    write_artifact("prepare_suite_result", result, subdir="blockchain")

 
    h = extract_suite_hash(result) or extract_suite_hash(val)
    assert h, f"prepare_suite success but missing suiteHash/suite_hash. value={val}"
    write_artifact("prepare_suite_suite_hash", {"suite_hash": h}, subdir="blockchain")

    assert isinstance(result, (dict, str, list)), "Unexpected task result shape"
