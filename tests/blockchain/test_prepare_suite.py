from __future__ import annotations

import pytest
from helpers import (
    getenv_bool,
    getenv_str,
    getenv_int,
    poll_task_until_ready,
    get_task_value,
    task_id_from_taskref,
    safe_call,
    extract_suite_hash,
)


@pytest.fixture(scope="session")
def suite_hash(client, network, requester):
    existing = getenv_str("DDM_TEST_SUITE_HASH")
    if existing:
        return existing

    if not getenv_bool("DDM_TEST_PREPARE_SUITE", False):
        print("Skipping prepare_suite (DDM_TEST_PREPARE_SUITE not enabled)")
        return None

    if not requester:
        print("Skipping prepare_suite (missing DDM_TEST_REQUESTER)")
        return None

    expectation_suite_id = getenv_str("DDM_TEST_SUITE_ID") or getenv_str("DDM_TEST_EXPECTATION_SUITE_ID")
    if not expectation_suite_id:
        print("Skipping prepare_suite (missing DDM_TEST_SUITE_ID)")
        return None

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

    t = safe_call(
        "blockchain.prepare_suite(frontend-style payload)",
        lambda: client.blockchain.prepare_suite(
            {
                "network": network,
                "requester": requester,
                "expectation_suite_id": expectation_suite_id,
                "suite": suite_payload,
                "category": category,
                "fileFormat": file_format,
                "deadline": deadline,
                "totalExpected": total_expected,
            }
        ),
    )
    tid = task_id_from_taskref(t)
    assert tid, "prepare_suite did not return task_id"

    st = poll_task_until_ready(client, tid, timeout_s=240, interval_s=2.0)
    assert st is not None, "Timed out waiting for suite task"
    assert st.is_success(), f"suite task failed: {st.error or st.message or st.result}"

    val = get_task_value(client, tid)
    h = extract_suite_hash(val)
    assert h, f"suite task success but missing suiteHash/suite_hash. value={val}"
    print("✅ resolved suite_hash:", h)
    return h
