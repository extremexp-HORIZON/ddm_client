from __future__ import annotations

import pytest
from helpers import (
    getenv_bool,
    getenv_str,
    poll_task_until_ready,
    get_task_value,
    task_id_from_taskref,
    safe_call,
    extract_report_uri,
)


@pytest.fixture(scope="session")
def catalog_id() -> str | None:
    return getenv_str("DDM_TEST_FILE_ID") or getenv_str("DDM_TEST_CATALOG_ID")


@pytest.fixture(scope="session")
def report_uri(client, network, catalog_id):
    existing = getenv_str("DDM_TEST_REPORT_URI")
    if existing:
        return existing

    if not getenv_bool("DDM_TEST_PREPARE_REPORT", False):
        print("Skipping prepare_report (DDM_TEST_PREPARE_REPORT not enabled)")
        return None

    if not catalog_id:
        print("Skipping prepare_report (no DDM_TEST_FILE_ID / DDM_TEST_CATALOG_ID)")
        return None

    t = safe_call(
        f"blockchain.prepare_report_ipfs_uri(catalog_id={catalog_id})",
        lambda: client.blockchain.prepare_report_ipfs_uri(network=network, catalog_id=catalog_id, include_report=True),
    )
    tid = task_id_from_taskref(t)
    assert tid, "prepare_report_ipfs_uri did not return task_id"

    st = poll_task_until_ready(client, tid, timeout_s=180, interval_s=2.0)
    assert st is not None, "Timed out waiting for report task"

    assert st.is_success(), f"report task failed: {st.error or st.message or st.result}"

    val = get_task_value(client, tid)
    uri = extract_report_uri(val)
    assert uri, f"report task success but missing report_uri. value={val}"
    print("✅ resolved report_uri:", uri)
    return uri
