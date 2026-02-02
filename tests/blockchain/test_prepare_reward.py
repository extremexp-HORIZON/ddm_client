from __future__ import annotations

import pytest
from helpers import (
    getenv_bool,
    getenv_str,
    poll_task_until_ready,
    get_task_value,
    task_id_from_taskref,
    safe_call,
)


def test_prepare_reward(client, network, dataset_fingerprint, uploader):
    if not getenv_bool("DDM_TEST_PREPARE_REWARD", False):
        pytest.skip("DDM_TEST_PREPARE_REWARD not enabled")

    uploader_env = getenv_str("DDM_TEST_UPLOADER")
    if not uploader_env:
        pytest.skip("Missing DDM_TEST_UPLOADER (0x...)")
    uploader = uploader_env


    category = getenv_str("DDM_TEST_CATEGORY", "dataset")
    if not dataset_fingerprint:
        pytest.skip("Missing dataset fingerprint (DDM_TEST_DATASET_FINGERPRINT or sample file path)")

    # Frontend-style payload
    t = safe_call(
        "blockchain.prepare_reward",
        lambda: client.blockchain.prepare_reward(
            {
                "network": network,
                "category": category,
                "dataset_fingerprint": dataset_fingerprint,
                "uploader": uploader,
                "expires_in_sec": 900,
            }
        ),
    )
    tid = task_id_from_taskref(t)
    assert tid, f"prepare_reward returned no task_id. resp={t}"

    st = poll_task_until_ready(client, tid, timeout_s=240, interval_s=2.0)
    assert st is not None, "Timed out waiting for reward task"

    if st.is_failure():
        msg = (st.error or st.message or str(st.result) or "").lower()
        if "dataset not found in index" in msg:
            pytest.skip(f"Backend has not indexed this dataset_fingerprint yet: {dataset_fingerprint}")
        pytest.fail(f"reward task failed: {st.error or st.message or st.result}")

    val = get_task_value(client, tid)
    result = val.get("result") if isinstance(val, dict) and "result" in val else val
    assert isinstance(result, dict)
    assert "signature" in result
    assert "typedData" in result
    print("✅ reward artifacts result:", result)
