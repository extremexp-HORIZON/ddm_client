from __future__ import annotations

import pytest
from helpers import (
    getenv_bool,
    getenv_str,
    safe_call,
    task_id_from_taskref,
    poll_task_until_ready,
    get_task_value,
    unwrap_task_value,
    write_artifact,
)

def test_prepare_reward(client, network, dataset_fingerprint, uploader):
    if not getenv_bool("DDM_TEST_PREPARE_REWARD", False):
        pytest.skip("DDM_TEST_PREPARE_REWARD not enabled")

    category = getenv_str("DDM_TEST_CATEGORY", "dataset")

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
    write_artifact("prepare_reward_taskref", t if isinstance(t, dict) else getattr(t, "model_dump", lambda: t)(), subdir="blockchain")

    tid = task_id_from_taskref(t)
    assert tid, f"prepare_reward returned no task_id. resp={t}"

    st = poll_task_until_ready(client, tid, timeout_s=240, interval_s=2.0)
    assert st is not None, "Timed out waiting for reward task"

    # always store status
    write_artifact("prepare_reward_status", st.model_dump() if hasattr(st, "model_dump") else getattr(st, "__dict__", str(st)), subdir="blockchain")

    if st.is_failure():
        msg = (st.error or st.message or str(st.result) or "").lower()
        if "dataset not found in index" in msg:
            pytest.skip(f"Backend has not indexed this dataset_fingerprint yet: {dataset_fingerprint}")
        pytest.fail(f"reward task failed: {st.error or st.message or st.result}")

    val = get_task_value(client, tid)
    write_artifact("prepare_reward_value_raw", val, subdir="blockchain")

    result = unwrap_task_value(val)
    write_artifact("prepare_reward_result", result)

    assert isinstance(result, dict)
    assert "signature" in result
    assert "typedData" in result
