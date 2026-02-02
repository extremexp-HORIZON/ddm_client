from __future__ import annotations

import pytest
from ddm_sdk.models.blockchain import PrepareValidationBody
from helpers import (
    getenv_bool,
    getenv_str,
    sha256_hex_of_file,
    poll_task_until_ready,
    get_task_value,
    task_id_from_taskref,
    safe_call,
)


@pytest.fixture(scope="session")
def dataset_fingerprint() -> str | None:
    fp = getenv_str("DDM_TEST_DATASET_FINGERPRINT")
    if not fp:
        fp_raw = sha256_hex_of_file(getenv_str("DDM_SAMPLE_PATH") or "")
        fp = f"0x{fp_raw}" if fp_raw else None
    if fp and not fp.startswith("0x"):
        fp = "0x" + fp
    return fp


def test_prepare_validation(client, network, dataset_fingerprint):
    if not getenv_bool("DDM_TEST_PREPARE_VALIDATION", False):
        pytest.skip("DDM_TEST_PREPARE_VALIDATION not enabled")

    uploader = getenv_str("DDM_TEST_UPLOADER")
    if not uploader:
        pytest.skip("Missing DDM_TEST_UPLOADER (0x...)")

    if not dataset_fingerprint:
        pytest.skip("Missing dataset fingerprint (DDM_TEST_DATASET_FINGERPRINT or sample file path)")

    payload = PrepareValidationBody(
        network=network,
        uploader=uploader,
        dataset_fingerprint=dataset_fingerprint,
        validation_json={"score": 100},
        include_report=True,
    )

    t = safe_call("blockchain.prepare_validation(frontend)", lambda: client.blockchain.prepare_validation(payload))
    tid = task_id_from_taskref(t)
    assert tid, f"prepare_validation returned no task_id. resp={t}"

    st = poll_task_until_ready(client, tid, timeout_s=240, interval_s=2.0)
    assert st is not None, "Timed out waiting for validation task"
    assert st.is_success(), f"validation task failed: {st.error or st.message or st.result}"

    val = get_task_value(client, tid)
    result = val.get("result") if isinstance(val, dict) and "result" in val else val
    assert isinstance(result, dict)
    assert "validation_hash" in result
    print("✅ validation artifacts result:", result)
