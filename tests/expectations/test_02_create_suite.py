from __future__ import annotations

from ddm_sdk.models.expectations import ExpectationSuiteCreate
from helpers import getenv_str, safe_call, write_artifact, unwrap_task_value, poll_task_until_ready , get_task_value_fallback_from_status
from tests.expectations.expectations_utils import build_basic_expectations

def test_02_expectations_create_suite(
    client,
    dataset_id_for_suite: str,
    suite_name: str,
    column_names: list[str],
    column_descriptions: dict[str, str],
):
    user_id = getenv_str("DDM_TEST_USER") or getenv_str("DDM_USERNAME") or "unknown"

    expectations_list = build_basic_expectations(column_names)

    body = ExpectationSuiteCreate(
        suite_name=suite_name,
        datasource_name="default",
        dataset_id=dataset_id_for_suite,
        file_types=[getenv_str("DDM_TEST_EXPECTATIONS_FILE_TYPE", "csv")],
        category=getenv_str("DDM_TEST_EXPECTATIONS_CATEGORY", "crisis"),
        description=getenv_str("DDM_TEST_EXPECTATIONS_DESCRIPTION", "test"),
        use_case=getenv_str("DDM_TEST_EXPECTATIONS_USE_CASE", suite_name),
        column_names=column_names,
        column_descriptions=column_descriptions,
        expectations={
            "expectation_suite_name": suite_name,
            "expectations": expectations_list,
            "meta": {
                "column_names": column_names,
                "column_descriptions": column_descriptions,
                "table_expectation_descriptions": {},
            },
        },
        user_id=user_id,
    )

    write_artifact("expectations_create_suite_request", body.model_dump(), subdir="expectations")


    resp = safe_call("expectations.create_suite", lambda: client.expectations.create_suite(body))
    assert resp is not None, "create_suite failed"

    write_artifact(
        "expectations_create_suite_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="expectations",
    )
    suite_id = getattr(resp, "suite_id", None)
    task_id  = getattr(resp, "task_id", None)
    assert suite_id, f"missing suite_id in create_suite response: {resp}"
    assert task_id,  f"missing task_id in create_suite response: {resp}"

    write_artifact(
        "expectations_create_suite_ids",
        {"suite_id": suite_id, "task_id": task_id},
        subdir="expectations",
    )

    # ✅ poll the validation task
    st = poll_task_until_ready(client, task_id, timeout_s=180, interval_s=1.0)
    assert st is not None and st.is_success(), f"validation task failed: {st}"

    # optional: write task value (helps debug)
    val = unwrap_task_value(get_task_value_fallback_from_status(client, task_id))
    write_artifact("expectations_create_suite_task_value", val, subdir="expectations")

    # ✅ fetch suite and verify expectations
    suite = safe_call("expectations.get_suite", lambda: client.expectations.get_suite(suite_id))
    assert suite is not None
    write_artifact("expectations_get_suite_after_create", suite.model_dump(), subdir="expectations")

    assert suite.expectations, "suite.expectations is empty even after validation task success"