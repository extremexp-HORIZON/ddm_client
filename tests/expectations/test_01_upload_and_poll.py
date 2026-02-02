from __future__ import annotations

from pathlib import Path

from helpers import (
    safe_call,
    write_artifact,
    poll_task_until_ready,
    get_task_value,
    unwrap_task_value,
)


def test_01_expectations_upload_and_poll(client, expectations_sample_file_path: Path, suite_name: str):
    up = safe_call(
        "expectations.upload_sample",
        lambda: client.expectations.upload_sample(str(expectations_sample_file_path), suite_name=suite_name),
    )
    assert up is not None, "upload_sample returned None"

    up_dump = up.model_dump() if hasattr(up, "model_dump") else up
    write_artifact("expectations_upload_sample_response", up_dump, subdir="expectations")

    dataset_id = getattr(up, "dataset_id", None)
    expectation_task_id = getattr(up, "expectation_task_id", None)
    description_task_id = getattr(up, "description_task_id", None)

    assert isinstance(dataset_id, str) and dataset_id
    assert isinstance(expectation_task_id, str) and expectation_task_id
    assert isinstance(description_task_id, str) and description_task_id

    write_artifact(
        "expectations_upload_sample_ids",
        {
            "dataset_id": dataset_id,
            "expectation_task_id": expectation_task_id,
            "description_task_id": description_task_id,
        },
        subdir="expectations",
    )

    st1 = poll_task_until_ready(client, expectation_task_id, timeout_s=180, interval_s=1.0)
    assert st1 is not None and st1.is_success()

    val1 = unwrap_task_value(get_task_value(client, expectation_task_id))
    write_artifact("expectation_task_value", val1, subdir="expectations")

    st2 = poll_task_until_ready(client, description_task_id, timeout_s=180, interval_s=1.0)
    st2_dump = st2.model_dump(mode="json", exclude_none=False) if hasattr(st2, "model_dump") else st2.__dict__
    write_artifact("description_task_status", st2_dump, subdir="expectations")


    val2 = unwrap_task_value(get_task_value(client, description_task_id))
    write_artifact("description_task_value", val2, subdir="expectations")
