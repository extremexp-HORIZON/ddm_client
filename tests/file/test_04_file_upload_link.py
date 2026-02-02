from __future__ import annotations

import pytest

from ddm_sdk.models.file import UploadLinkBody
from helpers import (
    getenv_bool,
    getenv_str,
    safe_call,
    write_artifact,
    OUT_DIR,
)

@pytest.fixture(scope="session")
def link_url() -> str:
    url = getenv_str("DDM_TEST_LINK_URL")
    if not url:
        pytest.skip("Missing DDM_TEST_LINK_URL")
    return url

@pytest.fixture(scope="session")
def link_project_id() -> str:
    return getenv_str("DDM_TEST_LINK_PROJECT_ID", getenv_str("DDM_TEST_PROJECT_ID", "projectB"))

def test_04_file_upload_link(client, link_url: str, link_project_id: str):
    if not getenv_bool("DDM_TEST_FILE_UPLOAD_LINK", True):
        pytest.skip("DDM_TEST_FILE_UPLOAD_LINK not enabled")

    body = UploadLinkBody(
        file_url=link_url,
        project_id=link_project_id,
        description=getenv_str("DDM_TEST_LINK_DESCRIPTION", "external import"),
        use_cases=(getenv_str("DDM_TEST_LINK_USE_CASES", "etl") or "etl").split(","),
        metadata={"source": getenv_str("DDM_TEST_LINK_SOURCE", "external")},
    )

    chain = safe_call("file.upload_link", lambda: client.file.upload_link(body))
    assert chain is not None, "file.upload_link returned None"

    write_artifact(
        "file_upload_link_response",
        chain.model_dump() if hasattr(chain, "model_dump") else chain,
        subdir="file",
    )

    fetch_task_id = getattr(chain, "fetch_task_id", None)
    process_task_id = getattr(chain, "process_task_id", None)
    assert fetch_task_id or process_task_id, f"Expected at least one task id. resp={chain}"

    write_artifact(
        "file_upload_link_task_ids",
        {"fetch_task_id": fetch_task_id, "process_task_id": process_task_id},
        subdir="file",
    )
