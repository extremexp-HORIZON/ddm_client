from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR

@pytest.fixture(scope="session")
def big_path() -> Path:
    p = getenv_str("DDM_BIG_PATH")
    if not p:
        pytest.skip("Missing DDM_BIG_PATH")
    pp = Path(p)
    if not pp.exists():
        pytest.skip(f"DDM_BIG_PATH does not exist: {pp}")
    return pp

@pytest.fixture(scope="session")
def big_project_id() -> str:
    return getenv_str("DDM_TEST_BIG_PROJECT_ID", "bigProject")

def test_06_file_upload_async(client, big_path: Path, big_project_id: str):
    if not getenv_bool("DDM_TEST_FILE_UPLOAD_ASYNC", False):
        pytest.skip("DDM_TEST_FILE_UPLOAD_ASYNC not enabled (defaults to False)")

    resp = safe_call(
        "file.upload_async",
        lambda: client.file.upload_async(project_id=big_project_id, file=str(big_path)),
    )
    assert resp is not None, "file.upload_async returned None"

    write_artifact(
        "file_upload_async_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="file",
    )

    # Typical fields (adjust if your model differs)
    file_id = getattr(resp, "file_id", None)
    merge_task_id = getattr(resp, "merge_task_id", None)
    metadata_task_id = getattr(resp, "metadata_task_id", None)

    assert file_id or merge_task_id or metadata_task_id, f"Unexpected async response: {resp}"
