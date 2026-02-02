from __future__ import annotations

import pytest
from pathlib import Path

from ddm_sdk.models.file import UploadSingleResponse
from helpers import (
    getenv_bool,
    getenv_str,
    safe_call,
    write_artifact,
    OUT_DIR,
)

@pytest.fixture(scope="session")
def file_project_id() -> str:
    return getenv_str("DDM_TEST_PROJECT_ID", "projectA/sub1")

@pytest.fixture(scope="session")
def sample_path() -> Path:
    p = Path(getenv_str("DDM_SAMPLE_PATH", "data.csv"))
    if not p.exists():
        pytest.skip(f"Sample file not found: {p} (set DDM_SAMPLE_PATH)")
    return p

@pytest.fixture(scope="session")
def sample_metadata_path() -> str | None:
    v = getenv_str("DDM_SAMPLE_METADATA_PATH")
    if not v:
        return None
    p = Path(v)
    if not p.exists():
        pytest.skip(f"Metadata file not found: {p} (set DDM_SAMPLE_METADATA_PATH)")
    return str(p)

def test_01_file_upload_single(client, file_project_id: str, sample_path: Path, sample_metadata_path: str | None):
    if not getenv_bool("DDM_TEST_FILE_UPLOAD", True):
        pytest.skip("DDM_TEST_FILE_UPLOAD not enabled")

    resp = safe_call(
        "file.upload",
        lambda: client.file.upload(
            project_id=file_project_id,
            file=str(sample_path),
            user_filename=getenv_str("DDM_TEST_USER_FILENAME", "my_clean_data.csv"),
            description=getenv_str("DDM_TEST_FILE_DESCRIPTION", "training dataset"),
            use_case=(getenv_str("DDM_TEST_FILE_USE_CASE", "ml") or "ml").split(","),
            metadata_file=sample_metadata_path,
        ),
    )
    assert resp is not None, "file.upload returned None"

    # Persist full response
    write_artifact(
        "file_upload_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="file",
    )

    # Basic sanity
    assert isinstance(resp, UploadSingleResponse) or hasattr(resp, "file"), f"Unexpected response type: {type(resp)}"
    file_obj = getattr(resp, "file", None)
    assert file_obj is not None, f"upload response missing .file. resp={resp}"

    file_id = getattr(file_obj, "id", None)
    assert isinstance(file_id, str) and file_id, f"upload response missing file.id. resp={resp}"

    # Optional: task id (metadata task)
    meta_task_id = getattr(file_obj, "metadata_task_id", None)
    write_artifact(
        "file_upload_ids",
        {"file_id": file_id, "metadata_task_id": meta_task_id},
        subdir="file",
    )
