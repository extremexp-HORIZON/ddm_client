from __future__ import annotations

import pytest

from ddm_sdk.models.file import FileUpdateBody
from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR

@pytest.fixture(scope="session")
def file_id_for_update() -> str:
    fid = getenv_str("DDM_TEST_FILE_ID")
    if not fid:
        pytest.skip("Missing DDM_TEST_FILE_ID (set this to a previously uploaded file.id)")
    return fid

def test_03_file_update(client, file_id_for_update: str):
    if not getenv_bool("DDM_TEST_FILE_UPDATE", True):
        pytest.skip("DDM_TEST_FILE_UPDATE not enabled")

    new_desc = getenv_str("DDM_TEST_FILE_UPDATE_DESCRIPTION", "updated description")
    use_cases = (getenv_str("DDM_TEST_FILE_UPDATE_USE_CASES", "ml") or "ml").split(",")

    upd = safe_call(
        "file.update",
        lambda: client.file.update(
            file_id_for_update,
            FileUpdateBody(description=new_desc, use_case=use_cases),
        ),
    )
    assert upd is not None, "file.update returned None"

    write_artifact(
        "file_update_response",
        upd.model_dump() if hasattr(upd, "model_dump") else upd,
        subdir="file",
    )

    # Minimal sanity: updated_data.description should match if backend echoes it
    updated_data = getattr(upd, "updated_data", None)
    if updated_data is not None:
        desc = getattr(updated_data, "description", None)
        assert desc == new_desc, f"Description mismatch. expected={new_desc} got={desc}"
