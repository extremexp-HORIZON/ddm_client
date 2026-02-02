from __future__ import annotations

import pytest
from helpers import getenv_bool, getenv_str, safe_call, write_artifact


@pytest.fixture(scope="session")
def file_id_for_bulk_update() -> str:
    fid = getenv_str("DDM_TEST_FILE_ID") or getenv_str("DDM_TEST_FILES_UPDATE_FILE_ID")
    if not fid:
        pytest.skip("Missing DDM_TEST_FILE_ID (or DDM_TEST_FILES_UPDATE_FILE_ID)")
    return fid


def test_04_files_update_bulk(client, file_id_for_bulk_update: str):
    if not getenv_bool("DDM_TEST_FILES_UPDATE", True):
        pytest.skip("DDM_TEST_FILES_UPDATE not enabled")

    new_desc = getenv_str("DDM_TEST_FILES_UPDATE_DESCRIPTION", "bulk updated description")

    # Backend expects: {"files": [...]}
    body = {
        "files": [
            {
                "id": file_id_for_bulk_update,
                "description": new_desc,

            }
        ]
    }

    resp = safe_call("files.update", lambda: client.files.update(body))
    assert resp is not None, "files.update returned None (see printed error above)"

    write_artifact(
        "files_update_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="files",
    )
