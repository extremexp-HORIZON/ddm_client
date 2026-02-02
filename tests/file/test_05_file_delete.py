from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR

@pytest.fixture(scope="session")
def file_id_for_delete() -> str:
    fid = getenv_str("DDM_TEST_DELETE_FILE_ID") or getenv_str("DDM_TEST_FILE_ID")
    if not fid:
        pytest.skip("Missing DDM_TEST_DELETE_FILE_ID (or DDM_TEST_FILE_ID)")
    return fid

def test_05_file_delete(client, file_id_for_delete: str):
    # off by default: it deletes data
    if not getenv_bool("DDM_TEST_FILE_DELETE", False):
        pytest.skip("DDM_TEST_FILE_DELETE not enabled (defaults to False)")

    resp = safe_call("file.delete", lambda: client.file.delete(file_id_for_delete))
    assert resp is not None, "file.delete returned None"

    write_artifact(
        "file_delete_response",
        resp,
        subdir= "file",
    )
