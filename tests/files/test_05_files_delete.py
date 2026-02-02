from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, safe_call, write_artifact


def _split_csv_env(name: str) -> list[str]:
    v = getenv_str(name)
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


@pytest.fixture(scope="session")
def delete_file_ids() -> list[str]:
    ids = _split_csv_env("DDM_TEST_FILES_DELETE_IDS")
    if not ids:
        pytest.skip("Missing DDM_TEST_FILES_DELETE_IDS")
    return ids


def test_05_files_delete_bulk(client, delete_file_ids: list[str]):
    # OFF by default; deleting data is dangerous
    if not getenv_bool("DDM_TEST_FILES_DELETE", False):
        pytest.skip("DDM_TEST_FILES_DELETE not enabled (defaults to False)")

    resp = safe_call("files.delete", lambda: client.files.delete(delete_file_ids))
    assert resp is not None, "files.delete returned None"

    write_artifact(
        "files_delete_response",
        resp,
        subdir="files",
    )
