from __future__ import annotations

import json
import pytest
from pathlib import Path

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR


def _artifact_file_ids() -> list[str]:
    """
    Optional fallback: reuse ids created by tests/files upload.
    Reads: out/tests/files/files_upload_file_ids.json -> {"file_ids":[...]}
    """
    p = OUT_DIR / "files" / "files_upload_file_ids.json"
    if not p.exists():
        return []
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    ids = d.get("file_ids") or []
    return [x for x in ids if isinstance(x, str) and x.strip()]


@pytest.fixture(scope="session")
def file_id() -> str:
    fid = getenv_str("DDM_TEST_FILE_ID")
    if fid:
        return fid

    # fallback: first uploaded id
    ids = _artifact_file_ids()
    if ids:
        return ids[0]

    pytest.skip("Missing DDM_TEST_FILE_ID and no out/tests/files/files_upload_file_ids.json found")


def test_01_file_metadata_get(client, file_id: str):
    if not getenv_bool("DDM_TEST_FILE_METADATA_GET", True):
        pytest.skip("DDM_TEST_FILE_METADATA_GET not enabled")

    resp = safe_call("file_metadata.get", lambda: client.file_metadata.get(file_id))
    assert resp is not None, "file_metadata.get returned None"

    write_artifact(
        "file_metadata_get_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="file_metadata",
    )
