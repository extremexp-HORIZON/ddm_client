from __future__ import annotations

import json
import pytest

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR


def _split_csv_env(name: str) -> list[str]:
    v = getenv_str(name)
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


def _artifact_file_ids() -> list[str]:
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
def file_ids() -> list[str]:
    ids = _split_csv_env("DDM_TEST_FILE_IDS")
    if ids:
        return ids

    ids = _artifact_file_ids()
    if ids:
        return ids

    fid = getenv_str("DDM_TEST_FILE_ID")
    if fid:
        return [fid]

    pytest.skip("Missing DDM_TEST_FILE_IDS/DDM_TEST_FILE_ID and no files upload artifact found")


def test_02_file_metadata_get_many(client, file_ids: list[str]):

    resp = safe_call("file_metadata.get_many", lambda: client.file_metadata.get_many(file_ids))
    assert resp is not None, "file_metadata.get_many returned None"

    write_artifact(
        "file_metadata_get_many_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="file_metadata",
    )

    if hasattr(resp, "metadata"):
        md = getattr(resp, "metadata")
        assert md is None or isinstance(md, dict), f"Expected metadata dict; got {type(md)}"
