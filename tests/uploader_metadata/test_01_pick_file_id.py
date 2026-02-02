from __future__ import annotations

import json
import pytest
from pathlib import Path

from helpers import getenv_str, safe_call, write_artifact, OUT_DIR


def _file_id_from_env() -> str | None:
    v = getenv_str("DDM_TEST_FILE_ID")
    return v.strip() if v and v.strip() else None


def _file_id_from_artifact() -> str | None:
    p = OUT_DIR / "file" / "file_upload_ids.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    v = d.get("file_id")
    return v.strip() if isinstance(v, str) and v.strip() else None


def test_01_pick_file_id(client):
    file_id = _file_id_from_env() or _file_id_from_artifact()
    if not file_id:
        pytest.skip("No file_id available (set DDM_TEST_FILE_ID or run file upload test)")

    write_artifact("uploader_metadata_file_id", {"file_id": file_id}, subdir="uploader_metadata")
