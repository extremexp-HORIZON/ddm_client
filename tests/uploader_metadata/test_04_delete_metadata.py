from __future__ import annotations

import json
import pytest

from helpers import safe_call, write_artifact, OUT_DIR


def _read_file_id() -> str:
    p = OUT_DIR / "uploader_metadata" / "uploader_metadata_file_id.json"
    if not p.exists():
        pytest.skip("Missing uploader_metadata_file_id.json (run test_01_pick_file_id first)")
    d = json.loads(p.read_text(encoding="utf-8"))
    return d["file_id"]


def test_04_delete_uploader_metadata(client):
    file_id = _read_file_id()

    resp = safe_call("uploader_metadata.delete", lambda: client.uploader_metadata.delete(file_id))
    assert resp is not None, "delete returned None"

    write_artifact(
        "uploader_metadata_delete_response",
        resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp,
        subdir="uploader_metadata",
    )

    # Optional: verify it's gone (some backends return 404; safe_call might return None)
    got = safe_call("uploader_metadata.get(after_delete)", lambda: client.uploader_metadata.get(file_id))
    write_artifact(
        "uploader_metadata_get_after_delete",
        got.model_dump(mode="json", exclude_none=False) if (got and hasattr(got, "model_dump")) else (got or {"note": "get failed or returned None (possibly expected)"}),
        subdir="uploader_metadata",
    )
