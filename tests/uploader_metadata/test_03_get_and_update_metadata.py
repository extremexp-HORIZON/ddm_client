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


def test_03_get_and_update_uploader_metadata(client):
    file_id = _read_file_id()

    # --- GET ---
    got1 = safe_call("uploader_metadata.get(before)", lambda: client.uploader_metadata.get(file_id))
    assert got1 is not None, "get returned None"
    write_artifact(
        "uploader_metadata_get_before",
        got1.model_dump(mode="json", exclude_none=False) if hasattr(got1, "model_dump") else got1,
        subdir="uploader_metadata",
    )

    # --- UPDATE ---
    upd = safe_call(
        "uploader_metadata.update",
        lambda: client.uploader_metadata.update(file_id, {"uploader_metadata": {"sensor": "A2"}}),
    )
    assert upd is not None, "update returned None"
    write_artifact(
        "uploader_metadata_update_response",
        upd.model_dump(mode="json", exclude_none=False) if hasattr(upd, "model_dump") else upd,
        subdir="uploader_metadata",
    )

    # --- GET again, verify ---
    got2 = safe_call("uploader_metadata.get(after)", lambda: client.uploader_metadata.get(file_id))
    assert got2 is not None, "get(after) returned None"
    write_artifact(
        "uploader_metadata_get_after",
        got2.model_dump(mode="json", exclude_none=False) if hasattr(got2, "model_dump") else got2,
        subdir="uploader_metadata",
    )

    meta = getattr(got2, "uploader_metadata", None)
    assert isinstance(meta, (dict, type(None))), f"unexpected uploader_metadata type: {type(meta)}"
    if isinstance(meta, dict):
        assert meta.get("sensor") == "A2", f"expected sensor=A2, got {meta}"
