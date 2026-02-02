from __future__ import annotations

import json
import pytest

from ddm_sdk.models.uploader_metadata import UploaderMetadataJSON
from helpers import safe_call, write_artifact, OUT_DIR


def _read_file_id() -> str:
    p = OUT_DIR / "uploader_metadata" / "uploader_metadata_file_id.json"
    if not p.exists():
        pytest.skip("Missing uploader_metadata_file_id.json (run test_01_pick_file_id first)")
    d = json.loads(p.read_text(encoding="utf-8"))
    return d["file_id"]


def test_02_attach_uploader_metadata(client):
    file_id = _read_file_id()

    body = UploaderMetadataJSON(uploader_metadata={"sensor": "A1"})

    resp = safe_call(
        "uploader_metadata.attach",
        lambda: client.uploader_metadata.attach(file_id, body),
    )
    assert resp is not None, "attach returned None"

    write_artifact(
        "uploader_metadata_attach_response",
        resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp,
        subdir="uploader_metadata",
    )
