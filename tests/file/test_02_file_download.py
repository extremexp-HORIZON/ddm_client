from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR

@pytest.fixture(scope="session")
def file_id_for_download() -> str:
    fid = getenv_str("DDM_TEST_FILE_ID")
    if not fid:
        pytest.skip("Missing DDM_TEST_FILE_ID (set this to a previously uploaded file.id)")
    return fid

def test_02_file_download(client, file_id_for_download: str):
    if not getenv_bool("DDM_TEST_FILE_DOWNLOAD", True):
        pytest.skip("DDM_TEST_FILE_DOWNLOAD not enabled")

    blob = safe_call("file.download", lambda: client.file.download(file_id_for_download))
    assert blob is not None, "file.download returned None"
    assert isinstance(blob, (bytes, bytearray)), f"Expected bytes; got {type(blob)}"
    assert len(blob) > 0, "Downloaded empty payload"

    out_dir = OUT_DIR / "file" / "download"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"downloaded_{file_id_for_download}.bin"
    Path(out_path).write_bytes(bytes(blob))

    write_artifact(
        "file_download_result",
        {"file_id": file_id_for_download, "bytes": len(blob), "path": str(out_path)},
        subdir="file",
    )
