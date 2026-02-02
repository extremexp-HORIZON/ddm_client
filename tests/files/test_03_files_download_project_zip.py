from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_bool, getenv_str, safe_call, write_artifact, OUT_DIR


@pytest.fixture(scope="session")
def project_id_for_zip() -> str:
    pid = getenv_str("DDM_TEST_PROJECT_ID")
    if not pid:
        pytest.skip("Missing DDM_TEST_PROJECT_ID")
    return pid


def test_03_files_download_project_zip(client, project_id_for_zip: str):
    if not getenv_bool("DDM_TEST_FILES_DOWNLOAD_PROJECT_ZIP", True):
        pytest.skip("DDM_TEST_FILES_DOWNLOAD_PROJECT_ZIP not enabled")

    blob = safe_call("files.download_project_zip", lambda: client.files.download_project_zip(project_id_for_zip))
    assert blob is not None, "files.download_project_zip returned None"
    assert isinstance(blob, (bytes, bytearray)), f"Expected bytes; got {type(blob)}"
    assert len(blob) > 0, "download_project_zip returned empty bytes"

    out_dir = OUT_DIR / "files" / "download"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"project_{project_id_for_zip.replace('/', '_')}.zip"
    Path(out_path).write_bytes(bytes(blob))

    write_artifact(
        "files_download_project_zip_result",
        {"project_id": project_id_for_zip, "bytes": len(blob), "path": str(out_path)},
        subdir="files",
    )
