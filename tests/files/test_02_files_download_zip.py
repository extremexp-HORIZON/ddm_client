from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_str, safe_call, write_artifact, OUT_DIR


def _split_csv_env(name: str) -> list[str]:
    v = getenv_str(name)
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


@pytest.fixture(scope="session")
def download_file_ids() -> list[str]:
    ids = _split_csv_env("DDM_TEST_FILE_IDS")
    if not ids:
        pytest.skip("Missing DDM_TEST_FILES_IDS (comma-separated file ids)")
    return ids


def test_02_files_download_zip(client, download_file_ids: list[str]):

    blob = safe_call("files.download_zip", lambda: client.files.download_zip(download_file_ids))
    assert blob is not None, "files.download_zip returned None"
    assert isinstance(blob, (bytes, bytearray)), f"Expected bytes; got {type(blob)}"
    assert len(blob) > 0, "download_zip returned empty bytes"

    out_dir = OUT_DIR / "files" / "download"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "files_download.zip"
    Path(out_path).write_bytes(bytes(blob))

    write_artifact(
        "files_download_zip_result",
        {"file_ids": download_file_ids, "bytes": len(blob), "path": str(out_path)},
        subdir="files",
    )
