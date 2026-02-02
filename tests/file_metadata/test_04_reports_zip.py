from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_bool, safe_call, write_artifact, OUT_DIR


@pytest.fixture(scope="session")
def zip_out_dir() -> Path:
    p = OUT_DIR / "file_metadata" / "downloads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def test_04_file_metadata_download_reports_zip(client, file_ids: list[str], zip_out_dir: Path):

    blob = safe_call("file_metadata.download_reports_zip", lambda: client.file_metadata.download_reports_zip(file_ids))
    assert blob is not None, "download_reports_zip returned None"
    assert isinstance(blob, (bytes, bytearray)), f"Expected bytes; got {type(blob)}"
    assert len(blob) > 0, "Empty ZIP bytes"

    out_path = zip_out_dir / "reports.zip"
    Path(out_path).write_bytes(bytes(blob))

    write_artifact(
        "file_metadata_reports_zip_result",
        {"file_ids": file_ids, "bytes": len(blob), "path": str(out_path)},
        subdir="file_metadata",
    )
