from __future__ import annotations

import pytest
from pathlib import Path

from helpers import getenv_bool, safe_call, write_artifact, OUT_DIR


@pytest.fixture(scope="session")
def report_out_dir() -> Path:
    p = OUT_DIR / "file_metadata" / "reports"
    p.mkdir(parents=True, exist_ok=True)
    return p


def test_03_file_metadata_report_html(client, file_id: str, report_out_dir: Path):
    html = safe_call("file_metadata.get_report_html", lambda: client.file_metadata.get_report_html(file_id))
    assert html is not None, "file_metadata.get_report_html returned None"
    assert isinstance(html, str), f"Expected HTML str; got {type(html)}"
    assert len(html) > 0, "Empty HTML report"

    # Write .html file (not json)
    out_path = report_out_dir / f"report_{file_id}.html"
    out_path.write_text(html, encoding="utf-8")

    write_artifact(
        "file_metadata_report_html_result",
        {"file_id": file_id, "path": str(out_path), "chars": len(html)},
        subdir="file_metadata",
    )
