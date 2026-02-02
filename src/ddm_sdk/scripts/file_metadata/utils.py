from __future__ import annotations

from typing import Any, Optional
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.files.utils import norm_project, ts_utc


def file_base_key(project_id: str, file_id: str) -> str:
    project_id = norm_project(project_id)
    return f"projects/{project_id}/files/{file_id}"


def metadata_json_key(project_id: str, file_id: str) -> str:
    return f"{file_base_key(project_id, file_id)}/metadata/metadata"


def report_html_key(project_id: str, file_id: str) -> str:
    # timestamped file to keep history
    return f"{file_base_key(project_id, file_id)}/metadata/reports/{ts_utc()}_profile_report"


def reports_zip_key(project_id: str) -> str:
    # timestamped zip for project-wide selection
    project_id = norm_project(project_id)
    return f"projects/{project_id}/metadata/reports_zip/{ts_utc()}_reports"


def append_file_log(
    client: DdmClient,
    project_id: str,
    file_id: str,
    *,
    action: str,
    ok: bool,
    details: Optional[dict[str, Any]] = None,
) -> None:
    if not client.storage:
        return

    key = f"{file_base_key(project_id, file_id)}/logs"
    existing = client.storage.read_json(key)
    if not isinstance(existing, list):
        existing = []

    existing.append(
        {
            "ts": ts_utc(),
            "action": action,
            "ok": ok,
            "details": details or {},
        }
    )
    client.storage.write_json(key, existing)
