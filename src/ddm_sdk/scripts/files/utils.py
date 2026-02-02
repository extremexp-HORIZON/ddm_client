from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from ddm_sdk.client import DdmClient


def norm_project(project_id: str) -> str:
    return project_id.strip().strip("/")


def file_dir_key(project_id: str, file_id: str) -> str:
    # projectA/sub1 -> projects/projectA/sub1/files/<file_id>
    project_id = norm_project(project_id)
    return f"projects/{project_id}/files/{file_id}"


def file_record_key(project_id: str, file_id: str) -> str:
    # .../file.json
    return f"{file_dir_key(project_id, file_id)}/file"


def append_project_log(client: DdmClient, project_id: str, *, action: str, ok: bool, details: Any) -> None:
    """
    Project-wide log for bulk operations.
    Writes JSONL to: projects/<project>/files/_logs.jsonl
    (We implement it as a json list in storage for simplicity if you only have JSON storage.)
    """
    if not client.storage:
        return

    key = f"projects/{norm_project(project_id)}/files/_logs"
    existing = client.storage.read_json(key)
    if not isinstance(existing, list):
        existing = []

    existing.append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "ok": ok,
        "details": details,
    })
    client.storage.write_json(key, existing)


def append_file_log(client: DdmClient, project_id: str, file_id: str, *, action: str, ok: bool, details: Any) -> None:
    if not client.storage:
        return

    key = f"{file_dir_key(project_id, file_id)}/log"
    existing = client.storage.read_json(key)
    if not isinstance(existing, list):
        existing = []

    existing.append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "ok": ok,
        "details": details,
    })
    client.storage.write_json(key, existing)


def persist_file_record(*, client: DdmClient, project_id: str, file_id: str, payload: Any) -> None:
    """
    Canonical record:
      projects/<project>/files/<file_id>/file.json
    """
    if not client.storage:
        return
    client.storage.write_json(file_record_key(project_id, file_id), payload)


def _get_file_id(obj: Any) -> Optional[str]:
    # supports dict or pydantic model-ish
    if obj is None:
        return None
    if hasattr(obj, "file_id"):
        v = getattr(obj, "file_id")
        return v if isinstance(v, str) else None
    if isinstance(obj, dict):
        v = obj.get("file_id") or obj.get("id")
        return v if isinstance(v, str) else None
    return None


def ts_utc() -> str:
    """Filesystem-safe UTC timestamp like 20260125_184233"""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")