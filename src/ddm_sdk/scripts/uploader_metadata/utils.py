from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient


def norm_project(project_id: str) -> str:
    return project_id.strip().strip("/")


def require_file_id(file_id: str) -> str:
    # keep consistent with your existing file.utils behavior if you want UUID validation
    # If you already have ddm_sdk.scripts.file.utils.require_file_id, import & use that instead.
    v = (file_id or "").strip()
    if not v:
        raise ValueError("Missing file_id")
    return v


def file_dir_key(project_id: str, file_id: str) -> str:
    project_id = norm_project(project_id)
    return f"projects/{project_id}/files/{file_id}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_log(
    client: DdmClient,
    project_id: str,
    file_id: str,
    *,
    action: str,
    ok: bool,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Writes json-lines log entry to:
      projects/<project>/files/<file_id>/log.jsonl  (stored as bytes)
    Requires FileStorage.write_bytes (your FS storage has it).
    """
    if not client.storage or not hasattr(client.storage, "write_bytes"):
        return

    entry = {
        "ts": _utc_now(),
        "action": action,
        "ok": ok,
        "project_id": norm_project(project_id),
        "file_id": file_id,
        "details": details or {},
    }
    line = (json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8")

    # append: read existing then rewrite (fine for small logs)
    base = file_dir_key(project_id, file_id)
    log_key = f"{base}/log"
    existing = None
    try:
        if hasattr(client.storage, "read_bytes"):
            existing = client.storage.read_bytes(log_key, ext=".jsonl")  # type: ignore[attr-defined]
    except Exception:
        existing = None

    new_bytes = (existing or b"") + line
    client.storage.write_bytes(log_key, new_bytes, ext=".jsonl")  # type: ignore[attr-defined]


def store_uploader_metadata_json(
    client: DdmClient,
    project_id: str,
    file_id: str,
    payload: Any,
) -> None:
    """
    Canonical location:
      projects/<project>/files/<file_id>/uploader_metadata.json
    """
    if not client.storage:
        return
    key = f"{file_dir_key(project_id, file_id)}/uploader_metadata"
    client.storage.write_json(key, payload)


def _load_json_arg(json_text: Optional[str], json_file: Optional[str]) -> Dict[str, Any]:
    if json_text and json_text.strip():
        try:
            obj = json.loads(json_text)
        except Exception as e:
            raise SystemExit(f"--json is not valid JSON: {e}")
        if not isinstance(obj, dict):
            raise SystemExit("--json must be a JSON object (e.g. {\"sensor\":\"A1\"})")
        return obj

    if json_file:
        p = Path(json_file).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"JSON file not found: {p}")
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            raise SystemExit(f"--json-file is not valid JSON: {e}")
        if not isinstance(obj, dict):
            raise SystemExit("--json-file must contain a JSON object")
        return obj

    raise SystemExit("Provide metadata via --json '{...}' or --json-file path.json")
