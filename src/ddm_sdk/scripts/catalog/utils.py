from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional
from pathlib import Path
from typing import Dict

from ddm_sdk.client import DdmClient


def norm_project(project_id: str) -> str:
    return project_id.strip().strip("/")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def project_catalog_root(project_id: str) -> str:
    project_id = norm_project(project_id)
    return f"projects/{project_id}/catalog"


def append_project_log(
    client: DdmClient,
    project_id: str,
    *,
    action: str,
    ok: bool,
    details: Any | None = None,
) -> None:
    """
    Appends a line-delimited JSON log in storage (if enabled).
    Stored at: projects/<project>/catalog/logs.ndjson
    """
    if not client.storage:
        return

    key = f"{project_catalog_root(project_id)}/logs"
    existing = client.storage.read_json(key)
    if isinstance(existing, list):
        logs = existing
    else:
        logs = []

    logs.append(
        {
            "ts": _utc_now_iso(),
            "action": action,
            "ok": ok,
            "details": details,
        }
    )
    client.storage.write_json(key, logs)


def store_result(
    client: DdmClient,
    project_id: str,
    *,
    name: str,
    payload: Any,
    no_store: bool,
) -> Optional[str]:
    """
    Stores payload as JSON under:
      projects/<project>/catalog/<name>/<timestamp>.json

    Returns saved path (string) if stored, else None.
    """
    if no_store or not client.storage:
        return None

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{project_catalog_root(project_id)}/{name}/{ts}"
    return client.storage.write_json(key, payload)

def load_filters(json_text: Optional[str], json_file: Optional[str]) -> Dict[str, Any]:
    if json_text and json_file:
        raise SystemExit("Use only one of --json or --json-file")

    if json_file:
        p = Path(json_file).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"JSON file not found: {p}")
        return json.loads(p.read_text(encoding="utf-8-sig"))

    if json_text:
        try:
            obj = json.loads(json_text)
        except Exception as e:
            raise SystemExit(f"--json is not valid JSON: {e}")
        if not isinstance(obj, dict):
            raise SystemExit("--json must be a JSON object")
        return obj

    raise SystemExit("Provide either --json or --json-file")

def csv_list(v: Optional[str]) -> Optional[list[str]]:
    if not v:
        return None
    parts = [x.strip() for x in v.split(",") if x and x.strip()]
    return parts or None
