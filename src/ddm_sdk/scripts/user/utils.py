from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from ddm_sdk.client import DdmClient


def norm_project(project_id: str) -> str:
    return project_id.strip().strip("/")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def user_root_key(username: str) -> str:
    u = username.strip()
    if not u:
        raise ValueError("username is empty")
    return f"users/{u}"


def store_user_result(
    client: DdmClient,
    username: str,
    *,
    name: str,
    payload: Any,
    no_store: bool,
) -> Optional[str]:
    """
    users/<username>/<name>/<timestamp>.json
    """
    if no_store or not client.storage:
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{user_root_key(username)}/{name}/{ts}"
    return client.storage.write_json(key, payload)


def append_user_log(
    client: DdmClient,
    username: str,
    *,
    action: str,
    ok: bool,
    details: Any | None = None,
) -> None:
    """
    users/<username>/logs.json  (list of dicts, append)
    """
    if not client.storage:
        return

    key = f"{user_root_key(username)}/logs"
    existing = client.storage.read_json(key)
    logs = existing if isinstance(existing, list) else []
    logs.append(
        {
            "ts": utc_now_iso(),
            "action": action,
            "ok": ok,
            "details": details,
        }
    )
    client.storage.write_json(key, logs)


def load_json_arg(*, json_text: Optional[str], json_file: Optional[str]) -> dict[str, Any]:
    """
    Prefer --json-file.
    """
    if json_file:
        p = Path(json_file).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"JSON file not found: {p}")
        return json.loads(p.read_text(encoding="utf-8-sig"))

    if json_text:
        try:
            v = json.loads(json_text)
        except Exception as e:
            raise SystemExit(f"--json is not valid JSON: {e}")
        if not isinstance(v, dict):
            raise SystemExit("--json must be a JSON object")
        return v

    raise SystemExit("Provide either --json or --json-file")
