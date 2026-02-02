from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parametrics_key(kind: str) -> str:
    kind = kind.strip().replace("\\", "/").strip("/")
    return f"parametrics/{kind}"


def append_parametrics_log(
    client: DdmClient,
    *,
    action: str,
    ok: bool,
    details: Dict[str, Any],
) -> None:
    """
    Writes: parametrics/log.jsonl (or json list, depending on your storage implementation)
    Here we keep it simple: store as JSON array in parametrics/log
    """
    if not client.storage:
        return

    key = parametrics_key("log")
    existing = client.storage.read_json(key)

    if not isinstance(existing, list):
        existing = []

    existing.append(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "ok": ok,
            "details": details,
        }
    )
    client.storage.write_json(key, existing)
