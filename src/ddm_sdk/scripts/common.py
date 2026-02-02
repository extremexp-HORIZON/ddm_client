from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ddm_sdk.client import DdmClient


def getenv_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def getenv_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def split_csv(v: Optional[str]) -> list[str]:
    if not v:
        return []
    return [x.strip() for x in v.split(",") if x.strip()]


@dataclass
class ScriptContext:
    client: DdmClient
    out_dir: Path


def get_context() -> ScriptContext:
    client = DdmClient.from_env()

    # default out dir if storage not configured
    out_dir = Path(getenv_str("DDM_SCRIPT_OUT_DIR", "out/runtime") or "out/runtime")
    out_dir.mkdir(parents=True, exist_ok=True)

    # optional login
    username = getenv_str("DDM_USERNAME")
    password = getenv_str("DDM_PASSWORD")
    if username and password:
        client.login(username, password)

    return ScriptContext(client=client, out_dir=out_dir)


def write_json(ctx: ScriptContext, key: str, payload: Any) -> str:
    """
    Prefer client.storage if configured; fallback to ctx.out_dir.
    key example: "file/upload_single_response"
    """
    if hasattr(payload, "model_dump"):
        payload = payload.model_dump(mode="json", exclude_none=False)

    if ctx.client.storage:
        # storage keys should be clean and consistent
        return ctx.client.storage.write_json(key, payload)

    # filesystem fallback
    p = ctx.out_dir / f"{key}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(p)


def read_json(ctx: ScriptContext, key: str) -> Optional[Any]:
    if ctx.client.storage:
        return ctx.client.storage.read_json(key)

    p = ctx.out_dir / f"{key}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
