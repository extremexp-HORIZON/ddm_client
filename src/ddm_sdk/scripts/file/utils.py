from __future__ import annotations

import re
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from ddm_sdk.client import DdmClient

# UUID v4-ish (good enough for CLI validation)
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)

def file_record_key(project_id: str, file_id: str) -> str:
    # canonical JSON record location (becomes .../file.json on disk)
    return f"{file_dir_key(project_id, file_id)}/file"


def project_latest_key(project_id: str) -> str:
    project_id = norm_project(project_id)
    return f"projects/{project_id}/latest_file"


def norm_project(project_id: str) -> str:
    """
    Normalize project id into a storage-safe path segment.
    Example: " projectA/sub1/ " -> "projectA/sub1"
    """
    p = (project_id or "").strip().strip("/").strip()
    if not p:
        raise ValueError("project_id is required")
    # prevent traversal
    if ".." in p.replace("\\", "/").split("/"):
        raise ValueError(f"Invalid project_id: {project_id}")
    return p.replace("\\", "/")


def require_file_id(file_id: str) -> str:
    """
    Validate file_id is a UUID-like string (your backend uses UUIDs).
    """
    v = (file_id or "").strip()
    if not v:
        raise ValueError("file_id is required")
    if v != "..." and not _UUID_RE.match(v):
        # allow "..." for docs/examples but fail for real usage
        raise ValueError(f"Invalid file_id (expected UUID): {file_id}")
    return v


def file_dir_key(project_id: str, file_id: str) -> str:
    """
    Base storage key for a file.
    Produces: projects/<project_id>/files/<file_id>
    """
    project_id = norm_project(project_id)
    file_id = require_file_id(file_id)
    return f"projects/{project_id}/files/{file_id}"


def _to_jsonable(payload: Any) -> Any:
    """
    Convert pydantic/dataclass/etc to jsonable dict.
    """
    if payload is None:
        return None
    if hasattr(payload, "model_dump"):
        return payload.model_dump(mode="json", exclude_none=False)
    if is_dataclass(payload):
        return asdict(payload)
    if isinstance(payload, Path):
        return str(payload)
    return payload


def store_file_json(client: Any, project_id: str, file_id: str, payload: Any) -> Optional[str]:
    """
    Store the file "record" under:
      projects/<project_id>/files/<file_id>/file.json
    """
    if not getattr(client, "storage", None):
        return None
    key = f"{file_dir_key(project_id, file_id)}/file"
    return client.storage.write_json(key, _to_jsonable(payload))


def append_log(
    client: Any,
    project_id: str,
    file_id: str,
    *,
    action: str,
    ok: bool,
    details: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Append a log entry to:
      projects/<project_id>/files/<file_id>/logs.json

    We read-modify-write because Storage is minimal (no append op).
    """
    if not getattr(client, "storage", None):
        return None

    key = f"{file_dir_key(project_id, file_id)}/logs"
    existing = client.storage.read_json(key)
    if not isinstance(existing, list):
        existing = []

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "action": action,
        "ok": bool(ok),
        "details": details or {},
    }
    existing.append(entry)
    return client.storage.write_json(key, existing)

def resolve_file_id(
    *,
    client: DdmClient,
    project_id: str,
    file_id: Optional[str] = None,
) -> str:
    """
    Resolve file_id from explicit arg or from storage: projects/<project>/files/_latest.json
    """
    if file_id and file_id.strip():
        return file_id.strip()

    if not client.storage:
        raise SystemExit("No --file_id provided and client.storage is not configured (DDM_STORAGE_DIR).")

    d = client.storage.read_json(project_latest_key(project_id)) or {}
    fid = d.get("file_id")
    if isinstance(fid, str) and fid.strip():
        return fid.strip()

    raise SystemExit(
        "No --file_id provided and no stored latest file found. "
        "Run upload_file first (with storage enabled) or pass --file_id."
    )



def persist_file_record(
    *,
    client: DdmClient,
    project_id: str,
    file_id: str,
    payload: Any,
) -> None:
    """
    Writes canonical record + updates per-project latest pointer.
    Canonical record: projects/<project>/files/<file_id>/file.json
    Latest pointer:  projects/<project>/latest_file.json
    """
    if not client.storage:
        return

    project_id = norm_project(project_id)

    # If payload is a pydantic model, storage will handle model_dump
    client.storage.write_json(file_record_key(project_id, file_id), payload)

    client.storage.write_json(
        project_latest_key(project_id),
        {
            "project_id": project_id,
            "file_id": file_id,
            "record_key": file_record_key(project_id, file_id),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    # ----------------------------
# Helpers
# ----------------------------

def _dump(obj: Any) -> Any:
    return obj.model_dump(mode="json", exclude_none=False) if hasattr(obj, "model_dump") else obj


def _safe_read_json(storage: Any, key: str) -> Any:
    try:
        return storage.read_json(key)
    except Exception:
        return None


def _ext_from_file_type(ft: str | None) -> str | None:
    if not ft:
        return None
    ft = ft.strip().lower()

    mapping = {
        "csv": ".csv",
        "parquet": ".parquet",
        "json": ".json",
        "ndjson": ".ndjson",
        "jsonl": ".jsonl",
        "html": ".html",
        "pdf": ".pdf",
        "txt": ".txt",
        "zip": ".zip",
        "xlsx": ".xlsx",
        "xls": ".xls",
        "png": ".png",
        "jpg": ".jpg",
        "jpeg": ".jpeg",
    }
    return mapping.get(ft)


def _fetch_file_meta_from_catalog(
    client: DdmClient,
    *,
    file_id: str,
    project_hint: Optional[str] = None,
    max_pages: int = 10,
    per_page: int = 200,
) -> Optional[Dict[str, Any]]:
    """
    Best-effort: find file metadata via /ddm/catalog/list.
    1) Try with project filter (fast).
    2) Fallback: page without project filter (slower).
    """
    # 1) project-filtered attempt
    if project_hint:
        try:
            resp = client.catalog.list(project_id=[project_hint], page=1, perPage=per_page)
            data = _dump(resp)
            items = data.get("data") if isinstance(data, dict) else None
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict) and it.get("id") == file_id:
                        return it
        except Exception:
            pass

    # 2) broad paging attempt
    for page in range(1, max_pages + 1):
        try:
            resp = client.catalog.list(page=page, perPage=per_page)
            data = _dump(resp)
            items = data.get("data") if isinstance(data, dict) else None
            if not isinstance(items, list):
                break

            for it in items:
                if isinstance(it, dict) and it.get("id") == file_id:
                    return it

            if len(items) < per_page:
                break
        except Exception:
            break

    return None


def _pick_filename_and_ext(
    stored: object,
    api_meta: Optional[Dict[str, Any]],
) -> Tuple[str, str | None]:
    """
    Prefer storage record; fallback to API meta.
    returns (filename, inferred_ext)
    """

    # --- 1) storage record ---
    if isinstance(stored, dict):
        f = stored.get("file") if isinstance(stored.get("file"), dict) else stored
        if isinstance(f, dict):
            filename = (f.get("filename") or f.get("upload_filename") or f.get("user_filename") or "").strip()
            if filename:
                suf = Path(filename).suffix
                if suf:
                    return filename, suf
                ext = _ext_from_file_type(f.get("file_type"))
                return filename, ext

    # --- 2) API meta (catalog) ---
    if isinstance(api_meta, dict):
        filename = (api_meta.get("filename") or api_meta.get("upload_filename") or "").strip()
        if filename:
            suf = Path(filename).suffix
            if suf:
                return filename, suf
        ext = _ext_from_file_type(api_meta.get("file_type"))
        if filename:
            return filename, ext
        if ext:
            return "download", ext

    return ("", None)


