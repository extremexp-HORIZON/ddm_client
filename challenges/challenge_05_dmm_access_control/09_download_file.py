from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional, Any, Dict, Tuple

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import (
    norm_project,
    require_file_id,
    append_log,
    file_dir_key,
    file_record_key,
    _safe_read_json,
    _fetch_file_meta_from_catalog,
    _pick_filename_and_ext)

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="challenge05-download-file",
        description="Challenge 09: Download a file from DDM (uses storage record if present; falls back to catalog API)",
    )
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", required=True)
    ap.add_argument("--out", default=None, help="Optional output path. If omitted, uses storage tree when enabled.")
    ap.add_argument("--ext", default=None, help="Force extension (e.g. .csv). Overrides inferred extension.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    # Download bytes
    blob = client.file.download(file_id)

    saved_to: Optional[str] = None

    # ------------------------------------------------------------
    # CASE 1: user explicitly provided --out
    # ------------------------------------------------------------
    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

        print(json.dumps({"ok": True, "file_id": file_id, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
        return 0

    # ------------------------------------------------------------
    # CASE 2: use storage tree when available
    # ------------------------------------------------------------
    if client.storage and hasattr(client.storage, "write_bytes") and (not args.no_store):
        base_key = file_dir_key(project_id, file_id)

        # Try storage record first
        stored_file = _safe_read_json(client.storage, file_record_key(project_id, file_id))

        # Always try API meta as a fallback (storage often missing)
        api_meta = _fetch_file_meta_from_catalog(client, file_id=file_id, project_hint=project_id)

        filename, inferred_ext = _pick_filename_and_ext(stored_file, api_meta)
        if not filename:
            filename = file_id  # final fallback

        # ext precedence: --ext > inferred_ext > suffix(filename) > .bin
        ext = args.ext
        if not ext:
            ext = inferred_ext or Path(filename).suffix
        if not ext:
            ext = ".bin"
        if not ext.startswith("."):
            ext = f".{ext}"

        stem = Path(filename).stem or file_id

        # final path: projects/<project>/files/<file_id>/<stem><ext>
        saved_to = client.storage.write_bytes(f"{base_key}/{stem}", blob, ext=ext)

        # update file record with last_download
        existing = _safe_read_json(client.storage, f"{base_key}/file")
        if not isinstance(existing, dict):
            existing = {}

        existing["last_download"] = {"path": saved_to, "bytes": len(blob)}
        client.storage.write_json(f"{base_key}/file", existing)

        append_log(
            client,
            project_id,
            file_id,
            action="download",
            ok=True,
            details={"path": saved_to, "bytes": len(blob), "ext": ext, "filename": filename},
        )

        print(json.dumps({"ok": True, "file_id": file_id, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
        return 0

    # ------------------------------------------------------------
    # CASE 3: storage disabled -> local fallback
    # ------------------------------------------------------------
    out_path = Path(f"{file_id}.bin").resolve()
    out_path.write_bytes(blob)
    saved_to = str(out_path)

    print(json.dumps({"ok": True, "file_id": file_id, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
