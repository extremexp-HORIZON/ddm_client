from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project, require_file_id, append_log, file_dir_key


def _pick_filename_from_stored_file_json(stored: object) -> Optional[str]:
    if not isinstance(stored, dict):
        return None

    f = stored.get("file") if isinstance(stored.get("file"), dict) else stored
    if not isinstance(f, dict):
        return None

    # 1) DDM backend filename includes extension
    v = f.get("filename")
    if isinstance(v, str) and v.strip():
        return v.strip()

    # 2) user-visible names (may not have extension)
    for k in ("user_filename", "upload_filename"):
        v = f.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # 3) fallback: infer from path if it contains a filename.ext
    z = f.get("zenoh_file_path") or f.get("file_path") or f.get("path")
    if isinstance(z, str) and z.strip():
        name = Path(z).name
        if name:
            return name

    return None



def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-download-file", description="Download a file from DDM")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", required=True)  # no latest
    ap.add_argument("--out", default=None, help="Optional output path. If omitted, uses storage tree when enabled.")
    ap.add_argument("--ext", default=None, help="Force extension (e.g. .csv). If omitted, tries filename suffix; else .bin.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    blob = client.file.download(file_id)

    saved_to: Optional[str] = None

    # If user explicitly provided --out, write exactly there
    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    # Else: prefer storage tree (real file, not blob name) if available
    elif client.storage and hasattr(client.storage, "write_bytes"):

        base_key = file_dir_key(project_id, file_id)

        stored_file = client.storage.read_json(f"{base_key}/file")  # file.json (without .json suffix in key)
        filename = _pick_filename_from_stored_file_json(stored_file) or file_id

        # ext precedence: --ext > suffix from filename > .bin
        ext = args.ext
        if not ext:
            ext = Path(filename).suffix or ".bin"
        if not ext.startswith("."):
            ext = f".{ext}"

        # keep filename without suffix because write_bytes appends ext
        stem = Path(filename).stem or file_id

        #final path: projects/<project>/files/<file_id>/<stem><ext>
        saved_to = client.storage.write_bytes(f"{base_key}/{stem}", blob, ext=ext)

        existing = client.storage.read_json(f"{base_key}/file")
        if not isinstance(existing, dict):
            existing = {}
        existing["last_download"] = {"path": saved_to, "bytes": len(blob)}
        client.storage.write_json(f"{base_key}/file", existing)

        append_log(client, project_id, file_id, action="download", ok=True, details={"path": saved_to, "bytes": len(blob)})

    # Else: storage disabled -> local fallback
    else:
        # fallback: current directory
        out_path = Path(f"{file_id}.bin").resolve()
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    print(json.dumps({"ok": True, "file_id": file_id, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
