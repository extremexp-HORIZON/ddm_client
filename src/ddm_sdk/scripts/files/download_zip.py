from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.files.utils import norm_project, append_project_log, ts_utc


def _zip_key_for_selection(project_id: str, file_ids: list[str]) -> str:
    """
    projects/<project>/zips/selection/<hash>/<timestamp>
    """
    project_id = norm_project(project_id)
    h = hashlib.sha1(("|".join(file_ids)).encode("utf-8")).hexdigest()[:12]
    return f"projects/{project_id}/zips/selection/{h}/{ts_utc()}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-download-zip", description="Download ZIP for selected file ids")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file-id", action="append", required=True, dest="file_ids",
                    help="Repeatable: --file-id <uuid>")
    ap.add_argument("--out", default=None, help="Optional output zip path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_ids = [x.strip() for x in (args.file_ids or []) if x and x.strip()]
    if not file_ids:
        raise SystemExit("No --file-id provided")

    client = DdmClient.from_env()
    ensure_authenticated(client)

    blob = client.files.download_zip(file_ids)

    saved_to: Optional[str] = None

    # 1) explicit --out 
    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    # 2) storage path if enabled and supports bytes
    elif client.storage and (not args.no_store) and hasattr(client.storage, "write_bytes"):
        key = _zip_key_for_selection(project_id, file_ids)  # includes timestamp
        saved_to = client.storage.write_bytes(key, blob, ext=".zip")

        # store a json receipt next to it (same key -> .json)
        client.storage.write_json(
            key,
            {
                "kind": "files_zip",
                "project_id": project_id,
                "file_ids": file_ids,
                "bytes": len(blob),
                "saved_to": saved_to,
                "created_at": ts_utc(),
            },
        )

        append_project_log(
            client,
            project_id,
            action="download_zip",
            ok=True,
            details={"saved_to": saved_to, "file_ids": file_ids, "bytes": len(blob)},
        )

    # 3) fallback local path
    else:
        h = hashlib.sha1(("|".join(file_ids)).encode("utf-8")).hexdigest()[:12]
        out_path = Path(f"files_{project_id.replace('/', '_')}_{h}_{ts_utc()}.zip").resolve()
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    print(json.dumps({"ok": True, "project_id": project_id, "file_ids": file_ids, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
