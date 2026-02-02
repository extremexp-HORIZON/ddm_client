from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.files.utils import norm_project
from ddm_sdk.scripts.file_metadata.utils import reports_zip_key


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-reports-zip", description="Download ZIP of profile reports for selected file ids")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file-id", action="append", required=True, dest="file_ids")
    ap.add_argument("--out", default=None, help="Optional output zip path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_ids = [x.strip() for x in (args.file_ids or []) if x and x.strip()]
    if not file_ids:
        raise SystemExit("No --file-id provided")

    client = DdmClient.from_env()
    ensure_authenticated(client)

    blob = client.file_metadata.download_reports_zip(file_ids)

    saved_to: Optional[str] = None

    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    elif client.storage and (not args.no_store) and hasattr(client.storage, "write_bytes"):
        key = reports_zip_key(project_id)
        saved_to = client.storage.write_bytes(key, blob, ext=".zip")
        client.storage.write_json(
            key,
            {"kind": "file_metadata_reports_zip", "project_id": project_id, "file_ids": file_ids, "bytes": len(blob), "saved_to": saved_to},
        )

    else:
        out_path = Path(f"{project_id.replace('/', '_')}_reports.zip").resolve()
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    print(json.dumps({"ok": True, "project_id": project_id, "file_ids": file_ids, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
