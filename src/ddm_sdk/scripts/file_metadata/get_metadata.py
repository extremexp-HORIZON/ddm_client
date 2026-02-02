from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import require_file_id
from ddm_sdk.scripts.files.utils import norm_project
from ddm_sdk.scripts.file_metadata.utils import metadata_json_key, append_file_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-get-metadata", description="Get file metadata JSON")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", required=True)
    ap.add_argument("--out", default=None, help="Optional output json path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    meta = client.file_metadata.get(file_id)  # dict

    saved_to: Optional[str] = None

    # 1) explicit out
    if args.out:
        from pathlib import Path
        p = Path(args.out).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        saved_to = str(p)

    # 2) storage
    elif client.storage and (not args.no_store):
        key = metadata_json_key(project_id, file_id)
        saved_to = client.storage.write_json(key, meta)
        append_file_log(client, project_id, file_id, action="get_metadata", ok=True, details={"saved_to": saved_to})

    print(json.dumps({"ok": True, "project_id": project_id, "file_id": file_id, "saved_to": saved_to, "metadata": meta}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
