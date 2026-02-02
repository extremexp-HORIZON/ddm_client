from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.files.utils import norm_project, ts_utc


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-get-many-metadata", description="Get metadata for many file ids")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file-id", action="append", required=True, dest="file_ids")
    ap.add_argument("--out", default=None, help="Optional output json path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_ids = [x.strip() for x in (args.file_ids or []) if x and x.strip()]
    if not file_ids:
        raise SystemExit("No --file-id provided")

    client = DdmClient.from_env()
    ensure_authenticated(client)

    m = client.file_metadata.get_many(file_ids)  # dict[file_id] -> dict

    saved_to: Optional[str] = None

    if args.out:
        from pathlib import Path
        p = Path(args.out).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(m, indent=2, ensure_ascii=False), encoding="utf-8")
        saved_to = str(p)

    elif client.storage and (not args.no_store):
        key = f"projects/{project_id}/metadata/many/{ts_utc()}"
        saved_to = client.storage.write_json(key, {"project_id": project_id, "file_ids": file_ids, "metadata": m})

    print(json.dumps({"ok": True, "project_id": project_id, "file_ids": file_ids, "saved_to": saved_to, "metadata": m}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
