from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.files.utils import norm_project, append_project_log, ts_utc


def _ts() -> str:
    # filesystem safe timestamp
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-download-project-zip", description="Download ZIP for entire project")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--out", default=None, help="Optional output zip path. If omitted, uses storage if enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    blob = client.files.download_project_zip(project_id)

    saved_to: Optional[str] = None

    # 1) explicit --out wins
    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    # 2) storage path (preferred)
    elif client.storage and (not args.no_store) and hasattr(client.storage, "write_bytes"):
        ts = ts_utc()
        key = f"projects/{project_id}/zips/project/{ts}"
        saved_to = client.storage.write_bytes(key, blob, ext=".zip")

        # receipt JSON (same key, but storage will add .json)
        client.storage.write_json(
            key,
            {
                "kind": "project_zip",
                "project_id": project_id,
                "saved_to": saved_to,
                "bytes": len(blob),
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    # 3) fallback local
    else:
        ts = ts_utc()
        safe_project = project_id.replace("/", "_")
        out_path = Path(f"project_{safe_project}_{ts}.zip").resolve()
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    # log (optional)
    if client.storage and (not args.no_store):
        append_project_log(
            client,
            project_id,
            action="download_project_zip",
            ok=True,
            details={"saved_to": saved_to, "bytes": len(blob)},
        )

    print(json.dumps({"ok": True, "project_id": project_id, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
