from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import require_file_id
from ddm_sdk.scripts.files.utils import norm_project
from ddm_sdk.scripts.file_metadata.utils import report_html_key, append_file_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-report-html", description="Download file profile report (HTML)")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", required=True)
    ap.add_argument("--out", default=None, help="Optional output html path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    html = client.file_metadata.get_report_html(file_id)

    saved_to: Optional[str] = None

    if args.out:
        p = Path(args.out).expanduser().resolve()
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(html, encoding="utf-8")
        saved_to = str(p)

    elif client.storage and (not args.no_store) and hasattr(client.storage, "write_bytes"):
        key = report_html_key(project_id, file_id)
        saved_to = client.storage.write_bytes(key, html.encode("utf-8"), ext=".html")
        append_file_log(client, project_id, file_id, action="report_html", ok=True, details={"saved_to": saved_to, "bytes": len(html)})

    else:
        p = Path(f"{file_id}_profile_report.html").resolve()
        p.write_text(html, encoding="utf-8")
        saved_to = str(p)

    print(json.dumps({"ok": True, "project_id": project_id, "file_id": file_id, "saved_to": saved_to, "bytes": len(html)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
