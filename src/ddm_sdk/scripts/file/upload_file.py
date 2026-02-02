from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import persist_file_record

def _norm_project(project_id: str) -> str:
    return project_id.strip().strip("/")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-upload-file", description="Upload a file to DDM")
    ap.add_argument("path", help="Local file path to upload")
    ap.add_argument("--project_id", required=True, help="Project id (e.g. projectA/sub1)")
    ap.add_argument("--name", default=None, help="User-visible filename (defaults to local filename)")
    ap.add_argument("--description", default="", help="Description")
    ap.add_argument("--use-case", action="append", default=[], help="Repeatable. e.g. --use-case ml")
    ap.add_argument("--metadata", default=None, help="Path to metadata JSON file (optional)")
    ap.add_argument("--no-store", action="store_true", help="Do not write to storage")

    args = ap.parse_args(argv)

    file_path = Path(args.path).expanduser().resolve()
    if not file_path.exists() or not file_path.is_file():
        raise SystemExit(f"File not found: {file_path}")

    meta_path: Optional[str] = None
    if args.metadata:
        mp = Path(args.metadata).expanduser().resolve()
        if not mp.exists() or not mp.is_file():
            raise SystemExit(f"Metadata file not found: {mp}")
        meta_path = str(mp)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    project_id = _norm_project(args.project_id)
    user_filename = args.name or file_path.name
    use_case = [x.strip() for x in (args.use_case or []) if x and x.strip()]

    resp = client.file.upload(
        project_id=project_id,
        file=str(file_path),
        user_filename=user_filename,
        description=args.description,
        use_case=use_case,
        metadata_file=meta_path,
    )

    f = getattr(resp, "file", None)
    if not f:
        raise SystemExit(f"Unexpected upload response (missing .file). type={type(resp)}")

    file_id = getattr(f, "id", None)
    if not isinstance(file_id, str) or not file_id.strip():
        raise SystemExit("Unexpected upload response (missing file.id)")

    out = {
        "ok": True,
        "file_id": file_id,
        "metadata_task_id": getattr(f, "metadata_task_id", None),
        "project_id": project_id,
        "user_filename": user_filename,
        "filename": user_filename,
        "path": str(file_path),
        "metadata_path": meta_path,
    }

    if client.storage and not args.no_store:
        persist_file_record(client=client, project_id=project_id, file_id=file_id, payload=resp)


        print(json.dumps(out, indent=2, ensure_ascii=False))
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
