from __future__ import annotations

import argparse
import json
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.files.utils import (
    norm_project,
    persist_file_record,
    append_file_log,
    append_project_log,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-upload-files", description="Upload multiple local files to DDM (multipart)")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("paths", nargs="+", help="One or more local file paths")
    ap.add_argument("--name", action="append", default=[], help="Optional per-file name override (repeat)")
    ap.add_argument("--description", action="append", default=[], help="Optional per-file description (repeat)")
    ap.add_argument("--use-case", action="append", default=[], help="Global use-case (repeatable)")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    files: list[str] = []
    for p in args.paths:
        fp = Path(p).expanduser().resolve()
        if not fp.exists() or not fp.is_file():
            raise SystemExit(f"File not found: {fp}")
        files.append(str(fp))

    user_filenames = args.name or None
    descriptions = args.description or None
    use_cases = [x.strip() for x in (args.use_case or []) if x and x.strip()]
    use_case_param = use_cases if use_cases else None

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.files.upload(
        project_id=project_id,
        files=files,
        user_filenames=user_filenames,
        descriptions=descriptions,
        use_case=use_case_param,
        metadata_files=None,
    )

    # Persist each returned file record under projects/<project>/files/<file_id>/file.json
    stored_ids: list[str] = []
    if client.storage and not args.no_store:
        for f in resp.files:
            # backend returns dicts
            fid = f.get("id") if isinstance(f, dict) else None
            if isinstance(fid, str) and fid.strip():
                stored_ids.append(fid)
                # Store a normalized record similar to single-file: {"message":..., "file": {...}}
                persist_file_record(client=client, project_id=project_id, file_id=fid, payload={"message": resp.message, "file": f})
                append_file_log(client, project_id, fid, action="upload_files", ok=True, details={"path": f.get("path")})

        append_project_log(client, project_id, action="upload_files", ok=True, details={"count": len(resp.files), "file_ids": stored_ids})

    print(json.dumps(resp.model_dump(mode="json", exclude_none=False), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
