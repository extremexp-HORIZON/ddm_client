from __future__ import annotations

import argparse
import json
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project, persist_file_record, append_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-upload-async", description="Chunk-upload a large file to DDM")
    ap.add_argument("path", help="Local file path")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--chunk-size", type=int, default=2 * 1024 * 1024, help="Bytes (default 2MB)")
    ap.add_argument("--filename", default=None, help="Override filename sent to server")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    file_path = Path(args.path).expanduser().resolve()
    if not file_path.exists() or not file_path.is_file():
        raise SystemExit(f"File not found: {file_path}")

    project_id = norm_project(args.project_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.file.upload_async(
        project_id=project_id,
        file=str(file_path),
        filename=args.filename,
        chunk_size=args.chunk_size,
    )

    file_id = getattr(resp, "file_id", None)
    if client.storage and not args.no_store and isinstance(file_id, str) and file_id.strip():
        persist_file_record(client=client, project_id=project_id, file_id=file_id, payload=resp)
        append_log(client, project_id, file_id, action="upload_async", ok=True, details={
            "chunk_size": args.chunk_size,
            "merge_task_id": getattr(resp, "merge_task_id", None),
            "metadata_task_id": getattr(resp, "metadata_task_id", None),
            "path": str(file_path),
        })

    print(json.dumps(resp.model_dump(mode="json", exclude_none=False), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
