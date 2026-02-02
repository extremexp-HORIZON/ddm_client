from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.models.files import UploadFileUrlsRequest, UploadFileUrlRequest
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.files.utils import norm_project, persist_file_record, append_file_log, append_project_log


def _parse_meta(items: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for kv in items or []:
        if "=" not in kv:
            raise SystemExit(f"Invalid --meta '{kv}'. Use k=v.")
        k, v = kv.split("=", 1)
        k, v = k.strip(), v.strip()
        if k:
            out[k] = v
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-upload-many-links", description="Upload multiple URLs to DDM")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--url", action="append", required=True, help="Repeatable URL: --url https://...")
    ap.add_argument("--filename", action="append", default=[], help="Optional per-url filename (repeat)")
    ap.add_argument("--description", default="", help="Shared description")
    ap.add_argument("--use-case", action="append", default=[], help="Shared use-case (repeat)")
    ap.add_argument("--meta", action="append", default=[], help="Shared metadata k=v (repeat)")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    use_cases = [x.strip() for x in (args.use_case or []) if x and x.strip()]
    metadata = _parse_meta(args.meta)

    files: list[UploadFileUrlRequest] = []
    for i, url in enumerate(args.url):
        fn = args.filename[i] if i < len(args.filename) else None
        files.append(UploadFileUrlRequest(
            file_url=url,
            filename=fn,
            description=args.description,
            use_cases=use_cases,
            metadata=metadata,
        ))

    body = UploadFileUrlsRequest(project_id=project_id, files=files)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.files.upload_links(body)

    stored_ids: list[str] = []
    if client.storage and not args.no_store:
        for item in resp.files:
            fid = item.file_id
            if isinstance(fid, str) and fid.strip():
                stored_ids.append(fid)
                persist_file_record(client=client, project_id=project_id, file_id=fid, payload=item)
                append_file_log(client, project_id, fid, action="upload_links_many", ok=True, details={
                    "url": item.file_url,
                    "fetch_task_id": item.fetch_task_id,
                    "process_task_id": item.process_task_id,
                })
        append_project_log(client, project_id, action="upload_many_links", ok=True, details={"count": len(resp.files), "file_ids": stored_ids})

    print(json.dumps(resp.model_dump(mode="json", exclude_none=False), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
