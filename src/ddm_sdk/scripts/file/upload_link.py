from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.models.file import UploadLinkBody
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project, persist_file_record, append_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-upload-link", description="Upload a file to DDM from a URL")
    ap.add_argument("--project_id", required=True, help="Project id (e.g. projectA/sub1)")
    ap.add_argument("--url", required=True, help="File URL")
    ap.add_argument("--description", default="", help="Description")
    ap.add_argument("--use-case", action="append", default=[], help="Repeatable. e.g. --use-case etl")
    ap.add_argument("--meta", action="append", default=[], help='Repeatable k=v pairs. e.g. --meta source=external')
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    use_cases = [x.strip() for x in (args.use_case or []) if x and x.strip()]

    metadata: dict[str, str] = {}
    for kv in args.meta or []:
        if "=" not in kv:
            raise SystemExit(f"Invalid --meta '{kv}'. Use k=v.")
        k, v = kv.split("=", 1)
        k, v = k.strip(), v.strip()
        if k:
            metadata[k] = v

    client = DdmClient.from_env()
    ensure_authenticated(client)

    body = UploadLinkBody(
        file_url=args.url,
        project_id=project_id,
        description=args.description,
        use_cases=use_cases,
        metadata=metadata,
    )

    resp = client.file.upload_link(body)

    # Persist under projects/<project>/files/<file_id>/file.json (same scheme)
    file_id = getattr(resp, "file_id", None)
    if client.storage and (not args.no_store) and isinstance(file_id, str) and file_id.strip():
        persist_file_record(client=client, project_id=project_id, file_id=file_id, payload=resp)
        append_log(client, project_id, file_id, action="upload_link", ok=True, details={
            "url": args.url,
            "fetch_task_id": getattr(resp, "fetch_task_id", None),
            "process_task_id": getattr(resp, "process_task_id", None),
        })

    print(json.dumps(resp.model_dump(mode="json", exclude_none=False), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
