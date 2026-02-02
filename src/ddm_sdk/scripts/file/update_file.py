from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.models.file import FileUpdateBody
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project, resolve_file_id, persist_file_record


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-update-file", description="Update a file in DDM")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file_id", default=None, help="If omitted, uses storage _latest for project.")
    ap.add_argument("--description", default=None)
    ap.add_argument("--use-case", action="append", default=[], help="Repeatable. e.g. --use-case ml")
    ap.add_argument("--filename", default=None, help="Rename (optional)")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    file_id = resolve_file_id(client=client, project_id=project_id, file_id=args.file_id)

    use_case = [x.strip() for x in (args.use_case or []) if x and x.strip()]
    body = FileUpdateBody(
        description=args.description,
        use_case=use_case or None,
        filename=args.filename,
    )

    resp = client.file.update(file_id, body)

    if client.storage and not args.no_store:
        persist_file_record(client=client, project_id=project_id, file_id=file_id, payload=resp)

    print(json.dumps(resp.model_dump(mode="json", exclude_none=False), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
