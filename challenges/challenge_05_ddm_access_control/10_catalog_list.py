from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.catalog.utils import norm_project, append_project_log, store_result, csv_list


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="challenge05-catalog-list", description="Catalog list (and optional store)")
    ap.add_argument("--project_id", default=None, help="Comma-separated project ids (optional)")
    ap.add_argument("--filename", default=None, help="Comma-separated filenames (optional)")
    ap.add_argument("--use_case", default=None, help="Comma-separated use_case values (optional)")
    ap.add_argument("--user_id", default=None, help="Comma-separated user ids (optional)")
    ap.add_argument("--file_type", default=None, help="Comma-separated file types (optional)")
    ap.add_argument("--parent_files", default=None, help="Comma-separated parent file ids (optional)")
    ap.add_argument("--created_from", default=None, help="ISO datetime (optional)")
    ap.add_argument("--created_to", default=None, help="ISO datetime (optional)")
    ap.add_argument("--size_from", type=int, default=None)
    ap.add_argument("--size_to", type=int, default=None)
    ap.add_argument("--sort", default="id,asc")
    ap.add_argument("--page", type=int, default=1)
    ap.add_argument("--perPage", type=int, default=10)

    # store under projects/<id>/... only if you set this
    ap.add_argument("--store_project", default=None, help="Store results under this project id (optional)")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.catalog.list(
        filename=csv_list(args.filename),
        use_case=csv_list(args.use_case),
        project_id=csv_list(args.project_id),
        created_from=args.created_from,
        created_to=args.created_to,
        user_id=csv_list(args.user_id),
        file_type=csv_list(args.file_type),
        parent_files=csv_list(args.parent_files),
        size_from=args.size_from,
        size_to=args.size_to,
        sort=args.sort,
        page=args.page,
        perPage=args.perPage,
    )

    out = resp.model_dump(mode="json", exclude_none=False)

    if args.store_project:
        pid = norm_project(args.store_project)
        saved = store_result(client, pid, name="list", payload=out, no_store=args.no_store)
        append_project_log(client, pid, action="challenge10_catalog_list", ok=True, details={"saved": saved})

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
