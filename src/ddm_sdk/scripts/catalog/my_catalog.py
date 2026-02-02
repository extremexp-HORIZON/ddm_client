from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.catalog.utils import norm_project, append_project_log, store_result


def _csv_list(v: Optional[str]) -> Optional[list[str]]:
    if not v:
        return None
    parts = [x.strip() for x in v.split(",") if x and x.strip()]
    return parts or None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-catalog-my", description="My catalog list")
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
    ap.add_argument("--store_project", default=None)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.catalog.my_catalog(
        filename=_csv_list(args.filename),
        use_case=_csv_list(args.use_case),
        project_id=_csv_list(args.project_id),
        created_from=args.created_from,
        created_to=args.created_to,
        user_id=_csv_list(args.user_id),
        file_type=_csv_list(args.file_type),
        parent_files=_csv_list(args.parent_files),
        size_from=args.size_from,
        size_to=args.size_to,
        sort=args.sort,
        page=args.page,
        perPage=args.perPage,
    )

    out = resp.model_dump(mode="json", exclude_none=False)

    if args.store_project:
        pid = norm_project(args.store_project)
        saved = store_result(client, pid, name="my_catalog", payload=out, no_store=args.no_store)
        append_project_log(client, pid, action="catalog_my_catalog", ok=True, details={"saved": saved})

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
