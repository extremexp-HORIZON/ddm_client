from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.catalog.utils import norm_project, append_project_log, store_result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-catalog-options", description="Catalog options")
    ap.add_argument("--project_id", default=None)
    ap.add_argument("--filename", default=None)
    ap.add_argument("--user_id", default=None)
    ap.add_argument("--store_project", default=None)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.catalog.options(project_id=args.project_id, filename=args.filename, user_id=args.user_id)
    out = [x.model_dump(mode="json", exclude_none=False) for x in resp]

    if args.store_project:
        pid = norm_project(args.store_project)
        saved = store_result(client, pid, name="options", payload=out, no_store=args.no_store)
        append_project_log(client, pid, action="catalog_options", ok=True, details={"saved": saved})

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
