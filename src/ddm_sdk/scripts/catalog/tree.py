from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.catalog.utils import norm_project, append_project_log, store_result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-catalog-tree", description="Catalog tree")
    ap.add_argument("--parent", default=None)
    ap.add_argument("--name", default=None)
    ap.add_argument("--size", type=int, default=None)
    ap.add_argument("--type", default=None)
    ap.add_argument("--sort", default=None)
    ap.add_argument("--page", type=int, default=0)
    ap.add_argument("--perPage", type=int, default=20)
    ap.add_argument("--filter", default=None)
    ap.add_argument("--store_project", default=None)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.catalog.tree(
        parent=args.parent,
        name=args.name,
        size=args.size,
        type=args.type,
        sort=args.sort,
        page=args.page,
        perPage=args.perPage,
        filter=args.filter,
    )

    out = resp.model_dump(mode="json", exclude_none=False)

    if args.store_project:
        pid = norm_project(args.store_project)
        saved = store_result(client, pid, name="tree", payload=out, no_store=args.no_store)
        append_project_log(client, pid, action="catalog_tree", ok=True, details={"saved": saved})

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
