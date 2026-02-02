from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.catalog.utils import norm_project, append_project_log, store_result, load_filters


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="challenge05-catalog-advanced", description="Catalog advanced search (and store)")
    ap.add_argument("--project_id", required=True, help="Used for storage/log grouping")
    ap.add_argument("--json", default=None, help="Filters as JSON string (object)")
    ap.add_argument("--json-file", default=None, help="Path to JSON filter file")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    filters = load_filters(args.json, args.json_file)

    # call API
    data = client.catalog.advanced(filters)

    # store response + also store request filters (super useful)
    saved = store_result(client, project_id, name="advanced", payload={"filters": filters, "response": data}, no_store=args.no_store)
    append_project_log(client, project_id, action="challenge05_catalog_advanced", ok=True, details={"saved": saved})

    #print(json.dumps(data, indent=2, ensure_ascii=False))
    print ("Files listed:", len(data))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
