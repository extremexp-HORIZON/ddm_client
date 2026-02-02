from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.catalog.utils import norm_project, append_project_log, store_result


def _load_filters(args: argparse.Namespace) -> Dict[str, Any]:
    if args.json_file:
        p = Path(args.json_file).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"JSON file not found: {p}")
        # handle BOM safely
        return json.loads(p.read_text(encoding="utf-8-sig"))

    if args.json:
        return json.loads(args.json)

    raise SystemExit("Provide either --json or --json-file")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-catalog-advanced", description="Catalog advanced search")
    ap.add_argument("--project_id", required=True, help="Used only for storage/log grouping")
    ap.add_argument("--json", default=None, help="Filters as JSON string")
    ap.add_argument("--json-file", default=None, help="Path to JSON filter file")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    filters = _load_filters(args)
    data = client.catalog.advanced(filters)

    saved = store_result(client, project_id, name="advanced", payload=data, no_store=args.no_store)
    append_project_log(client, project_id, action="catalog_advanced", ok=True, details={"saved": saved})

    #print(json.dumps(data, indent=2, ensure_ascii=False))
    print ("Files listed:", len(data.get("items", [])))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
