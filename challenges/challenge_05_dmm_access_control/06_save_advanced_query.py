from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.models.user import PreferredQueryCreateRequest
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.user.utils import load_json_arg, store_user_result, append_user_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-user-save-advanced-query",
        description="Save a Catalog Advanced filters JSON as a preferred query",
    )
    ap.add_argument("--username", required=True, help="Username for storage/log grouping")
    ap.add_argument("--name", required=True, help="Human name for this saved query (e.g. 'tutorial crisis files')")
    ap.add_argument("--json", default=None, help="JSON object to store (advanced filters)")
    ap.add_argument("--json-file", default=None, help="Path to JSON file containing the filters object")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    # Load the *filters* object you want saved
    filters_obj = load_json_arg(json_text=args.json, json_file=args.json_file)

    # Wrap it so later you know what API it targets
    query_obj = {
        "kind": "catalog.advanced",
        "filters": filters_obj,
    }

    body = PreferredQueryCreateRequest(name=args.name, query=query_obj.get("filters"))
    resp = client.user.save_preferred_query(body)
    out = resp.model_dump(mode="json", exclude_none=False)

    saved = store_user_result(client, args.username, name="queries/save", payload=out, no_store=args.no_store)
    append_user_log(client, args.username, action="save_preferred_query", ok=True, details={"saved": saved})

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
