from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated


def _csv(v: list[str]) -> list[str]:
    return [x.strip() for x in (v or []) if x and x.strip()]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-list-suites", description="List expectation suites")
    ap.add_argument("--suite-name", action="append", default=[], help="Repeatable")
    ap.add_argument("--suite-id", action="append", default=[], help="Repeatable")
    ap.add_argument("--file-types", action="append", default=[], help="Repeatable")
    ap.add_argument("--category", action="append", default=[], help="Repeatable")
    ap.add_argument("--use-case", action="append", default=[], help="Repeatable")
    ap.add_argument("--user-id", action="append", default=[], help="Repeatable")
    ap.add_argument("--created-from", default=None, help="ISO 8601")
    ap.add_argument("--created-to", default=None, help="ISO 8601")
    ap.add_argument("--sort", default="created,desc")
    ap.add_argument("--page", type=int, default=1)
    ap.add_argument("--per-page", type=int, default=10)
    ap.add_argument("--out", default=None, help="Optional output json path")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.expectations.list_suites(
        suite_name=_csv(args.suite_name) or None,
        suite_id=_csv(args.suite_id) or None,
        file_types=_csv(args.file_types) or None,
        category=_csv(args.category) or None,
        use_case=_csv(args.use_case) or None,
        user_id=_csv(args.user_id) or None,
        created_from=args.created_from,
        created_to=args.created_to,
        sort=args.sort,
        page=args.page,
        perPage=args.per_page,
    )

    payload = resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp

    saved_to: Optional[str] = None
    if args.out:
        import pathlib
        op = pathlib.Path(args.out).expanduser().resolve()
        op.parent.mkdir(parents=True, exist_ok=True)
        op.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        saved_to = str(op)
    elif client.storage and not args.no_store:
        saved_to = client.storage.write_json("expectations/suites_list", payload)

    print(json.dumps({"ok": True, "saved_to": saved_to, "data": payload}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
