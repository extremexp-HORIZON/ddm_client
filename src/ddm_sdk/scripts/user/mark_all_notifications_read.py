from __future__ import annotations

import argparse
import json

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.user.utils import store_user_result, append_user_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-user-notifs-readall", description="Mark all notifications read")
    ap.add_argument("--username", required=True)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.user.mark_all_notifications_read()
    out = resp.model_dump(mode="json", exclude_none=False)

    saved = store_user_result(client, args.username, name="notifications/mark_all_read", payload=out, no_store=args.no_store)
    append_user_log(client, args.username, action="mark_all_notifications_read", ok=True, details={"saved": saved})

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
