from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.parametrics.utils import utc_ts, parametrics_key, append_parametrics_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-suite-tuples", description="Get suite tuples")
    ap.add_argument("--out", default=None, help="Optional output json path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    rows = client.parametrics.suite_tuples()
    payload = [r.model_dump(mode="json", exclude_none=False) if hasattr(r, "model_dump") else r for r in rows]

    saved_to: Optional[str] = None
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        saved_to = args.out
    elif client.storage and not args.no_store:
        key = parametrics_key("suite_tuples")
        saved_to = client.storage.write_json(key, payload)

    if client.storage and not args.no_store:
        append_parametrics_log(
            client,
            action="suite_tuples",
            ok=True,
            details={"saved_to": saved_to, "count": len(payload), "ts": utc_ts()},
        )

    print(json.dumps({"ok": True, "saved_to": saved_to, "count": len(payload), "data": payload}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
