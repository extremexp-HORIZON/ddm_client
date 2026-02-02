from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.parametrics.utils import utc_ts, parametrics_key, append_parametrics_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-df-supported-file-types", description="Get dataframe-supported file types")
    ap.add_argument("--out", default=None, help="Optional output json path. If omitted, uses storage when enabled.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    data = client.parametrics.df_supported_file_types()

    saved_to: Optional[str] = None
    if args.out:
        # explicit out
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        saved_to = args.out
    elif client.storage and not args.no_store:
        key = parametrics_key("df_supported_file_types")  # writes df_supported_file_types.json
        saved_to = client.storage.write_json(key, data)
    # else: no storage + no out => just print

    if client.storage and not args.no_store:
        append_parametrics_log(
            client,
            action="df_supported_file_types",
            ok=True,
            details={"saved_to": saved_to, "ts": utc_ts()},
        )

    print(json.dumps({"ok": True, "saved_to": saved_to, "data": data}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
