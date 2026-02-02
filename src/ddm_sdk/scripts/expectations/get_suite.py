from __future__ import annotations

import argparse
import json
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.expectations.utils import norm_suite_id, persist_suite_record, append_suite_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-get-suite", description="Get an expectation suite by id")
    ap.add_argument("--suite-id", required=True)
    ap.add_argument("--out", default=None, help="Optional output json path")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    suite_id = norm_suite_id(args.suite_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.expectations.get_suite(suite_id)
    payload = resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp

    saved_to: Optional[str] = None
    if args.out:
        from pathlib import Path
        op = Path(args.out).expanduser().resolve()
        op.parent.mkdir(parents=True, exist_ok=True)
        op.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        saved_to = str(op)
    elif client.storage and not args.no_store:
        persist_suite_record(client=client, suite_id=suite_id, payload=payload)
        saved_to = "storage:" + f"expectations/suites/{suite_id}/suite.json"

    if client.storage and not args.no_store:
        append_suite_log(
            client,
            suite_id=suite_id,
            action="get_suite",
            ok=True,
            details={"saved_to": saved_to},
        )

    print(json.dumps({"ok": True, "suite_id": suite_id, "saved_to": saved_to, "data": payload}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
