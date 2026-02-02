from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project, require_file_id
from ddm_sdk.scripts.validations.utils import append_validation_log, store_validation_result_snapshot


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-validate-files-against-suite", description="Validate many files against one suite")
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--suite-id", required=True, dest="suite_id")
    ap.add_argument("--file-id", action="append", required=True, dest="file_ids", help="Repeatable: --file-id <uuid>")
    ap.add_argument("--poll", action="store_true", help="Poll tasks to completion and print final statuses")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    suite_id = (args.suite_id or "").strip()
    if not suite_id:
        raise SystemExit("suite_id is required")

    file_ids = [require_file_id(x) for x in (args.file_ids or [])]

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.validations.validate_files_against_suite({"suite_id": suite_id, "file_ids": file_ids})
    out = resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp

    # 409: already validated
    if getattr(resp, "error", None):
        print(json.dumps({"ok": False, "error": resp.error, "already_validated_file_ids": getattr(resp, "already_validated_file_ids", [])}, indent=2))
        if client.storage and not args.no_store:
            append_validation_log(client, project_id=project_id, action="validate_files_against_suite", ok=False, details=out if isinstance(out, dict) else {"raw": out})
        return 2

    tasks = getattr(resp, "tasks", []) or []
    task_pairs: List[Dict[str, str]] = []
    for t in tasks:
        fid = getattr(t, "file_id", None)
        tid = getattr(t, "task_id", None)
        if isinstance(fid, str) and isinstance(tid, str):
            task_pairs.append({"file_id": fid, "task_id": tid})

    statuses: Dict[str, Any] = {}

    if args.poll:
        for pair in task_pairs:
            st = client.tasks.wait(pair["task_id"], timeout_s=args.timeout, poll_interval_s=args.interval, raise_on_failure=False)
            statuses[pair["task_id"]] = st.model_dump(mode="json", exclude_none=False) if hasattr(st, "model_dump") else getattr(st, "__dict__", {"state": getattr(st, "state", None)})

    payload = {
        "ok": True,
        "project_id": project_id,
        "suite_id": suite_id,
        "tasks": task_pairs,
        "tasks_status": statuses if args.poll else None,
    }

    if client.storage and not args.no_store:
        store_validation_result_snapshot(client, project_id=project_id, name="validate_files_against_suite", payload=payload)
        append_validation_log(
            client,
            project_id=project_id,
            action="validate_files_against_suite",
            ok=True,
            details={"suite_id": suite_id, "file_ids": file_ids, "tasks": task_pairs},
        )

    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
