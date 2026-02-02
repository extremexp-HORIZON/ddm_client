from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project, require_file_id
from ddm_sdk.scripts.validations.utils import (
    append_validation_log, 
    store_validation_result_snapshot,
    summarize_validation,
    _dump,
    unwrap_task_value,
    pick_task_payload,
    get_dataset_id_from_suite,
    fetch_persisted_validation_results,
    extract_latest_result_id,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-validate-file-against-suites",
        description="Validate one file against many suites",
    )
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--file-id", required=True, dest="file_id")
    ap.add_argument("--suite-id", action="append", required=True, dest="suite_ids", help="Repeatable: --suite-id <id>")

    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)

    # how far back to look in validations/results after task SUCCESS
    ap.add_argument("--lookback-minutes", type=int, default=60)

    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    file_id = require_file_id(args.file_id)
    suite_ids = [s.strip() for s in (args.suite_ids or []) if s and s.strip()]
    if not suite_ids:
        raise SystemExit("At least one --suite-id is required")

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.validations.validate_file_against_suites({"file_id": file_id, "suite_ids": suite_ids})
    out = _dump(resp)

    # backend error synchronously
    if getattr(resp, "error", None):
        payload = {"ok": False, "error": resp.error, "existing_suite_ids": getattr(resp, "existing_suite_ids", [])}
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        if client.storage and not args.no_store:
            append_validation_log(
                client,
                project_id=project_id,
                action="validate_file_against_suites",
                ok=False,
                details=payload,
            )
        return 2

    task_id = getattr(resp, "task_id", None)

    task_status: Optional[Dict[str, Any]] = None
    task_value: Any = None

    persisted_results: Optional[Dict[str, Any]] = None
    persisted_result_id: Optional[str] = None
    persisted_result_full: Optional[Dict[str, Any]] = None

    # poll and then fetch the actual task value (may be empty) + persisted validations/results (real)
    if args.poll and isinstance(task_id, str) and task_id.strip():
        st = client.tasks.wait(
            task_id,
            timeout_s=args.timeout,
            poll_interval_s=args.interval,
            raise_on_failure=False,
        )
        st_d = _dump(st)
        task_status = st_d if isinstance(st_d, dict) else {"raw": st_d}

        # detect success
        try:
            is_success = bool(st.is_success()) if getattr(st, "is_success", None) else (getattr(st, "state", None) == "SUCCESS")
        except Exception:
            is_success = (getattr(st, "state", None) == "SUCCESS")

        if is_success:
            raw_payload = pick_task_payload(client, st, task_id)
            task_value = unwrap_task_value(raw_payload)
            dataset_id = get_dataset_id_from_suite(client, suite_ids[0])
            persisted_results = fetch_persisted_validation_results(
                client,
                suite_ids=suite_ids,
                dataset_id=dataset_id,
                lookback_minutes=int(args.lookback_minutes),
            )
            persisted_result_id = extract_latest_result_id(persisted_results)
            if persisted_result_id:
                try:
                    full = client.validations.get_result(persisted_result_id)
                    persisted_result_full = _dump(full)
                    if not isinstance(persisted_result_full, dict):
                        persisted_result_full = {"raw": persisted_result_full}
                except Exception as e:
                    persisted_result_full = {"error": str(e)}

        else:
            task_value = {"error": "task not success", "task_status": task_status}

    payload: Dict[str, Any] = {
        "ok": True,
        "project_id": project_id,
        "file_id": file_id,
        "suite_ids": suite_ids,
        "task_id": task_id,
        "task_status": task_status,
        "task_value": task_value,
        "response": out if isinstance(out, dict) else {"raw": out},
        "persisted_results": persisted_results,
        "persisted_result_id": persisted_result_id,
        "persisted_result": persisted_result_full,
    }

    # keep convenience view for task_value
    if isinstance(task_value, dict) and "results" in task_value:
        payload["validation_results"] = task_value.get("results")

    if client.storage and not args.no_store:
        store_validation_result_snapshot(client, project_id=project_id, name="validate_file_against_suites", payload=payload)
        append_validation_log(
            client,
            project_id=project_id,
            action="validate_file_against_suites",
            ok=True,
            details={"file_id": file_id, "suite_ids": suite_ids, "task_id": task_id},
        )
    
        # OPTIONAL: also write “two levels higher” under expectations/
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        client.storage.write_json(f"expectations/validations/{project_id}/{ts}", payload)

    # keep store_validation_result_snapshot.
    persisted = payload.get("persisted_result")
    if isinstance(persisted, dict):
        summary = summarize_validation(persisted)
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        # fallback if persisted_result wasn't found
        print(json.dumps(payload, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
