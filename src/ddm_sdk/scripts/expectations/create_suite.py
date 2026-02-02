from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.models.expectations import ExpectationSuiteCreate
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project
from ddm_sdk.scripts.expectations.utils import (
    append_suite_log,
    persist_suite_record,
    create_suite_req_key,
    link_suite_dataset
)


def _load_json_arg(json_s: Optional[str], json_file: Optional[str]) -> Dict[str, Any]:
    if json_s and json_file:
        raise SystemExit("Use only one of --json or --json-file")

    if json_file:
        p = Path(json_file).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"JSON file not found: {p}")
        try:
            txt = p.read_text(encoding="utf-8-sig")  # BOM-safe
            obj = json.loads(txt)
        except Exception as e:
            raise SystemExit(f"--json-file is not valid JSON: {e}")
    elif json_s:
        try:
            obj = json.loads(json_s)
        except Exception as e:
            raise SystemExit(f"--json is not valid JSON: {e}")
    else:
        raise SystemExit("Missing payload: provide --json or --json-file")

    if not isinstance(obj, dict):
        raise SystemExit("Payload must be a JSON object")
    return obj


def _dump(obj: Any) -> Any:
    return obj.model_dump(mode="json", exclude_none=False) if hasattr(obj, "model_dump") else obj


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-create-suite", description="Create an expectation suite")
    ap.add_argument("--project_id", required=True, help="Used for grouping request snapshots/logs only")
    ap.add_argument("--json", default=None)
    ap.add_argument("--json-file", default=None)
    ap.add_argument("--no-store", action="store_true")

    ap.add_argument("--poll", action="store_true", help="Wait task_id and then GET suite and store it")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)

    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    obj = _load_json_arg(args.json, args.json_file)
    body = ExpectationSuiteCreate.model_validate(obj)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.expectations.create_suite(body)
    resp_payload = _dump(resp)
    suite_id = getattr(resp, "suite_id", None)
    task_id = getattr(resp, "task_id", None)

    saved: Dict[str, Any] = {
        "request": None,
        "create_response": None,
        "task_status": None,
        "task_value": None,
        "suite": None,
    }

    # -----------------------
    # Save request snapshot
    # -----------------------
    req_key = None
    if client.storage and not args.no_store:
        req_key = create_suite_req_key(project_id=project_id, suite_name=body.suite_name)
        saved["request"] = client.storage.write_json(f"{req_key}/request", _dump(body))

    # -----------------------
    # Save create response
    # -----------------------
    if client.storage and not args.no_store and isinstance(suite_id, str) and suite_id.strip():
        saved["create_response"] = client.storage.write_json(
            f"expectations/suites/{suite_id}/create_response",
            resp_payload,
        )

    
    # -----------------------
    # Poll + save task artifacts + save full suite.json
    # -----------------------
    status_payload = None
    value_payload = None
    suite_payload = None

    if args.poll and isinstance(task_id, str) and task_id.strip():
        st = client.tasks.wait(
            task_id,
            timeout_s=args.timeout,
            poll_interval_s=args.interval,
            raise_on_failure=False,
        )
        status_payload = _dump(st)

        # task value is optional
        try:
            v = client.tasks.value(task_id)
            value_payload = _dump(v)
        except Exception:
            value_payload = None

        if isinstance(suite_id, str) and suite_id.strip():
            suite = client.expectations.get_suite(suite_id)
            suite_payload = _dump(suite)

        # persist all artifacts under expectations/suites/<suite_id>/...
        if client.storage and not args.no_store and isinstance(suite_id, str) and suite_id.strip():
            if status_payload is not None:
                saved["task_status"] = client.storage.write_json(
                    f"expectations/suites/{suite_id}/task_status",
                    status_payload,
                )
            if value_payload is not None:
                saved["task_value"] = client.storage.write_json(
                    f"expectations/suites/{suite_id}/task_value",
                    value_payload,
                )
            if suite_payload is not None:
                # writes to expectations/suites/<suite_id>/suite.json via helper
                persist_suite_record(client=client, project_id=project_id, suite_id=suite_id, payload=suite_payload)
                # persist_suite_record doesn't return it, so we write it to capture the path.
                saved["suite"] = client.storage.write_json(
                    f"expectations/suites/{suite_id}/suite",
                    suite_payload,
                )

            append_suite_log(
                client,
                project_id=project_id,
                suite_id=suite_id,
                action="create_suite",
                ok=(getattr(st, "state", None) != "FAILURE"),
                details={
                    "task_id": task_id,
                    "req_key": req_key,
                    "saved": saved,
                },
            )

        if client.storage and not args.no_store and isinstance(suite_id, str) and suite_id.strip():
            dsid = getattr(body, "dataset_id", None)
            if isinstance(dsid, str) and dsid.strip():
                link_suite_dataset(client=client, suite_id=suite_id.strip(), dataset_id=dsid.strip())
                # if you want to expose where it lands, you can set a hint:
                saved["relation"] = {
                    "suite_datasets": f"expectations/suites/{suite_id}/datasets",
                    "dataset_suites": f"expectations/datasets/{dsid}/suites",
                }

    print(
        json.dumps(
            {
                "ok": True,
                "project_id": project_id,
                "suite_id": suite_id,
                "task_id": task_id,
                "saved": saved,
                "task_status": status_payload.get("state") if isinstance(status_payload, dict) else status_payload,

                "dataset_id": getattr(body, "dataset_id", None),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    print("suite_id:", suite_id)
    print("task_id:", task_id)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
