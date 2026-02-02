from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from ddm_sdk.client import DdmClient
from ddm_sdk.models.expectations import ExpectationSuiteCreate
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project
from ddm_sdk.scripts.expectations.utils import (
    append_suite_log,
    persist_suite_record,
    create_suite_req_key,
    build_suite_create_payload_from_saved_sample,
    link_suite_dataset
)


def _dump(obj: Any) -> Any:
    return obj.model_dump(mode="json", exclude_none=False) if hasattr(obj, "model_dump") else obj


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-create-suite-from-sample",
        description="Create expectation suite using saved upload_sample artifact (storage)",
    )
    ap.add_argument("--project_id", required=True, help="Used only for grouping request snapshots/logs")
    ap.add_argument("--dataset_id", required=True, help="dataset_id returned by upload_sample")
    ap.add_argument("--suite_name", required=True)
    ap.add_argument("--user_id", required=True)
    ap.add_argument("--file_type", action="append", default=[], help="Repeatable. Example: --file_type csv")
    ap.add_argument("--datasource_name", default="default")
    ap.add_argument("--category", default=None)
    ap.add_argument("--description", default=None)
    ap.add_argument("--use_case", default=None)

    ap.add_argument("--mostly", type=float, default=0.95)
    ap.add_argument("--max-columns", type=int, default=150)

    ap.add_argument("--no-store", action="store_true")

    # polling options
    ap.add_argument("--poll", action="store_true", help="Wait task_id and then GET suite and store it")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)

    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    file_types: List[str] = args.file_type or []
    if not file_types:
        file_types = ["csv"]

    client = DdmClient.from_env()
    ensure_authenticated(client)

    # build payload from saved sample artifact
    payload = build_suite_create_payload_from_saved_sample(
        client=client,
        suite_name=args.suite_name,
        dataset_id=args.dataset_id,
        user_id=args.user_id,
        file_types=file_types,
        datasource_name=args.datasource_name,
        category=args.category,
        description=args.description,
        use_case=args.use_case,
        mostly=args.mostly,
        max_columns=args.max_columns,
    )

    # validate against model
    body = ExpectationSuiteCreate.model_validate(payload)

    # create suite (async validation task)
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

    req_key = None
    if client.storage and not args.no_store:
        req_key = create_suite_req_key(project_id=project_id, suite_name=body.suite_name)
        saved["request"] = client.storage.write_json(f"{req_key}/request", _dump(body))

        if isinstance(suite_id, str) and suite_id.strip():
            saved["create_response"] = client.storage.write_json(
                f"expectations/suites/{suite_id}/create_response",
                resp_payload,
            )

    # poll -> store task status/value -> GET suite -> store suite.json
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

        try:
            v = client.tasks.value(task_id)
            value_payload = _dump(v)
        except Exception:
            value_payload = None

        if isinstance(suite_id, str) and suite_id.strip():
            suite = client.expectations.get_suite(suite_id)
            suite_payload = _dump(suite)

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
                # helper writes expectations/suites/<suite_id>/suite.json
                persist_suite_record(client=client, project_id=project_id, suite_id=suite_id, payload=suite_payload)
                # write to get actual path returned
                saved["suite"] = client.storage.write_json(
                    f"expectations/suites/{suite_id}/suite",
                    suite_payload,
                )

            append_suite_log(
                client,
                project_id=project_id,
                suite_id=suite_id,
                action="create_suite_from_sample",
                ok=(getattr(st, "state", None) != "FAILURE"),
                details={
                    "task_id": task_id,
                    "req_key": req_key,
                    "dataset_id": args.dataset_id,
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
                "dataset_id": args.dataset_id,
                "task_id": task_id,
                "saved": saved,
                "task_status": status_payload.get("state") if isinstance(status_payload, dict) else None,
                "suite_id": suite_id,
                "dataset_id": payload.get("dataset_id"), 
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
