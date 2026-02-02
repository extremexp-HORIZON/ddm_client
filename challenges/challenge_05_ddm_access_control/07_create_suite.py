from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.expectations.create_suite import main as create_suite_main
from ddm_sdk.scripts.expectations.get_suite import main as get_suite_main

from ddm_sdk.scripts.expectations.utils import build_suite_create_payload_from_saved_sample


def _dump(obj: Any) -> Any:
    return obj.model_dump(mode="json", exclude_none=False) if hasattr(obj, "model_dump") else obj


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="challenge05-07-create-suite",
        description="Upload sample -> poll -> build suite.json from saved sample -> create suite -> poll -> get suite",
    )

    ap.add_argument("path", help="Local sample file path")
    ap.add_argument("--project_id", required=True, help="Used for grouping/storage/logs")

    ap.add_argument("--suite-name", required=True, dest="suite_name")
    ap.add_argument("--datasource-name", default="default", dest="datasource_name")
    ap.add_argument("--user_id", required=True)
    ap.add_argument("--file_type", action="append", default=[], help="Repeatable. Example: --file_type csv")
    ap.add_argument("--category", default=None)
    ap.add_argument("--description", default=None)
    ap.add_argument("--use_case", default=None)
    ap.add_argument("--mostly", type=float, default=0.95)
    ap.add_argument("--max-columns", type=int, default=150)
    ap.add_argument("--expectations-file", default=None, help="Path to JSON file with expectations list")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--no-store", action="store_true")
    ap.add_argument("--suite-json-out", default=None, help="Where to write the generated suite.json (optional)")
    ap.add_argument("--suite-out", default=None, help="Where to write the fetched suite output JSON (optional)")

    args = ap.parse_args(argv)

    sample_path = Path(args.path).expanduser().resolve()
    if not sample_path.exists() or not sample_path.is_file():
        raise SystemExit(f"File not found: {sample_path}")

    file_types: List[str] = args.file_type or ["csv"]

    client = DdmClient.from_env()
    ensure_authenticated(client)

    if (not client.storage) and (not args.no_store):
        raise SystemExit(
            "Storage not configured (DDM_STORAGE_DIR). "
            "This flow needs storage because we build suite.json from the saved upload_sample artifact."
        )

    # ============================================================
    # 1) upload_sample (DIRECT), then optionally poll tasks
    # ============================================================
    upload_resp = client.expectations.upload_sample(
        str(sample_path),
        suite_name=args.suite_name,
        datasource_name=args.datasource_name,
    )

    dataset_id = getattr(upload_resp, "dataset_id", None)
    exp_task = getattr(upload_resp, "expectation_task_id", None)
    desc_task = getattr(upload_resp, "description_task_id", None)

    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise SystemExit(f"upload_sample did not return dataset_id. resp={_dump(upload_resp)}")

    upload_payload = _dump(upload_resp)
    result_block: Dict[str, Any] = {"upload": upload_payload}

    if args.poll:
        task_ids = [t for t in (exp_task, desc_task) if isinstance(t, str) and t.strip()]
        if task_ids:
            wait = client.tasks.wait_many(
                task_ids,
                timeout_s=args.timeout,
                poll_interval_s=args.interval,
                raise_on_failure=False,
                print_state=True,
            )
            result_block["tasks_status"] = {
                tid: wait.statuses[tid].model_dump(mode="json", exclude_none=False)
                for tid in wait.statuses
            }

            values: Dict[str, Any] = {}
            for tid in task_ids:
                try:
                    values[tid] = _dump(client.tasks.value(tid))
                except Exception:
                    values[tid] = None
            result_block["tasks_value"] = values

    # persist exactly where build_suite_create_payload_from_saved_sample expects it
    if client.storage and not args.no_store:
        key = f"expectations/datasets/{dataset_id}/sample"
        client.storage.write_json(key, result_block)

    # ============================================================
    # 2) build suite create payload (DICT) from saved sample artifact
    # ============================================================
    suite_payload = build_suite_create_payload_from_saved_sample(
        client=client,
        suite_name=args.suite_name,
        dataset_id=dataset_id,
        user_id=args.user_id,
        file_types=file_types,
        datasource_name=args.datasource_name,
        category=args.category,
        description=args.description,
        use_case=args.use_case,
        mostly=args.mostly,
        max_columns=args.max_columns,
    )

    if args.expectations_file:
        p = Path(args.expectations_file).expanduser().resolve()
        if not p.exists():
            raise SystemExit(f"Expectations file not found: {p}")

        expectations_obj = json.loads(p.read_text(encoding="utf-8-sig"))

        if not isinstance(expectations_obj, list):
            raise SystemExit("Expectations JSON must be a LIST of expectations")

        suite_payload["expectations"] = {"expectations": expectations_obj}


    # write suite.json so you can inspect/reuse it

    if args.suite_json_out:
        suite_json_path = Path(args.suite_json_out).expanduser().resolve()
    else:
        suite_json_path = Path("out/runtime/expectations") / f"suite_{args.suite_name}.json"

    suite_json_path.parent.mkdir(parents=True, exist_ok=True)
    suite_json_path.write_text(
        json.dumps(suite_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


    # ============================================================
    # 3) run create_suite.py (your JSON-based one) with --json-file suite.json
    # ============================================================
    create_argv: List[str] = [
        "--project_id",
        args.project_id,
        "--json-file",
        str(suite_json_path),
    ]
    if args.poll:
        create_argv += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.interval)]
    if args.no_store:
        create_argv += ["--no-store"]

    # This prints JSON itself; we do NOT need to parse it.
    rc = create_suite_main(create_argv)
    if rc != 0:
        raise SystemExit(rc)

    
    print(
        json.dumps(
            {
                "ok": True,
                "dataset_id": dataset_id,
                "suite_json": str(suite_json_path),
                "note": (
                    "create_suite.py prints suite_id. "
                    "If you used --poll, the suite JSON is also stored under out/runtime/expectations/suites/<suite_id>/suite.json "
                ),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
