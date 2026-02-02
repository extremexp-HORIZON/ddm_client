from __future__ import annotations

import argparse
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-upload-sample", description="Upload sample file for expectations")
    ap.add_argument("path", help="Local sample file path")
    ap.add_argument("--suite-name", default=None)
    ap.add_argument("--datasource-name", default=None)

    ap.add_argument("--poll", action="store_true", help="Poll returned tasks until done")
    ap.add_argument("--timeout", type=float, default=180.0)
    ap.add_argument("--interval", type=float, default=1.0)

    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    p = Path(args.path).expanduser().resolve()
    if not p.exists() or not p.is_file():
        raise SystemExit(f"File not found: {p}")

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.expectations.upload_sample(
        str(p),
        suite_name=args.suite_name,
        datasource_name=args.datasource_name,
    )

    out = resp.model_dump(mode="json", exclude_none=False) if hasattr(resp, "model_dump") else resp
    dataset_id = getattr(resp, "dataset_id", None)
    exp_task = getattr(resp, "expectation_task_id", None)
    desc_task = getattr(resp, "description_task_id", None)

    result_block = {"upload": out}

    if args.poll:
        task_ids = [t for t in (exp_task, desc_task) if isinstance(t, str) and t.strip()]
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

        # also fetch task values (if your deployment supports /result)
        values = {}
        for tid in task_ids:
            try:
                values[tid] = client.tasks.value(tid)
            except Exception:
                values[tid] = None
        result_block["tasks_value"] = values

    if client.storage and not args.no_store and isinstance(dataset_id, str) and dataset_id.strip():
        key = f"expectations/datasets/{dataset_id}/sample"
        client.storage.write_json(key, result_block)

        print("dataset_id:", dataset_id)
        print("expectation_task_id:", exp_task)
        print("description_task_id:", desc_task)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
