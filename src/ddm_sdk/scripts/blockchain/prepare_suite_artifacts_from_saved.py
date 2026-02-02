from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project
from ddm_sdk.scripts.blockchain.utils import load_saved_suite_record, store_prepare_suite_latest, dataset_id_from_suite_links_or_log
from ddm_sdk.scripts.blockchain.builders import build_prepare_suite_payload_from_suite_record
from ddm_sdk.scripts.blockchain.task_runner import run_task_and_store
from ddm_sdk.scripts.blockchain.extractors import extract_suite_hash



def _store_under_blockchain_expectations_suite(
    client: DdmClient,
    *,
    suite_id: str,
    action: str,
    request_payload: Dict[str, Any],
    response_payload: Dict[str, Any],
) -> Dict[str, str]:
    base = f"blockchain/expectations/suites/{suite_id}/{action}"
    return {
        "request": client.storage.write_json(f"{base}/request", request_payload),
        "response": client.storage.write_json(f"{base}/response", response_payload),
    }



def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-prepare-suite-from-saved",
        description="Prepare on-chain suite from saved expectations suite record",
    )
    ap.add_argument("--project_id", required=True)
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--requester", required=True)
    ap.add_argument("--deadline", type=int, default=None, help="Unix timestamp seconds. Default: now+7d")
    ap.add_argument("--totalExpected", type=int, default=10)
    ap.add_argument("--fileFormat", default=None)

    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)
    deadline = args.deadline if args.deadline is not None else int(time.time()) + 7 * 24 * 3600

    client = DdmClient.from_env()
    ensure_authenticated(client)

    suite_id = (args.suite_id or "").strip()
    if not suite_id:
        raise SystemExit("suite_id is empty")

    suite_record = load_saved_suite_record(client, project_id=project_id, suite_id=suite_id)

    payload = build_prepare_suite_payload_from_suite_record(
        suite_record,
        network=args.network,
        requester=args.requester,
        deadline=deadline,
        total_expected=args.totalExpected,
        file_format=args.fileFormat,
    )

    out = run_task_and_store(
        client,
        action=f"{suite_id}/prepare_suite_artifacts",
        request_payload=payload,
        call_fn=lambda: client.blockchain.prepare_suite(payload),
        poll=args.poll,
        timeout_s=args.timeout,
        interval_s=args.interval,
        no_store=args.no_store,
    )

    task_val = None
    if isinstance(out.get("result"), dict):
        task_val = out["result"].get("value") or out["result"].get("result")
    if task_val is None and isinstance(out.get("status"), dict):
        task_val = out["status"].get("result")

    out["suite_hash"] = extract_suite_hash(task_val)

    if client.storage and not args.no_store:
        out["saved_suite_blockchain"] = _store_under_blockchain_expectations_suite(
            client,
            suite_id=suite_id,
            action="prepare_suite_artifacts",
            request_payload=payload,
            response_payload=out,
        )

        # dataset_id comes from the suite->datasets link (first element)
        dataset_id = dataset_id_from_suite_links_or_log(client, project_id=project_id, suite_id=suite_id)
        if dataset_id:
            out["saved_suite_dataset_blockchain"] = store_prepare_suite_latest(
                client,
                suite_id=suite_id,
                dataset_id=dataset_id,
                request_payload=payload,
                response_payload=out,
            )
        else:
            # make it visible in output
            out["saved_suite_dataset_blockchain"] = None
            out["warning"] = "No linked dataset_id found at expectations/suites/<suite_id>/datasets.json"

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
