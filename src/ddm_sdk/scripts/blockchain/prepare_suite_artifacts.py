from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.blockchain.utils import load_json_arg, store_prepare_suite_latest
from ddm_sdk.scripts.blockchain.task_runner import run_task_and_store
from ddm_sdk.scripts.blockchain.extractors import extract_suite_hash
from ddm_sdk.scripts.expectations.utils import suite_datasets_key, suite_logs_key


def _pick_suite_id(payload: Dict[str, Any]) -> Optional[str]:
    for k in ("suite_id", "expectations_suite_id", "expectation_suite_id"):
        v = payload.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


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


def _dataset_id_from_suite_links_or_log(
    client: DdmClient,
    *,
    suite_id: str,
    project_id: Optional[str] = None,
) -> Optional[str]:
    """
    Try:
      1) expectations/suites/<suite_id>/datasets.json (first non-empty)
      2) expectations/suites/<suite_id>/log.json -> latest details.dataset_id
         (project_id is optional; suite_logs_key ignores it in your current utils anyway)
    """
    if not client.storage:
        return None

    # 1) datasets.json
    ds_obj = client.storage.read_json(suite_datasets_key(suite_id=suite_id))
    if isinstance(ds_obj, list):
        for x in ds_obj:
            if isinstance(x, str) and x.strip():
                return x.strip()

    # 2) log.json
    try:
        log_key = suite_logs_key(project_id=project_id, suite_id=suite_id)  # project_id may be None
    except TypeError:
        # in case your suite_logs_key signature doesn't accept project_id anymore
        log_key = f"expectations/suites/{suite_id}/log"

    log_obj = client.storage.read_json(log_key)
    if isinstance(log_obj, list):
        for entry in reversed(log_obj):
            if not isinstance(entry, dict):
                continue
            details = entry.get("details")
            if not isinstance(details, dict):
                continue
            did = details.get("dataset_id")
            if isinstance(did, str) and did.strip():
                return did.strip()

    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-prepare-suite", description="Prepare suite on-chain (task)")
    ap.add_argument("--json", default=None)
    ap.add_argument("--json-file", default=None)
    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--no-store", action="store_true")

    # ✅ optional, only used to help read log.json (datasets.json works without it)
    ap.add_argument("--project_id", default=None, help="Optional. Helps locate suite log.json if needed")

    args = ap.parse_args(argv)

    payload: Dict[str, Any] = load_json_arg(json_text=args.json, json_file=args.json_file)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    suite_id = _pick_suite_id(payload)

    # Optional: enrich payload with dataset_ids from link
    if (
        suite_id
        and client.storage
        and not args.no_store
        and "dataset_id" not in payload
        and "dataset_ids" not in payload
    ):
        ds = client.storage.read_json(suite_datasets_key(suite_id=suite_id))
        if isinstance(ds, list):
            ds_ids = [x.strip() for x in ds if isinstance(x, str) and x.strip()]
            if ds_ids:
                payload["dataset_ids"] = ds_ids

    out = run_task_and_store(
        client,
        action=(payload.get("expectation_suite_id", "unknown") + "/prepare_suite_artifacts"),
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

    if suite_id and client.storage and not args.no_store:
        out["saved_suite_blockchain"] = _store_under_blockchain_expectations_suite(
            client,
            suite_id=suite_id,
            action="prepare_suite_artifacts",
            request_payload=payload,
            response_payload=out,
        )

        dataset_id = _dataset_id_from_suite_links_or_log(
            client,
            suite_id=suite_id,
            project_id=args.project_id,
        )
        if dataset_id:
            out["saved_suite_dataset_blockchain"] = store_prepare_suite_latest(
                client,
                suite_id=suite_id,
                dataset_id=dataset_id,
                request_payload=payload,
                response_payload=out,
            )
        else:
            out["saved_suite_dataset_blockchain"] = None
            out["warning"] = f"No dataset_id found for suite_id={suite_id} (no datasets.json and none in log.json)"

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
