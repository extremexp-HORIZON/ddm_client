from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

from web3 import Web3

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.file.utils import norm_project
from ddm_sdk.scripts.blockchain.utils import (
    load_json_arg,
    storage_read_json,
    _jsonify,
    user_pk,
)
from ddm_sdk.scripts.blockchain.task_runner import run_task_and_store


# -----------------------
# helpers
# -----------------------

def _unwrap_task_envelope(obj: Any) -> Any:
    if isinstance(obj, dict):
        if isinstance(obj.get("result"), dict):
            r = obj["result"].get("result") or obj["result"].get("value") or obj["result"]
            if r is not None:
                return r
        if isinstance(obj.get("status"), dict):
            r = obj["status"].get("result")
            if r is not None:
                return r
    return obj


def _get_uploader_from_env() -> str:
    """
    Prefer deriving address from DDM_USER_PK (same signer you use elsewhere),
    fallback to DDM_USER_ADDRESS if present.
    """
    addr = os.getenv("DDM_USER_ADDRESS")
    if addr and addr.strip():
        return Web3.to_checksum_address(addr.strip())

    pk = user_pk()  # raises if missing
    acct = Web3().eth.account.from_key(pk)
    return Web3.to_checksum_address(acct.address)


def _extract_dataset_fingerprint_from_register_dataset(resp_obj: Any) -> Optional[str]:
    """
    Best effort from:
      - receipt.logs[0].topics[3]
      - request_meta.call_args.datasetFingerprint (if you store it later)
    """
    if not isinstance(resp_obj, dict):
        return None

    # 1) request_meta.call_args.datasetFingerprint (optional)
    rm = resp_obj.get("request_meta")
    if isinstance(rm, dict):
        ca = rm.get("call_args")
        if isinstance(ca, dict):
            fp = ca.get("datasetFingerprint") or ca.get("dataset_fingerprint") or ca.get("fingerprint")
            if isinstance(fp, str) and fp.strip():
                fp = fp.strip()
                return fp if fp.startswith("0x") else "0x" + fp

    # 2) receipt.logs[0].topics[3]
    receipt = resp_obj.get("receipt")
    if isinstance(receipt, dict):
        logs = receipt.get("logs")
        if isinstance(logs, list) and logs:
            first = logs[0]
            if isinstance(first, dict):
                topics = first.get("topics")
                if isinstance(topics, list) and len(topics) >= 4:
                    t = topics[3]
                    if isinstance(t, str) and t.strip():
                        t = t.strip()
                        return t if t.startswith("0x") else "0x" + t

    return None


def _load_dataset_fingerprint_from_storage(client: DdmClient, *, suite_id: str, catalog_id: str) -> str:
    key = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/register_dataset/response"
    obj = storage_read_json(client, key)
    obj = _unwrap_task_envelope(obj)
    fp = _extract_dataset_fingerprint_from_register_dataset(obj)
    if not fp:
        raise SystemExit(f"dataset_fingerprint not found in: {key}")
    return fp


# -----------------------
# main
# -----------------------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-prepare-reward", description="Prepare reward (task) without needing a json file")
    ap.add_argument("--project_id", required=True)

    # no-json args
    ap.add_argument("--suite_id", default=None)
    ap.add_argument("--catalog_id", default=None)
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--category", default="dataset")
    ap.add_argument("--expires-in-sec", type=int, default=900)

    # old interface supported too
    ap.add_argument("--json", default=None)
    ap.add_argument("--json-file", default=None)

    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    project_id = norm_project(args.project_id)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    if args.json or args.json_file:
        payload: Dict[str, Any] = load_json_arg(json_text=args.json, json_file=args.json_file)
    else:
        if not (isinstance(args.suite_id, str) and args.suite_id.strip()):
            raise SystemExit("Missing --suite_id (required when not using --json/--json-file)")
        if not (isinstance(args.catalog_id, str) and args.catalog_id.strip()):
            raise SystemExit("Missing --catalog_id (required when not using --json/--json-file)")

        suite_id = args.suite_id.strip()
        catalog_id = args.catalog_id.strip()

        uploader = _get_uploader_from_env()
        dataset_fingerprint = _load_dataset_fingerprint_from_storage(
            client, suite_id=suite_id, catalog_id=catalog_id
        )

        payload = {
            "network": args.network.strip(),
            "category": args.category.strip(),
            "dataset_fingerprint": dataset_fingerprint,
            "uploader": uploader,
            "expires_in_sec": int(args.expires_in_sec),

            # keep for storage grouping
            "suite_id": suite_id,
            "catalog_id": catalog_id,
            "project_id": project_id,
        }


    out = run_task_and_store(
        client,
        action="prepare_reward",
        request_payload=payload,
        call_fn=lambda: client.blockchain.prepare_reward(payload),
        poll=args.poll,
        timeout_s=args.timeout,
        interval_s=args.interval,
        no_store=args.no_store,
    )

    # store under suite/dataset path you want
    suite_id = payload.get("suite_id")
    catalog_id = payload.get("catalog_id")
    if client.storage and not args.no_store and isinstance(suite_id, str) and isinstance(catalog_id, str):
        base = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/prepare_reward"
        client.storage.write_json(f"{base}/request", _jsonify(payload))
        client.storage.write_json(f"{base}/response", _jsonify(out))
        out["saved_suite_dataset_blockchain"] = {
            "request": f"{base}/request.json",
            "response": f"{base}/response.json",
        }

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
