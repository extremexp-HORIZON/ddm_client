from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Optional

from web3 import Web3
from web3.exceptions import ContractLogicError

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.blockchain.utils import (
    registry_address_from_index,
    load_abi_from_storage,
    storage_read_json,
    storage_write_pair,
    rpc_url,
    user_pk,
    _jsonify,
    fail_out,
    revert_reason,
)

def _dump(obj: Any) -> Any:
    return obj.model_dump(mode="json", exclude_none=False) if hasattr(obj, "model_dump") else obj


def _load_prepare_request(client: DdmClient, *, suite_id: str) -> Optional[Dict[str, Any]]:
    key = f"blockchain/expectations/suites/{suite_id}/prepare_suite_artifacts/request"
    obj = storage_read_json(client, key)
    return obj if isinstance(obj, dict) else None


def _load_prepared_response(client: DdmClient, *, suite_id: str) -> Dict[str, Any]:
    key = f"blockchain/expectations/suites/{suite_id}/prepare_suite_artifacts/response"
    obj = storage_read_json(client, key)
    if not isinstance(obj, dict):
        raise FileNotFoundError(f"Prepared response not found at storage key: {key}")

    # unwrap common envelopes produced by run_task_and_store
    prepared: Any = None
    if isinstance(obj.get("result"), dict):
        prepared = obj["result"].get("result") or obj["result"].get("value") or obj["result"]
    if prepared is None and isinstance(obj.get("status"), dict):
        prepared = obj["status"].get("result")
    if prepared is None:
        prepared = obj

    if not isinstance(prepared, dict):
        raise RuntimeError("Prepared response could not be unwrapped into a dict.")

    return prepared


def _get_str(d: Dict[str, Any], *keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default


def _get_int(d: Dict[str, Any], *keys: str, default: Optional[int] = None) -> Optional[int]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.strip().isdigit():
            return int(v.strip())
    return default


def _pick_method(prepared: Dict[str, Any], force: Optional[str]) -> str:
    if force in ("plain", "sig"):
        return force
    sig = _get_str(prepared, "signature")
    nonce = _get_int(prepared, "nonce")
    exp = _get_int(prepared, "expiresAt", "expires_at")
    if sig and nonce is not None and exp is not None:
        return "sig"
    return "plain"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-register-suite",
        description="Send DatasetRequestRegistry tx using saved prepare artifacts",
    )
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--registry-address", default=None, help="Optional override. Default: from saved contracts index.")
    ap.add_argument("--registry-name", default="DatasetRequestRegistry", help="Name in contracts index.")
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--bounty-eth", type=float, required=True)
    ap.add_argument("--method", choices=["plain", "sig"], default=None, help="Default: auto")
    ap.add_argument("--poll", action="store_true", help="Poll ingest task")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--no-store", action="store_true")
    ap.add_argument("--max-fee-gwei", type=float, default=None)
    ap.add_argument("--max-priority-fee-gwei", type=float, default=None)
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    if not client.storage:
        raise SystemExit("Storage not configured (DDM_STORAGE_DIR).")

    network = args.network.strip()
    suite_id = args.suite_id.strip()

    # registry address
    if args.registry_address and args.registry_address.strip():
        registry_addr_raw = args.registry_address.strip()
    else:
        registry_addr_raw = registry_address_from_index(client, network=network, name=args.registry_name)

    registry_addr = Web3.to_checksum_address(registry_addr_raw)

    # ABI from storage
    abi = load_abi_from_storage(client, network=network, address=registry_addr_raw)

    # load artifacts
    prepared = _load_prepared_response(client, suite_id=suite_id)
    prep_req = _load_prepare_request(client, suite_id=suite_id) or {}

    merged: Dict[str, Any] = dict(prep_req)
    merged.update(prepared)

    suite_hash = _get_str(merged, "suiteHash", "suite_hash")
    suite_uri = _get_str(merged, "suiteURI", "suite_uri")
    docs_uri = _get_str(merged, "docsURI", "docs_uri", default="") or ""
    cert_uri = _get_str(merged, "certificateURI", "certificate_uri")

    category = _get_str(merged, "category")
    file_format = _get_str(merged, "fileFormat", "file_format")
    deadline = _get_int(merged, "deadline")
    total_expected = _get_int(merged, "totalExpected", "total_expected")

    missing = [
        k for k, v in {
            "suiteHash": suite_hash,
            "suiteURI": suite_uri,
            "certificateURI": cert_uri,
            "category": category,
            "fileFormat": file_format,
            "deadline": deadline,
            "totalExpected": total_expected,
        }.items()
        if v is None or v == ""
    ]
    if missing:
        raise SystemExit(f"Missing fields in prepared artifacts (or request fallback): {missing}")

    method = _pick_method(merged, args.method)

    # web3
    w3 = Web3(Web3.HTTPProvider(rpc_url(network)))
    if not w3.is_connected():
        raise SystemExit(f"Web3 cannot connect to RPC for network={network}")

    acct = w3.eth.account.from_key(user_pk())
    sender = acct.address

    contract = w3.eth.contract(address=registry_addr, abi=abi)
    value_wei = w3.to_wei(args.bounty_eth, "ether")

    # build function call
    if method == "plain":
        fn = contract.functions.createDatasetRequest(
            suite_hash,
            suite_uri,
            docs_uri,
            cert_uri,
            category,
            file_format,
            int(deadline),
            int(total_expected),
        )
        nonce = None
        expires_at = None
    else:
        nonce = _get_int(merged, "nonce")
        expires_at = _get_int(merged, "expiresAt", "expires_at")
        signature = _get_str(merged, "signature")
        if nonce is None or expires_at is None or not signature:
            raise SystemExit("method=sig but missing nonce/expiresAt/signature in prepared artifacts")

        fn = contract.functions.createDatasetRequestWithSig(
            suite_hash,
            suite_uri,
            docs_uri,
            cert_uri,
            category,
            file_format,
            int(deadline),
            int(total_expected),
            int(nonce),
            int(expires_at),
            signature,
        )

    # request_meta (used for saving and error paths)
    request_meta: Dict[str, Any] = {
        "network": network,
        "suite_id": suite_id,
        "registry_name": args.registry_name,
        "registry_address": registry_addr_raw,
        "method": method,
        "bounty_eth": args.bounty_eth,
        "sender": sender,
        "call_args": {
            "suiteHash": suite_hash,
            "suiteURI": suite_uri,
            "docsURI": docs_uri,
            "certificateURI": cert_uri,
            "category": category,
            "fileFormat": file_format,
            "deadline": int(deadline),
            "totalExpected": int(total_expected),
        },
        "prepare_request_key": f"blockchain/expectations/suites/{suite_id}/prepare_suite_artifacts/request.json",
        "prepare_response_key": f"blockchain/expectations/suites/{suite_id}/prepare_suite_artifacts/response.json",
    }
    if method == "sig":
        request_meta["sig"] = {"nonce": nonce, "expiresAt": expires_at}

    base = f"blockchain/expectations/suites/{suite_id}/register_suite"

    # deadline sanity (chain time)
    now = int(w3.eth.get_block("latest")["timestamp"])
    if int(deadline) <= now:
        out = fail_out(
            "DEADLINE_PAST",
            f"Deadline in past (deadline={deadline}, chain_now={now})",
            details={"stage": "precheck", "method": method},
        )
        if client.storage and not args.no_store:
            out["saved"] = storage_write_pair(client, base, request_meta, out)
        print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
        return 1

    # estimate gas
    try:
        gas_est = fn.estimate_gas({"from": sender, "value": value_wei})
    except ContractLogicError as e:
        reason = revert_reason(e)
        out = fail_out("EVM_REVERT", reason, details={"stage": "estimate_gas", "method": method})
        if reason.lower() == "nonce used":
            out["error"]["hint"] = "Re-run prepare_suite_artifacts to get a fresh nonce/signature, then register again."

        if client.storage and not args.no_store:
            out["saved"] = storage_write_pair(client, base, request_meta, out)
        print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
        return 1

    # build tx
    tx = fn.build_transaction(
        {
            "from": sender,
            "value": value_wei,
            "nonce": w3.eth.get_transaction_count(sender),
            "chainId": w3.eth.chain_id,
            "gas": int(gas_est * 1.2),
        }
    )

    # optional EIP-1559 knobs
    if args.max_fee_gwei is not None:
        tx["maxFeePerGas"] = w3.to_wei(args.max_fee_gwei, "gwei")
    if args.max_priority_fee_gwei is not None:
        tx["maxPriorityFeePerGas"] = w3.to_wei(args.max_priority_fee_gwei, "gwei")

    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=int(args.timeout))

    out: Dict[str, Any] = {
        "ok": True,
        "network": network,
        "registry_address": registry_addr_raw,
        "suite_id": suite_id,
        "method": method,
        "sender": sender,
        "bounty_eth": args.bounty_eth,
        "gas_estimate": gas_est,
        "tx_hash": tx_hash,
        "receipt": _jsonify(dict(receipt)),
    }

    # ingest backend
    ingest_ref = client.blockchain.ingest_tx({"network": network, "address": registry_addr_raw, "tx_hash": tx_hash})
    ingest_ref = _dump(ingest_ref)
    out["ingest"] = {"task": ingest_ref, "status": None}

    if args.poll and isinstance(ingest_ref, dict) and isinstance(ingest_ref.get("task_id"), str):
        st = client.tasks.wait(
            ingest_ref["task_id"],
            timeout_s=args.timeout,
            poll_interval_s=args.interval,
            raise_on_failure=False,
        )
        out["ingest"]["status"] = _dump(st)

    # save override artifacts
    if client.storage and not args.no_store:
        out["saved"] = storage_write_pair(client, base, request_meta, out)

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
