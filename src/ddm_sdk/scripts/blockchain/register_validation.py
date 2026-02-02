from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

from web3 import Web3
from web3.exceptions import ContractLogicError

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.blockchain.utils import _jsonify, fail_out, revert_reason


def _rpc_url(network: str) -> str:
    env1 = f"DDM_RPC_{network.upper()}"
    env2 = f"{network.upper()}_RPC_URL"
    v = os.getenv(env1) or os.getenv(env2) or os.getenv("DDM_RPC_URL")
    if not v:
        raise SystemExit(f"Missing RPC url. Set {env1} or {env2} or DDM_RPC_URL")
    return v


def _pk() -> str:
    v = os.getenv("DDM_HUMANVAL_PK")
    if not v or not v.strip():
        raise SystemExit("Missing human validator private key. Set DDM_HUMANVAL_PK.")
    return v.strip()


def _store_latest(client: DdmClient, base: str, req: Dict[str, Any], resp: Dict[str, Any]) -> Dict[str, str]:
    return {
        "request": client.storage.write_json(f"{base}/request", _jsonify(req)),
        "response": client.storage.write_json(f"{base}/response", _jsonify(resp)),
    }


def _load_json_storage(client: DdmClient, key: str) -> Any:
    if not client.storage:
        raise RuntimeError("Storage not configured (DDM_STORAGE_DIR).")
    return client.storage.read_json(key)


def _load_abi(client: DdmClient, *, network: str, address: str) -> Any:
    # your dump script uses: blockchain/contracts/<network>/<addr>.abi
    key = f"blockchain/contracts/{network}/{address}.abi"
    abi = _load_json_storage(client, key)
    if abi is None:
        raise FileNotFoundError(f"ABI not found at storage key: {key}")
    return abi


def _registry_address_from_index(client: DdmClient, *, network: str, name: str) -> str:
    idx = _load_json_storage(client, f"blockchain/contracts/{network}/_index")
    if not isinstance(idx, dict):
        raise FileNotFoundError(f"Contracts index missing at blockchain/contracts/{network}/_index.json")

    contracts = idx.get("contracts")
    if not isinstance(contracts, list):
        raise RuntimeError("Contracts index malformed: missing 'contracts' list")

    for c in contracts:
        if isinstance(c, dict) and c.get("name") == name and isinstance(c.get("address"), str):
            return c["address"]
    raise SystemExit(f"Contract {name} not found in saved index for network={network}")


def _unwrap_prepared(obj: Dict[str, Any]) -> Dict[str, Any]:
    prepared = None
    if isinstance(obj.get("result"), dict):
        prepared = obj["result"].get("result") or obj["result"].get("value") or obj["result"]
    if prepared is None and isinstance(obj.get("status"), dict):
        prepared = obj["status"].get("result")
    if prepared is None:
        prepared = obj
    if not isinstance(prepared, dict):
        raise RuntimeError("Prepared validation response could not be unwrapped into a dict.")
    return prepared


def _get_str(d: Dict[str, Any], *keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default


def _get_bool(d: Dict[str, Any], *keys: str, default: Optional[bool] = None) -> Optional[bool]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in ("true", "1", "yes"):
                return True
            if vv in ("false", "0", "no"):
                return False
    return default


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-register-validation", description="Submit ValidationRegistry.submitValidation using saved prepare_validation artifacts")
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--registry-address", default=None, help="Optional override. Default: from saved contracts index.")
    ap.add_argument("--registry-name", default="ValidationRegistry", help="Name in contracts index.")
    ap.add_argument("--dataset-fingerprint", required=True)

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
    fp = args.dataset_fingerprint.strip()

    # contract + abi
    if args.registry_address and args.registry_address.strip():
        reg_addr_raw = args.registry_address.strip()
    else:
        reg_addr_raw = _registry_address_from_index(client, network=network, name=args.registry_name)

    reg_addr = Web3.to_checksum_address(reg_addr_raw)
    abi = _load_abi(client, network=network, address=reg_addr_raw)

    # load prepared artifacts
    prep_key = f"blockchain/validations/{fp}/prepare_validation/response"
    prep_obj = _load_json_storage(client, prep_key)
    if not isinstance(prep_obj, dict):
        raise FileNotFoundError(f"Prepared validation response not found at storage key: {prep_key}")

    prepared = _unwrap_prepared(prep_obj)

    validation_hash = _get_str(prepared, "validationHash", "validation_hash")
    result_uri = _get_str(prepared, "resultURI", "result_uri")
    report_uri = _get_str(prepared, "reportURI", "report_uri", default="") or ""
    successful = _get_bool(prepared, "successful", default=True)


    missing = [k for k, v in {
        "validationHash": validation_hash,
        "resultURI": result_uri,
    }.items() if v is None or v == ""]
    if missing:
        raise SystemExit(f"Missing fields in prepared validation artifacts: {missing}")

    # web3
    w3 = Web3(Web3.HTTPProvider(_rpc_url(network)))
    if not w3.is_connected():
        raise SystemExit(f"Web3 cannot connect to RPC for network={network}")

    acct = w3.eth.account.from_key(_pk())
    sender = acct.address

    contract = w3.eth.contract(address=reg_addr, abi=abi)

    fn = contract.functions.submitValidation(
        fp,
        validation_hash,
        result_uri,
        report_uri,
        bool(successful),
    )

    request_meta = {
        "network": network,
        "registry_address": reg_addr_raw,
        "registry_name": args.registry_name,
        "dataset_fingerprint": fp,
        "sender": sender,
        "prepared_key": f"{prep_key}.json",
    }

    # estimate gas (catch all reverts)
    try:
        gas_est = fn.estimate_gas({"from": sender})
    except ContractLogicError as e:
        reason = revert_reason(e)
        out = fail_out(
            "EVM_REVERT",
            reason,
            details={"stage": "estimate_gas", "fn": "submitValidation"},
        )
        if client.storage and not args.no_store:
            base = f"blockchain/validations/{fp}/register_validation"
            out["saved"] = _store_latest(client, base, request_meta, out)
        print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
        return 1

    tx = fn.build_transaction(
        {
            "from": sender,
            "nonce": w3.eth.get_transaction_count(sender),
            "chainId": w3.eth.chain_id,
            "gas": int(gas_est * 1.2),
        }
    )

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
        "registry_address": reg_addr_raw,
        "dataset_fingerprint": fp,
        "sender": sender,
        "gas_estimate": gas_est,
        "tx_hash": tx_hash,
        "receipt": _jsonify(dict(receipt)),
    }
    # ingest backend 
    ingest_ref = client.blockchain.ingest_tx({"network": network, "address": reg_addr_raw, "tx_hash": tx_hash})
    ingest_ref = ingest_ref.model_dump() if hasattr(ingest_ref, "model_dump") else dict(ingest_ref)
    out["ingest"] = {"task": ingest_ref, "status": None}

    if args.poll and isinstance(ingest_ref, dict) and isinstance(ingest_ref.get("task_id"), str):
        st = client.tasks.wait(
            ingest_ref["task_id"],
            timeout_s=args.timeout,
            poll_interval_s=args.interval,
            raise_on_failure=False,
        )
        out["ingest"]["status"] = st.model_dump() if hasattr(st, "model_dump") else st

    # save stable override
    if client.storage and not args.no_store:
        base = f"blockchain/validations/{fp}/register_validation"
        out["saved"] = _store_latest(client, base, request_meta, out)

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
