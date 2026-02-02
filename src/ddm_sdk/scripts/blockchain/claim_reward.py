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
    normalize_sig,
    _unwrap_task_envelope,
    _normalize_0x,
    _require_str
)

# -----------------------
# main
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-claim-reward-and-mint",
        description="Claim ETH reward + mint NFT in one tx via Request contract",
    )
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--catalog_id", required=True)

    ap.add_argument("--request-id", required=True, type=int, help="DatasetRequest id (uint256) on your Request contract")

    ap.add_argument(
        "--request-contract-name",
        default="DatasetRequestRegistry",
        help="Name in contracts index for the contract that has claimRewardForDatasetAndMint",
    )
    ap.add_argument("--request-contract-address", default=None, help="Override request contract address (optional)")

    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    client = DdmClient.from_env()
    ensure_authenticated(client)
    if not client.storage:
        raise SystemExit("Storage not configured (DDM_STORAGE_DIR).")

    network = args.network.strip()
    suite_id = args.suite_id.strip()
    catalog_id = args.catalog_id.strip()
    request_id = int(args.request_id)

    # --- load prepared reward artifact (your standard path)
    prep_key = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/prepare_reward/response"
    prep_obj = storage_read_json(client, prep_key)
    prep_val = _unwrap_task_envelope(prep_obj)

    if isinstance(prep_val, dict) and isinstance(prep_val.get("value"), dict):
        reward = prep_val["value"]
    elif isinstance(prep_val, dict):
        reward = prep_val
    else:
        raise SystemExit(f"prepare_reward response invalid at: {prep_key}")

    # values for claimRewardForDatasetAndMint
    dataset_fingerprint = _require_str(reward, "datasetFingerprint")
    nft_category = _require_str(reward, "category")
    level_hex = _require_str(reward, "level")
    metadata_uri = _require_str(reward, "metadataURI")
    deadline_s = _require_str(reward, "deadline")
    signature_hex = _require_str(reward, "signature")

    # request contract address
    req_addr_raw = (
        (args.request_contract_address.strip() if args.request_contract_address else None)
        or registry_address_from_index(client, network=network, name=args.request_contract_name)
    )
    req_addr = Web3.to_checksum_address(req_addr_raw)
    abi = load_abi_from_storage(client, network=network, address=req_addr_raw)

    # web3
    w3 = Web3(Web3.HTTPProvider(rpc_url(network)))
    if not w3.is_connected():
        raise SystemExit(f"Web3 cannot connect to RPC for network={network}")

    pk = user_pk()
    acct = w3.eth.account.from_key(pk)
    sender = acct.address

    contract = w3.eth.contract(address=req_addr, abi=abi)

    sig_bytes = normalize_sig(signature_hex)

    request_meta: Dict[str, Any] = {
        "network": network,
        "suite_id": suite_id,
        "catalog_id": catalog_id,
        "request_contract_address": req_addr_raw,
        "request_id": request_id,
        "sender": sender,
        "call_args": {
            "id": request_id,
            "datasetFingerprint": dataset_fingerprint,
            "nftCategory": nft_category,
            "level": level_hex,
            "metadataURI": metadata_uri,
            "deadline": deadline_s,
            "claimSignature": "0x" + sig_bytes.hex(),
        },
        "prepare_reward_key": prep_key,
    }

    # estimate + send
    try:
        fn = contract.functions.claimRewardForDatasetAndMint(
            int(request_id),
            Web3.to_bytes(hexstr=dataset_fingerprint),
            nft_category,
            Web3.to_bytes(hexstr=level_hex),
            metadata_uri,
            int(deadline_s),
            sig_bytes,
        )
        gas_est = fn.estimate_gas({"from": sender})
    except ContractLogicError as e:
        out = fail_out("EVM_REVERT", revert_reason(e), details={"stage": "estimate_gas"})
        if client.storage and not args.no_store:
            base = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/claim_reward_and_mint"
            out["saved"] = storage_write_pair(client, base, request_meta, out)
        print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
        return 1

    tx = fn.build_transaction(
        {
            "from": sender,
            "nonce": w3.eth.get_transaction_count(sender),
            "chainId": w3.eth.chain_id,
            "gas": int(gas_est * 12 // 10),
        }
    )
    signed = w3.eth.account.sign_transaction(tx, private_key=pk)
    tx_hash = _normalize_0x(w3.eth.send_raw_transaction(signed.raw_transaction).hex())
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=int(args.timeout))

    out: Dict[str, Any] = {
        "ok": True,
        "network": network,
        "request_contract_address": req_addr_raw,
        "request_id": request_id,
        "suite_id": suite_id,
        "catalog_id": catalog_id,
        "sender": sender,
        "tx_hash": tx_hash,
        "receipt": _jsonify(dict(receipt)),
        "request_meta": request_meta,
    }

    # ingest
    ingest_ref = client.blockchain.ingest_tx({"network": network, "address": req_addr_raw, "tx_hash": tx_hash})
    ingest_ref = ingest_ref.model_dump() if hasattr(ingest_ref, "model_dump") else _jsonify(ingest_ref)
    out["ingest"] = {"task": ingest_ref, "status": None}

    if args.poll and isinstance(ingest_ref, dict) and isinstance(ingest_ref.get("task_id"), str):
        st = client.tasks.wait(
            ingest_ref["task_id"],
            timeout_s=args.timeout,
            poll_interval_s=args.interval,
            raise_on_failure=False,
        )
        out["ingest"]["status"] = st.model_dump() if hasattr(st, "model_dump") else _jsonify(st)

    if client.storage and not args.no_store:
        base = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/claim_reward_and_mint"
        out["saved"] = storage_write_pair(client, base, request_meta, out)

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
