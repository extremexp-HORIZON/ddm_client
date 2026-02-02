from __future__ import annotations

import argparse
from http import client
import json
from typing import Any, Dict

from web3 import Web3
from web3.exceptions import ContractLogicError
from eth_abi import encode as abi_encode

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.blockchain.extractors import extract_suite_hash, extract_report_uri
from ddm_sdk.scripts.blockchain.utils import _dataset_file_format_from_suite, normalize_sig

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


def _to_eth_signed_message_hash(inner32: bytes) -> bytes:
    # OZ ECDSA.toEthSignedMessageHash for 32 bytes
    return Web3.keccak(b"\x19Ethereum Signed Message:\n32" + inner32)


def _normalize_0x(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    return s if s.startswith("0x") else "0x" + s


def _register_dataset_inner_hash(
    *,
    uri: str,
    suite_hash: str,
    file_format: str,
    report_uri: str,
    uploader: str,
    nonce: int,
) -> bytes:
    """
    EXACT Solidity match:
      keccak256(abi.encode("Register dataset:", uri, suiteHash, fileFormat, reportUri, msg.sender, nonce))
    """
    suite_b32 = Web3.to_bytes(hexstr=suite_hash)
    if len(suite_b32) != 32:
        raise ValueError(f"suiteHash must be bytes32 (32 bytes). got len={len(suite_b32)}")

    encoded = abi_encode(
        ["string", "string", "bytes32", "string", "string", "address", "uint256"],
        [
            "Register dataset:",
            uri,
            suite_b32,
            file_format,
            report_uri,
            Web3.to_checksum_address(uploader),
            int(nonce),
        ],
    )
    return Web3.keccak(encoded)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-register-dataset", description="Register dataset on-chain (DatasetRegistry)")
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--catalog_id", required=True)

    ap.add_argument("--dataset-uri", required=True, help='e.g. "projects/.../file.csv" or ipfs://...')
    ap.add_argument("--registry-name", default="DatasetRegistry", help="Name in contracts index")
    ap.add_argument("--registry-address", default=None, help="Optional override DatasetRegistry address")

    ap.add_argument("--signature", default=None, help="Optional MetaMask signature 0x... (otherwise sign with DDM_USER_PK)")
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
    dataset_uri = args.dataset_uri.strip()

    # registry addr
    registry_addr_raw = (args.registry_address.strip() if args.registry_address else None) or registry_address_from_index(
        client, network=network, name=args.registry_name
    )
    registry_addr = Web3.to_checksum_address(registry_addr_raw)
    abi = load_abi_from_storage(client, network=network, address=registry_addr_raw)

    # read suite_hash from saved prepare_suite artifacts
    suite_prepare_key = f"blockchain/expectations/suites/{suite_id}/prepare_suite_artifacts/response"
    suite_prepare_obj = storage_read_json(client, suite_prepare_key)
    suite_prepare_val = _unwrap_task_envelope(suite_prepare_obj)
    suite_hash = extract_suite_hash(suite_prepare_val)
    if not isinstance(suite_hash, str) or not suite_hash.startswith("0x"):
        raise SystemExit(f"Missing suite_hash in {suite_prepare_key}")

    # read report_uri (+ file_format if present) from saved prepare_report artifacts
    report_prepare_key = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/prepare_report/response"
    report_prepare_obj = storage_read_json(client, report_prepare_key)
    report_prepare_val = _unwrap_task_envelope(report_prepare_obj)

    report_uri = extract_report_uri(report_prepare_val)
    if not isinstance(report_uri, str) or not report_uri.startswith("ipfs://"):
        raise SystemExit(f"Missing report_uri in {report_prepare_key}")
    
    file_format = _dataset_file_format_from_suite(client, suite_id=suite_id)


    # web3 init
    w3 = Web3(Web3.HTTPProvider(rpc_url(network)))
    if not w3.is_connected():
        raise SystemExit(f"Web3 cannot connect to RPC for network={network}")

    pk = user_pk()  # this script SENDS the tx, so we need the pk
    acct = w3.eth.account.from_key(pk)
    sender = acct.address

    contract = w3.eth.contract(address=registry_addr, abi=abi)

    # nonce comes from chain
    chain_nonce = contract.functions.nonces(sender).call()
    if not isinstance(chain_nonce, int):
        raise SystemExit("Could not read nonces(sender) from chain")

    # Build hashes exactly like Solidity
    inner = _register_dataset_inner_hash(
        uri=dataset_uri,
        suite_hash=suite_hash,
        file_format=file_format,
        report_uri=report_uri,
        uploader=sender,
        nonce=chain_nonce,
    )
    eth_hash = _to_eth_signed_message_hash(inner)

    # signature bytes for contract + hex for request_meta
    if isinstance(args.signature, str) and args.signature.strip():
        sig_bytes = normalize_sig(args.signature)  # returns bytes
    else:
        sig_obj = w3.eth.account._sign_hash(eth_hash, private_key=pk)
        sig_bytes = sig_obj.signature

    signature_hex = "0x" + sig_bytes.hex()

    request_meta: Dict[str, Any] = {
        "network": network,
        "suite_id": suite_id,
        "catalog_id": catalog_id,
        "registry_name": args.registry_name,
        "registry_address": registry_addr_raw,
        "sender": sender,
        "call_args": {
            "uri": dataset_uri,
            "suiteHash": suite_hash,
            "fileFormat": file_format,
            "reportUri": report_uri,
            "nonce": chain_nonce,
            "signature": signature_hex,
            "inner_hash_hex": "0x" + inner.hex(),
            "message_hash_hex": "0x" + eth_hash.hex(),
        },
        "suite_prepare_key": suite_prepare_key,
        "report_prepare_key": report_prepare_key,
    }

    # estimate gas
    try:
        fn = contract.functions.registerDataset(
            dataset_uri,
            Web3.to_bytes(hexstr=suite_hash),
            file_format,
            report_uri,
            int(chain_nonce),
            sig_bytes,  # bytes
        )
        gas_est = fn.estimate_gas({"from": sender})
    except ContractLogicError as e:
        out = fail_out("EVM_REVERT", revert_reason(e), details={"stage": "estimate_gas"})
        if client.storage and not args.no_store:
            base = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/register_dataset"
            out["saved"] = storage_write_pair(client, base, request_meta, out)
        print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
        return 1

    # send tx
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

    # fingerprint from event
    fingerprint = None
    try:
        for lg in receipt.get("logs", []):
            # must be our registry contract
            if (lg.get("address") or "").lower() != registry_addr.lower():
                continue

            topics = lg.get("topics") or []
            # need at least 4 topics: sig, uploader, suiteHash, fingerprint
            if len(topics) >= 4:
                t3 = topics[3]
                if isinstance(t3, (bytes, bytearray)):
                    fingerprint = "0x" + bytes(t3).hex()
                elif isinstance(t3, str):
                    # already hex string without 0x sometimes
                    fingerprint = t3 if t3.startswith("0x") else "0x" + t3
                break
    except Exception:
        fingerprint = None


    out: Dict[str, Any] = {
        "ok": True,
        "network": network,
        "registry_address": registry_addr_raw,
        "suite_id": suite_id,
        "catalog_id": catalog_id,
        "sender": sender,
        "tx_hash": tx_hash,  # ✅ always 0x
        "receipt": _jsonify(dict(receipt)),
        "fingerprint": fingerprint,
        "request_meta": request_meta,
    }

    # ingest backend (✅ always 0x)
    ingest_ref = client.blockchain.ingest_tx(
        {"network": network, "address": registry_addr_raw, "tx_hash": tx_hash}
    )
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
        base = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/register_dataset"
        out["saved"] = storage_write_pair(client, base, request_meta, out)

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
