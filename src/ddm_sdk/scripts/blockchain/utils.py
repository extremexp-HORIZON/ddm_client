from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, List
from hexbytes import HexBytes
import os
from ddm_sdk.scripts.expectations.utils import suite_dir_key, suite_datasets_key, suite_logs_key, norm_suite_id
from ddm_sdk.client import DdmClient

try:
    from web3.datastructures import AttributeDict
except Exception:
    AttributeDict = None
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------
# Keys (GLOBAL root)
# ---------------------------

def blockchain_root_key() -> str:
    # global root
    return "blockchain"


def blockchain_action_dir(action: str) -> str:
    # blockchain/<action>
    a = (action or "").strip().strip("/")
    if not a:
        raise ValueError("action is empty")
    return f"{blockchain_root_key()}/{a}"


# ---------------------------
# Logging + snapshots
# ---------------------------

def append_blockchain_log(
    client: DdmClient,
    *,
    action: str,
    ok: bool,
    details: Dict[str, Any] | None = None,
) -> None:
    """
    blockchain/logs.json  (append list of dicts)
    """
    if not client.storage:
        return

    key = f"{blockchain_root_key()}/logs"
    existing = client.storage.read_json(key)
    logs: List[Dict[str, Any]] = existing if isinstance(existing, list) else []

    logs.append(
        {
            "ts": utc_now_iso(),
            "action": action,
            "ok": bool(ok),
            "details": details or {},
        }
    )
    client.storage.write_json(key, logs)


def store_blockchain_snapshot(
    client: DdmClient,
    *,
    action: str,
    payload: Any,
    name: str = "run",
) -> Optional[str]:
    """
    blockchain/<action>/<timestamp>_<name>.json
    """
    if not client.storage:
        return None

    n = (name or "run").strip()
    key = f"{blockchain_action_dir(action)}/{utc_ts()}_{n}"
    return client.storage.write_json(key, payload)


# ---------------------------
# CLI JSON arg loader
# ---------------------------

def load_json_arg(*, json_text: Optional[str], json_file: Optional[str]) -> Dict[str, Any]:
    """
    Prefer --json-file. BOM-safe.
    """
    if json_text and json_file:
        raise SystemExit("Use only one of --json or --json-file")

    if json_file:
        p = Path(json_file).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"JSON file not found: {p}")
        try:
            obj = json.loads(p.read_text(encoding="utf-8-sig"))
        except Exception as e:
            raise SystemExit(f"--json-file is not valid JSON: {e}")
        if not isinstance(obj, dict):
            raise SystemExit("--json-file must contain a JSON object")
        return obj

    if json_text:
        try:
            obj = json.loads(json_text)
        except Exception as e:
            raise SystemExit(f"--json is not valid JSON: {e}")
        if not isinstance(obj, dict):
            raise SystemExit("--json must be a JSON object")
        return obj

    raise SystemExit("Provide either --json or --json-file")

def load_saved_suite_record(client: DdmClient, *, project_id: str, suite_id: str) -> Dict[str, Any]:
    if not client.storage:
        raise RuntimeError("Storage not configured (DDM_STORAGE_DIR).")

    key = f"expectations/suites/{suite_id}/suite"
    obj = client.storage.read_json(key)
    if not isinstance(obj, dict):
        raise FileNotFoundError(f"Suite record not found or not JSON object at storage key: {key}")
    return obj

def suite_datasets_key(*, suite_id: str) -> str:
    # -> expectations/suites/<suite_id>/datasets.json
    return f"{suite_dir_key(suite_id=suite_id)}/datasets"


def dataset_suites_key(*, dataset_id: str) -> str:
    # -> expectations/datasets/<dataset_id>/suites.json
    did = norm_suite_id(dataset_id)  # or norm_dataset_id if you have one
    return f"expectations/datasets/{did}/suites"

def store_suite_blockchain_artifacts(
    client: DdmClient,
    *,
    suite_id: str,
    action: str,
    payload: Dict[str, Any],
    out: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Store blockchain artifacts under:
      expectations/suites/<suite_id>/blockchain/<action>/
    """
    if not client.storage:
        return {}

    base = f"{suite_dir_key(suite_id=suite_id)}/blockchain/{action}"

    saved: Dict[str, Any] = {}
    saved["request"] = client.storage.write_json(f"{base}/request", payload)
    saved["response"] = client.storage.write_json(f"{base}/response", out)

    # convenience: store extracted suite_hash if present
    if out.get("suite_hash") is not None:
        saved["suite_hash"] = client.storage.write_json(f"{base}/suite_hash", {"suite_hash": out["suite_hash"]})

    return saved

def store_suite_blockchain_artifacts(
    client: DdmClient,
    *,
    suite_id: str,
    action: str,
    payload: Dict[str, Any],
    out: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Store blockchain artifacts under:
      expectations/suites/<suite_id>/blockchain/<action>/
    """
    if not client.storage:
        return {}

    base = f"{suite_dir_key(suite_id=suite_id)}/blockchain/{action}"

    saved: Dict[str, Any] = {}
    saved["request"] = client.storage.write_json(f"{base}/request", payload)
    saved["response"] = client.storage.write_json(f"{base}/response", out)

    # convenience: store extracted suite_hash if present
    if out.get("suite_hash") is not None:
        saved["suite_hash"] = client.storage.write_json(f"{base}/suite_hash", {"suite_hash": out["suite_hash"]})

    return saved

def store_prepare_suite_latest(
    client: DdmClient,
    *,
    suite_id: str,
    dataset_id: str,
    request_payload: dict,
    response_payload: dict,
) -> dict:
    """
    Stores latest request/response for suite+dataset.
    """
    base = f"blockchain/expectations/suites/{suite_id}/datasets/{dataset_id}/prepare_suite_artifacts"
    return {
        "request": client.storage.write_json(f"{base}/request", request_payload),
        "response": client.storage.write_json(f"{base}/response", response_payload),
    }




def dataset_id_from_suite_links_or_log(client: DdmClient, *, project_id: str, suite_id: str) -> Optional[str]:
    if not client.storage:
        return None

    # 1) datasets.json link
    ds_obj = client.storage.read_json(suite_datasets_key(suite_id=suite_id))
    if isinstance(ds_obj, list):
        for x in ds_obj:
            if isinstance(x, str) and x.strip():
                return x.strip()

    # 2) log.json details.dataset_id (use most recent)
    log_obj = client.storage.read_json(suite_logs_key(project_id=project_id, suite_id=suite_id))
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

def load_contract_index(client: DdmClient, *, network: str) -> Dict[str, Any]:
    """
    Reads storage key:
      blockchain/contracts/<network>/_index
    which you save as .../_index.json on disk.
    """
    if not client.storage:
        raise RuntimeError("Storage not configured (DDM_STORAGE_DIR).")
    key = f"blockchain/contracts/{network}/_index"
    obj = client.storage.read_json(key)
    if not isinstance(obj, dict):
        raise FileNotFoundError(f"Contract index not found at storage key: {key}")
    return obj


def registry_address_from_index(client: DdmClient, *, network: str, name: str) -> str:
    """
    Finds contract by name in the saved index.
    """
    idx = load_contract_index(client, network=network)
    items = idx.get("contracts")
    if not isinstance(items, list):
        raise RuntimeError("Invalid index format: missing 'contracts' list")

    for it in items:
        if isinstance(it, dict) and (it.get("name") == name):
            addr = it.get("address")
            if isinstance(addr, str) and addr.strip():
                return addr.strip()

    raise FileNotFoundError(f"Contract '{name}' not found in blockchain/contracts/{network}/_index")



def _jsonify(x: Any) -> Any:
    """
    Recursively convert common web3 / pydantic objects to JSON-serializable primitives:
      - pydantic model -> dict via model_dump(mode="json")
      - web3 AttributeDict -> dict (if available)
      - mapping-ish objects -> dict (best effort)
      - HexBytes/bytes/bytearray -> 0x hex string
      - sets/tuples -> lists
      - unknown -> str(x) (last resort)
    """
    # local imports to avoid hard deps / import-time failures
    try:
        from hexbytes import HexBytes as _HexBytes  # type: ignore
    except Exception:
        _HexBytes = ()  # type: ignore

    try:
        from web3.datastructures import AttributeDict as _AttributeDict  # type: ignore
    except Exception:
        _AttributeDict = ()  # type: ignore

    if x is None or isinstance(x, (str, int, float, bool)):
        return x

    # pydantic models (v2)
    if hasattr(x, "model_dump"):
        return _jsonify(x.model_dump(mode="json", exclude_none=False))

    # web3 AttributeDict (if present)
    if isinstance(x, _AttributeDict):
        return _jsonify(dict(x))

    # HexBytes / bytes-like
    if isinstance(x, _HexBytes):
        return x.hex()
    if isinstance(x, (bytes, bytearray)):
        return "0x" + bytes(x).hex()

    # dict
    if isinstance(x, dict):
        return {str(k): _jsonify(v) for k, v in x.items()}

    # list/tuple/set
    if isinstance(x, (list, tuple, set)):
        return [_jsonify(v) for v in x]

    # "AttributeDict-ish" / mapping-ish fallback:
    # must be after bytes/str checks so we don't mis-handle them
    if hasattr(x, "items"):
        try:
            return {str(k): _jsonify(v) for k, v in dict(x).items()}
        except Exception:
            pass

    return str(x)


def revert_reason(e: Exception) -> str:
    """
    Extraction of revert reason from web3 ContractLogicError.
    Typical string: "execution reverted: <reason>"
    """
    msg = str(e) or ""
    # common pattern
    if "execution reverted:" in msg:
        return msg.split("execution reverted:", 1)[1].strip().strip("'").strip('"')
    if "execution reverted" in msg:
        return "execution reverted"
    return msg

def fail_out(code: str, message: str, *, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {"ok": False, "error": {"code": code, "message": message}}
    if details:
        out["error"]["details"] = details
    return out

def rpc_url(network: str) -> str:
    env1 = f"DDM_RPC_{network.upper()}"
    env2 = f"{network.upper()}_RPC_URL"
    v = os.getenv(env1) or os.getenv(env2) or os.getenv("DDM_RPC_URL")
    if not v:
        raise SystemExit(f"Missing RPC url. Set {env1} or {env2} or DDM_RPC_URL")
    return v

def user_pk() -> str:
    v = os.getenv("DDM_USER_PK")
    if not v or not v.strip():
        raise SystemExit("Missing signer private key. Set DDM_USER_PK.")
    return v.strip()

def storage_read_json(client: DdmClient, key: str) -> Any:
    if not client.storage:
        raise RuntimeError("Storage not configured (DDM_STORAGE_DIR).")
    return client.storage.read_json(key)

def storage_write_pair(client: DdmClient, base: str, req: Dict[str, Any], resp: Dict[str, Any]) -> Dict[str, str]:
    if not client.storage:
        raise RuntimeError("Storage not configured (DDM_STORAGE_DIR).")
    return {
        "request": client.storage.write_json(f"{base}/request", _jsonify(req)),
        "response": client.storage.write_json(f"{base}/response", _jsonify(resp)),
    }

def load_abi_from_storage(client: DdmClient, *, network: str, address: str) -> Any:
    # ABI in: blockchain/contracts/<network>/<address>.abi.json on disk
    # but fs storage keys are without ".json"
    key = f"blockchain/contracts/{network}/{address}.abi"
    abi = storage_read_json(client, key)
    if abi is None:
        raise FileNotFoundError(f"ABI not found at storage key: {key}")
    return abi


def normalize_sig(sig: str) -> bytes:
    """
    Returns 65-byte signature suitable for Solidity ECDSA.recover.
    Accepts '0x...' or '...' hex.
    Fixes:
      - missing 0x
      - odd-length hex
      - v in {0,1} -> {27,28}
    """
    if not isinstance(sig, str) or not sig.strip():
        raise ValueError("signature is empty")

    s = sig.strip()
    if s.startswith("0x") or s.startswith("0X"):
        s = s[2:]

    # must be even length hex
    if len(s) % 2 == 1:
        s = "0" + s

    b = bytes.fromhex(s)

    if len(b) != 65:
        raise ValueError(f"signature must be 65 bytes, got {len(b)} bytes (hex len={len(s)})")

    v = b[64]
    if v in (0, 1):
        b = b[:64] + bytes([v + 27])

    return b

def _dataset_file_format_from_suite(client: DdmClient, *, suite_id: str) -> str:
    key = f"expectations/suites/{suite_id}/suite"
    suite = storage_read_json(client, key)

    # suite might be wrapped or not; handle both
    if isinstance(suite, dict) and isinstance(suite.get("suite"), dict):
        suite = suite["suite"]

    if not isinstance(suite, dict):
        raise SystemExit(f"Suite record not found or invalid at {key}")

    ft = suite.get("file_types") or suite.get("fileTypes")
    if isinstance(ft, list):
        for x in ft:
            if isinstance(x, str) and x.strip():
                return x.strip().lower()

    # fallback if missing
    return "csv"

# -----------------------
# reward helpers
# -----------------------


def _unwrap_task_envelope(obj: Any) -> Any:
    if isinstance(obj, dict):
        if isinstance(obj.get("result"), dict):
            r = obj["result"].get("value") or obj["result"].get("result") or obj["result"]
            if r is not None:
                return r
        if isinstance(obj.get("status"), dict):
            r = obj["status"].get("result")
            if r is not None:
                return r
    return obj

def _write_latest_pair(client: DdmClient, base: str, req: Dict[str, Any], resp: Dict[str, Any]) -> Dict[str, str]:
    return {
        "request": client.storage.write_json(f"{base}/request", _jsonify(req)),
        "response": client.storage.write_json(f"{base}/response", _jsonify(resp)),
    }

def _require_str(d: Dict[str, Any], key: str) -> str:
    v = d.get(key)
    if not isinstance(v, str) or not v.strip():
        raise SystemExit(f"Missing '{key}' in prepare_reward response")
    return v.strip()


def _normalize_0x(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return s
    return s if s.startswith("0x") else "0x" + s


def _topic_to_int(topic_hex: str) -> int:
    # topic is 32-byte hex string (no 0x sometimes)
    t = topic_hex.strip()
    if not t.startswith("0x"):
        t = "0x" + t
    return int(t, 16)


def _derive_request_id_from_register_suite_storage(client: DdmClient, suite_id: str) -> int:
    """
    Request-id discovery using saved register_suite receipt.
    Storage key used by scripts typically:
      blockchain/expectations/suites/<suite_id>/register_suite/response
      1) Look for an event log that clearly contains requestId in topic[1]
      2) Fallback: scan logs for any topic that looks like a small integer (works in many challenge setups)
    """
    key = f"blockchain/expectations/suites/{suite_id}/register_suite/response"
    obj = storage_read_json(client, key)
    val = _unwrap_task_envelope(obj)

    if not isinstance(val, dict):
        raise SystemExit(f"register_suite response at {key} is not a dict")

    receipt = val.get("receipt")
    if not isinstance(receipt, dict):
        raise SystemExit(f"Missing receipt in {key}")

    logs = receipt.get("logs")
    if not isinstance(logs, list) or not logs:
        raise SystemExit(f"No logs in receipt in {key}")

    # ---- Strategy 1: find a log with 3-4 topics where topic[1] looks like request id ----
    # topics[0]=eventSigHash, topics[1]=requestId (indexed), topics[2]=requester (indexed), ...
    for lg in logs:
        if not isinstance(lg, dict):
            continue
        topics = lg.get("topics")
        if not isinstance(topics, list) or len(topics) < 2:
            continue

        # topic[1] is the indexed requestId
        t1 = topics[1]
        if isinstance(t1, str):
            rid = _topic_to_int(t1)
            if 0 < rid < 10**12:
                return rid

    # ---- Strategy 2: scan all topics for a small-ish integer ----
    for lg in logs:
        if not isinstance(lg, dict):
            continue
        topics = lg.get("topics")
        if not isinstance(topics, list):
            continue
        for t in topics:
            if isinstance(t, str):
                rid = _topic_to_int(t)
                if 0 < rid < 10**12:
                    return rid

    raise SystemExit(
        f"Could not auto-derive request-id from {key}. "
        f"Pass --request-id explicitly or inspect that receipt's logs/topics."
    )



