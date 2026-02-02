from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional
import hashlib
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.models.blockchain import (
    PrepareSuiteBody,
    PrepareRewardBody,
    PrepareValidationBody,
    IngestTxBody,
)


def task_id_from_taskref(obj: Any) -> Optional[str]:
    # supports TaskRef model or dict-ish
    for k in ("task_id", "id"):
        v = getattr(obj, k, None)
        if isinstance(v, str) and v:
            return v
        if isinstance(obj, dict) and isinstance(obj.get(k), str) and obj[k]:
            return obj[k]
    return None

def poll_task_until_ready(client: DdmClient, task_id: str, *, timeout_s: int = 60, interval_s: float = 1.0):
    t0 = time.time()
    last_state = None
    while time.time() - t0 < timeout_s:
        st = client.tasks.status(task_id)
        if st.state != last_state:
            print(f"  ⏳ task {task_id} state={st.state}")
            last_state = st.state
        if st.is_ready():
            return st
        time.sleep(interval_s)
    return None

def get_task_value(client: DdmClient, task_id: str) -> Any:
    # Prefer /result because you said backend returns ready/successful/value.
    res = client.tasks.result(task_id)
    if not res.ready:
        return None
    return res.value

def find_first_str(d: Any, keys: list[str]) -> Optional[str]:
    if isinstance(d, dict):
        for k in keys:
            v = d.get(k)
            if isinstance(v, str) and v:
                return v
        # search one level deep
        for v in d.values():
            if isinstance(v, dict):
                out = find_first_str(v, keys)
                if out:
                    return out
    return None

def extract_report_uri(task_value: Any) -> Optional[str]:
    return find_first_str(task_value, ["report_uri", "ipfs_uri", "uri"])

def extract_suite_hash(task_value: Any) -> Optional[str]:
    return find_first_str(task_value, ["suite_hash", "hash"])

def extract_dataset_uri(task_value: Any) -> Optional[str]:
    return find_first_str(task_value, ["dataset_uri", "datasetURI", "uri"])



def pick_first_nonempty(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v and str(v).strip():
            return str(v).strip()
    return None

def task_id_from_taskref(obj: Any) -> Optional[str]:
    # TaskRef model might be {task_id: "..."} or dict
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get("task_id") or obj.get("id")
    return getattr(obj, "task_id", None) or getattr(obj, "id", None)

def try_prepare_report_task(client: DdmClient, *, network: str, catalog_id: Optional[str]) -> Optional[str]:
    """
    Calls prepare_report_ipfs_uri and returns task_id (NOT the report_uri).
    """
    if not catalog_id:
        return None
    t = safe_call(
        f"blockchain.prepare_report_ipfs_uri(catalog_id={catalog_id})",
        lambda: client.blockchain.prepare_report_ipfs_uri(
            catalog_id=catalog_id,
            network=network,
            include_report=True,
        ),
    )
    return task_id_from_taskref(t)

def must_str(label: str, v: Optional[str]) -> Optional[str]:
    if not v:
        print(f"⚠️ missing {label}")
    return v


def sha256_hex_of_file(path: str) -> Optional[str]:
    p = Path(path)
    if not p.exists():
        return None
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

# ----------------------------
# helpers (same style everywhere)
# ----------------------------

def safe_call(label: str, fn: Callable[[], Any]) -> Any:
    """Run a call, print a friendly result, keep the test running on errors."""
    try:
        out = fn()
        print(f"✅ {label}")
        return out
    except Exception as e:
        print(f"❌ {label} failed: {type(e).__name__}: {e}")
        # uncomment if you want full trace every time:
        # traceback.print_exc()
        return None

def getenv_str(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name, default)
    if v is None:
        return None
    v = v.strip()
    return v if v else None

def getenv_bool(name: str, default: bool = False) -> bool:
    v = getenv_str(name)
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "y", "on"}

def getenv_int(name: str) -> Optional[int]:
    v = getenv_str(name)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        print(f"⚠️ {name} should be int, got {v!r}")
        return None

def safe_filename(s: str) -> str:
    # windows-safe
    return "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in s)

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def normalize_abi(abi: Any) -> Any:
    # backend sometimes returns abi as list, sometimes dict, sometimes json-string.
    if isinstance(abi, str):
        try:
            return json.loads(abi)
        except Exception:
            return abi
    return abi

def suite_id_from_suite_payload(suite_payload: Any) -> Optional[str]:
    if isinstance(suite_payload, dict):
        return (
            suite_payload.get("id")
            or suite_payload.get("suite_id")
            or suite_payload.get("expectation_suite_id")
        )
    return None

def build_suite_dict(suite_payload: dict, *, fallback_suite_id: str, fallback_category: str) -> dict:
    sid = suite_id_from_suite_payload(suite_payload) or fallback_suite_id
    file_types = suite_payload.get("file_types") or suite_payload.get("fileTypes") or ["csv"]
    suite_name = suite_payload.get("suite_name") or suite_payload.get("name") or "new"

    return {
        "expectation_suite_id": sid,
        "name": suite_name,
        "description": suite_payload.get("description") or "",
        "expectations": suite_payload.get("expectations") or [],
        "column_descriptions": suite_payload.get("column_descriptions") or suite_payload.get("columnDescriptions") or {},
        "column_names": suite_payload.get("column_names") or suite_payload.get("columnNames") or [],
        "file_types": file_types,
        "category": suite_payload.get("category") or fallback_category,
    }



# ----------------------------
# dump all contracts
# ----------------------------

def dump_all_contracts(client: DdmClient, *, network: str) -> list[dict[str, Any]]:
    """
    Fetch ALL contracts (paged) and write their details+ABI to out/blockchain/<network>/.
    Returns a list of summary dicts for index file.
    """
    out_dir = Path("out") / "blockchain" / safe_filename(network)
    out_dir.mkdir(parents=True, exist_ok=True)

    page = 1
    per_page = 50
    summaries: list[dict[str, Any]] = []

    print(f"📦 Dumping contracts for network={network} -> {out_dir}")

    while True:
        paged = safe_call(
            f"blockchain.list_contracts(page={page}, perPage={per_page}, includeAbi=0)",
            lambda: client.blockchain.list_contracts(
                network=[network],      
                withEventsCount=1,
                includeAbi=0,
                sort="id,asc",
                page=page,
                perPage=per_page,
            ),
        )
        if not paged:
            break

        data = getattr(paged, "data", None) or []
        if not data:
            break

        print(f"Page {page}: {len(data)} contracts")

        for item in data:
            addr = getattr(item, "address", None)
            name = getattr(item, "name", None)
            net = getattr(item, "network", None) or network

            if not addr:
                print("⚠️ contract without address? skipping")
                continue

            c = safe_call(
                f"  blockchain.get_contract({addr}, includeAbi=1)",
                lambda a=addr: client.blockchain.get_contract(a, includeAbi=1, withEventsCount=1),
            )
            if not c:
                continue

            abi = normalize_abi(getattr(c, "abi", None))

            payload: Dict[str, Any] = {
                "address": getattr(c, "address", addr),
                "name": getattr(c, "name", name),
                "network": getattr(c, "network", net),
                "status": getattr(c, "status", None),
                "tx_hash": getattr(c, "tx_hash", None),
                "start_block": getattr(c, "start_block", None),
                "last_scanned_block": getattr(c, "last_scanned_block", None),
                "confirmations": getattr(c, "confirmations", None),
                "events_count": getattr(c, "events_count", None),
                "abi": abi,
            }

            # write full contract json
            full_path = out_dir / f"{safe_filename(addr)}.json"
            write_json(full_path, payload)

            # write ABI-only json (nice to have)
            abi_path = out_dir / f"{safe_filename(addr)}.abi.json"
            write_json(abi_path, abi)

            summaries.append(
                {
                    "network": payload["network"],
                    "address": payload["address"],
                    "name": payload["name"],
                    "events_count": payload["events_count"],
                    "file": str(full_path),
                    "abi_file": str(abi_path),
                }
            )

            print(f"    ✅ wrote {addr} ({payload.get('name')})")

        page += 1

    # write an index file
    index_path = out_dir / "_index.json"
    write_json(index_path, {"network": network, "count": len(summaries), "contracts": summaries})
    print(f"✅ wrote index: {index_path}")

    return summaries

# ----------------------------
# main
# ----------------------------

def main() -> int:
    client = DdmClient.from_env()

    username = getenv_str("DDM_USERNAME")
    password = getenv_str("DDM_PASSWORD")

    if username and password:
        safe_call("login", lambda: client.login(username, password))
    else:
        print("⚠️ DDM_USERNAME/DDM_PASSWORD not set; relying on DDM_TOKEN if present")

    network = getenv_str("DDM_TEST_NETWORK", "sepolia")

    # ------------- dump all contracts + abi -------------
    summaries = dump_all_contracts(client, network=network)
    if not summaries:
        print("No contracts found. Seed DB or deploy/ingest one first.")
        return 0

   
    # pick one contract to continue the rest of the tests (first in index)
    contract_address = summaries[0]["address"]
    print(f"📦 Using first dumped contract for deeper tests:", contract_address)

    # ------------- registry -------------
    reg = safe_call("blockchain.registry()", lambda: client.blockchain.registry())
    if isinstance(reg, dict):
        print("     Registry count:", reg.get("count"))

    # ------------- contract_events -------------
    events = safe_call(
        "blockchain.contract_events(page=1, perPage=10)",
        lambda: client.blockchain.contract_events(contract_address, page=1, perPage=10),
    )
    if events and getattr(events, "data", None) is not None:
        print(f"📦 Contract events page size:", len(events.data))
        if events.data:
            e0 = events.data[0]
            print("     First event:", getattr(e0, "name", None), "tx_hash:", getattr(e0, "tx_hash", None))

    # ------------- all_events -------------
    safe_call(
        "blockchain.all_events(network filter, page=1, perPage=5)",
        lambda: client.blockchain.all_events(network=[network], page=1, perPage=5),
    )

    # ------------- contract_txs -------------
    txs = safe_call(
        "blockchain.contract_txs(page=1, perPage=10)",
        lambda: client.blockchain.contract_txs(contract_address, page=1, perPage=10),
    )
    picked_tx_hash = None
    if txs and getattr(txs, "data", None) is not None:
        print(f"📦Contract txs page size:", len(txs.data))
        if txs.data:
            t0 = txs.data[0]
            picked_tx_hash = getattr(t0, "tx_hash", None) or getattr(t0, "hash", None)
            print("First tx hash:", picked_tx_hash)

    # ------------- all_txs -------------
    safe_call(
        "blockchain.all_txs(network filter, page=1, perPage=5)",
        lambda: client.blockchain.all_txs(network=[network], page=1, perPage=5),
    )

    # ------------- get_tx -------------
    if picked_tx_hash:
        safe_call("blockchain.get_tx(first tx hash)", lambda: client.blockchain.get_tx(picked_tx_hash))
    else:
        print("⚠️ No tx hash available; skipping get_tx")

   # ============================================================
    # TASK endpoints (now fully resolvable via /ddm/tasks/*)
    # ============================================================

    requester = getenv_str("DDM_TEST_REQUESTER")
    if not requester:
        print("Skipping prepare_* tasks (set DDM_TEST_REQUESTER to a 0x address)")
        print("✅ blockchain test finished")
        return 0

    deadline = getenv_int("DDM_TEST_DEADLINE") or 1893456000

    # ----------------------------
    # prepare_report_ipfs_uri -> report_uri
    # ----------------------------
    catalog_id = getenv_str("DDM_TEST_FILE_ID") or getenv_str("DDM_TEST_CATALOG_ID")
    report_uri = getenv_str("DDM_TEST_REPORT_URI")

    if not report_uri and getenv_bool("DDM_TEST_PREPARE_REPORT", False):
        if not catalog_id:
            print("Skipping prepare_report_ipfs_uri (set DDM_TEST_FILE_ID or DDM_TEST_CATALOG_ID)")
        else:
            t = safe_call(
                f"blockchain.prepare_report_ipfs_uri(catalog_id={catalog_id})",
                lambda: client.blockchain.prepare_report_ipfs_uri(
                    catalog_id=catalog_id, network=network, include_report=True
                ),
            )
            tid = task_id_from_taskref(t) if t else None
            if tid:
                st = poll_task_until_ready(client, tid, timeout_s=120, interval_s=2.0)
                if st and st.is_success():
                    val = get_task_value(client, tid)
                    report_uri = extract_report_uri(val)
                    if report_uri:
                        print(f"✅ resolved report_uri: {report_uri}")
                    else:
                        print("⚠️ report task SUCCESS but no report_uri in task value")
                elif st and st.is_failure():
                    print(f"❌ report task FAILURE: {st.error or st.message or st.result}")
                else:
                    print("⚠️ timed out waiting for report task to finish")

    

    # ----------------------------
    # prepare_suite -> suite_hash
    # ----------------------------
    suite_hash = getenv_str("DDM_TEST_SUITE_HASH")
    dataset_uri = getenv_str("DDM_TEST_DATASET_URI")

    if getenv_bool("DDM_TEST_PREPARE_SUITE", False):
        expectation_suite_id = getenv_str("DDM_TEST_SUITE_ID") or getenv_str("DDM_TEST_SUITE_ID")
        if not expectation_suite_id:
            print("Skipping prepare_suite (set DDM_TEST_SUITE_ID or DDM_TEST_SUITE_ID)")
        else:
            suite_payload: dict = {
                "id": expectation_suite_id,
                "suite_name": getenv_str("DDM_TEST_SUITE_NAME", "new"),
                "description": getenv_str("DDM_TEST_SUITE_DESCRIPTION", ""),
                "category": getenv_str("DDM_TEST_CATEGORY", "tutorial"),
                "file_types": [getenv_str("DDM_TEST_FILE_FORMAT", "csv")],
                "expectations": [],
                "column_descriptions": {},
                "column_names": [],
            }

            category = suite_payload.get("category") or "tutorial"
            total_expected = getenv_int("DDM_TEST_TOTAL_EXPECTED") or max(1, len(suite_payload.get("expectations") or []))

            suite_dict = build_suite_dict(
                suite_payload,
                fallback_suite_id=expectation_suite_id,
                fallback_category=category,
            )

            task = safe_call(
                "blockchain.prepare_suite()",
                lambda: client.blockchain.prepare_suite({
                    "network": network,
                    "requester": requester,
                    "expectation_suite_id": expectation_suite_id,
                    "suite": suite_dict,
                    "category": category,
                    "fileFormat": (suite_dict.get("file_types") or ["csv"])[0],
                    "deadline": deadline,
                    "totalExpected": total_expected,
                }),
            )

            tid = task_id_from_taskref(task) if task else None
            if tid:
                suite_st = poll_task_until_ready(client, tid, timeout_s=180, interval_s=2.0)
                if suite_st and suite_st.is_success():
                    val = get_task_value(client, tid)
                    suite_hash = suite_hash or find_first_str(val, ["suiteHash", "suite_hash", "hash"])
                    if suite_hash:
                        print(f"✅ resolved suite_hash: {suite_hash}")
                    else:
                        print("⚠️ suite task SUCCESS but no suiteHash in task value")
                elif suite_st and suite_st.is_failure():
                    print(f"❌ suite task FAILURE: {suite_st.error or suite_st.message or suite_st.result}")
                else:
                    print("⚠️ timed out waiting for suite task")

    # ----------------------------
    # prepare_dataset_ipfs_uri -> dataset_uri
    # ----------------------------
    if not dataset_uri and getenv_bool("DDM_TEST_PREPARE_DATASET", True):
        if not catalog_id:
            print("Skipping prepare_dataset_ipfs_uri (set DDM_TEST_FILE_ID / DDM_TEST_CATALOG_ID)")
        else:
            t = safe_call(
                f"blockchain.prepare_dataset_ipfs_uri(catalog_id={catalog_id})",
                lambda: client.blockchain.prepare_dataset_ipfs_uri(
                    network=network,
                    catalog_id=catalog_id,
                    include_report=True,
                ),
            )
            tid = task_id_from_taskref(t) if t else None
            if tid:
                st = poll_task_until_ready(client, tid, timeout_s=180, interval_s=2.0)
                if st and st.is_success():
                    val = get_task_value(client, tid)
                    dataset_uri = extract_dataset_uri(val)
                    if dataset_uri:
                        print(f"✅ resolved dataset_uri: {dataset_uri}")
                    else:
                        print("⚠️ dataset task SUCCESS but no dataset_uri in value")
                        print("↳ task value:", val)
                elif st and st.is_failure():
                    print(f"❌ dataset task FAILURE: {st.error or st.message or st.result}")
                else:
                    print("⚠️ timed out waiting for dataset task")


    # ----------------------------
    # prepare_reward
    # ----------------------------
    if getenv_bool("DDM_TEST_PREPARE_REWARD", False):
        uploader = getenv_str("DDM_TEST_UPLOADER")
        category = getenv_str("DDM_TEST_CATEGORY", "dataset") 

        dataset_fingerprint = getenv_str("DDM_TEST_DATASET_FINGERPRINT")
        if not dataset_fingerprint:
            fp = sha256_hex_of_file(getenv_str("DDM_SAMPLE_PATH") or "")
            dataset_fingerprint = f"0x{fp}" if fp else None
        if dataset_fingerprint and not dataset_fingerprint.startswith("0x"):
            dataset_fingerprint = "0x" + dataset_fingerprint

        # ONLY require what the frontend endpoint requires:
        missing = [k for k, v in {
            "dataset_fingerprint": dataset_fingerprint,
            "uploader": uploader,
            "category": category,
        }.items() if not v]

        if missing:
            print("Skipping prepare_reward (missing: " + ", ".join(missing) + ")")
            print("  dataset_fingerprint:", dataset_fingerprint)
            print("  uploader:", uploader)
            print("  category:", category)
        else:
            t = safe_call(
                "blockchain.prepare_reward(frontend)",
                lambda: client.blockchain.prepare_reward({
                    "network": network,
                    "category": category,
                    "dataset_fingerprint": dataset_fingerprint,
                    "uploader": uploader,
                    "expires_in_sec": 900,
                }),
            )
            tid = task_id_from_taskref(t) if t else None
            if tid:
                st = poll_task_until_ready(client, tid, timeout_s=180, interval_s=2.0)
                if st and st.is_success():
                    val = get_task_value(client, tid)
                    result = val.get("result") if isinstance(val, dict) and "result" in val else val
                    print("✅ reward artifacts result:", result)
                elif st and st.is_failure():
                    print(f"❌ reward task FAILURE: {st.error or st.message or st.result}")
    else:
        print("Skipping prepare_reward (set DDM_TEST_PREPARE_REWARD=1)")


    # ----------------------------
    # prepare_validation (frontend-style)
    # ----------------------------
    if getenv_bool("DDM_TEST_PREPARE_VALIDATION", False):
        uploader = getenv_str("DDM_TEST_UPLOADER")
        if not uploader:
            print("Skipping prepare_validation (need DDM_TEST_UPLOADER = 0x...)")
        else:
            dataset_fingerprint = getenv_str("DDM_TEST_DATASET_FINGERPRINT")
            if not dataset_fingerprint:
                fp = sha256_hex_of_file( getenv_str("DDM_SAMPLE_PATH") or "")
                dataset_fingerprint = f"0x{fp}" if fp else None

            if not dataset_fingerprint:
                print("Skipping prepare_validation (need DDM_TEST_DATASET_FINGERPRINT or sample file path)")
            else:
                # normalize
                if not dataset_fingerprint.startswith("0x"):
                    dataset_fingerprint = "0x" + dataset_fingerprint

                payload = PrepareValidationBody(
                    network=network,
                    uploader=uploader,
                    dataset_fingerprint=dataset_fingerprint,
                    validation_json={"score": 100},   # exactly like your frontend example
                    include_report=True,
                )

                out = safe_call("blockchain.prepare_validation", lambda: client.blockchain.prepare_validation(payload))
                tid = task_id_from_taskref(out) if out else None

                if not tid:
                    print("❌ prepare_validation returned empty task_id")
                    print("↳ payload:", payload.model_dump())
                    print("↳ raw response:", out)
                else:
                    st = poll_task_until_ready(client, tid, timeout_s=180, interval_s=2.0)
                    if not st:
                        print("⚠️ timed out waiting for validation task")
                    elif st.is_failure():
                        print(f"❌ validation task FAILURE: {st.error or st.message or st.result}")
                    else:
                        val = get_task_value(client, tid)
                        # depending on your tasks endpoint, might be {"result": {...}} or direct dict
                        result = val.get("result") if isinstance(val, dict) and "result" in val else val
                        print("✅ validation artifacts result:", result)
    else:
        print("Skipping prepare_validation (set DDM_TEST_PREPARE_VALIDATION=1)")



if __name__ == "__main__":
    raise SystemExit(main())
