from __future__ import annotations

import argparse
import json
from typing import Any, Dict

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated


def _jsonify(x: Any) -> Any:
    try:
        from hexbytes import HexBytes
    except Exception:
        HexBytes = ()  # type: ignore

    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, HexBytes):
        return x.hex()
    if isinstance(x, (bytes, bytearray)):
        return "0x" + bytes(x).hex()
    if isinstance(x, dict):
        return {str(k): _jsonify(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, set)):
        return [_jsonify(v) for v in x]
    if hasattr(x, "model_dump"):
        return _jsonify(x.model_dump(mode="json", exclude_none=False))
    # AttributeDict-ish
    if hasattr(x, "items"):
        try:
            return {str(k): _jsonify(v) for k, v in dict(x).items()}
        except Exception:
            pass
    return str(x)


def _write_latest_pair(client: DdmClient, base: str, req: Dict[str, Any], resp: Dict[str, Any]) -> Dict[str, str]:
    return {
        "request": client.storage.write_json(f"{base}/request", _jsonify(req)),
        "response": client.storage.write_json(f"{base}/response", _jsonify(resp)),
    }


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


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-prepare-dataset-report", description="Prepare dataset HTML report URI (task)")
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--catalog_id", required=True)
    ap.add_argument("--include-report", action="store_true", default=True)
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
    include_report = bool(args.include_report)

    request_meta = {
        "network": network,
        "suite_id": suite_id,
        "catalog_id": catalog_id,
        "include_report": include_report,
    }

    # call backend
    ref = client.blockchain.prepare_report_ipfs_uri(
        network=network,
        catalog_id=catalog_id,
        include_report=include_report,
    )
    ref_d = ref.model_dump() if hasattr(ref, "model_dump") else dict(ref)
    out: Dict[str, Any] = {"ok": True, "task": ref_d, "status": None, "result": None}

    if args.poll and isinstance(ref_d, dict) and isinstance(ref_d.get("task_id"), str):
        st = client.tasks.wait(
            ref_d["task_id"],
            timeout_s=args.timeout,
            poll_interval_s=args.interval,
            raise_on_failure=False,
        )
        st_d = st.model_dump() if hasattr(st, "model_dump") else _jsonify(st)
        out["status"] = st_d

        # best-effort normalize result
        out["result"] = _unwrap_task_envelope(st_d)

    # store at your desired path
    if not args.no_store:
        base = f"blockchain/expectations/suites/{suite_id}/datasets/{catalog_id}/prepare_report"
        out["saved"] = _write_latest_pair(client, base, request_meta, out)

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
