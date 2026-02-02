from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.blockchain.utils import load_json_arg, _jsonify
from ddm_sdk.scripts.blockchain.task_runner import run_task_and_store
from ddm_sdk.scripts.blockchain.extractors import extract_report_uri


def _get_str(d: Dict[str, Any], *keys: str) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _store_latest(client: DdmClient, base: str, req: Dict[str, Any], resp: Dict[str, Any]) -> Dict[str, str]:
    return {
        "request": client.storage.write_json(f"{base}/request", _jsonify(req)),
        "response": client.storage.write_json(f"{base}/response", _jsonify(resp)),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-prepare-validation", description="Prepare validation on-chain (task)")
    ap.add_argument("--json", default=None)
    ap.add_argument("--json-file", default=None)
    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--no-store", action="store_true")
    ap.add_argument("--successful", choices=["true", "false"], default="true", help="Attach success flag into saved artifacts (default: true)")

    args = ap.parse_args(argv)

    payload: Dict[str, Any] = load_json_arg(json_text=args.json, json_file=args.json_file)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    # We need a stable key root; fingerprint is the natural grouping key.
    payload = load_json_arg(json_text=args.json, json_file=args.json_file)

    # ensure successful exists
    if "successful" not in payload:
        payload["successful"] = (args.successful == "true")

    fp = _get_str(payload, "dataset_fingerprint", "datasetFingerprint")
    if not fp:
        raise SystemExit("Missing dataset_fingerprint in payload (required to store prepare_validation artifacts).")

    out = run_task_and_store(
        client,
        action=f"{fp}/prepare_validation",
        request_payload=payload,
        call_fn=lambda: client.blockchain.prepare_validation(payload),
        poll=args.poll,
        timeout_s=args.timeout,
        interval_s=args.interval,
        no_store=args.no_store,
    )

    # extract report uri (optional)
    task_val = None
    if isinstance(out.get("result"), dict):
        task_val = out["result"].get("value") or out["result"].get("result")
    if task_val is None and isinstance(out.get("status"), dict):
        task_val = out["status"].get("result")
    out["report_uri"] = extract_report_uri(task_val)

    # stable overwrite artifacts
    if client.storage and not args.no_store:
        base = f"blockchain/validations/{fp}/prepare_validation"
        out["saved_latest"] = _store_latest(client, base, payload, out)

    print(json.dumps(_jsonify(out), indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
