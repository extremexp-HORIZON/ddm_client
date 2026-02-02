from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, List, Optional, Tuple

from ddm_sdk.scripts.blockchain.prepare_suite_artifacts import main as prepare_suite_artifacts_main
from ddm_sdk.scripts.blockchain.register_suite import main as register_suite_main


def _read_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise SystemExit(f"Expected JSON object in {path}")
    return obj


def _suite_id(suite_obj: Dict[str, Any]) -> str:
    # most of your suite objects use "id"
    sid = suite_obj.get("id")
    if isinstance(sid, str) and sid.strip():
        return sid.strip()
    raise SystemExit("suite.json missing 'id'")


def _suite_name(suite_obj: Dict[str, Any]) -> str:
    for k in ("suite_name", "expectation_suite_name", "name", "suiteName"):
        v = suite_obj.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return "unnamed_suite"


def _pick_file_format(suite_obj: Dict[str, Any], override: Optional[str]) -> str:
    if override and override.strip():
        return override.strip()
    ft = suite_obj.get("file_types")
    if isinstance(ft, list) and ft and isinstance(ft[0], str) and ft[0].strip():
        return ft[0].strip()
    return "csv"


def _extract_ge_expectations(suite_obj: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Supports:
      A) suite_obj["expectations"] is a LIST  (older suite.json)
      B) suite_obj["expectations"] is a DICT with key "expectations": [ ... ] (newer UI/backend suite payload)
    """
    ex = suite_obj.get("expectations")

    if isinstance(ex, list):
        return [e for e in ex if isinstance(e, dict)]

    if isinstance(ex, dict):
        inner = ex.get("expectations")
        if isinstance(inner, list):
            return [e for e in inner if isinstance(e, dict)]

    return []


def _enabled_maps_from_expectation_list(
    exp_list: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Dict[str, Any]]], Dict[str, Dict[str, Any]]]:
    """
    Convert Great Expectations list into the UI-style enabled maps that backend flatten expects:

    selectedExpectations = {
      "Age": {
        "expect_column_values_to_not_be_null": {"mostly": 0.95, "_enabled": True},
        ...
      },
      ...
    }

    tableExpectations = {
      "expect_table_columns_to_match_ordered_list": {"column_list": [...], "_enabled": True},
      ...
    }
    """
    selected: Dict[str, Dict[str, Dict[str, Any]]] = {}
    table: Dict[str, Dict[str, Any]] = {}

    for e in exp_list:
        et = e.get("expectation_type")
        kwargs = e.get("kwargs")

        if not isinstance(et, str) or not et.strip():
            continue
        if not isinstance(kwargs, dict):
            kwargs = {}

        # mark enabled
        params = dict(kwargs)
        params["_enabled"] = True

        col = kwargs.get("column")
        if isinstance(col, str) and col.strip():
            col = col.strip()
            selected.setdefault(col, {})
            selected[col][et] = params
        else:
            table[et] = params

    return selected, table


def _build_prepare_payload(
    suite_obj: Dict[str, Any],
    *,
    network: str,
    requester: str,
    deadline: int,
    category: str,
    total_expected: int,
    file_format: str,
) -> Dict[str, Any]:
    sid = _suite_id(suite_obj)

    exp_list = _extract_ge_expectations(suite_obj)
    if not exp_list:
        raise SystemExit("No expectations found in suite.json (expected list or expectations.expectations list)")

    selected_map, table_map = _enabled_maps_from_expectation_list(exp_list)

    # IMPORTANT: this is the suite_object backend flattener wants
    suite_payload: Dict[str, Any] = {
        "expectation_suite_id": sid,
        "name": _suite_name(suite_obj),
        "description": suite_obj.get("description") or "",
        "category": category,
        "fileFormats": suite_obj.get("file_types") or [file_format],
        "column_names": suite_obj.get("column_names") or [],
        "column_descriptions": suite_obj.get("column_descriptions") or {},
        "expectations": exp_list,
        #(backend expects dicts with _enabled)
        "selectedExpectations": selected_map,
        "tableExpectations": table_map,

        "expectation_descriptions": suite_obj.get("expectation_descriptions") or {},
    }

    return {
        "network": network,
        "requester": requester,
        "expectation_suite_id": sid,
        "category": category,
        "fileFormat": file_format,
        "deadline": int(deadline),
        "totalExpected": int(total_expected),
        "suite": suite_payload,
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(prog="challenge08-register-suite")

    ap.add_argument("--json-file", required=True, help="Path to suite.json")
    ap.add_argument("--network", default="sepolia")

    ap.add_argument("--requester", required=True)
    ap.add_argument("--deadline", type=int, required=True)
    ap.add_argument("--category", required=True)
    ap.add_argument("--totalExpected", type=int, required=True)

    ap.add_argument("--fileFormat", default=None)
    ap.add_argument("--bounty-eth", type=float, required=True)

    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--prepare-interval", type=float, default=1.0)
    ap.add_argument("--register-interval", type=float, default=2.0)

    ap.add_argument("--method", choices=["plain", "sig"], default=None)
    ap.add_argument("--registry-name", default="DatasetRequestRegistry")
    ap.add_argument("--registry-address", default=None)
    ap.add_argument("--max-fee-gwei", type=float, default=None)
    ap.add_argument("--max-priority-fee-gwei", type=float, default=None)

    ap.add_argument("--project_id", default=None)
    ap.add_argument("--no-store", action="store_true")

    args = ap.parse_args(argv)

    suite_obj = _read_json(args.json_file)
    sid = _suite_id(suite_obj)
    file_format = _pick_file_format(suite_obj, args.fileFormat)

    prepare_payload = _build_prepare_payload(
        suite_obj,
        network=args.network,
        requester=args.requester,
        deadline=args.deadline,
        category=args.category,
        total_expected=args.totalExpected,
        file_format=file_format,
    )

    # ---- 1) PREPARE ----
    prepare_args: List[str] = ["--json", json.dumps(prepare_payload, ensure_ascii=False)]
    if args.poll:
        prepare_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.prepare_interval)]
    if args.project_id:
        prepare_args += ["--project_id", args.project_id]
    if args.no_store:
        prepare_args += ["--no-store"]

    rc1 = prepare_suite_artifacts_main(prepare_args)
    if rc1 != 0:
        return rc1

    # ---- 2) REGISTER ----
    register_args: List[str] = [
        "--network", args.network,
        "--suite_id", sid,
        "--bounty-eth", str(args.bounty_eth),
        "--registry-name", args.registry_name,
    ]
    if args.registry_address:
        register_args += ["--registry-address", args.registry_address]
    if args.method:
        register_args += ["--method", args.method]
    if args.poll:
        register_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.register_interval)]
    if args.no_store:
        register_args += ["--no-store"]
    if args.max_fee_gwei is not None:
        register_args += ["--max-fee-gwei", str(args.max_fee_gwei)]
    if args.max_priority_fee_gwei is not None:
        register_args += ["--max-priority-fee-gwei", str(args.max_priority_fee_gwei)]

    return register_suite_main(register_args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
