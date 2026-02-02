from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.file.utils import norm_project
from datetime import timedelta

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def validations_root_key(project_id: str) -> str:
    return f"projects/{norm_project(project_id)}/validations"


def append_validation_log(
    client: DdmClient,
    *,
    project_id: str,
    action: str,
    ok: bool,
    details: Dict[str, Any] | None = None,
) -> None:
    """
    projects/<project>/validations/logs.json  (append list)
    """
    if not client.storage:
        return

    key = f"{validations_root_key(project_id)}/logs"
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


def store_validation_result_snapshot(
    client: DdmClient,
    *,
    project_id: str,
    name: str,
    payload: Any,
) -> Optional[str]:
    """
    projects/<project>/validations/<name>/<timestamp>.json
    """
    if not client.storage:
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{validations_root_key(project_id)}/{name}/{ts}"
    return client.storage.write_json(key, payload)



def _safe(d: Any, *keys: str, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

from typing import Any, Dict, List, Optional, Set

def summarize_validation(persisted_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns:
      {
        "suite_name": ...,
        "run_time": ...,
        "overall_success": bool,
        "stats": {...},
        "columns": [
            {"column": ..., "description": ..., "passed": bool, "reason": "..."},
            ...
        ],
        "non_column": [
            {"expectation": ..., "passed": bool, "reason": "..."},
            ...
        ]
      }
    """
    detailed = persisted_result.get("detailed_results") or {}

    suite_name = (
        detailed.get("suite_name")
        or _safe(detailed, "meta", "expectation_suite_name")
        or persisted_result.get("suite_name")
    )
    run_time = (
        _safe(detailed, "meta", "run_id", "run_time")
        or detailed.get("run_time")
        or persisted_result.get("run_time")
    )

    results = detailed.get("results") or []
    overall_success = bool(detailed.get("success"))
    stats = detailed.get("statistics") or {}

    # ------------------------------------------------------------
    # 1) Get column info from BOTH places (detailed OR top-level)
    # ------------------------------------------------------------
    col_desc = (
        detailed.get("column_descriptions")
        or persisted_result.get("column_descriptions")
        or {}
    ) or {}

    col_names = (
        detailed.get("column_names")
        or persisted_result.get("column_names")
        or []
    ) or []

    # ------------------------------------------------------------
    # 2) If still missing, infer from results
    # ------------------------------------------------------------
    inferred_cols: Set[str] = set()

    # infer from column expectations (kwargs.column)
    for r in results:
        kwargs = _safe(r, "expectation_config", "kwargs", default={}) or {}
        col = kwargs.get("column")
        if isinstance(col, str) and col.strip():
            inferred_cols.add(col.strip())

    # infer from table column list expectation (observed_value or kwargs.column_list)
    if not col_names:
        for r in results:
            exp_type = _safe(r, "expectation_config", "type")
            if exp_type == "expect_table_columns_to_match_ordered_list":
                obs = _safe(r, "result", "observed_value")
                if isinstance(obs, list) and obs:
                    inferred_cols.update(str(x) for x in obs if x is not None)
                    break
                kwargs = _safe(r, "expectation_config", "kwargs", default={}) or {}
                expected = kwargs.get("column_list")
                if isinstance(expected, list) and expected:
                    inferred_cols.update(str(x) for x in expected if x is not None)
                    break

    # finalize column names
    if col_names:
        # keep the backend order
        col_names = [str(x) for x in col_names if x is not None]
    elif col_desc:
        # preserve description map order (py3.7+ keeps dict order)
        col_names = list(col_desc.keys())
    else:
        # stable order for inferred columns
        col_names = sorted(inferred_cols)

    # ensure col_desc exists for any inferred columns
    if isinstance(col_desc, dict):
        for c in col_names:
            col_desc.setdefault(c, "")

    # ------------------------------------------------------------
    # 3) Aggregate checks
    # ------------------------------------------------------------
    per_col: Dict[str, List[Dict[str, Any]]] = {c: [] for c in col_names}
    non_column: List[Dict[str, Any]] = []

    for r in results:
        exp_type = _safe(r, "expectation_config", "type")
        kwargs = _safe(r, "expectation_config", "kwargs", default={}) or {}
        col = kwargs.get("column")

        success = bool(r.get("success"))
        exception_msg = _safe(r, "exception_info", "exception_message")

        reason_parts: List[str] = []

        if not success:
            # column-style failures
            uc = _safe(r, "result", "unexpected_count")
            up = _safe(r, "result", "unexpected_percent")
            if uc is not None:
                reason_parts.append(f"unexpected_count={uc}")
            if up is not None:
                try:
                    reason_parts.append(f"unexpected_percent={float(up):.3f}")
                except Exception:
                    reason_parts.append(f"unexpected_percent={up}")

            # table-level failures often have observed_value
            observed = _safe(r, "result", "observed_value")
            if observed is not None:
                reason_parts.append(f"observed={observed}")

            # expected constraints in kwargs (common ones)
            for k in ("min_value", "max_value", "value", "row_count", "column_list"):
                if k in kwargs:
                    reason_parts.append(f"expected_{k}={kwargs[k]}")

            if exception_msg:
                reason_parts.append(f"exception={exception_msg}")

        reason = "OK" if success else ("; ".join(reason_parts) or "failed")

        if isinstance(col, str) and col.strip():
            c = col.strip()
            # ensure we don't drop a column that wasn't in col_names
            if c not in per_col:
                per_col[c] = []
                col_names.append(c)
                if isinstance(col_desc, dict):
                    col_desc.setdefault(c, "")
            per_col[c].append({"expectation": exp_type, "passed": success, "reason": reason})
        else:
            non_column.append({"expectation": exp_type, "passed": success, "reason": reason})

    # ------------------------------------------------------------
    # 4) Produce per-column summary
    # ------------------------------------------------------------
    cols_out: List[Dict[str, Any]] = []
    for c in col_names:
        checks = per_col.get(c, [])
        passed = all(x["passed"] for x in checks) if checks else True

        if passed:
            col_reason = "OK"
        else:
            fails = [f"{x['expectation']}: {x['reason']}" for x in checks if not x["passed"]]
            col_reason = " | ".join(fails) if fails else "failed"

        cols_out.append(
            {
                "column": c,
                "description": col_desc.get(c, "") if isinstance(col_desc, dict) else "",
                "passed": passed,
                "reason": col_reason,
            }
        )

    return {
        "suite_name": suite_name,
        "run_time": run_time,
        "overall_success": overall_success,
        "stats": stats,
        "columns": cols_out,
        "non_column": non_column,
    }



def _dump(obj: Any) -> Any:
    return obj.model_dump(mode="json", exclude_none=False) if hasattr(obj, "model_dump") else obj


def unwrap_task_value(val: Any) -> Any:
    """
    Normalize task payload shapes.
    """
    if isinstance(val, dict) and "result" in val:
        return val["result"]
    return val


def pick_task_payload(client: DdmClient, st: Any, task_id: str) -> Any:
    """
    Prefer /ddm/tasks/result/<id>.value, fallback to /ddm/tasks/status/<id>.result.
    """
    try:
        v = client.tasks.value(task_id)
        if v is not None:
            return v
    except Exception:
        pass
    return getattr(st, "result", None)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def get_dataset_id_from_suite(client: DdmClient, suite_id: str) -> Optional[str]:
    """
    Best-effort: fetch suite and read dataset_id.
    """
    try:
        suite = client.expectations.get_suite(suite_id)
        suite_d = _dump(suite)
        if isinstance(suite_d, dict):
            v = suite_d.get("dataset_id")
            return v if isinstance(v, str) and v.strip() else None
    except Exception:
        return None
    return None


def fetch_persisted_validation_results(
    client: DdmClient,
    *,
    suite_ids: List[str],
    dataset_id: Optional[str],
    lookback_minutes: int = 60,
    per_page: int = 50,
) -> Optional[Dict[str, Any]]:
    """
    After task SUCCESS, query /ddm/validations/results because task payload may be empty.
    """
    try:
        now = datetime.now(timezone.utc)
        run_time_from = _iso(now - timedelta(minutes=lookback_minutes))

        resp = client.validations.list_results(
            suite_id=suite_ids,
            dataset_id=[dataset_id] if dataset_id else None,
            run_time_from=run_time_from,
            sort="run_time,desc",
            page=1,
            perPage=per_page,
        )
        out = _dump(resp)
        return out if isinstance(out, dict) else {"raw": out}
    except Exception as e:
        return {"error": str(e)}


def extract_latest_result_id(persisted: Optional[Dict[str, Any]]) -> Optional[str]:
    """
    Tries common list_results shapes:
      - {"items":[{"result_id":...}]} or {"data":[...]} etc.
    """
    if not isinstance(persisted, dict):
        return None

    for key in ("items", "data", "results"):
        arr = persisted.get(key)
        if isinstance(arr, list) and arr:
            first = arr[0]
            if isinstance(first, dict):
                for rid_key in ("result_id", "id", "validation_result_id"):
                    rid = first.get(rid_key)
                    if isinstance(rid, str) and rid.strip():
                        return rid
    return None

