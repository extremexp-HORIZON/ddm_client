from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List, Tuple

from ddm_sdk.client import DdmClient


def utc_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def norm_project(project_id: str) -> str:
    p = (project_id or "").strip().strip("/")
    if not p:
        raise ValueError("project_id is empty")
    return p


def norm_suite_id(suite_id: str) -> str:
    s = (suite_id or "").strip()
    if not s:
        raise ValueError("suite_id is empty")
    return s


def norm_file_id(file_id: Optional[str]) -> Optional[str]:
    if not file_id:
        return None
    f = file_id.strip()
    return f or None


# ---------------------------
# Keys (project-root)
# ---------------------------

def project_root_key(project_id: str) -> str:
    return f"projects/{norm_project(project_id)}"


def suite_dir_key(*, suite_id: str) -> str:
    return f"expectations/suites/{norm_suite_id(suite_id)}"


def suite_record_key(*, project_id: Optional[str] = None, suite_id: str) -> str:
    # -> expectations/suites/<suite_id>/suite.json
    return f"{suite_dir_key(suite_id=suite_id)}/suite"


def suite_logs_key(*, project_id: Optional[str] = None, suite_id: str) -> str:
    # -> expectations/suites/<suite_id>/log.json
    return f"{suite_dir_key( suite_id=suite_id)}/log"


def create_suite_req_key(*, project_id: str, suite_name: str) -> str:
    """
    Request snapshot root for create_suite.

    expectations/create_suite/<project>/<ts>_<hash>

    Keep signature for backwards compatibility.
    """
    ts = utc_ts()
    h = hashlib.sha1(suite_name.encode("utf-8")).hexdigest()[:10]

    safe_proj = "".join(
        ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in norm_project(project_id)
    ) or "project"

    return f"expectations/create_suite/{safe_proj}/{ts}_{h}"



def file_expectations_dir_key(*, project_id: str, file_id: str) -> str:
    # -> projects/<project>/files/<file_id>/expectations
    pid = norm_project(project_id)
    fid = norm_suite_id(file_id)  # just non-empty check
    return f"projects/{pid}/files/{fid}/expectations"


def file_latest_suite_key(*, project_id: str, file_id: str) -> str:
    # -> projects/<project>/files/<file_id>/expectations/latest.json
    return f"{file_expectations_dir_key(project_id=project_id, file_id=file_id)}/latest"


def file_logs_key(*, project_id: str, file_id: str) -> str:
    # -> projects/<project>/files/<file_id>/expectations/log.json
    return f"{file_expectations_dir_key(project_id=project_id, file_id=file_id)}/log"


# ---------------------------
# Persistence helpers
# ---------------------------

def append_suite_log(
    client: DdmClient,
    *,
    project_id: str,
    suite_id: str,
    action: str,
    ok: bool,
    details: Dict[str, Any],
) -> None:
    if not client.storage:
        return

    key = suite_logs_key(project_id=project_id, suite_id=suite_id)
    existing = client.storage.read_json(key)
    logs = existing if isinstance(existing, list) else []

    logs.append(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "ok": ok,
            "details": details,
        }
    )
    client.storage.write_json(key, logs)


def persist_suite_record(
    *,
    client: DdmClient,
    project_id: str,
    suite_id: str,
    payload: Any,
) -> None:
    if not client.storage:
        return
    client.storage.write_json(
        suite_record_key(project_id=project_id, suite_id=suite_id),
        payload,
    )



def persist_suite_pointer_for_file(
    *,
    client: DdmClient,
    project_id: str,
    file_id: Optional[str],
    suite_id: str,
    payload: Any,
) -> None:
    """
    Optional: if you know which file the suite is for, keep a pointer under that file.
    """
    if not client.storage:
        return
    fid = norm_file_id(file_id)
    if not fid:
        return

    client.storage.write_json(
        file_latest_suite_key(project_id=project_id, file_id=fid),
        {
            "project_id": norm_project(project_id),
            "file_id": fid,
            "suite_id": norm_suite_id(suite_id),
        },
    )
    # also store the payload snapshot (handy)
    client.storage.write_json(
        f"{file_expectations_dir_key(project_id=project_id, file_id=fid)}/suite_{norm_suite_id(suite_id)}",
        payload,
    )

def _unwrap_result_list(obj: Any) -> Optional[List[Dict[str, Any]]]:
    """
    Accept:
      - [{"column": "...", "description": "..."}, ...]
      - {"result": [ ... ]}
      - {"state": "SUCCESS", "result": [ ... ]}  (status response)
    """
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        v = obj.get("result")
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return None


def column_desc_map_from_task(description_task_result: Any) -> Tuple[Dict[str, str], List[str]]:
    """
    Returns:
      (column_descriptions: {col: desc}, column_names_inferred: [col1, col2, ...])
    """
    items = _unwrap_result_list(description_task_result)
    if not items:
        return {}, []

    out: Dict[str, str] = {}
    cols: List[str] = []
    for it in items:
        c = it.get("column")
        d = it.get("description")
        if isinstance(c, str) and c.strip() and isinstance(d, str) and d.strip():
            c = c.strip()
            out[c] = d.strip()
            cols.append(c)
    return out, cols


def build_expectations_suite(
    *,
    suite_name: str,
    column_names: List[str],
    column_descriptions: Dict[str, str],
    mostly: float = 0.95,
    max_columns: int = 150,
    include_row_count_between: bool = False,
    row_min: Optional[int] = None,
    row_max: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Builds the Great Expectations suite dict:
      {"expectation_suite_name":..., "expectations":[...], "meta": {...}}
    """

    expectations: List[Dict[str, Any]] = []

    # Column-level: not null for every column
    for col in column_names:
        expectations.append(
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col, "mostly": mostly},
            }
        )

    # Table-level expectations
    expectations.append(
        {
            "expectation_type": "expect_table_column_count_to_be_between",
            "kwargs": {"min_value": 1, "max_value": max_columns},
        }
    )

    expectations.append(
        {
            "expectation_type": "expect_table_columns_to_match_ordered_list",
            "kwargs": {"column_list": column_names},
        }
    )

    if include_row_count_between:
        kwargs: Dict[str, Any] = {}
        if row_min is not None:
            kwargs["min_value"] = int(row_min)
        if row_max is not None:
            kwargs["max_value"] = int(row_max)
        if kwargs:
            expectations.append(
                {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": kwargs}
            )

    return {
        "expectation_suite_name": suite_name,
        "expectations": expectations,
        "meta": {
            "column_names": column_names,
            "column_descriptions": column_descriptions,
            "table_expectation_descriptions": {},
        },
    }


def build_suite_create_payload_from_tasks(
    *,
    suite_name: str,
    dataset_id: str,
    user_id: str,
    file_types: List[str],
    # optional extras
    category: Optional[str] = None,
    description: Optional[str] = None,
    use_case: Optional[str] = None,
    datasource_name: str = "default",
    # inputs from tasks
    description_task_result: Any,
    column_names: Optional[List[str]] = None,
    mostly: float = 0.95,
    max_columns: int = 150,
) -> Dict[str, Any]:
    """
    Returns EXACT UI payload for POST /ddm/expectations/suites:

    {
      suite_name, datasource_name, dataset_id, file_types, category, description, use_case, user_id,
      column_names, column_descriptions,
      expectations: { expectation_suite_name, expectations:[...], meta:{...} }
    }
    """

    col_desc, inferred_cols = column_desc_map_from_task(description_task_result)

    cols = column_names if (column_names and len(column_names) > 0) else inferred_cols
    if not cols:
        raise ValueError("No column names provided and none inferred from description_task_result")

    # Keep descriptions only for known columns (avoid junk keys)
    col_desc_filtered = {c: col_desc[c] for c in cols if c in col_desc}

    expectations_suite = build_expectations_suite(
        suite_name=suite_name,
        column_names=cols,
        column_descriptions=col_desc_filtered,
        mostly=mostly,
        max_columns=max_columns,
    )

    payload: Dict[str, Any] = {
        "suite_name": suite_name,
        "datasource_name": datasource_name,
        "dataset_id": dataset_id,
        "file_types": file_types,
        "category": category,
        "description": description,
        "use_case": use_case or suite_name,
        "user_id": user_id,
        "column_names": cols,
        "column_descriptions": col_desc_filtered,
        "expectations": expectations_suite,
    }

    # drop None fields so it matches UI style cleaner
    return {k: v for k, v in payload.items() if v is not None}



def load_saved_sample_artifact(
    client: DdmClient,
    *,
    dataset_id: str,
) -> Dict[str, Any]:
    """
    Load the artifact written by ddm-upload-sample:
      storage key: expectations/datasets/<dataset_id>/sample

    Returns the JSON object from storage.
    """
    if not client.storage:
        raise RuntimeError("client.storage is not configured")

    key = f"expectations/datasets/{dataset_id}/sample"
    obj = client.storage.read_json(key)
    if not isinstance(obj, dict):
        raise FileNotFoundError(f"Sample artifact not found or invalid JSON object at key: {key}")
    return obj


def _extract_desc_task_result_from_sample(sample_obj: Dict[str, Any]) -> Tuple[Optional[str], Any]:
    """
    Returns (description_task_id, description_task_result_value)

    We prefer:
      - tasks_status[desc_task_id]["result"]  (this is what you showed)
    Fallback:
      - tasks_value[desc_task_id]
    """
    upload = sample_obj.get("upload") if isinstance(sample_obj.get("upload"), dict) else {}
    desc_task_id = upload.get("description_task_id")

    tasks_status = sample_obj.get("tasks_status")
    if isinstance(desc_task_id, str) and isinstance(tasks_status, dict):
        st = tasks_status.get(desc_task_id)
        if isinstance(st, dict) and "result" in st:
            return desc_task_id, st.get("result")

    tasks_value = sample_obj.get("tasks_value")
    if isinstance(desc_task_id, str) and isinstance(tasks_value, dict):
        return desc_task_id, tasks_value.get(desc_task_id)

    return (desc_task_id if isinstance(desc_task_id, str) else None), None


def build_suite_create_payload_from_saved_sample(
    *,
    client: DdmClient,
    suite_name: str,
    dataset_id: str,
    user_id: str,
    file_types: List[str],
    datasource_name: str = "default",
    category: Optional[str] = None,
    description: Optional[str] = None,
    use_case: Optional[str] = None,
    column_names: Optional[List[str]] = None,
    mostly: float = 0.95,
    max_columns: int = 150,
) -> Dict[str, Any]:
    """
    High-level helper:
      - reads expectations/datasets/<dataset_id>/sample
      - extracts description task result
      - calls your existing build_suite_create_payload_from_tasks(...)
    """
    sample_obj = load_saved_sample_artifact(client, dataset_id=dataset_id)
    _, desc_result = _extract_desc_task_result_from_sample(sample_obj)

    if desc_result is None:
        raise ValueError(
            "Could not find description task result in sample artifact. "
            "Run upload_sample with --poll so tasks_status.result is saved."
        )

    # Reuse your existing builder (do NOT duplicate logic)
    return build_suite_create_payload_from_tasks(
        suite_name=suite_name,
        dataset_id=dataset_id,
        user_id=user_id,
        file_types=file_types,
        category=category,
        description=description,
        use_case=use_case,
        datasource_name=datasource_name,
        description_task_result=desc_result,
        column_names=column_names,
        mostly=mostly,
        max_columns=max_columns,
    )

from typing import Set

def suite_datasets_key(*, suite_id: str) -> str:
    # expectations/suites/<suite_id>/datasets.json
    return f"{suite_dir_key(suite_id=suite_id)}/datasets"


def dataset_suites_key(*, dataset_id: str) -> str:
    # expectations/datasets/<dataset_id>/suites.json
    did = (dataset_id or "").strip()
    if not did:
        raise ValueError("dataset_id is empty")
    return f"expectations/datasets/{did}/suites"


def _read_str_set(client: DdmClient, key: str) -> Set[str]:
    if not client.storage:
        return set()
    obj = client.storage.read_json(key)
    if not isinstance(obj, list):
        return set()
    out: Set[str] = set()
    for x in obj:
        if isinstance(x, str) and x.strip():
            out.add(x.strip())
    return out


def _write_str_list(client: DdmClient, key: str, items: Set[str]) -> None:
    if not client.storage:
        return
    client.storage.write_json(key, sorted(items))


def link_suite_dataset(
    *,
    client: DdmClient,
    suite_id: str,
    dataset_id: str,
) -> None:
    """
    Many-to-many link:
      - add dataset_id to expectations/suites/<suite_id>/datasets.json
      - add suite_id to expectations/datasets/<dataset_id>/suites.json

    Safe to call multiple times (no duplicates).
    """
    if not client.storage:
        return

    sid = (suite_id or "").strip()
    did = (dataset_id or "").strip()
    if not sid or not did:
        return

    # suite -> datasets
    s_key = suite_datasets_key(suite_id=sid)
    ds = _read_str_set(client, s_key)
    ds.add(did)
    _write_str_list(client, s_key, ds)

    # dataset -> suites
    d_key = dataset_suites_key(dataset_id=did)
    ss = _read_str_set(client, d_key)
    ss.add(sid)
    _write_str_list(client, d_key, ss)
