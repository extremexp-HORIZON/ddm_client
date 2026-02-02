from __future__ import annotations
from typing import Any
from helpers import OUT_DIR
import json

def parse_column_descriptions(val: Any) -> tuple[dict[str, str], list[str]]:
    """
    Accepts many backend shapes and returns:
      (column_descriptions: {col: desc}, column_names: [col1, col2, ...])

    Supported shapes:
      1) {"result": [{"column": "...", "description": "..."}, ...]}
      2) [{"column": "...", "description": "..."}, ...]
      3) {"result": {"result": [...]}}   (double-wrapped)
      4) {"columns": [...]} or {"data": [...]} or {"items": [...]}
      5) {"result": {"columns": [...]}} etc.
    """
    def _as_list(x: Any) -> list[Any] | None:
        if isinstance(x, list):
            return x
        if isinstance(x, dict):
            for k in ("result", "columns", "data", "items"):
                v = x.get(k)
                if isinstance(v, list):
                    return v
                # sometimes nested dict
                if isinstance(v, dict):
                    vv = _as_list(v)
                    if vv is not None:
                        return vv
        return None

    items = _as_list(val)
    if not items:
        return {}, []

    col_desc: dict[str, str] = {}
    col_names: list[str] = []

    for it in items:
        if not isinstance(it, dict):
            continue
        c = it.get("column") or it.get("name")
        d = it.get("description") or it.get("desc")
        if isinstance(c, str) and c and isinstance(d, str):
            col_desc[c] = d
            col_names.append(c)

    return col_desc, col_names


def build_basic_expectations(column_names: list[str]) -> list[dict]:
    exps: list[dict] = []

    for idx, col in enumerate(column_names):
        exps.append(
            {"expectation_type": "expect_column_to_exist", "kwargs": {"column": col, "column_index": idx}}
        )
        exps.append(
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": col, "mostly": 0.95}}
        )

    exps.append(
        {"expectation_type": "expect_table_column_count_to_be_between", "kwargs": {"min_value": 1, "max_value": 150}}
    )
    exps.append(
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1, "max_value": 150, "strict_min": False, "strict_max": False},
        }
    )

    return exps

def _suite_id_from_create_response() -> str | None:
    p = OUT_DIR / "expectations" / "expectations_create_suite_response.json"
    if not p.exists():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    v = d.get("suite_id")
    return v if isinstance(v, str) and v.strip() else None