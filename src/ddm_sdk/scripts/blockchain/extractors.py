from __future__ import annotations

from typing import Any, Optional


def _find_first_str(obj: Any, keys: list[str]) -> Optional[str]:
    # dict: check keys + recurse values
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in obj.values():
            out = _find_first_str(v, keys)
            if out:
                return out

    # list/tuple: recurse items
    elif isinstance(obj, (list, tuple)):
        for it in obj:
            out = _find_first_str(it, keys)
            if out:
                return out

    return None


def extract_suite_hash(task_value: Any) -> Optional[str]:
    return _find_first_str(task_value, ["suiteHash", "suite_hash", "suitehash", "hash"])


def extract_report_uri(task_value: Any) -> Optional[str]:
    return _find_first_str(task_value, ["report_uri", "reportURI", "ipfs_uri", "uri"])
