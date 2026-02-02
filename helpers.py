from __future__ import annotations

import os
import time
import hashlib
from pathlib import Path
from typing import Any, Callable, Optional, Tuple, Dict, List
from ddm_sdk.transport.errors import ServerError
from dataclasses import dataclass
import json

# ----------------------------
# env helpers
# ----------------------------
OUT_DIR = Path("out") / "tests"

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
        return None


# ----------------------------
# generic helpers
# ----------------------------

def safe_call(label: str, fn: Callable[[], Any]) -> Any:
    """Run a call, print a friendly result, keep tests running on errors."""
    try:
        out = fn()
        print(f"✅ {label}")
        return out
    except Exception as e:
        print(f"❌ {label} failed: {type(e).__name__}: {e}")
        return None

def sha256_hex_of_file(path: str) -> Optional[str]:
    if not path:
        return None
    p = Path(path)
    if not p.exists():
        return None
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ----------------------------
# tasks helpers
# ----------------------------

def task_id_from_taskref(obj: Any) -> Optional[str]:
    """
    TaskRef can be:
      - {"task_id": "..."}  (dict)
      - {"id": "..."}       (dict)
      - model with .task_id or .id
    """
    if obj is None:
        return None
    if isinstance(obj, dict):
        v = obj.get("task_id") or obj.get("id")
        return v if isinstance(v, str) and v else None
    for k in ("task_id", "id"):
        v = getattr(obj, k, None)
        if isinstance(v, str) and v:
            return v
    return None

@dataclass
class _FallbackTaskStatus:
    state: str
    error: Optional[str] = None
    message: Optional[str] = None
    result: Any = None

    def is_ready(self) -> bool:
        return True

    def is_success(self) -> bool:
        return self.state == "SUCCESS"

    def is_failure(self) -> bool:
        return self.state == "FAILURE"


def poll_task_until_ready(client, task_id: str, *, timeout_s: int = 60, interval_s: float = 1.0):
    t0 = time.time()
    last_state = None

    while time.time() - t0 < timeout_s:
        try:
            st = client.tasks.status(task_id)
        except ServerError as e:
            msg = str(e)
            return _FallbackTaskStatus(state="FAILURE", error=msg, message=msg, result={"raw_error": msg})

        if getattr(st, "state", None) != last_state:
            print(f"  ⏳ task {task_id} state={st.state}")
            last_state = st.state

        if st.is_ready():
            return st

        time.sleep(interval_s)

    return None

def get_task_value(client, task_id: str) -> Any:
    """
    Uses client.tasks.result(task_id) -> TaskResultResponse {ready, successful, value}
    """
    res = client.tasks.result(task_id)
    if not getattr(res, "ready", False):
        return None
    return getattr(res, "value", None)


# ----------------------------
# tiny extraction helpers
# ----------------------------

def find_first_str(d: Any, keys: list[str]) -> Optional[str]:
    if isinstance(d, dict):
        for k in keys:
            v = d.get(k)
            if isinstance(v, str) and v:
                return v
        for v in d.values():
            if isinstance(v, dict):
                out = find_first_str(v, keys)
                if out:
                    return out
    return None

def extract_report_uri(task_value: Any) -> Optional[str]:
    # backend report task returns {catalog_id,file_format,network,report_uri}
    return find_first_str(task_value, ["report_uri", "reportURI", "ipfs_uri", "uri"])

def extract_suite_hash(task_value: Any) -> Optional[str]:
    return find_first_str(task_value, ["suiteHash", "suite_hash", "hash"])

def safe_filename(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in s)

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")



def write_artifact(name: str, obj: Any, *, subdir: str | Path = "", suffix: str = "") -> Path:
    # If subdir is absolute, don't join again
    sub = Path(subdir)
    base = sub if sub.is_absolute() else (OUT_DIR / sub if subdir else OUT_DIR)

    base.mkdir(parents=True, exist_ok=True)
    p = base / f"{safe_filename(name)}{suffix}.json"
    write_json(p, obj)
    print(f"📄 wrote artifact: {p}")
    return p


def normalize_abi(abi: Any) -> Any:
    if isinstance(abi, str):
        try:
            return json.loads(abi)
        except Exception:
            return abi
    return abi

def unwrap_task_value(val: Any) -> Any:
    # backend sometimes returns {"result": {...}}
    if isinstance(val, dict) and "result" in val:
        return val["result"]
    return val


def parse_expectations_catalog(task_value: Any) -> dict:
    """
    Returns the task 'result' dict that contains categorized expectations.
    Expected shape:
      {"result": {"categorized": {...}}}  OR {"categorized": {...}}
    """
    if isinstance(task_value, dict):
        if "result" in task_value and isinstance(task_value["result"], dict):
            return task_value["result"]
        return task_value
    return {}


def parse_column_descriptions(task_value: Any) -> Tuple[Dict[str, str], List[str]]:
    """
    Converts the description task output into:
      - column_descriptions: {col: description}
      - column_names: [col1, col2, ...]  (in returned order)
    Expected shape:
      {"result": [{"column": "...", "description": "..."}, ...]}
      OR directly [{"column": "...", "description": "..."}, ...]
    """
    items = None
    if isinstance(task_value, dict) and isinstance(task_value.get("result"), list):
        items = task_value["result"]
    elif isinstance(task_value, list):
        items = task_value

    col_desc: Dict[str, str] = {}
    col_names: List[str] = []
    if not items:
        return col_desc, col_names

    for it in items:
        if not isinstance(it, dict):
            continue
        c = it.get("column")
        d = it.get("description")
        if isinstance(c, str) and c and isinstance(d, str):
            col_desc[c] = d
            col_names.append(c)

    return col_desc, col_names

# in helpers.py
def get_task_value_fallback_from_status(client, task_id: str) -> Any:
    """
    Some deployments forbid /ddm/tasks/result but allow /ddm/tasks/status.
    Try to pull 'result' off status if backend includes it.
    """
    st = client.tasks.status(task_id)
    # common names: result, value, output
    for k in ("result", "value", "output"):
        v = getattr(st, k, None)
        if v is not None:
            return v
    return None
