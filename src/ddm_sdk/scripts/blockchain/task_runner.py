from __future__ import annotations

from typing import Any, Dict, Optional, Callable

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.blockchain.utils import store_blockchain_snapshot, append_blockchain_log


def run_task_and_store(
    client: DdmClient,
    *,
    action: str,
    request_payload: Dict[str, Any],
    call_fn: Callable[[], Any],
    poll: bool = True,
    timeout_s: float = 300.0,
    interval_s: float = 1.0,
    no_store: bool = False,
) -> Dict[str, Any]:
    """
    Standardize:
      - store request
      - call
      - optional poll task
      - store status/result
      - append logs
    """
    saved: Dict[str, Optional[str]] = {"request": None, "response": None, "status": None, "result": None}

    if client.storage and not no_store:
        saved["request"] = store_blockchain_snapshot(client, action=action, payload=request_payload, name="request")

    ref = call_fn()
    task_id = getattr(ref, "task_id", None)
    resp_payload = ref.model_dump(mode="json", exclude_none=False) if hasattr(ref, "model_dump") else {"task_id": task_id }
    if client.storage and not no_store:
        saved["response"] = store_blockchain_snapshot(client, action=action, payload=resp_payload, name="taskref")

    status_payload = None
    result_payload = None

    if poll and isinstance(task_id, str) and task_id:
        st = client.tasks.wait(task_id, timeout_s=timeout_s, poll_interval_s=interval_s, raise_on_failure=False)
        status_payload = st.model_dump(mode="json", exclude_none=False) if hasattr(st, "model_dump") else getattr(st, "__dict__", {"state": getattr(st, "state", None)})
        if getattr(st, "state", None) == "SUCCESS":
            try:
                res = client.tasks.result(task_id)
                result_payload = res.model_dump(mode="json", exclude_none=False) if hasattr(res, "model_dump") else getattr(res, "__dict__", {})
            except Exception:

                result_payload = {"result": getattr(st, "result", None)}

        if client.storage and not no_store:
            saved["status"] = store_blockchain_snapshot(client, action=action, payload=status_payload, name="status")
            if result_payload is not None:
                saved["result"] = store_blockchain_snapshot(client, action=action, payload=result_payload, name="result")

    ok = True
    if status_payload and isinstance(status_payload, dict) and status_payload.get("state") == "FAILURE":
        ok = False

    if client.storage and not no_store:
        append_blockchain_log(
            client,
            action=action,
            ok=ok,
            details={"task_id": task_id, "saved": saved},
        )

    return {
        "ok": ok,
        "task_id": task_id,
        "taskref": resp_payload,
        "status": status_payload,
        "result": result_payload,
        "saved": saved,
    }
