from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from ..transport.http import HttpTransport
from ..models.tasks import TaskResultResponse, TaskStatusResponse

class TaskFailedError(RuntimeError):
    def __init__(self, task_id: str, error: str | None = None):
        super().__init__(f"Task {task_id} failed: {error or 'unknown error'}")
        self.task_id = task_id
        self.error = error


@dataclass(frozen=True)
class WaitManyResult:
    statuses: Dict[str, TaskStatusResponse]
    pending: set[str]
    succeeded: set[str]
    failed: set[str]
    timed_out: bool

class TaskFailedError(RuntimeError):
    def __init__(self, task_id: str, error: str | None = None):
        super().__init__(f"Task {task_id} failed: {error or 'unknown error'}")
        self.task_id = task_id
        self.error = error


class TasksAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    def result(self, task_id: str) -> TaskResultResponse:
        resp = self._http.request("GET", f"/ddm/tasks/result/{task_id}")
        return TaskResultResponse.model_validate(resp)

    def status(self, task_id: str) -> TaskStatusResponse:
        try:
            resp = self._http.request("GET", f"/ddm/tasks/status/{task_id}")
            return TaskStatusResponse.model_validate(resp)
        except Exception as e:
            data = getattr(e, "response_json", None)
            if isinstance(data, dict):
                return TaskStatusResponse.model_validate(data)
            raise


    def wait(
        self,
        task_id: str,
        *,
        timeout_s: float = 300.0,
        poll_interval_s: float = 1.0,
        raise_on_failure: bool = True,
    ) -> TaskStatusResponse:
        """
        Poll /status until SUCCESS/FAILURE or timeout.
        """
        deadline = time.time() + float(timeout_s)

        while True:
            st = self.status(task_id)

            if st.is_ready():
                if raise_on_failure and st.is_failure():
                    raise TaskFailedError(task_id, st.error)
                return st

            if time.time() >= deadline:
                # Return last known state (caller decides what to do)
                return st

            time.sleep(float(poll_interval_s))

    def wait_for_result(
        self,
        task_id: str,
        *,
        timeout_s: float = 300.0,
        poll_interval_s: float = 1.0,
        raise_on_failure: bool = True,
    ):
        """
        Convenience: returns st.result on SUCCESS, raises on FAILURE (optional),
        returns None on timeout.
        """
        st = self.wait(
            task_id,
            timeout_s=timeout_s,
            poll_interval_s=poll_interval_s,
            raise_on_failure=raise_on_failure,
        )
        return st.result if st.is_success() else None
    
    def wait_many(
        self,
        task_ids: Iterable[str],
        *,
        timeout_s: float = 300.0,
        poll_interval_s: float = 1.0,
        raise_on_failure: bool = True,
        print_state: bool = False,
    ) -> WaitManyResult:
        ids = [str(t).strip() for t in task_ids if str(t).strip()]
        statuses: Dict[str, TaskStatusResponse] = {}
        pending = set(ids)
        succeeded: set[str] = set()
        failed: set[str] = set()

        deadline = time.time() + float(timeout_s)
        last_states: Dict[str, str] = {}

        while pending:
            for tid in list(pending):
                st = self.status(tid)
                statuses[tid] = st

                if print_state:
                    prev = last_states.get(tid)
                    if st.state != prev:
                        print(f"  ⏳ task {tid} state={st.state}")
                        last_states[tid] = st.state

                if st.is_ready():
                    pending.remove(tid)
                    if st.is_success():
                        succeeded.add(tid)
                    else:
                        failed.add(tid)
                        if raise_on_failure:
                            raise TaskFailedError(tid, st.error)

            if not pending:
                break
            if time.time() >= deadline:
                break

            time.sleep(float(poll_interval_s))

        return WaitManyResult(
            statuses=statuses,
            pending=pending,
            succeeded=succeeded,
            failed=failed,
            timed_out=bool(pending),
        )

    def value(self, task_id: str) -> Any:
        """
        Convenience: returns result.value from /result if ready, else None.
        """
        r = self.result(task_id)
        if not r.ready:
            return None
        return r.value
