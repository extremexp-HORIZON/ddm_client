from __future__ import annotations

from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict


class TaskResultResponse(BaseModel):
    """
    GET /ddm/tasks/result/<id>

    Backend returns:
      {
        "ready": bool,
        "successful": bool | null,
        "value": <any>
      }
    """
    model_config = ConfigDict(extra="allow")

    ready: bool
    successful: Optional[bool] = None
    value: Any = None


class TaskStatusResponse(BaseModel):
    """
    GET /ddm/tasks/status/<task_id>

    Backend returns one of:
      {"state":"PENDING","message":...}
      {"state":"SUCCESS","result": ...}
      {"state":"FAILURE","error": "..."}  (HTTP 500)
      {"state": <other>, "message": ...}
    """
    model_config = ConfigDict(extra="allow")

    state: str
    message: Optional[str] = None
    result: Any = None
    error: Optional[str] = None

    def is_ready(self) -> bool:
        return self.state in ("SUCCESS", "FAILURE")

    def is_success(self) -> bool:
        return self.state == "SUCCESS"

    def is_failure(self) -> bool:
        return self.state == "FAILURE"
