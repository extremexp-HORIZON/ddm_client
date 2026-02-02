from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field


# ---------- Core result models ----------

class ValidationResultCreate(BaseModel):
    """
    POST /ddm/validations/results
    """
    model_config = ConfigDict(extra="allow")

    user_id: str
    suite_id: str
    dataset_name: str

    # optional / backend-dependent
    suite_name: Optional[str] = None
    dataset_id: Optional[str] = None
    result_summary: Optional[Dict[str, Any]] = None
    detailed_results: Optional[Dict[str, Any]] = None
    path: Optional[str] = None


class ValidationResultResponse(BaseModel):
    """
    GET /ddm/validations/results/{id} or list item.
    Allow extras because backend can include lots of fields.
    """
    model_config = ConfigDict(extra="allow")

    id: str
    user_id: Optional[str] = None
    suite_id: Optional[str] = None

    suite_name: Optional[str] = None
    dataset_name: Optional[str] = None
    dataset_id: Optional[str] = None

    # payloads
    result_summary: Any = None
    detailed_results: Any = None
    column_descriptions: Any = None
    column_names: Any = None
    expectation_descriptions: Any = None

    run_time: Optional[str] = None


class ValidationResultsListResponse(BaseModel):
    """
    GET /ddm/validations/results
    """
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    # Some backends may return null for data; tolerate that.
    data: List[ValidationResultResponse] = Field(default_factory=list)

    total: int = 0
    filtered_total: int = Field(default=0, validation_alias="filtered_total")
    page: int = 1
    perPage: int = Field(default=10, validation_alias="perPage")

    # ---- common alternate keys from other backends ----
    # If your backend ever returns filteredTotal/per_page etc.,
    # these aliases make it “just work”.
    # (Pydantic v2: use validation_alias)
    # You can uncomment if needed:
    # filtered_total: int = Field(default=0, validation_alias=("filtered_total", "filteredTotal"))
    # perPage: int = Field(default=10, validation_alias=("perPage", "per_page"))


class SaveValidationResultResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    id: Optional[str] = None


# ---------- Validation task triggers ----------

class ValidateFilesAgainstSuiteRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    suite_id: str
    file_ids: List[str]


class ValidateFileAgainstSuitesRequest(BaseModel):
    model_config = ConfigDict(extra="allow")
    file_id: str
    suite_ids: List[str]


class ValidationTaskRef(BaseModel):
    model_config = ConfigDict(extra="allow")

    file_id: Optional[str] = None
    task_id: Optional[str] = None


class ValidateFilesAgainstSuiteResponse(BaseModel):
    """
    202 response: { message, tasks: [{file_id, task_id}, ...] }
    409 response: { error, already_validated_file_ids: [...] }
    """
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    tasks: List[ValidationTaskRef] = Field(default_factory=list)

    error: Optional[str] = None
    already_validated_file_ids: List[str] = Field(default_factory=list)


class ValidateFileAgainstSuitesResponse(BaseModel):
    """
    202: { message, task_id }
    409: { error, existing_suite_ids }
    """
    model_config = ConfigDict(extra="allow")

    message: Optional[str] = None
    task_id: Optional[str] = None

    error: Optional[str] = None
    existing_suite_ids: List[str] = Field(default_factory=list)
