from __future__ import annotations

from typing import Any, Dict, Optional, Sequence, Union

from ..transport.http import HttpTransport
from ..models.validations import (
    SaveValidationResultResponse,
    ValidateFileAgainstSuitesRequest,
    ValidateFileAgainstSuitesResponse,
    ValidateFilesAgainstSuiteRequest,
    ValidateFilesAgainstSuiteResponse,
    ValidationResultCreate,
    ValidationResultResponse,
    ValidationResultsListResponse,
)


class ValidationsAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    # ---------- POST /ddm/validations/results ----------
    def save_result(
        self,
        body: Union[ValidationResultCreate, Dict[str, Any]],
    ) -> SaveValidationResultResponse:
        payload = body.model_dump(exclude_none=True) if isinstance(body, ValidationResultCreate) else dict(body)
        resp = self._http.request("POST", "/ddm/validations/results", json=payload)
        return SaveValidationResultResponse.model_validate(resp)

    # ---------- GET /ddm/validations/results ----------
    def list_results(
        self,
        *,
        dataset_name: Optional[Sequence[str]] = None,
        dataset_id: Optional[Sequence[str]] = None,
        user_id: Optional[Sequence[str]] = None,
        suite_id: Optional[Sequence[str]] = None,
        run_time_from: Optional[str] = None,  # ISO 8601
        run_time_to: Optional[str] = None,    # ISO 8601
        sort: str = "run_time,desc",
        page: int = 1,
        perPage: int = 10,
    ) -> ValidationResultsListResponse:
        params: Dict[str, Any] = {
            "sort": sort,
            "page": page,
            "perPage": perPage,
        }
        # Your parser is likely split-based; send CSV.
        if dataset_name:
            params["dataset_name"] = ",".join(dataset_name)
        if dataset_id:
            params["dataset_id"] = ",".join(dataset_id)
        if user_id:
            params["user_id"] = ",".join(user_id)
        if suite_id:
            params["suite_id"] = ",".join(suite_id)
        if run_time_from:
            params["run_time_from"] = run_time_from
        if run_time_to:
            params["run_time_to"] = run_time_to

        resp = self._http.request("GET", "/ddm/validations/results", params=params)
        return ValidationResultsListResponse.model_validate(resp)

    # ---------- GET /ddm/validations/results/{result_id} ----------
    def get_result(self, result_id: str) -> ValidationResultResponse:
        resp = self._http.request("GET", f"/ddm/validations/results/{result_id}")
        return ValidationResultResponse.model_validate(resp)

    # ---------- POST /ddm/validations/validate/files-against-suite ----------
    def validate_files_against_suite(
        self,
        body: Union[ValidateFilesAgainstSuiteRequest, Dict[str, Any]],
    ) -> ValidateFilesAgainstSuiteResponse:
        payload = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else dict(body)
        resp = self._http.request("POST", "/ddm/validations/validate/files-against-suite", json=payload)
        return ValidateFilesAgainstSuiteResponse.model_validate(resp)

    # ---------- POST /ddm/validations/validate/file-against-suites ----------
    def validate_file_against_suites(
        self,
        body: Union[ValidateFileAgainstSuitesRequest, Dict[str, Any]],
    ) -> ValidateFileAgainstSuitesResponse:
        payload = body.model_dump(exclude_none=True) if hasattr(body, "model_dump") else dict(body)
        resp = self._http.request("POST", "/ddm/validations/validate/file-against-suites", json=payload)
        return ValidateFileAgainstSuitesResponse.model_validate(resp)
