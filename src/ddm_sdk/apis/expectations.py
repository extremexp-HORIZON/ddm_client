from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple, Union, BinaryIO
from pathlib import Path

from ..transport.http import HttpTransport
from ..models.expectations import (
    CreateSuiteResponse,
    ExpectationSuiteCreate,
    ExpectationSuiteListResponse,
    ExpectationSuiteResponse,
    UploadSampleResponse,
)


class ExpectationsAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    def upload_sample(
        self,
        file: Union[str, BinaryIO, Tuple[str, bytes], Tuple[str, bytes, str]],
        *,
        suite_name: Optional[str] = None,
        datasource_name: Optional[str] = None,
    ) -> UploadSampleResponse:
        data: Dict[str, Any] = {}
        if suite_name is not None:
            data["suite_name"] = suite_name
        if datasource_name is not None:
            data["datasource_name"] = datasource_name

        files: Dict[str, Any]

        # --- normalize file into requests-friendly multipart ---
        if isinstance(file, str):
            p = Path(file)
            f = p.open("rb")
            # Let requests close it after request? It won't. So we must close ourselves.
            try:
                files = {"file": (p.name, f, "text/csv")}
                resp = self._http.request(
                    "POST",
                    "/ddm/expectations/upload-sample",
                    data=data,
                    files=files,
                )
            finally:
                f.close()
            return UploadSampleResponse.model_validate(resp)

        # file-like object (BinaryIO)
        if hasattr(file, "read"):
            # Best effort filename; Flask needs one for extension
            name = getattr(file, "name", "upload.csv")
            name = Path(str(name)).name  # strip directories if any
            files = {"file": (name, file, "text/csv")}
            resp = self._http.request("POST", "/ddm/expectations/upload-sample", data=data, files=files)
            return UploadSampleResponse.model_validate(resp)

        # (filename, bytes) or (filename, bytes, content_type)
        if isinstance(file, tuple):
            if len(file) == 2:
                filename, content = file
                files = {"file": (filename, content, "text/csv")}
            elif len(file) == 3:
                filename, content, content_type = file
                files = {"file": (filename, content, content_type)}
            else:
                raise TypeError("file tuple must be (filename, bytes) or (filename, bytes, content_type)")

            resp = self._http.request("POST", "/ddm/expectations/upload-sample", data=data, files=files)
            return UploadSampleResponse.model_validate(resp)

        raise TypeError("Unsupported file type for upload_sample")


    # ---------- GET /ddm/expectations/suites ----------
    def list_suites(
        self,
        *,
        suite_name: Optional[Sequence[str]] = None,
        suite_id: Optional[Sequence[str]] = None,
        file_types: Optional[Sequence[str]] = None,
        category: Optional[Sequence[str]] = None,
        use_case: Optional[Sequence[str]] = None,
        user_id: Optional[Sequence[str]] = None,
        created_from: Optional[str] = None,   # ISO 8601
        created_to: Optional[str] = None,     # ISO 8601
        sort: str = "created,desc",
        page: int = 1,
        perPage: int = 10,
    ) -> ExpectationSuiteListResponse:
        """
        Filters match reqparse behavior: arrays are CSV in swagger.
        DDM backend parser uses action='split' (comma separated).
        """
        params: Dict[str, Any] = {
            "sort": sort,
            "page": page,
            "perPage": perPage,
        }
        if suite_name:
            params["suite_name"] = ",".join(suite_name)
        if suite_id:
            params["suite_id"] = ",".join(suite_id)
        if file_types:
            params["file_types"] = ",".join(file_types)
        if category:
            params["category"] = ",".join(category)
        if use_case:
            params["use_case"] = ",".join(use_case)
        if user_id:
            params["user_id"] = ",".join(user_id)
        if created_from:
            params["created_from"] = created_from
        if created_to:
            params["created_to"] = created_to

        resp = self._http.request("GET", "/ddm/expectations/suites", params=params)
        return ExpectationSuiteListResponse.model_validate(resp)

    # ---------- POST /ddm/expectations/suites ----------
    def create_suite(
        self,
        body: Union[ExpectationSuiteCreate, Dict[str, Any]],
    ) -> CreateSuiteResponse:
        payload = body.model_dump(exclude_none=True) if isinstance(body, ExpectationSuiteCreate) else dict(body)
        resp = self._http.request("POST", "/ddm/expectations/suites", json=payload)
        return CreateSuiteResponse.model_validate(resp)

    # ---------- GET /ddm/expectations/suites/{suite_id} ----------
    def get_suite(self, suite_id: str) -> ExpectationSuiteResponse:
        resp = self._http.request("GET", f"/ddm/expectations/suites/{suite_id}")
        return ExpectationSuiteResponse.model_validate(resp)
