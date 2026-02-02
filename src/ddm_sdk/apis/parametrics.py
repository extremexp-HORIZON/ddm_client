from __future__ import annotations

from typing import Any, Dict, List

from ..transport.http import HttpTransport
from ..models.parametrics import (
    AllExpectationsResponse,
    CategorizedExpectationsResponse,
    SuiteTuple,
)


class ParametricsAPI:
    def __init__(self, http: HttpTransport):
        self._http = http

    def df_supported_file_types(self) -> Dict[str, Any]:
        """
        GET /ddm/parametrics/df-supported-file-types
        Returns GROUPED_DF_SUPPORTED_EXTENSIONS (raw JSON)
        """
        return self._http.request("GET", "/ddm/parametrics/df-supported-file-types")

    def all_supported_file_types(self) -> Dict[str, Any]:
        """
        GET /ddm/parametrics/all-supported-file-types
        Returns GROUPED_SUPPORTED_EXTENSIONS (raw JSON)
        """
        return self._http.request("GET", "/ddm/parametrics/all-supported-file-types")

    def categorized_expectations(self) -> CategorizedExpectationsResponse:
        """
        GET /ddm/parametrics/categorized-expectations
        Returns build_metadata() dict
        """
        raw = self._http.request("GET", "/ddm/parametrics/categorized-expectations")
        return CategorizedExpectationsResponse(data=raw)

    def all_expectations(self) -> AllExpectationsResponse:
        """
        GET /ddm/parametrics/all-expectations
        Returns {"all_expectations": [...]}
        """
        raw = self._http.request("GET", "/ddm/parametrics/all-expectations")
        return AllExpectationsResponse.model_validate(raw)

    def suite_tuples(self) -> List[SuiteTuple]:
        """
        GET /ddm/parametrics/suite-tuples
        Returns list like: [[id, name, use_case], ...]
        """
        raw = self._http.request("GET", "/ddm/parametrics/suite-tuples")
        return [SuiteTuple.from_row(row) for row in (raw or [])]
