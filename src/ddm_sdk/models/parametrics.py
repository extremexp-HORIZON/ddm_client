from __future__ import annotations

from typing import Any, Dict, List, Tuple
from pydantic import BaseModel, ConfigDict


class CategorizedExpectationsResponse(BaseModel):
    """
    GET /parametrics/categorized-expectations
    Returns: dict (nested) produced by build_metadata()
    """
    model_config = ConfigDict(extra="allow")
    data: Dict[str, Any]


class AllExpectationsResponse(BaseModel):
    """
    GET /parametrics/all-expectations
    Returns: {"all_expectations": [...]}
    """
    model_config = ConfigDict(extra="allow")
    all_expectations: List[Dict[str, Any]]


class SuiteTuple(BaseModel):
    """
    /parametrics/suite-tuples returns list of tuples:
      (id, suite_name, use_case)
    Flask jsonify will turn tuples into lists.
    """
    id: str
    suite_name: str
    use_case: Any = None

    @classmethod
    def from_row(cls, row: Any) -> "SuiteTuple":
        # row is likely [id, name, use_case]
        return cls(id=row[0], suite_name=row[1], use_case=row[2] if len(row) > 2 else None)


class SupportedFileTypesResponse(BaseModel):
    """
    Both supported-file-types endpoints return GROUPED_* constants,
    shaped like:
      { "csv": ["csv"], "excel": ["xls","xlsx"], ... }
    """
    model_config = ConfigDict(extra="allow")
    grouped: Dict[str, Any]
