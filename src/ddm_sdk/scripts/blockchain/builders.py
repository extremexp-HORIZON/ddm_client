from __future__ import annotations

from typing import Any, Dict, Optional


def _first_str(v: Any) -> Optional[str]:
    return v if isinstance(v, str) and v.strip() else None


def build_prepare_suite_payload_from_suite_record(
    suite_record: Dict[str, Any],
    *,
    network: str,
    requester: str,
    deadline: int,
    total_expected: int,
    file_format: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convert a stored expectations suite record (what you save after create+poll)
    into the payload required by:
      POST /ddm/blockchain/suites/prepare

    Required by backend: requester, expectation_suite_id, suite, fileFormat, deadline, totalExpected (+network/category)
    """

    # suite id (must exist)
    suite_id = _first_str(suite_record.get("id")) or _first_str(suite_record.get("suite_id"))
    if not suite_id:
        raise ValueError("suite_record is missing suite id (expected key 'id' or 'suite_id').")

    # file formats
    file_formats = suite_record.get("fileFormats") or suite_record.get("file_formats") or suite_record.get("file_types")
    if isinstance(file_formats, str):
        file_formats = [file_formats]
    if not isinstance(file_formats, list):
        file_formats = []

    # choose file format
    ff = _first_str(file_format) or (_first_str(file_formats[0]) if file_formats else None) or "csv"

    # category
    category = _first_str(suite_record.get("category")) or "default"

    # Build the nested suite object in the exact shape DDM expects
    suite_obj: Dict[str, Any] = {
        "name": _first_str(suite_record.get("name")) or _first_str(suite_record.get("suite_name")) or suite_id,
        "description": suite_record.get("description"),
        "category": category,
        "fileFormats": file_formats or [ff],
        "column_names": suite_record.get("column_names") or suite_record.get("columnNames") or [],
        "column_descriptions": suite_record.get("column_descriptions") or suite_record.get("columnDescriptions") or {},
        "expectations": suite_record.get("expectations") or {},
        "selectedExpectations": suite_record.get("selectedExpectations") or suite_record.get("selected_expectations") or {},
        "tableExpectations": suite_record.get("tableExpectations") or suite_record.get("table_expectations") or {},
        "expectation_descriptions": suite_record.get("expectation_descriptions") or suite_record.get("expectationDescriptions") or {},
        "expectation_suite_id": suite_id,
    }

    payload: Dict[str, Any] = {
        "network": network,
        "requester": requester,
        "expectation_suite_id": suite_id,
        "category": category,
        "fileFormat": ff,
        "deadline": int(deadline),
        "totalExpected": int(total_expected),
        "suite": suite_obj,
    }
    return payload
