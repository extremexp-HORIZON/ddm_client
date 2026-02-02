from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, getenv_int, safe_call, write_artifact


def test_01_catalog_list(client):
    if not getenv_bool("DDM_TEST_CATALOG_LIST", True):
        pytest.skip("DDM_TEST_CATALOG_LIST not enabled")

    page = getenv_int("DDM_TEST_CATALOG_PAGE") or 1
    per_page = getenv_int("DDM_TEST_CATALOG_PER_PAGE") or 10
    sort = getenv_str("DDM_TEST_CATALOG_SORT", "created,desc")

    project_id = getenv_str("DDM_TEST_CATALOG_PROJECT_ID")
    filename = getenv_str("DDM_TEST_CATALOG_FILENAME")
    user_id = getenv_str("DDM_TEST_CATALOG_USER_ID")
    file_type = getenv_str("DDM_TEST_CATALOG_FILE_TYPE")

    resp = safe_call(
        "catalog.list",
        lambda: client.catalog.list(
            project_id=[project_id] if project_id else None,
            filename=[filename] if filename else None,
            user_id=[user_id] if user_id else None,
            file_type=[file_type] if file_type else None,
            sort=sort,
            page=page,
            perPage=per_page,
        ),
    )
    assert resp is not None, "catalog.list returned None"

    write_artifact(
        "catalog_list_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="catalog",
    )

    # sanity checks
    total = getattr(resp, "total", None)
    data = getattr(resp, "data", None)
    assert data is not None, "catalog.list missing data"
    assert isinstance(data, list), f"catalog.list data not a list: {type(data)}"
    if total is not None:
        assert isinstance(total, int), f"catalog.list total not int: {type(total)}"
