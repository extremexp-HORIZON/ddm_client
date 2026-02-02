from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, getenv_int, safe_call, write_artifact


def test_02_catalog_my_catalog(client):
    if not getenv_bool("DDM_TEST_CATALOG_MY_CATALOG", True):
        pytest.skip("DDM_TEST_CATALOG_MY_CATALOG not enabled")

    page = getenv_int("DDM_TEST_CATALOG_PAGE") or 1
    per_page = getenv_int("DDM_TEST_CATALOG_PER_PAGE") or 10
    sort = getenv_str("DDM_TEST_CATALOG_SORT", "created,desc")

    project_id = getenv_str("DDM_TEST_CATALOG_PROJECT_ID")
    filename = getenv_str("DDM_TEST_CATALOG_FILENAME")
    user_id = getenv_str("DDM_TEST_CATALOG_USER_ID")
    file_type = getenv_str("DDM_TEST_CATALOG_FILE_TYPE")

    resp = safe_call(
        "catalog.my_catalog",
        lambda: client.catalog.my_catalog(
            project_id=[project_id] if project_id else None,
            filename=[filename] if filename else None,
            user_id=[user_id] if user_id else None,
            file_type=[file_type] if file_type else None,
            sort=sort,
            page=page,
            perPage=per_page,
        ),
    )
    assert resp is not None, "catalog.my_catalog returned None"

    write_artifact(
        "catalog_my_catalog_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="catalog",
    )

    data = getattr(resp, "data", None)
    assert isinstance(data, list), "catalog.my_catalog data missing or not list"
