from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, safe_call, write_artifact


def test_03_catalog_options(client):
    if not getenv_bool("DDM_TEST_CATALOG_OPTIONS", True):
        pytest.skip("DDM_TEST_CATALOG_OPTIONS not enabled")

    project_id = getenv_str("DDM_TEST_CATALOG_PROJECT_ID")
    filename = getenv_str("DDM_TEST_CATALOG_FILENAME")
    user_id = getenv_str("DDM_TEST_CATALOG_USER_ID")

    resp = safe_call(
        "catalog.options",
        lambda: client.catalog.options(
            project_id=project_id,
            filename=filename,
            user_id=user_id,
        ),
    )
    assert resp is not None, "catalog.options returned None"
    assert isinstance(resp, list), f"catalog.options expected list got {type(resp)}"

    write_artifact(
        "catalog_options_response",
        [x.model_dump() if hasattr(x, "model_dump") else x for x in resp],
        subdir="catalog",
    )
