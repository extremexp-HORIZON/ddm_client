from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, safe_call, write_artifact


def test_05_catalog_advanced(client):

    user = getenv_str("DDM_TEST_USER")
    users = [user] if user else None


    metadata_flag = True 

    filters = {"metadata": metadata_flag}
    if users:
        filters["user_id"] = users

    resp = safe_call("catalog.advanced", lambda: client.catalog.advanced(filters))
    assert resp is not None, "catalog.advanced returned None"
    assert isinstance(resp, list), f"catalog.advanced expected list got {type(resp)}"

    write_artifact(
        "catalog_advanced_response",
        resp,
        subdir="catalog",
    )
