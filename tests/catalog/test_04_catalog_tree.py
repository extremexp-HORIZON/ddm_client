from __future__ import annotations

import pytest

from helpers import getenv_bool, getenv_str, getenv_int, safe_call, write_artifact


def test_04_catalog_tree(client):
    if not getenv_bool("DDM_TEST_CATALOG_TREE", True):
        pytest.skip("DDM_TEST_CATALOG_TREE not enabled")

    parent = getenv_str("DDM_TEST_CATALOG_TREE_PARENT", "")
    page = getenv_int("DDM_TEST_CATALOG_TREE_PAGE") or 0
    per_page = getenv_int("DDM_TEST_CATALOG_TREE_PER_PAGE") or 20
    sort = getenv_str("DDM_TEST_CATALOG_TREE_SORT", "name,asc")

    resp = safe_call(
        "catalog.tree",
        lambda: client.catalog.tree(
            parent=parent,
            page=page,
            perPage=per_page,
            sort=sort,
        ),
    )
    assert resp is not None, "catalog.tree returned None"

    write_artifact(
        "catalog_tree_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="catalog",
    )

    # sanity
    nodes = getattr(resp, "nodes", None)
    assert nodes is not None, "catalog.tree missing nodes"
    assert isinstance(nodes, list), f"catalog.tree nodes not list: {type(nodes)}"
