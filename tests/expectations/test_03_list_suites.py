from __future__ import annotations


from helpers import getenv_bool, getenv_str, getenv_int, safe_call, write_artifact


def test_03_expectations_list_suites(client):

    page = getenv_int("DDM_TEST_EXPECTATIONS_PAGE") or 1
    per_page = getenv_int("DDM_TEST_EXPECTATIONS_PER_PAGE") or 10
    sort = getenv_str("DDM_TEST_EXPECTATIONS_SORT", "created,desc")

    resp = safe_call(
        "expectations.list_suites",
        lambda: client.expectations.list_suites(page=page, perPage=per_page, sort=sort),
    )
    assert resp is not None, "list_suites returned None"

    write_artifact(
        "expectations_list_suites_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="expectations",
    )

    data = getattr(resp, "data", None)
    assert isinstance(data, list), f"list_suites data missing or not list: {type(data)}"
