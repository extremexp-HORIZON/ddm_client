from __future__ import annotations

from helpers import safe_call, write_artifact


def test_04_expectations_get_suite(client, suite_id: str):

    resp = safe_call("expectations.get_suite", lambda: client.expectations.get_suite(suite_id))
    assert resp is not None, "get_suite returned None"

    write_artifact(
        "expectations_get_suite_response",
        resp.model_dump() if hasattr(resp, "model_dump") else resp,
        subdir="expectations",
    )

    got_id = getattr(resp, "suite_id", None)
    if got_id:
        assert got_id == suite_id, f"suite_id mismatch: expected={suite_id} got={got_id}"
