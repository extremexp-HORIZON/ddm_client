from __future__ import annotations

import pytest
from helpers import getenv_str, safe_call, write_artifact
from ddm_sdk.models.user import PreferredQueryCreateRequest


def test_04_user_preferred_queries_roundtrip(client):
    # Build a dict query from env (simple + stable)
    name = getenv_str("DDM_TEST_PREFERRED_QUERY_NAME", "pytest-query")

    query_dict = {
        "suite_name": getenv_str("DDM_TEST_EXPECT_SUITE_NAME", "my_suite"),
        "sort": "created,desc",
        "page": 1,
        "perPage": 10,
    }

    created = safe_call(
        "user.save_preferred_query",
        lambda: client.user.save_preferred_query(
            PreferredQueryCreateRequest(name=name, query=query_dict)
        ),
    )
    assert created is not None, "save_preferred_query returned None"

    write_artifact(
        "user_save_preferred_query_response",
        created.model_dump(mode="json", exclude_none=False),
        subdir="user",
    )

    assert created.query is not None, "save_preferred_query response missing query"
    qid = created.query.id

    # Verify list includes it
    listed = safe_call("user.list_preferred_queries", lambda: client.user.list_preferred_queries(limit=50))
    assert listed is not None, "list_preferred_queries returned None"

    write_artifact(
        "user_list_preferred_queries",
        listed.model_dump(mode="json", exclude_none=False),
        subdir="user",
    )

    found = [q for q in listed.data if q.id == qid]
    assert found, "Created preferred query id not found in list"

    # Delete it
    deleted = safe_call("user.delete_preferred_query", lambda: client.user.delete_preferred_query(qid))
    assert deleted is not None, "delete_preferred_query returned None"

    write_artifact(
        "user_delete_preferred_query_response",
        deleted.model_dump(mode="json", exclude_none=False),
        subdir="user",
    )

    # Some backends return deleted.id
    if deleted.id is not None:
        assert deleted.id == qid
