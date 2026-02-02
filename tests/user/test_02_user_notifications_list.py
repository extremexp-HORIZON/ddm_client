from __future__ import annotations

from helpers import getenv_int, safe_call, write_artifact


def test_02_user_list_notifications_unread(client):
    limit = getenv_int("DDM_TEST_NOTIFICATIONS_LIMIT") or 10

    resp = safe_call(
        "user.list_notifications",
        lambda: client.user.list_notifications(onlyUnread=True, limit=limit),
    )
    assert resp is not None, "list_notifications returned None"

    write_artifact(
        "user_list_notifications_unread",
        resp.model_dump(mode="json", exclude_none=False),
        subdir="user",
    )

    # schema sanity
    assert isinstance(resp.data, list)
    assert isinstance(resp.total, int)
    assert isinstance(resp.unread, int)
