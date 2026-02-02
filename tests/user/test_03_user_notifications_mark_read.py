from __future__ import annotations

import pytest
from helpers import getenv_int, safe_call, write_artifact


def test_03_user_mark_one_notification_read(client):
    limit = getenv_int("DDM_TEST_NOTIFICATIONS_LIMIT") or 10

    resp = safe_call(
        "user.list_notifications(unread)",
        lambda: client.user.list_notifications(onlyUnread=True, limit=limit),
    )
    assert resp is not None, "list_notifications returned None"

    if not resp.data:
        pytest.skip("No unread notifications to mark read")

    n0 = resp.data[0]
    nid = n0.id

    marked = safe_call(
        "user.mark_notification_read",
        lambda: client.user.mark_notification_read(nid),
    )
    assert marked is not None, "mark_notification_read returned None"

    write_artifact(
        "user_mark_notification_read_response",
        marked.model_dump(mode="json", exclude_none=False),
        subdir="user",
    )

    # If backend returns notification object, assert it’s read.
    if marked.notification is not None:
        assert marked.notification.id == nid
        assert marked.notification.is_read is True
