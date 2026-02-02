from __future__ import annotations

import pytest
from helpers import getenv_str, safe_call, write_artifact


@pytest.fixture(scope="session")
def username() -> str:
    u = getenv_str("DDM_TEST_USER") or getenv_str("DDM_USERNAME")
    if not u:
        pytest.skip("Missing DDM_USERNAME (or DDM_TEST_USER)")
    return u


def test_01_user_get_profile(client, username: str):
    resp = safe_call("user.get_profile", lambda: client.user.get_profile(username))
    assert resp is not None, "get_profile returned None"

    write_artifact(
        "user_get_profile_response",
        resp.model_dump(mode="json", exclude_none=False),
        subdir="user",
    )

    assert resp.user.username == username, f"username mismatch: {resp.user.username} != {username}"
    assert isinstance(resp.user.sub, str) and resp.user.sub, "missing user.sub"
