from __future__ import annotations

import os

from ddm_sdk.client import DdmClient


def ensure_authenticated(client: DdmClient) -> None:
    """
    Ensure client has a token.

    Order:
    1) token already present (env DDM_TOKEN or loaded from storage by DdmClient.from_env)
    2) login using env DDM_USERNAME/DDM_PASSWORD (and client will persist token to storage)

    No CLI args, no prompts.
    """
    if client.token:
        return

    username = os.getenv("DDM_USERNAME", "").strip()
    password = os.getenv("DDM_PASSWORD", "").strip()

    if not username or not password:
        raise SystemExit(
            "Not authenticated.\n"
            "Set DDM_USERNAME and DDM_PASSWORD in .env (or set DDM_TOKEN).\n"
        )

    client.login(username, password)
