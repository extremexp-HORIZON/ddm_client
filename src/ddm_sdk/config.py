# config.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def _load_env() -> None:
    cwd_env = Path.cwd() / ".env"
    pkg_env = Path(__file__).resolve().parents[2] / ".env"

    if pkg_env.exists():
        load_dotenv(pkg_env, override=False)
    if cwd_env.exists():
        load_dotenv(cwd_env, override=False)


_load_env()


@dataclass(frozen=True)
class Settings:
    base_url: str
    token: Optional[str] = None
    timeout: int = 30

    # 🔐 auth service
    auth_url: Optional[str] = None

    # 💾 storage (optional)
    storage_backend: str = "fs"          # reserved for future
    storage_dir: Optional[str] = None    # None => disabled or default chosen elsewhere

    # 🧪 optional test helpers
    test_network: str = "sepolia"
    test_tx_hash: Optional[str] = None
    test_contract_address: Optional[str] = None
    test_requester: Optional[str] = None


def get_settings() -> Settings:
    base_url = os.getenv("DDM_BASE_URL", "").strip()
    if not base_url:
        raise RuntimeError("DDM_BASE_URL is missing. Put it in .env or environment variables.")

    token = os.getenv("DDM_TOKEN")
    timeout_s = os.getenv("DDM_TIMEOUT", "30").strip()
    try:
        timeout = int(timeout_s)
    except ValueError:
        timeout = 30

    auth_url = os.getenv("DDM_AUTH_URL", "").strip() or None

    # storage config (optional)
    storage_backend = os.getenv("DDM_STORAGE_BACKEND", "fs").strip() or "fs"
    storage_dir = os.getenv("DDM_STORAGE_DIR", "").strip() or None

    return Settings(
        base_url=base_url,
        token=(token.strip() if token else None),
        timeout=timeout,
        auth_url=auth_url,

        storage_backend=storage_backend,
        storage_dir=storage_dir,

        test_network=os.getenv("DDM_TEST_NETWORK", "sepolia").strip(),
        test_tx_hash=os.getenv("DDM_TEST_TX_HASH") or None,
        test_contract_address=os.getenv("DDM_TEST_CONTRACT_ADDRESS") or None,
        test_requester=os.getenv("DDM_TEST_REQUESTER") or None,
    )
