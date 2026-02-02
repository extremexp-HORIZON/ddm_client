from __future__ import annotations

from pathlib import Path
from typing import Optional

from .base import Storage
from .fs import FileStorage


def make_storage(backend: str, storage_dir: Optional[str]) -> Optional[Storage]:
    """
    Returns a Storage implementation or None if disabled.
    """
    if not storage_dir:
        return None

    backend = (backend or "fs").lower().strip()
    root = Path(storage_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)

    if backend in ("fs", "file", "json"):
        return FileStorage(root)

    # future:
    # if backend == "sqlite": return SqliteStorage(...)
    # if backend == "mongo": return MongoStorage(...)
    raise ValueError(f"Unsupported storage backend: {backend}")
