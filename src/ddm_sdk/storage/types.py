# src/ddm_sdk/storage/capabilities.py
from __future__ import annotations
from typing import Protocol

class BytesStorage(Protocol):
    def write_bytes(self, key: str, data: bytes, *, ext: str = ".bin") -> str: ...
    def read_bytes(self, key: str, *, ext: str = ".bin") -> bytes | None: ...
