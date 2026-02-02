from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass
class FileStorage:
    root: Path

    def _norm_key(self, key: str) -> str:
        key = key.replace("\\", "/").strip("/")
        if not key:
            raise ValueError("Invalid storage key: empty")
        if ".." in key.split("/"):
            raise ValueError(f"Invalid storage key: {key}")
        return key

    def _path_json(self, key: str) -> Path:
        key = self._norm_key(key)
        p = self.root / f"{key}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _path_blob(self, key: str, ext: str) -> Path:
        key = self._norm_key(key)
        ext = ext if ext.startswith(".") else f".{ext}"
        p = self.root / f"{key}{ext}"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    # ---- JSON ----

    def write_json(self, key: str, payload: Any) -> str:
        p = self._path_json(key)

        if hasattr(payload, "model_dump"):
            payload = payload.model_dump(mode="json", exclude_none=False)

        p.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return str(p)

    def read_json(self, key: str) -> Optional[Any]:
        p = self._path_json(key)
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None

    def delete(self, key: str) -> None:
        # deletes JSON key only (kept compatible)
        p = self._path_json(key)
        if p.exists():
            p.unlink()

    def write_bytes(self, key: str, data: bytes, *, ext: str = ".bin") -> str:
        key = key.replace("\\", "/").strip("/")
        if ".." in key.split("/"):
            raise ValueError(f"Invalid storage key: {key}")

        ext = ext if ext.startswith(".") else f".{ext}"
        p = self.root / f"{key}{ext}"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return str(p)


    def read_bytes(self, key: str, *, ext: str = ".bin") -> Optional[bytes]:
        p = self._path_blob(key, ext)
        if not p.exists():
            return None
        return p.read_bytes()

    def copy_file(self, key: str, src_path: str | Path, *, ext: Optional[str] = None) -> str:
        """
        Copy a local file into storage.
        If ext is None, uses source suffix; else uses ext.
        """
        src = Path(src_path).expanduser().resolve()
        if not src.exists() or not src.is_file():
            raise FileNotFoundError(f"Source file not found: {src}")

        use_ext = ext if ext is not None else (src.suffix or ".bin")
        p = self._path_blob(key, use_ext)
        p.write_bytes(src.read_bytes())
        return str(p)
