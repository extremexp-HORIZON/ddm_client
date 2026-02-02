from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated


def _norm_username(username: str) -> str:
    u = (username or "").strip()
    if not u:
        raise ValueError("username is required")
    return u


def _user_pic_base_key(username: str) -> str:
    # users/<username>/profile/pic/<stem><ext>
    username = _norm_username(username)
    return f"users/{username}/profile/pic"


def _pick_ext_from_filename(filename: str, forced_ext: Optional[str]) -> str:
    # ext precedence: --ext > suffix(filename) > .png
    ext = forced_ext or Path(filename).suffix or ".png"
    if not ext.startswith("."):
        ext = f".{ext}"
    return ext


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ddm-download-profile-pic",
        description="Download a user's profile picture",
    )
    ap.add_argument("--username", required=True, help="Username (used for storage tree)")
    ap.add_argument("--filename", required=True, help="Profile pic filename on server (e.g. abc.png)")
    ap.add_argument("--out", default=None, help="Optional output path. If omitted, uses storage when enabled.")
    ap.add_argument("--ext", default=None, help="Force extension (e.g. .png). If omitted, uses filename suffix; else .png.")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    username = _norm_username(args.username)
    filename = (args.filename or "").strip()
    if not filename:
        raise SystemExit("Missing --filename")

    client = DdmClient.from_env()
    ensure_authenticated(client)

    blob = client.user.get_profile_picture_bytes(filename)

    saved_to: Optional[str] = None

    # 1) explicit --out always wins
    if args.out:
        out_path = Path(args.out).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    # 2) storage path if enabled and supports bytes
    elif client.storage and (not args.no_store) and hasattr(client.storage, "write_bytes"):
        ext = _pick_ext_from_filename(filename, args.ext)
        stem = Path(filename).stem or "profile_pic"
        base_key = _user_pic_base_key(username)

        # final: users/<username>/profile/pic/<stem><ext>
        saved_to = client.storage.write_bytes(f"{base_key}/{stem}", blob, ext=ext)

        # store a small json receipt next to it (optional, but consistent with your style)
        if hasattr(client.storage, "write_json"):
            client.storage.write_json(
                f"{base_key}/pic",
                {
                    "username": username,
                    "filename": filename,
                    "saved_to": saved_to,
                    "bytes": len(blob),
                },
            )

    # 3) fallback local path (real filename, not .bin)
    else:
        ext = _pick_ext_from_filename(filename, args.ext)
        out_path = Path(Path(filename).stem + ext).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(blob)
        saved_to = str(out_path)

    print(json.dumps({"ok": True, "username": username, "filename": filename, "saved_to": saved_to, "bytes": len(blob)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
