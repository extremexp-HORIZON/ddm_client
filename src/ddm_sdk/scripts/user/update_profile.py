from __future__ import annotations

import argparse
import json
from pathlib import Path

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.user.utils import store_user_result, append_user_log


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-user-update-profile", description="Update user profile")
    ap.add_argument("--username", required=True)
    ap.add_argument("--public_key", default=None)
    ap.add_argument("--profile_pic", default=None, help="Path to image file to upload")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    if not args.public_key and not args.profile_pic:
        raise SystemExit("Nothing to update: provide --public_key and/or --profile_pic")

    pic_path = None
    if args.profile_pic:
        p = Path(args.profile_pic).expanduser().resolve()
        if not p.exists() or not p.is_file():
            raise SystemExit(f"profile_pic not found: {p}")
        pic_path = str(p)

    client = DdmClient.from_env()
    ensure_authenticated(client)

    resp = client.user.update_profile(
        args.username,
        public_key=args.public_key,
        profile_pic_path=pic_path,
    )
    out = resp.model_dump(mode="json", exclude_none=False)

    saved = store_user_result(client, args.username, name="profile/update", payload=out, no_store=args.no_store)
    append_user_log(
        client,
        args.username,
        action="update_profile",
        ok=True,
        details={"saved": saved, "public_key": bool(args.public_key), "profile_pic": bool(pic_path)},
    )

    print(json.dumps(out, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
