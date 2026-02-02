from __future__ import annotations

import sys

from ddm_sdk.scripts.user.update_profile import main as update_profile_main


def main(argv: list[str] | None = None) -> int:
    return update_profile_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
