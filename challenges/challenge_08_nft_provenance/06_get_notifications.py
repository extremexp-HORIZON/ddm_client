from __future__ import annotations

import sys

from ddm_sdk.scripts.user.list_notifications import main as list_notifications_main


def main(argv: list[str] | None = None) -> int:

    return list_notifications_main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
