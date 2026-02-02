from __future__ import annotations
import sys

from ddm_sdk.scripts.files.upload_files import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
