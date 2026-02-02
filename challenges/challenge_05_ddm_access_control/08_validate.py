from __future__ import annotations
import sys

from ddm_sdk.scripts.validations.validate_file_against_suites import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
