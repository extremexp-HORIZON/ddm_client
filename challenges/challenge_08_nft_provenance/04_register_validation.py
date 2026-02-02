# challenges/challenge_08_nft_provenance/04_register_validation.py
from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from ddm_sdk.scripts.blockchain.prepare_validation import main as prepare_validation_main
from ddm_sdk.scripts.blockchain.register_validation import main as register_validation_main


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="challenge08-register-validation",
        description="Prepare validation artifacts then register validation on-chain (ValidationRegistry).",
    )

    # shared / identity
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--dataset-fingerprint", required=True, help="0x... fingerprint")

    # prepare_validation input payload (either --json or --json-file)
    ap.add_argument("--json", default=None, help="Prepare-validation payload JSON string")
    ap.add_argument("--json-file", default=None, help="Prepare-validation payload JSON file")

    # prepare-specific
    ap.add_argument(
        "--successful",
        choices=["true", "false"],
        default="true",
        help="(prepare step) Attach success flag into saved artifacts (default: true)",
    )

    # register_validation passthrough
    ap.add_argument("--registry-name", default="ValidationRegistry")
    ap.add_argument("--registry-address", default=None)

    # shared polling knobs (applies to BOTH steps)
    ap.add_argument("--poll", action="store_true", help="Poll tasks (prepare + ingest)")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--prepare-interval", type=float, default=1.0)
    ap.add_argument("--register-interval", type=float, default=2.0)

    # fee knobs (register step)
    ap.add_argument("--max-fee-gwei", type=float, default=None)
    ap.add_argument("--max-priority-fee-gwei", type=float, default=None)

    ap.add_argument("--no-store", action="store_true")

    args = ap.parse_args(argv)

    network = args.network.strip()
    fp = args.dataset_fingerprint.strip()

    # -----------------------
    # 1) PREPARE VALIDATION
    # -----------------------
    prep_args: List[str] = []
    if args.json and args.json_file:
        raise SystemExit("Use only one of --json or --json-file")

    if args.json_file:
        prep_args += ["--json-file", args.json_file]
    elif args.json:
        prep_args += ["--json", args.json]
    else:
        raise SystemExit("Missing payload for prepare step: provide --json or --json-file")

    prep_args += ["--successful", args.successful]

    if args.poll:
        prep_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.prepare_interval)]
    if args.no_store:
        prep_args.append("--no-store")

    rc1 = prepare_validation_main(prep_args)
    if rc1 != 0:
        return rc1

    # -----------------------
    # 2) REGISTER VALIDATION
    # -----------------------
    reg_args: List[str] = [
        "--network",
        network,
        "--dataset-fingerprint",
        fp,
        "--registry-name",
        args.registry_name,
    ]
    if args.registry_address:
        reg_args += ["--registry-address", args.registry_address]

    if args.max_fee_gwei is not None:
        reg_args += ["--max-fee-gwei", str(args.max_fee_gwei)]
    if args.max_priority_fee_gwei is not None:
        reg_args += ["--max-priority-fee-gwei", str(args.max_priority_fee_gwei)]

    if args.poll:
        reg_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.register_interval)]
    if args.no_store:
        reg_args.append("--no-store")

    return register_validation_main(reg_args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
