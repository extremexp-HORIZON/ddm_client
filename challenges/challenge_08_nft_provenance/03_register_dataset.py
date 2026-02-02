# challenges/challenge_08_nft_provenance/03_register_dataset.py
from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from ddm_sdk.scripts.blockchain.prepare_dataset_report import main as prepare_dataset_report_main
from ddm_sdk.scripts.blockchain.register_dataset import main as register_dataset_main


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="challenge08-register-dataset",
        description="Prepare dataset report (IPFS) then register dataset on-chain (DatasetRegistry).",
    )
    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--catalog_id", required=True)

    ap.add_argument("--dataset-uri", required=True, help='e.g. "projects/.../result.csv" or ipfs://...')

    # report step
    ap.add_argument("--include-report", action="store_true", default=True)
    ap.add_argument("--no-include-report", dest="include_report", action="store_false")
    ap.set_defaults(include_report=True)

    # shared polling knobs
    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--prepare-interval", type=float, default=2.0)
    ap.add_argument("--register-interval", type=float, default=2.0)

    # passthrough options for register_dataset
    ap.add_argument("--registry-name", default="DatasetRegistry")
    ap.add_argument("--registry-address", default=None)
    ap.add_argument("--signature", default=None, help="Optional MetaMask signature 0x... (else sign with DDM_USER_PK)")
    ap.add_argument("--fileFormat", default=None, help="Optional override for file format used in signature/tx")
    ap.add_argument("--no-store", action="store_true")

    args = ap.parse_args(argv)

    network = args.network.strip()
    suite_id = args.suite_id.strip()
    catalog_id = args.catalog_id.strip()

    # ---- 1) PREPARE DATASET REPORT ----
    prep_args: List[str] = [
        "--network",
        network,
        "--suite_id",
        suite_id,
        "--catalog_id",
        catalog_id,
    ]
    if args.include_report:
        prep_args.append("--include-report")
    if args.poll:
        prep_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.prepare_interval)]
    if args.no_store:
        prep_args.append("--no-store")

    rc1 = prepare_dataset_report_main(prep_args)
    if rc1 != 0:
        return rc1

    # ---- 2) REGISTER DATASET ----
    reg_args: List[str] = [
        "--network",
        network,
        "--suite_id",
        suite_id,
        "--catalog_id",
        catalog_id,
        "--dataset-uri",
        args.dataset_uri,
        "--registry-name",
        args.registry_name,
    ]
    if args.registry_address:
        reg_args += ["--registry-address", args.registry_address]
    if args.signature:
        reg_args += ["--signature", args.signature]
    if args.fileFormat:
        reg_args += ["--fileFormat", args.fileFormat]
    if args.poll:
        reg_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.register_interval)]
    if args.no_store:
        reg_args.append("--no-store")

    return register_dataset_main(reg_args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
