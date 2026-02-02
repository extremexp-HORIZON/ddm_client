# challenges/challenge_08_nft_provenance/04_claim_reward_and_mint.py
from __future__ import annotations

import argparse
import sys
from typing import Any, Dict, List, Optional

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated
from ddm_sdk.scripts.blockchain.utils import _derive_request_id_from_register_suite_storage

from ddm_sdk.scripts.blockchain.prepare_reward import main as prepare_reward_main
from ddm_sdk.scripts.blockchain.claim_reward import main as claim_reward_and_mint_main




# -----------------------
# main
# -----------------------

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="challenge08-claim-reward-and-mint",
        description="Prepare reward artifacts then claim reward + mint NFT on-chain.",
    )

    ap.add_argument("--project_id", required=True)

    ap.add_argument("--network", default="sepolia")
    ap.add_argument("--suite_id", required=True)
    ap.add_argument("--catalog_id", required=True)

    # ✅ make optional now
    ap.add_argument("--request-id", default=None, type=int, help="DatasetRequest id (uint256). If omitted, auto-derive.")

    # prepare_reward knobs (no-json mode)
    ap.add_argument("--category", default="dataset")
    ap.add_argument("--expires-in-sec", type=int, default=900)

    # also support old prepare_reward interface
    ap.add_argument("--json", default=None)
    ap.add_argument("--json-file", default=None)

    # request contract info for claim tx
    ap.add_argument("--request-contract-name", default="DatasetRequestRegistry")
    ap.add_argument("--request-contract-address", default=None)

    # shared polling knobs
    ap.add_argument("--poll", action="store_true")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--prepare-interval", type=float, default=1.0)
    ap.add_argument("--claim-interval", type=float, default=2.0)

    ap.add_argument("--no-store", action="store_true")

    args = ap.parse_args(argv)

    # -----------------------
    # 0) AUTO-DERIVE request-id if missing
    # -----------------------
    request_id = args.request_id
    if request_id is None:
        client = DdmClient.from_env()
        ensure_authenticated(client)
        if not client.storage:
            raise SystemExit("Storage not configured (DDM_STORAGE_DIR). Cannot auto-derive request-id.")
        request_id = _derive_request_id_from_register_suite_storage(client, suite_id=args.suite_id)

    # -----------------------
    # 1) PREPARE REWARD
    # -----------------------
    prep_args: List[str] = ["--project_id", args.project_id]

    if args.json and args.json_file:
        raise SystemExit("Use only one of --json or --json-file")

    if args.json_file:
        prep_args += ["--json-file", args.json_file]
    elif args.json:
        prep_args += ["--json", args.json]
    else:
        # no-json mode
        prep_args += [
            "--network", args.network,
            "--suite_id", args.suite_id,
            "--catalog_id", args.catalog_id,
            "--category", args.category,
            "--expires-in-sec", str(args.expires_in_sec),
        ]

    if args.poll:
        prep_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.prepare_interval)]
    if args.no_store:
        prep_args.append("--no-store")

    rc1 = prepare_reward_main(prep_args)
    if rc1 != 0:
        return rc1

    # -----------------------
    # 2) CLAIM REWARD + MINT
    # -----------------------
    claim_args: List[str] = [
        "--network", args.network,
        "--suite_id", args.suite_id,
        "--catalog_id", args.catalog_id,
        "--request-id", str(request_id),  # ✅ derived here
        "--request-contract-name", args.request_contract_name,
    ]
    if args.request_contract_address:
        claim_args += ["--request-contract-address", args.request_contract_address]

    if args.poll:
        claim_args += ["--poll", "--timeout", str(args.timeout), "--interval", str(args.claim_interval)]
    if args.no_store:
        claim_args.append("--no-store")

    return claim_reward_and_mint_main(claim_args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
