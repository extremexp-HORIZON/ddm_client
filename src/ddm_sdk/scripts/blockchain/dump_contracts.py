from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from ddm_sdk.client import DdmClient
from ddm_sdk.scripts.auth.utils import ensure_authenticated


def _normalize_abi(abi: Any) -> Any:
    if isinstance(abi, str):
        try:
            return json.loads(abi)
        except Exception:
            return abi
    return abi


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="ddm-dump-contracts", description="Dump all contracts (+ ABI) to storage")
    ap.add_argument("--network", required=True)
    ap.add_argument("--per-page", type=int, default=50)
    ap.add_argument("--include-abi", action="store_true")
    ap.add_argument("--no-store", action="store_true")
    args = ap.parse_args(argv)

    network = args.network.strip()

    client = DdmClient.from_env()
    ensure_authenticated(client)

    page = 1
    summaries: List[Dict[str, Any]] = []

    while True:
        paged = client.blockchain.list_contracts(
            network=[network],
            withEventsCount=1,
            includeAbi=0,
            sort="id,asc",
            page=page,
            perPage=args.per_page,
        )
        data = getattr(paged, "data", None) or []
        if not data:
            break

        for item in data:
            addr = getattr(item, "address", None)
            if not isinstance(addr, str) or not addr:
                continue

            c = client.blockchain.get_contract(
                addr,
                includeAbi=1 if args.include_abi else 0,
                withEventsCount=1,
            )
            abi = _normalize_abi(getattr(c, "abi", None))

            payload: Dict[str, Any] = {
                "address": getattr(c, "address", addr),
                "name": getattr(c, "name", None),
                "network": getattr(c, "network", network),
                "status": getattr(c, "status", None),
                "tx_hash": getattr(c, "tx_hash", None),
                "start_block": getattr(c, "start_block", None),
                "last_scanned_block": getattr(c, "last_scanned_block", None),
                "confirmations": getattr(c, "confirmations", None),
                "events_count": getattr(c, "events_count", None),
                "abi": abi if args.include_abi else None,
            }

            # storage: blockchain root (no project)
            if client.storage and not args.no_store:
                base = f"blockchain/contracts/{network}/{addr}"
                client.storage.write_json(base, payload)

                if args.include_abi:
                    client.storage.write_json(f"{base}.abi", abi)

            summaries.append(
                {
                    "network": network,
                    "address": addr,
                    "name": payload.get("name"),
                    "events_count": payload.get("events_count"),
                }
            )

        page += 1

    out = {
        "ok": True,
        "network": network,
        "count": len(summaries),
        "contracts": summaries,
    }

    # snapshot/index + log
    if client.storage and not args.no_store:
        client.storage.write_json(f"blockchain/contracts/{network}/_index", out)

        existing = client.storage.read_json("blockchain/logs")
        logs = existing if isinstance(existing, list) else []
        logs.append(
            {
                "action": "dump_contracts",
                "ok": True,
                "details": {"network": network, "count": len(summaries)},
            }
        )
        client.storage.write_json("blockchain/logs", logs)

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
