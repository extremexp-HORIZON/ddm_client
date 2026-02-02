from __future__ import annotations

from typing import Any, Dict

from helpers import safe_call, write_artifact, normalize_abi, safe_filename, OUT_DIR

def test_dump_all_contracts(client, network):
    out_dir = OUT_DIR / "contracts" / network
    out_dir.mkdir(parents=True, exist_ok=True)

    page = 1
    per_page = 50
    summaries: list[dict[str, Any]] = []

    while True:
        paged = safe_call(
            f"blockchain.list_contracts(page={page})",
            lambda: client.blockchain.list_contracts(
                network=[network],
                withEventsCount=1,
                includeAbi=0,
                sort="id,asc",
                page=page,
                perPage=per_page,
            ),
        )
        data = getattr(paged, "data", None) or []
        if not data:
            break

        for item in data:
            addr = getattr(item, "address", None)
            if not addr:
                continue

            c = safe_call(
                f"blockchain.get_contract({addr}, includeAbi=1)",
                lambda a=addr: client.blockchain.get_contract(a, includeAbi=1, withEventsCount=1),
            )
            abi = normalize_abi(getattr(c, "abi", None))

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
                "abi": abi,
            }

            # full contract + abi-only
            write_artifact(addr, payload, subdir=f"blockchain/contracts/{network}")
            write_artifact(addr, abi, subdir=f"contracts/{network}", suffix=".abi")

            summaries.append(
                {
                    "network": payload["network"],
                    "address": payload["address"],
                    "name": payload["name"],
                    "events_count": payload["events_count"],
                    "file": f"out/tests/blockchain/contracts/{network}/{safe_filename(addr)}.json",
                    "abi_file": f"out/tests/blockchain/contracts/{network}/{safe_filename(addr)}.abi.json",
                }
            )

        page += 1

    write_artifact("_index", {"network": network, "count": len(summaries), "contracts": summaries}, subdir=f"blockchain/contracts/{network}")

    assert len(summaries) > 0, "No contracts found to dump"
