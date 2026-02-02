from __future__ import annotations

from helpers import safe_call


def test_contracts_smoke(client, network):
    paged = safe_call(
        "blockchain.list_contracts(page=1)",
        lambda: client.blockchain.list_contracts(network=[network], includeAbi=0, withEventsCount=1, page=1, perPage=5),
    )
    assert paged is not None
    assert getattr(paged, "data", None) is not None

    if not paged.data:
        # Not failing: environment might have no contracts yet.
        print("⚠️ No contracts found; skipping deeper contract tests.")
        return

    contract_address = paged.data[0].address
    assert contract_address

    safe_call(
        "blockchain.contract_events(page=1)",
        lambda: client.blockchain.contract_events(contract_address, network=[network], page=1, perPage=5),
    )
    safe_call(
        "blockchain.contract_txs(page=1)",
        lambda: client.blockchain.contract_txs(contract_address, network=[network], page=1, perPage=5),
    )
